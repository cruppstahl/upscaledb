/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#include <math.h>
#include <float.h>

#include "blob.h"
#include "btree.h"
#include "cache.h"
#include "cursor.h"
#include "device.h"
#include "btree_cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "freelist.h"
#include "log.h"
#include "journal.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "txn_cursor.h"
#include "version.h"

namespace ham {

#define PURGE_THRESHOLD       (500 * 1024 * 1024) /* 500 mb */
#define DUMMY_LSN                               1

typedef struct
{
    Database *db;               /* [in] */
    ham_u32_t flags;            /* [in] */
    ham_offset_t total_count;   /* [out] */
    bool is_leaf;         /* [scratch] */
}  calckeys_context_t;

/*
 * callback function for estimating / counting the number of keys stored
 * in the database
 */
static ham_status_t
__calc_keys_cb(int event, void *param1, void *param2, void *context)
{
    BtreeKey *key;
    calckeys_context_t *c;
    ham_size_t count;

    c=(calckeys_context_t *)context;

    switch (event) {
    case HAM_ENUM_EVENT_DESCEND:
        break;

    case HAM_ENUM_EVENT_PAGE_START:
        c->is_leaf=*(bool *)param2;
        break;

    case HAM_ENUM_EVENT_PAGE_STOP:
        break;

    case HAM_ENUM_EVENT_ITEM:
        key=(BtreeKey *)param1;
        count=*(ham_size_t *)param2;

        if (c->is_leaf) {
            ham_size_t dupcount=1;

            if (!(c->flags&HAM_SKIP_DUPLICATES)
                    && (key->get_flags()&BtreeKey::KEY_HAS_DUPLICATES)) {
                ham_status_t st=c->db->get_env()->get_duplicate_manager()->get_count(
                            key->get_ptr(), &dupcount, 0);
                if (st)
                    return (st);
                c->total_count+=dupcount;
            }
            else {
                c->total_count++;
            }

            if (c->flags&HAM_FAST_ESTIMATE) {
                /*
                 * fast mode: just grab the keys-per-page value and
                 * call it a day for this page.
                 *
                 * Assume all keys in this page have the same number
                 * of dupes (=1 if no dupes)
                 */
                c->total_count+=(count-1)*dupcount;
                return (HAM_ENUM_DO_NOT_DESCEND);
            }
        }
        break;

    default:
        ham_assert(!"unknown callback event");
        break;
    }

    return (HAM_ENUM_CONTINUE);
}


typedef struct free_cb_context_t
{
    Database *db;
    bool is_leaf;

} free_cb_context_t;

/*
 * callback function for freeing blobs of an in-memory-database
 */
ham_status_t
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context)
{
    ham_status_t st;
    BtreeKey *key;
    free_cb_context_t *c;

    c=(free_cb_context_t *)context;

    switch (event) {
    case HAM_ENUM_EVENT_DESCEND:
        break;

    case HAM_ENUM_EVENT_PAGE_START:
        c->is_leaf=*(bool *)param2;
        break;

    case HAM_ENUM_EVENT_PAGE_STOP:
        /* nop */
        break;

    case HAM_ENUM_EVENT_ITEM:
        key=(BtreeKey *)param1;

        if (key->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
            ham_offset_t blobid=key->get_extended_rid(c->db);
            /* delete the extended key */
            st=c->db->remove_extkey(blobid);
            if (st)
                return (st);
        }

        if (key->get_flags()&(BtreeKey::KEY_BLOB_SIZE_TINY
                            |BtreeKey::KEY_BLOB_SIZE_SMALL
                            |BtreeKey::KEY_BLOB_SIZE_EMPTY))
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf) {
            st=key->erase_record(c->db, 0, 0, true);
            if (st)
                return (st);
        }
        break;

    default:
        ham_assert(!"unknown callback event");
        return (HAM_ENUM_STOP);
    }

    return (HAM_ENUM_CONTINUE);
}

inline ham_bool_t
__cache_needs_purge(Environment *env)
{
    Cache *cache=env->get_cache();
    if (!cache)
        return (HAM_FALSE);

    /* purge the cache, if necessary. if cache is unlimited, then we purge very
     * very rarely (but we nevertheless purge to avoid OUT OF MEMORY conditions
     * which can happen on 32bit Windows) */
    if (cache && !(env->get_flags()&HAM_IN_MEMORY)) {
        ham_bool_t purge=cache->is_too_big();
#if defined(WIN32) && defined(HAM_32BIT)
        if (env->get_flags()&HAM_CACHE_UNLIMITED) {
            if (cache->get_cur_elements()*env->get_pagesize()
                    > PURGE_THRESHOLD)
                return (HAM_FALSE);
        }
#endif
        return (purge);
    }
    return (HAM_FALSE);
}

static ham_status_t
__record_filters_before_write(Database *db, ham_record_t *record)
{
    ham_status_t st=0;
    ham_record_filter_t *record_head;

    record_head=db->get_record_filter();
    while (record_head) {
        if (record_head->before_write_cb) {
            st=record_head->before_write_cb((ham_db_t *)db,
                    record_head, record);
            if (st)
                break;
        }
        record_head=record_head->_next;
    }

    return (st);
}

/*
 * WATCH IT!
 *
 * as with the page filters, there was a bug in PRE-1.1.0 which would execute
 * a record filter chain in the same order for both write (insert) and read
 * (find), which means chained record filters would process invalid data in
 * one of these, as a correct filter chain must traverse the transformation
 * process IN REVERSE for one of these actions.
 *
 * As with the page filters, we've chosen the WRITE direction to be the
 * FORWARD direction, i.e. added filters end up processing data WRITTEN by
 * the previous filter.
 *
 * This also means the READ==FIND action must walk this chain in reverse.
 *
 * See the documentation about the cyclic prev chain: the point is
 * that FIND must traverse the record filter chain in REVERSE order so we
 * should start with the LAST filter registered and stop once we've DONE
 * calling the FIRST.
 */
static ham_status_t
__record_filters_after_find(Database *db, ham_record_t *record)
{
    ham_status_t st = 0;
    ham_record_filter_t *record_head;

    record_head=db->get_record_filter();
    if (record_head) {
        record_head = record_head->_prev;
        do {
            if (record_head->after_read_cb) {
                st=record_head->after_read_cb((ham_db_t *)db,
                        record_head, record);
                if (st)
                      break;
            }
            record_head = record_head->_prev;
        } while (record_head->_prev->_next);
    }
    return (st);
}

Database::Database()
  : m_error(0), m_context(0), m_backend(0), m_cursors(0),
    m_prefix_func(0), m_cmp_func(0), m_duperec_func(0),
    m_rt_flags(0), m_env(0), m_next(0), m_extkey_cache(0),
    m_indexdata_offset(0), m_record_filters(0), m_data_access_mode(0),
    m_is_active(0), m_impl(0)
{
#if HAM_ENABLE_REMOTE
    m_remote_handle=0;
#endif
    txn_tree_init(this, &m_optree);
}

ham_u16_t
Database::get_name(void)
{
    db_indexdata_t *idx=get_env()->get_indexdata_ptr(get_indexdata_offset());
    return (index_get_dbname(idx));
}

ham_status_t
Database::remove_extkey(ham_offset_t blobid)
{
  if (get_extkey_cache())
    get_extkey_cache()->remove(blobid);
  return (get_env()->get_blob_manager()->free(this, blobid, 0));
}

Database::~Database()
{
    delete m_impl;
}

int HAM_CALLCONV
db_default_prefix_compare(ham_db_t *db,
                   const ham_u8_t *lhs, ham_size_t lhs_length,
                   ham_size_t lhs_real_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length,
                   ham_size_t rhs_real_length)
{
    int m;

    (void)db;

    /*
     * the default compare uses memcmp
     *
     * treat shorter strings as "higher"

    when one of the keys is NOT extended we don't need to request the other (extended) key:
    shorter is "higher" anyhow, and one of 'em is short enough to know already. Two scenarios
    here: the simple one and the little less simple one:

    1) when the key lengths differ, one of them is NOT extended for sure.

    2) when one of the key lengths equals their real_length, that one is NOT extended.

    Saves fetching extended keys whenever possible.

    The few extra comparisons in here outweigh the overhead of fetching one extended key by far.


    Note:

    there's a 'tiny' caveat to it all: often these comparisons are between a database key
    (BtreeKey based) and a user-specified key (ham_key_t based), where the latter will always
    appear in the LHS and the important part here is: ham_key_t-based comparisons will have
    their key lengths possibly LARGER than the usual 'short' BtreeKey key length as the
    ham_key_t data doesn't need extending - the result is that simply looking at the lhs_length
    is not good enough here to ensure the key is actually shorter than the other.
     */
    if (lhs_length < rhs_length)
    {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        //ham_assert(lhs_real_length < rhs_real_length);

        /* scenario (2) check: */
        if (lhs_length == lhs_real_length) {
            ham_assert(lhs_real_length < rhs_real_length);
            return (-1);
        }
    }
    else if (rhs_length < lhs_length)
    {
        m=memcmp(lhs, rhs, rhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        //ham_assert(lhs_real_length > rhs_real_length);

        /* scenario (2) check: */
        if (rhs_length == rhs_real_length) {
            ham_assert(lhs_real_length > rhs_real_length);
            return (+1);
        }
    }
    else
    {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);

        /* scenario (2) check: */
        if (lhs_length == lhs_real_length) {
            if (lhs_real_length < rhs_real_length)
                return (-1);
        }
        else if (rhs_length == rhs_real_length) {
            if (lhs_real_length > rhs_real_length)
                return (+1);
        }
    }

    return (HAM_PREFIX_REQUEST_FULLKEY);
}

int HAM_CALLCONV
db_default_compare(ham_db_t *db,
                   const ham_u8_t *lhs, ham_size_t lhs_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int m;

    (void)db;

    /*
     * the default compare uses memcmp
     *
     * treat shorter strings as "higher"
     */
    if (lhs_length<rhs_length) {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        return (-1);
    }

    else if (rhs_length<lhs_length) {
        m=memcmp(lhs, rhs, rhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        return (+1);
    }

    m=memcmp(lhs, rhs, lhs_length);
    if (m<0)
        return (-1);
    if (m>0)
        return (+1);
    return (0);
}

int HAM_CALLCONV
db_default_recno_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    ham_u64_t ulhs, urhs;

    (void)db;

    memcpy(&ulhs, lhs, 8);
    memcpy(&urhs, rhs, 8);

    ulhs=ham_db2h64(ulhs);
    urhs=ham_db2h64(urhs);

    if (ulhs<urhs)
        return -1;
    if (ulhs==urhs)
        return 0;
    return 1;
}

ham_status_t
Database::get_extended_key(ham_u8_t *key_data,
        ham_size_t key_length, ham_u32_t key_flags, ham_key_t *ext_key)
{
    ham_offset_t blobid;
    ham_status_t st;
    ham_size_t temp;
    ham_record_t record;
    ham_u8_t *ptr;
    Allocator *alloc=get_env()->get_allocator();

    ham_assert(key_flags&BtreeKey::KEY_IS_EXTENDED);

    /*
     * make sure that we have an extended key-cache
     *
     * in in-memory-db, the extkey-cache doesn't lead to performance
     * advantages; it only duplicates the data and wastes memory.
     * therefore we don't use it.
     */
    if (!(get_env()->get_flags()&HAM_IN_MEMORY)) {
        if (!get_extkey_cache())
            set_extkey_cache(new ExtKeyCache(this));
    }

    /* almost the same as: blobid = key->get_extended_rid(db); */
    memcpy(&blobid, key_data+(db_get_keysize(this)-sizeof(ham_offset_t)),
            sizeof(blobid));
    blobid=ham_db2h_offset(blobid);

    /* fetch from the cache */
    if (!(get_env()->get_flags()&HAM_IN_MEMORY)) {
        st=get_extkey_cache()->fetch(blobid, &temp, &ptr);
        if (!st) {
            ham_assert(temp==key_length);

            if (!(ext_key->flags&HAM_KEY_USER_ALLOC)) {
                ext_key->data=(ham_u8_t *)alloc->alloc(key_length);
                if (!ext_key->data)
                    return (HAM_OUT_OF_MEMORY);
            }
            memcpy(ext_key->data, ptr, key_length);
            ext_key->size=(ham_u16_t)key_length;
            return (0);
        }
        else if (st!=HAM_KEY_NOT_FOUND) {
            return (st);
        }
    }

    /*
     * not cached - fetch from disk;
     * we allocate the memory here to avoid that the global record
     * pointer is overwritten
     *
     * Note that the key is fetched in two parts: we already have the front
     * part of the key in key_data and now we only need to fetch the blob
     * remainder, which size is:
     *    key_length - (db_get_keysize(db)-sizeof(ham_offset_t))
     *
     * To prevent another round of memcpy and heap allocation here, we
     * simply allocate sufficient space for the entire key as it should be
     * passed back through (*ext_key) and adjust the pointer into that
     * memory space for the faked record-based blob_read() below.
     */
    if (!(ext_key->flags & HAM_KEY_USER_ALLOC)) {
        ext_key->data=(ham_u8_t *)alloc->alloc(key_length);
        if (!ext_key->data)
            return (HAM_OUT_OF_MEMORY);
    }

    memmove(ext_key->data, key_data, db_get_keysize(this)-sizeof(ham_offset_t));

    /*
     * now read the remainder of the key
     */
    memset(&record, 0, sizeof(record));
    record.data=(((ham_u8_t *)ext_key->data) +
                    db_get_keysize(this)-sizeof(ham_offset_t));
    record.size=key_length-(db_get_keysize(this)-sizeof(ham_offset_t));
    record.flags=HAM_RECORD_USER_ALLOC;

    st=get_env()->get_blob_manager()->read(this, 0, blobid, &record, 0);
    if (st)
        return (st);

    /* insert the FULL key in the extkey-cache */
    if (get_extkey_cache()) {
        ExtKeyCache *cache=get_extkey_cache();
        cache->insert(blobid, key_length, (ham_u8_t *)ext_key->data);
    }

    ext_key->size=(ham_u16_t)key_length;
    return (0);
}

ham_status_t
db_alloc_page_impl(Page **page_ref, Environment *env, Database *db,
                ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos=0;
    Page *page=NULL;
    ham_bool_t allocated_by_me=HAM_FALSE;

    *page_ref = 0;
    ham_assert(0==(flags&~(PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO)));

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST) && env->get_freelist()) {
        st=env->get_freelist()->alloc_page(&tellpos, db);
        if (st)
            return (st);
        if (tellpos) {
            ham_assert(tellpos%env->get_pagesize()==0);
            /* try to fetch the page from the cache */
            page=env->get_cache()->get_page(tellpos, 0);
            if (page)
                goto done;
            /* allocate a new page structure and read the page from disk */
            page=new Page(env, db);
            st=page->fetch(tellpos);
            if (st) {
                delete page;
                return (st);
            }
            goto done;
        }
    }

    if (!page) {
        page=new Page(env, db);
        allocated_by_me=HAM_TRUE;
    }

    /* can we allocate a new page for the cache? */
    if (env->get_cache()->is_too_big()) {
        if (env->get_flags()&HAM_CACHE_STRICT) {
            if (allocated_by_me)
                delete page;
            return (HAM_CACHE_FULL);
        }
    }

    ham_assert(tellpos==0);
    st=page->allocate();
    if (st)
        return (st);

done:
    /* initialize the page; also set the 'dirty' flag to force logging */
    page->set_type(type);
    page->set_dirty(true);
    page->set_db(db);

    /* clear the page with zeroes?  */
    if (flags&PAGE_CLEAR_WITH_ZERO)
        memset(page->get_pers(), 0, env->get_pagesize());

    /* an allocated page is always flushed if recovery is enabled */
    if (env->get_flags()&HAM_ENABLE_RECOVERY)
        env->get_changeset().add_page(page);

    /* store the page in the cache */
    env->get_cache()->put_page(page);

    *page_ref = page;
    return (HAM_SUCCESS);
}

ham_status_t
db_alloc_page(Page **page_ref, Database *db,
                ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    st=db_alloc_page_impl(page_ref, db->get_env(), db, type, flags);
    if (st)
        return (st);

    /* hack: prior to 2.0, the type of btree root pages was not set
     * correctly */
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    if ((*page_ref)->get_self()==be->get_rootpage()
            && !(db->get_rt_flags()&HAM_READ_ONLY))
        (*page_ref)->set_type(Page::TYPE_B_ROOT);
    return (0);
}

ham_status_t
db_fetch_page_impl(Page **page_ref, Environment *env, Database *db,
                ham_offset_t address, ham_u32_t flags)
{
    Page *page=0;
    ham_status_t st;

    ham_assert(0 == (flags & ~(HAM_HINTS_MASK|DB_ONLY_FROM_CACHE)));

    *page_ref = 0;

    /* fetch the page from the cache */
    page=env->get_cache()->get_page(address, Cache::NOREMOVE);
    if (page) {
        *page_ref = page;
        ham_assert(page->get_pers());
        /* store the page in the changeset if recovery is enabled */
        if (env->get_flags()&HAM_ENABLE_RECOVERY)
            env->get_changeset().add_page(page);
        return (HAM_SUCCESS);
    }

    if (flags&DB_ONLY_FROM_CACHE)
        return (HAM_SUCCESS);

#if HAM_DEBUG
    ham_assert(env->get_cache()->get_page(address)==0);
#endif

    /* can we allocate a new page for the cache? */
    if (env->get_cache()->is_too_big()) {
        if (env->get_flags()&HAM_CACHE_STRICT)
            return (HAM_CACHE_FULL);
    }

    page=new Page(env, db);
    st=page->fetch(address);
    if (st) {
        delete page;
        return (st);
    }

    ham_assert(page->get_pers());

    /* store the page in the cache */
    env->get_cache()->put_page(page);

    /* store the page in the changeset */
    if (env->get_flags()&HAM_ENABLE_RECOVERY)
        env->get_changeset().add_page(page);

    *page_ref = page;
    return (HAM_SUCCESS);
}

ham_status_t
db_fetch_page(Page **page_ref, Database *db,
                ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, db->get_env(), db, address, flags));
}

static bool
db_flush_callback(Page *page, Database *db, ham_u32_t flags)
{
    (void)db;

    (void)page->flush();

    /*
     * if the page is deleted, uncouple all cursors, then
     * free the memory of the page, then remove from the cache
     */
    if (!(flags&DB_FLUSH_NODELETE)) {
        (void)page->uncouple_all_cursors();
        (void)page->free();
        return (true);
    }

    return (false);
}

ham_status_t
db_flush_all(Cache *cache, ham_u32_t flags)
{
    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE));

    if (!cache)
        return (0);

    return (cache->visit(db_flush_callback, 0, flags));
}

void
Database::clone_cursor(Cursor *src, Cursor **dest)
{
    *dest=m_impl->cursor_clone(src);

    /* fix the linked list of cursors */
    (*dest)->set_previous(0);
    (*dest)->set_next(get_cursors());
    ham_assert(get_cursors()!=0);
    get_cursors()->set_previous(*dest);
    set_cursors(*dest);

    /* initialize the remaining fields */
    (*dest)->set_txn(src->get_txn());

    if (src->get_txn())
        src->get_txn()->set_cursor_refcount(src->get_txn()->get_cursor_refcount()+1);
}

void
Database::close_cursor(Cursor *cursor)
{
    Cursor *p, *n;

    /* decrease the transaction refcount; the refcount specifies how many
     * cursors are attached to the transaction */
    if (cursor->get_txn()) {
        ham_assert(cursor->get_txn()->get_cursor_refcount()>0);
        cursor->get_txn()->set_cursor_refcount(cursor->get_txn()->get_cursor_refcount()-1);
    }

    /* now finally close the cursor */
    m_impl->cursor_close(cursor);

    /* fix the linked list of cursors */
    p=cursor->get_previous();
    n=cursor->get_next();

    if (p)
        p->set_next(n);
    else
        set_cursors(n);

    if (n)
        n->set_previous(p);

    cursor->set_next(0);
    cursor->set_previous(0);

    delete cursor;
}

struct keycount_t
{
    ham_u64_t c;
    ham_u32_t flags;
    Transaction *txn;
    Database *db;
};

static void
__get_key_count_txn(txn_opnode_t *node, void *data)
{
    struct keycount_t *kc=(struct keycount_t *)data;
    Backend *be=kc->db->get_backend();
    txn_op_t *op;

    /*
     * look at each tree_node and walk through each operation
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then include it
     * - is this op part of an txn which is still active? then include it
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns of the same key
     *
     * !!
     * if keys are overwritten or a duplicate key is inserted, then
     * we have to consolidate the btree keys with the txn-tree keys.
     */
    op=txn_opnode_get_newest_op(node);
    while (op) {
        Transaction *optxn=txn_op_get_txn(op);
        if (optxn->get_flags()&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((optxn->get_flags()&TXN_STATE_COMMITTED)
                    || (kc->txn==optxn)) {
            if (txn_op_get_flags(op)&TXN_OP_FLUSHED)
                ; /* nop */
            /* if key was erased then it doesn't exist */
            else if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return;
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            else if (txn_op_get_flags(op)&TXN_OP_INSERT) {
                kc->c++;
                return;
            }
            /* key exists - include it */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)) {
                /* check if the key already exists in the btree - if yes,
                 * we do not count it (it will be counted later) */
                if (kc->flags&HAM_FAST_ESTIMATE)
                    kc->c++;
                else if (HAM_KEY_NOT_FOUND==be->find(0,
                                    txn_opnode_get_key(node), 0, 0))
                    kc->c++;
                return;
            }
            else if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP) {
                /* check if the key already exists in the btree - if yes,
                 * we do not count it (it will be counted later) */
                if (kc->flags&HAM_FAST_ESTIMATE)
                    kc->c++;
                else {
                    /* check if btree has other duplicates */
                    if (0==be->find(0, txn_opnode_get_key(node), 0, 0)) {
                        /* yes, there's another one */
                        if (kc->flags&HAM_SKIP_DUPLICATES)
                            return;
                        else
                            kc->c++;
                    }
                    else {
                        /* check if other key is in this node */
                        kc->c++;
                        if (kc->flags&HAM_SKIP_DUPLICATES)
                            return;
                    }
                }
            }
            else {
                ham_assert(!"shouldn't be here");
                return;
            }
        }
        else { /* txn is still active */
            kc->c++;
        }

        op=txn_op_get_previous_in_node(op);
    }
}

static ham_status_t
db_check_insert_conflicts(Database *db, Transaction *txn,
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    txn_op_t *op=0;
    Backend *be=db->get_backend();

    /*
     * pick the tree_node of this key, and walk through each operation
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then look at the
     *      operation in detail
     * - is this op part of an txn which is still active? return an error
     *      because we've found a conflict
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns
     */
    op=txn_opnode_get_newest_op(node);
    while (op) {
        Transaction *optxn=txn_op_get_txn(op);
        if (optxn->get_flags()&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((optxn->get_flags()&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            /* if key was erased then it doesn't exist and can be
             * inserted without problems */
            if (txn_op_get_flags(op)&TXN_OP_FLUSHED)
                ; /* nop */
            else if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (0);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key already exists then we can only continue if
             * we're allowed to overwrite it or to insert a duplicate */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE))
                    return (0);
                else
                    return (HAM_DUPLICATE_KEY);
            }
            else {
                ham_assert(!"shouldn't be here");
                return (HAM_DUPLICATE_KEY);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_previous_in_node(op);
    }

    /*
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts. Now check all transactions which are already
     * flushed - basically that's identical to a btree lookup.
     *
     * however we can skip this check if we do not care about duplicates.
     */
    if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE))
        return (0);
    st=be->find(0, key, 0, flags);
    if (st==HAM_KEY_NOT_FOUND)
        return (0);
    if (st==HAM_SUCCESS)
        return (HAM_DUPLICATE_KEY);
    return (st);
}

static ham_status_t
db_check_erase_conflicts(Database *db, Transaction *txn,
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    txn_op_t *op=0;
    Backend *be=db->get_backend();

    /*
     * pick the tree_node of this key, and walk through each operation
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then look at the
     *      operation in detail
     * - is this op part of an txn which is still active? return an error
     *      because we've found a conflict
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns
     */
    op=txn_opnode_get_newest_op(node);
    while (op) {
        Transaction *optxn=txn_op_get_txn(op);
        if (optxn->get_flags()&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((optxn->get_flags()&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            if (txn_op_get_flags(op)&TXN_OP_FLUSHED)
                ; /* nop */
            /* if key was erased then it doesn't exist and we fail with
             * an error */
            else if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (HAM_KEY_NOT_FOUND);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key exists then we're successful */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                return (0);
            }
            else {
                ham_assert(!"shouldn't be here");
                return (HAM_KEY_NOT_FOUND);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_previous_in_node(op);
    }

    /*
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts. Now check all transactions which are already
     * flushed - basically that's identical to a btree lookup.
     */
    return (be->find(0, key, 0, flags));
}

static void
__increment_dupe_index(Database *db, txn_opnode_t *node,
                Cursor *skip, ham_u32_t start)
{
    Cursor *c=db->get_cursors();

    while (c) {
        ham_bool_t hit=HAM_FALSE;

        if (c==skip || c->is_nil(0))
            goto next;

        /* if cursor is coupled to an op in the same node: increment
         * duplicate index (if required) */
        if (c->is_coupled_to_txnop()) {
            txn_cursor_t *txnc=c->get_txn_cursor();
            txn_opnode_t *n=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
            if (n==node)
                hit=HAM_TRUE;
        }
        /* if cursor is coupled to the same key in the btree: increment
         * duplicate index (if required) */
        else if (c->get_btree_cursor()->points_to(txn_opnode_get_key(node))) {
            hit=HAM_TRUE;
        }

        if (hit) {
            if (c->get_dupecache_index()>start)
                c->set_dupecache_index(c->get_dupecache_index()+1);
        }

next:
        c=c->get_next();
    }
}

ham_status_t
db_insert_txn(Database *db, Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags,
                struct txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;
    Environment *env=db->get_env();

    /* get (or create) the node for this key */
    node=txn_opnode_get(db, key, 0);
    if (!node) {
        node=txn_opnode_create(db, key);
        if (!node)
            return (HAM_OUT_OF_MEMORY);
        node_created=HAM_TRUE;
    }

    /* check for conflicts of this key
     *
     * !!
     * afterwards, clear the changeset; db_check_insert_conflicts() sometimes
     * checks if a key already exists, and this fills the changeset
     */
    st=db_check_insert_conflicts(db, txn, node, key, flags);
    env->get_changeset().clear();
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
        return (st);
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(env, &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
        return (st);
    }

    /* append a new operation to this node */
    op=txn_opnode_append(txn, node, flags,
                    (flags&HAM_PARTIAL) |
                    ((flags&HAM_DUPLICATE)
                        ? TXN_OP_INSERT_DUP
                        : (flags&HAM_OVERWRITE)
                            ? TXN_OP_INSERT_OW
                            : TXN_OP_INSERT),
                    lsn, record);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    /* if there's a cursor then couple it to the op; also store the
     * dupecache-index in the op (it's needed for
     * DUPLICATE_INSERT_BEFORE/NEXT) */
    if (cursor) {
        Cursor *c=txn_cursor_get_parent(cursor);
        if (c->get_dupecache_index())
            txn_op_set_referenced_dupe(op, c->get_dupecache_index());

        c->set_to_nil(Cursor::CURSOR_TXN);
        txn_cursor_couple(cursor, op);

        /* all other cursors need to increment their dupe index, if their
         * index is > this cursor's index */
        __increment_dupe_index(db, node, c, c->get_dupecache_index());
    }

    /* append journal entry */
    if (env->get_flags()&HAM_ENABLE_RECOVERY
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        Journal *j=env->get_journal();
        st=j->append_insert(db, txn, key, record,
                            flags&HAM_DUPLICATE ? flags : flags|HAM_OVERWRITE,
                            txn_op_get_lsn(op));
    }

    return (st);
}

static void
__nil_all_cursors_in_node(Transaction *txn, Cursor *current,
                txn_opnode_t *node)
{
    txn_op_t *op=txn_opnode_get_newest_op(node);
    while (op) {
        txn_cursor_t *cursor=txn_op_get_cursors(op);
        while (cursor) {
            Cursor *pc=txn_cursor_get_parent(cursor);
            /* is the current cursor to a duplicate? then adjust the
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (current->get_dupecache_index()) {
                    if (current->get_dupecache_index()
                            <pc->get_dupecache_index()) {
                        pc->set_dupecache_index(pc->get_dupecache_index()-1);
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    else if (current->get_dupecache_index()
                            >pc->get_dupecache_index()) {
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    /* else fall through */
                }
            }
            pc->couple_to_btree();
            pc->set_to_nil(Cursor::CURSOR_TXN);
            cursor=txn_op_get_cursors(op);
            /* set a flag that the cursor just completed an Insert-or-find
             * operation; this information is needed in ham_cursor_move
             * (in this aspect, an erase is the same as insert/find) */
            pc->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);
        }

        op=txn_op_get_previous_in_node(op);
    }
}

static void
__nil_all_cursors_in_btree(Database *db, Cursor *current, ham_key_t *key)
{
    Cursor *c=db->get_cursors();

    /* foreach cursor in this database:
     *  if it's nil or coupled to the txn: skip it
     *  if it's coupled to btree AND uncoupled: compare keys; set to nil
     *      if keys are identical
     *  if it's uncoupled to btree AND coupled: compare keys; set to nil
     *      if keys are identical; (TODO - improve performance by nil'ling
     *      all other cursors from the same btree page)
     *
     *  do NOT nil the current cursor - it's coupled to the key, and the
     *  coupled key is still needed by the caller
     */
    while (c) {
        if (c->is_nil(0) || c==current)
            goto next;
        if (c->is_coupled_to_txnop())
            goto next;

        if (c->get_btree_cursor()->points_to(key)) {
            /* is the current cursor to a duplicate? then adjust the
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (current->get_dupecache_index()) {
                    if (current->get_dupecache_index()
                            <c->get_dupecache_index()) {
                        c->set_dupecache_index(c->get_dupecache_index()-1);
                        goto next;
                    }
                    else if (current->get_dupecache_index()
                            >c->get_dupecache_index()) {
                        goto next;
                    }
                    /* else fall through */
                }
            }
            c->set_to_nil(0);
        }
next:
        c=c->get_next();
    }
}

ham_status_t
db_erase_txn(Database *db, Transaction *txn, ham_key_t *key, ham_u32_t flags,
                txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;
    Environment *env=db->get_env();
    Cursor *pc=0;
    if (cursor)
        pc=txn_cursor_get_parent(cursor);

    /* get (or create) the node for this key */
    node=txn_opnode_get(db, key, 0);
    if (!node) {
        node=txn_opnode_create(db, key);
        if (!node)
            return (HAM_OUT_OF_MEMORY);
        node_created=HAM_TRUE;
    }

    /* check for conflicts of this key - but only if we're not erasing a
     * duplicate key. dupes are checked for conflicts in _local_cursor_move */
    if (!pc || (!pc->get_dupecache_index())) {
        st=db_check_erase_conflicts(db, txn, node, key, flags);
        env->get_changeset().clear();
        if (st) {
            if (node_created)
                txn_opnode_free(env, node);
            return (st);
        }
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(env, &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
        return (st);
    }

    /* append a new operation to this node */
    op=txn_opnode_append(txn, node, flags, TXN_OP_ERASE, lsn, 0);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    /* is this function called through ham_cursor_erase? then add the
     * duplicate ID */
    if (cursor) {
        if (pc->get_dupecache_index())
            txn_op_set_referenced_dupe(op, pc->get_dupecache_index());
    }

    /* the current op has no cursors attached; but if there are any
     * other ops in this node and in this transaction, then they have to
     * be set to nil. This only nil's txn-cursors! */
    __nil_all_cursors_in_node(txn, pc, node);

    /* in addition we nil all btree cursors which are coupled to this key */
    __nil_all_cursors_in_btree(db, pc, txn_opnode_get_key(node));

    /* append journal entry */
    if (env->get_flags()&HAM_ENABLE_RECOVERY
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        Journal *j=env->get_journal();
        st=j->append_erase(db, txn, key, 0, flags|HAM_ERASE_ALL_DUPLICATES,
                            txn_op_get_lsn(op));
    }

    return (st);
}

static ham_status_t
copy_record(Database *db, Transaction *txn, txn_op_t *op, ham_record_t *record)
{
    ByteArray *arena=(txn==0 || (txn->get_flags()&HAM_TXN_TEMPORARY))
                        ? &db->get_record_arena()
                        : &txn->get_record_arena();

    if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
        arena->resize(txn_op_get_record(op)->size);
        record->data=arena->get_ptr();
    }
    memcpy(record->data, txn_op_get_record(op)->data,
                txn_op_get_record(op)->size);
    record->size=txn_op_get_record(op)->size;
    return (0);
}

static ham_status_t
db_find_txn(Database *db, Transaction *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    txn_optree_t *tree=0;
    txn_opnode_t *node=0;
    txn_op_t *op=0;
    Backend *be=db->get_backend();
    bool first_loop=true;
    bool exact_is_erased=false;

    ByteArray *arena=(txn==0 || (txn->get_flags()&HAM_TXN_TEMPORARY))
                        ? &db->get_key_arena()
                        : &txn->get_key_arena();

    /* get the txn-tree for this database; if there's no tree then
     * there's no need to create a new one - we'll just skip the whole
     * tree-related code */
    tree=db->get_optree();

    ham_key_set_intflags(key,
        (ham_key_get_intflags(key)&(~BtreeKey::KEY_IS_APPROXIMATE)));

    /* get the node for this key (but don't create a new one if it does
     * not yet exist) */
    if (tree)
        node=txn_opnode_get(db, key, flags);

    /*
     * pick the tree_node of this key, and walk through each operation
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then look at the
     *      operation in detail
     * - is this op part of an txn which is still active? return an error
     *      because we've found a conflict
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns
     */
retry:
    if (tree && node)
        op=txn_opnode_get_newest_op(node);
    while (op) {
        Transaction *optxn=txn_op_get_txn(op);
        if (optxn->get_flags()&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((optxn->get_flags()&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            if (txn_op_get_flags(op)&TXN_OP_FLUSHED)
                ; /* nop */
            /* if key was erased then it doesn't exist and we can return
             * immediately
             *
             * if an approximate match is requested then move to the next
             * or previous node
             */
            else if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                if (first_loop
                        && !(ham_key_get_intflags(key)&BtreeKey::KEY_IS_APPROXIMATE))
                    exact_is_erased=true;
                first_loop=false;
                if (flags&HAM_FIND_LT_MATCH) {
                    node=txn_opnode_get_previous_sibling(node);
                    ham_key_set_intflags(key,
                        (ham_key_get_intflags(key)|BtreeKey::KEY_IS_APPROXIMATE));
                    goto retry;
                }
                else if (flags&HAM_FIND_GT_MATCH) {
                    node=txn_opnode_get_next_sibling(node);
                    ham_key_set_intflags(key,
                        (ham_key_get_intflags(key)|BtreeKey::KEY_IS_APPROXIMATE));
                    goto retry;
                }
                return (HAM_KEY_NOT_FOUND);
            }
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key already exists then return its record; do not
             * return pointers to txn_op_get_record, because it may be
             * flushed and the user's pointers would be invalid */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                // approx match? leave the loop and continue
                // with the btree
                if (ham_key_get_intflags(key)&BtreeKey::KEY_IS_APPROXIMATE)
                    break;
                // otherwise copy the record and return
                return (copy_record(db, txn, op, record));
            }
            else {
                ham_assert(!"shouldn't be here");
                return (HAM_KEY_NOT_FOUND);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_previous_in_node(op);
    }

    /*
     * if there was an approximate match: check if the btree provides
     * a better match
     */
    if (op && ham_key_get_intflags(key)&BtreeKey::KEY_IS_APPROXIMATE) {
        ham_key_t txnkey={0};
        ham_key_t *k=txn_opnode_get_key(txn_op_get_node(op));
        txnkey.size=k->size;
        txnkey._flags=BtreeKey::KEY_IS_APPROXIMATE;
        txnkey.data=db->get_env()->get_allocator()->alloc(txnkey.size);
        memcpy(txnkey.data, k->data, txnkey.size);

        ham_key_set_intflags(key, 0);

        // the "exact match" key was erased? then don't fetch it again
        if (exact_is_erased)
            flags=flags&(~HAM_FIND_EXACT_MATCH);

        // now lookup in the btree
        st=be->find(txn, key, record, flags);
        if (st==HAM_KEY_NOT_FOUND) {
            if (!(key->flags&HAM_KEY_USER_ALLOC) && txnkey.data) {
                arena->resize(txnkey.size);
                key->data=arena->get_ptr();
            }
            if (txnkey.data) {
                memcpy(key->data, txnkey.data, txnkey.size);
                db->get_env()->get_allocator()->free(txnkey.data);
            }
            key->size=txnkey.size;
            key->_flags=txnkey._flags;

            return (copy_record(db, txn, op, record));
        }
        else if (st)
            return (st);
        // the btree key is a direct match? then return it
        if ((!(ham_key_get_intflags(key)&BtreeKey::KEY_IS_APPROXIMATE))
                && (flags&HAM_FIND_EXACT_MATCH)) {
            if (txnkey.data)
                db->get_env()->get_allocator()->free(txnkey.data);
            return (0);
        }
        // if there's an approx match in the btree: compare both keys and
        // use the one that is closer. if the btree is closer: make sure
        // that it was not erased or overwritten in a transaction
        int cmp=db->compare_keys(key, &txnkey);
        bool use_btree=false;
        if (flags&HAM_FIND_GT_MATCH) {
            if (cmp<0)
                use_btree=true;
        }
        else if (flags&HAM_FIND_LT_MATCH) {
            if (cmp>0)
                use_btree=true;
        }
        else
            ham_assert(!"shouldn't be here");

        if (use_btree) {
            if (txnkey.data)
                db->get_env()->get_allocator()->free(txnkey.data);
            // lookup again, with the same flags and the btree key.
            // this will check if the key was erased or overwritten
            // in a transaction
            st=db_find_txn(db, txn, key, record,
                            flags|HAM_FIND_EXACT_MATCH);
            if (st==0)
                ham_key_set_intflags(key,
                    (ham_key_get_intflags(key)|BtreeKey::KEY_IS_APPROXIMATE));
            return (st);
        }
        else { // use txn
            if (!(key->flags&HAM_KEY_USER_ALLOC) && txnkey.data) {
                arena->resize(txnkey.size);
                key->data=arena->get_ptr();
            }
            if (txnkey.data) {
                memcpy(key->data, txnkey.data, txnkey.size);
                db->get_env()->get_allocator()->free(txnkey.data);
            }
            key->size=txnkey.size;
            key->_flags=txnkey._flags;

            return (copy_record(db, txn, op, record));
        }
    }

    /*
     * no approximate match:
     *
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts, and we have not found the key: now try to
     * lookup the key in the btree.
     */
    return (be->find(txn, key, record, flags));
}

ham_status_t
DatabaseImplementationLocal::get_parameters(ham_parameter_t *param)
{
    ham_parameter_t *p=param;
    Environment *env=m_db->get_env();

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env->get_cache()->get_capacity();
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env->get_pagesize();
                break;
            case HAM_PARAM_KEYSIZE:
                p->value=m_db->get_backend() ? db_get_keysize(m_db) : 21;
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                p->value=env->get_max_databases();
                break;
            case HAM_PARAM_GET_FLAGS:
                p->value=m_db->get_rt_flags();
                break;
            case HAM_PARAM_GET_FILEMODE:
                p->value=m_db->get_env()->get_file_mode();
                break;
            case HAM_PARAM_GET_FILENAME:
                if (env->get_filename().size())
                    p->value=(ham_u64_t)PTR_TO_U64(env->get_filename().c_str());
                else
                    p->value=0;
                break;
            case HAM_PARAM_LOG_DIRECTORY:
                if (env->get_log_directory().size())
                    p->value=(ham_u64_t)(PTR_TO_U64(env->get_log_directory().c_str()));
                else
                    p->value=0;
                break;
            case HAM_PARAM_GET_DATABASE_NAME:
                p->value=(ham_offset_t)m_db->get_name();
                break;
            case HAM_PARAM_GET_KEYS_PER_PAGE:
                if (m_db->get_backend()) {
                    ham_size_t count=0, size=db_get_keysize(m_db);
                    Backend *be = m_db->get_backend();
                    ham_status_t st;

                    st=be->calc_keycount_per_page(&count, size);
                    if (st)
                        return (st);
                    p->value=count;
                }
                break;
            case HAM_PARAM_GET_DATA_ACCESS_MODE:
                p->value=m_db->get_data_access_mode();
                break;
            default:
                ham_trace(("unknown parameter %d", (int)p->name));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    return (0);
}

ham_status_t
DatabaseImplementationLocal::check_integrity(Transaction *txn)
{
    ham_status_t st;
    Backend *be;

    be=m_db->get_backend();

    /* check the cache integrity */
    if (!(m_db->get_rt_flags()&HAM_IN_MEMORY)) {
        st=m_db->get_env()->get_cache()->check_integrity();
        if (st)
            return (st);
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(m_db->get_env())) {
        st=env_purge_cache(m_db->get_env());
        if (st)
            return (st);
    }

    /* call the backend function */
    st=be->check_integrity();
    m_db->get_env()->get_changeset().clear();

    return (st);
}

ham_status_t
DatabaseImplementationLocal::get_key_count(Transaction *txn, ham_u32_t flags,
                ham_offset_t *keycount)
{
    ham_status_t st;
    Backend *be;
    Environment *env=m_db->get_env();

    calckeys_context_t ctx = {m_db, flags, 0, HAM_FALSE};

    if (flags & ~(HAM_SKIP_DUPLICATES|HAM_FAST_ESTIMATE)) {
        ham_trace(("parameter 'flag' contains unsupported flag bits: %08x",
                  flags & ~(HAM_SKIP_DUPLICATES|HAM_FAST_ESTIMATE)));
        return (HAM_INV_PARAMETER);
    }

    be = m_db->get_backend();

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    /*
     * call the backend function - this will retrieve the number of keys
     * in the btree
     */
    st=be->enumerate(__calc_keys_cb, &ctx);
    if (st)
        goto bail;
    *keycount=ctx.total_count;

    /*
     * if transactions are enabled, then also sum up the number of keys
     * from the transaction tree
     */
    if ((m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)
            && (m_db->get_optree())) {
        struct keycount_t k;
        k.c=0;
        k.flags=flags;
        k.txn=txn;
        k.db=m_db;
        txn_tree_enumerate(m_db->get_optree(), __get_key_count_txn, (void *)&k);
        *keycount+=k.c;
    }

bail:
    env->get_changeset().clear();
    return (st);
}

ham_status_t
DatabaseImplementationLocal::insert(Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;
    ham_status_t st;
    Backend *be=m_db->get_backend();
    ham_u64_t recno=0;
    ham_record_t temprec;

    ByteArray *arena=(txn==0 || (txn->get_flags()&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    if (!txn && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS))
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            ham_assert(key->size==sizeof(ham_u64_t));
            ham_assert(key->data!=0);
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /* get the record number (host endian) and increment it */
            recno=be->get_recno();
            recno++;
        }

        /* allocate memory for the key */
        if (!key->data) {
            arena->resize(sizeof(ham_u64_t));
            key->data=arena->get_ptr();
            key->size=sizeof(ham_u64_t);
        }

        /* store it in db endian */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);

        /* we're appending this key sequentially */
        flags|=HAM_HINT_APPEND;

        /* transactions are faster if HAM_OVERWRITE is specified */
        if (txn)
            flags|=HAM_OVERWRITE;
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(m_db, &temprec);

    /*
     * if transactions are enabled: only insert the key/record pair into
     * the Transaction structure. Otherwise immediately write to the btree.
     */
    if (!st) {
        if (txn || local_txn) {
            st=db_insert_txn(m_db, txn ? txn : local_txn,
                            key, &temprec, flags, 0);
        }
        else
            st=be->insert(txn, key, &temprec, flags);
    }

    if (temprec.data!=record->data)
        env->get_allocator()->free(temprec.data);

    if (st) {
        if (local_txn)
            local_txn->abort();

        if ((m_db->get_rt_flags()&HAM_RECORD_NUMBER)
                && !(flags&HAM_OVERWRITE)) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key->data=0;
                key->size=0;
            }
            ham_assert(st!=HAM_DUPLICATE_KEY);
        }

        env->get_changeset().clear();
        return (st);
    }

    /*
     * record numbers: return key in host endian! and store the incremented
     * record number
     */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        recno=ham_db2h64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
        if (!(flags&HAM_OVERWRITE)) {
            be->set_recno(recno);
            be->flush_indexdata();
        }
    }

    ham_assert(st==0);

    if (local_txn)
        return (local_txn->commit());
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::erase(Transaction *txn, ham_key_t *key,
                ham_u32_t flags)
{
    ham_status_t st;
    Transaction *local_txn=0;
    Environment *env=m_db->get_env();
    Backend *be;
    ham_offset_t recno=0;

    be=m_db->get_backend();
    if (m_db->get_rt_flags()&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (HAM_DB_READ_ONLY);
    }

    /* record number: make sure that we have a valid key structure */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (HAM_INV_PARAMETER);
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    if (!txn && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS))
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);

    /*
     * if transactions are enabled: append a 'erase key' operation into
     * the txn tree; otherwise immediately erase the key from disk
     */
    if (txn || local_txn)
        st=db_erase_txn(m_db, txn ? txn : local_txn, key, flags, 0);
    else
        st=be->erase(txn, key, flags);

    if (st) {
        if (local_txn)
            local_txn->abort();

        env->get_changeset().clear();
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER)
        *(ham_offset_t *)key->data=ham_db2h64(recno);

    ham_assert(st==0);

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::find(Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;
    ham_status_t st;
    Backend *be=m_db->get_backend();

    ham_offset_t recno=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    if ((db_get_keysize(m_db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(m_db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /* if this database has duplicates, then we use ham_cursor_find
     * because we have to build a duplicate list, and this is currently
     * only available in ham_cursor_find */
    if (m_db->get_rt_flags()&HAM_ENABLE_DUPLICATES) {
        Cursor *c;
        st=ham_cursor_create((ham_db_t *)m_db, (ham_txn_t *)txn,
                HAM_DONT_LOCK, (ham_cursor_t **)&c);
        if (st)
            return (st);
        st=ham_cursor_find_ex((ham_cursor_t *)c, key, record,
                            flags|HAM_DONT_LOCK);
        m_db->close_cursor(c);
        return (st);
    }

    /* record number: make sure we have a number in little endian */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        ham_assert(key->size==sizeof(ham_u64_t));
        ham_assert(key->data!=0);
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!txn && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS))
        local_txn = new Transaction(env, 0,
                    HAM_TXN_READ_ONLY|HAM_TXN_TEMPORARY);

    /*
     * if transactions are enabled: read keys from transaction trees,
     * otherwise read immediately from disk
     */
    if (txn || local_txn)
        st=db_find_txn(m_db, txn ? txn : local_txn, key, record, flags);
    else
        st=be->find(txn, key, record, flags);

    if (st) {
        if (local_txn)
            local_txn->abort();

        env->get_changeset().clear();
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER)
        *(ham_offset_t *)key->data=ham_db2h64(recno);

    /* run the record-level filters */
    st=__record_filters_after_find(m_db, record);
    if (st) {
        if (local_txn)
            local_txn->abort();

        env->get_changeset().clear();
        return (st);
    }

    ham_assert(st==0);
    env->get_changeset().clear();

    if (local_txn)
        return (local_txn->commit());
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
                && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

Cursor *
DatabaseImplementationLocal::cursor_create(Transaction *txn, ham_u32_t flags)
{
    Backend *be;

    be=m_db->get_backend();
    if (!be || !be->is_active())
        return (0);

    return (new Cursor(m_db, txn, flags));
}

Cursor *
DatabaseImplementationLocal::cursor_clone(Cursor *src)
{
    return (new Cursor(*src));
}

ham_status_t
DatabaseImplementationLocal::cursor_insert(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Backend *be=m_db->get_backend();
    ham_u64_t recno = 0;
    ham_record_t temprec;
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;
    Transaction *txn=cursor->get_txn();

    ByteArray *arena=(txn==0 || (txn->get_flags()&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();

    if ((db_get_keysize(m_db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(m_db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            ham_assert(key->size==sizeof(ham_u64_t));
            ham_assert(key->data!=0);
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /* get the record number (host endian) and increment it */
            recno=be->get_recno();
            recno++;
        }

        /* allocate memory for the key */
        if (!key->data) {
            arena->resize(sizeof(ham_u64_t));
            key->data=arena->get_ptr();
            key->size=sizeof(ham_u64_t);
        }

        /* store it in db endian */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);

        /* we're appending this key sequentially */
        flags|=HAM_HINT_APPEND;

        /* transactions are faster if HAM_OVERWRITE is specified */
        if (cursor->get_txn())
            flags|=HAM_OVERWRITE;
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(m_db, &temprec);
    if (st)
        return (st);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    if (cursor->get_txn() || local_txn) {
        st=db_insert_txn(m_db,
                    cursor->get_txn()
                        ? cursor->get_txn()
                        : local_txn,
                    key, &temprec, flags, cursor->get_txn_cursor());
        if (st==0) {
            DupeCache *dc=cursor->get_dupecache();
            cursor->couple_to_txnop();
            /* reset the dupecache, otherwise cursor->get_dupecache_count()
             * does not update the dupecache correctly */
            dc->clear();
            /* if duplicate keys are enabled: set the duplicate index of
             * the new key  */
            if (st==0 && cursor->get_dupecache_count()) {
                ham_size_t i;
                txn_cursor_t *txnc=cursor->get_txn_cursor();
                txn_op_t *op=txn_cursor_get_coupled_op(txnc);
                ham_assert(op!=0);

                for (i=0; i<dc->get_count(); i++) {
                    DupeCacheLine *l=dc->get_element(i);
                    if (!l->use_btree() && l->get_txn_op()==op) {
                        cursor->set_dupecache_index(i+1);
                        break;
                    }
                }
            }
            env->get_changeset().clear();
        }
    }
    else {
        st=cursor->get_btree_cursor()->insert(key, &temprec, flags);
        if (st==0)
            cursor->couple_to_btree();
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (temprec.data!=record->data)
        env->get_allocator()->free(temprec.data);

    if (st) {
        if (local_txn)
            local_txn->abort();
        if ((m_db->get_rt_flags()&HAM_RECORD_NUMBER)
                && !(flags&HAM_OVERWRITE)) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key->data=0;
                key->size=0;
            }
            ham_assert(st!=HAM_DUPLICATE_KEY);
        }

        env->get_changeset().clear();
        return (st);
    }

    /* no need to append the journal entry - it's appended in db_insert_txn(),
     * which is called by db_insert_txn() */

    /*
     * record numbers: return key in host endian! and store the incremented
     * record number
     */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        recno=ham_db2h64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
        if (!(flags&HAM_OVERWRITE)) {
            be->set_recno(recno);
            be->flush_indexdata();
        }
    }

    ham_assert(st==0);

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
                && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->erase(cursor->get_txn() ? cursor->get_txn() : local_txn, flags);

    /* clear the changeset */
    env->get_changeset().clear();

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    /* on success: verify that cursor is now nil */
    if (st==0) {
        cursor->couple_to_btree();
        ham_assert(txn_cursor_is_nil(cursor->get_txn_cursor()));
        ham_assert(cursor->is_nil(0));
        cursor->clear_dupecache();
    }
    else {
        if (local_txn)
            local_txn->abort();
        env->get_changeset().clear();
        return (st);
    }

    ham_assert(st==0);

    /* no need to append the journal entry - it's appended in db_erase_txn(),
     * which is called by txn_cursor_erase() */

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}


ham_status_t
DatabaseImplementationLocal::cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t recno=0;
    Transaction *local_txn=0;
    Environment *env=m_db->get_env();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /*
     * record number: make sure that we have a valid key structure,
     * and translate the record number to database endian
     */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (HAM_INV_PARAMETER);
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* reset the dupecache */
    cursor->clear_dupecache();

    /*
     * first try to find the key in the transaction tree. If it exists and
     * is NOT a duplicate then return its record. If it does not exist or
     * it has duplicates then lookup the key in the btree.
     *
     * in non-Transaction mode directly search through the btree.
     */
    if (cursor->get_txn() || local_txn) {
        st=txn_cursor_find(cursor->get_txn_cursor(), key, flags);
        /* if the key was erased in a transaction then fail with an error
         * (unless we have duplicates - they're checked below) */
        if (st) {
            if (st==HAM_KEY_NOT_FOUND)
                goto btree;
            if (st==HAM_KEY_ERASED_IN_TXN) {
                /* performance hack: if coupled op erases ALL duplicates
                 * then we know that the key no longer exists. if coupled op
                 * references a single duplicate w/ index > 1 then
                 * we know that there are other duplicates. if coupled op
                 * references the FIRST duplicate (idx 1) then we have
                 * to check if there are other duplicates */
                txn_op_t *op=txn_cursor_get_coupled_op(txnc);
                ham_assert(txn_op_get_flags(op)&TXN_OP_ERASE);
                if (!txn_op_get_referenced_dupe(op)) {
                    // ALL!
                    st=HAM_KEY_NOT_FOUND;
                }
                else if (txn_op_get_referenced_dupe(op)>1) {
                    // not the first dupe - there are other dupes
                    st=0;
                }
                else if (txn_op_get_referenced_dupe(op)==1) {
                    // check if there are other dupes
                    ham_bool_t is_equal;
                    (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY,
                                    &is_equal);
                    if (!is_equal)
                        cursor->set_to_nil(Cursor::CURSOR_BTREE);
                    if (!cursor->get_dupecache_count())
                        st=HAM_KEY_NOT_FOUND;
                    else
                        st=0;
                }
            }
            if (st)
                goto bail;
        }
        else {
            ham_bool_t is_equal;
            (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY, &is_equal);
            if (!is_equal)
                cursor->set_to_nil(Cursor::CURSOR_BTREE);
        }
        cursor->couple_to_txnop();
        if (!cursor->get_dupecache_count()) {
            if (record)
                st=txn_cursor_get_record(txnc, record);
            goto bail;
        }
        if (st==0)
            goto check_dupes;
    }

btree:
    st=cursor->get_btree_cursor()->find(key, record, flags);
    if (st==0) {
        cursor->couple_to_btree();
        /* if btree keys were found: reset the dupecache. The previous
         * call to cursor_get_dupecache_count() already initialized the
         * dupecache, but only with txn keys because the cursor was only
         * coupled to the txn */
        cursor->clear_dupecache();
    }

check_dupes:
    /* if the key has duplicates: build a duplicate table, then
     * couple to the first/oldest duplicate */
    if (cursor->get_dupecache_count()) {
        DupeCacheLine *e=cursor->get_dupecache()->get_first_element();
        if (e->use_btree())
            cursor->couple_to_btree();
        else
            cursor->couple_to_txnop();
        cursor->couple_to_dupe(1);
        st=0;

        /* now read the record */
        if (record) {
            /* TODO this works, but in case of the btree key w/ duplicates
            * it's possible that we read the record twice. I'm not sure if
            * this can be avoided, though. */
            if (cursor->is_coupled_to_txnop())
                st=txn_cursor_get_record(cursor->get_txn_cursor(),
                        record);
            else
                st=cursor->get_btree_cursor()->move(0, record, 0);
        }
    }
    else {
        if (cursor->is_coupled_to_txnop() && record)
            st=txn_cursor_get_record(cursor->get_txn_cursor(), record);
    }

bail:
    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (st) {
        if (local_txn)
            local_txn->abort();
        env->get_changeset().clear();
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER)
        *(ham_offset_t *)key->data=ham_db2h64(recno);

    /* run the record-level filters */
    if (record) {
        st=__record_filters_after_find(m_db, record);
        if (st) {
            if (local_txn)
                local_txn->abort();
            env->get_changeset().clear();
            return (st);
        }
    }

    ham_assert(st==0);

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::cursor_get_duplicate_count(Cursor *cursor,
                    ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st=0;
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    if (cursor->is_nil(0) && txn_cursor_is_nil(txnc))
        return (HAM_CURSOR_IS_NIL);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->get_duplicate_count(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    count, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (st) {
        if (local_txn)
            local_txn->abort();
        env->get_changeset().clear();
        return (st);
    }

    ham_assert(st==0);

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::cursor_get_record_size(Cursor *cursor,
                    ham_offset_t *size)
{
    ham_status_t st=0;
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    if (cursor->is_nil(0) && txn_cursor_is_nil(txnc))
        return (HAM_CURSOR_IS_NIL);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->get_record_size(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    size);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    env->get_changeset().clear();

    if (st) {
        if (local_txn)
            local_txn->abort();
        return (st);
    }

    ham_assert(st==0);

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags)
{
    Environment *env=m_db->get_env();
    ham_status_t st;
    ham_record_t temprec;
    Transaction *local_txn=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(m_db, &temprec);
    if (st)
        return (st);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->overwrite(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    &temprec, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (temprec.data != record->data)
        env->get_allocator()->free(temprec.data);

    if (st) {
        if (local_txn)
            local_txn->abort();
        env->get_changeset().clear();
        return (st);
    }

    ham_assert(st==0);

    /* the journal entry is appended in db_insert_txn() */

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else if (env->get_flags()&HAM_ENABLE_RECOVERY
            && !(env->get_flags()&HAM_ENABLE_TRANSACTIONS))
        return (env->get_changeset().flush(DUMMY_LSN));
    else
        return (st);
}

ham_status_t
DatabaseImplementationLocal::cursor_move(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    Environment *env=m_db->get_env();
    Transaction *local_txn=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(env)) {
        st=env_purge_cache(env);
        if (st)
            return (st);
    }

    /*
     * if the cursor was never used before and the user requests a NEXT then
     * move the cursor to FIRST; if the user requests a PREVIOUS we set it
     * to LAST, resp.
     *
     * if the cursor was already used but is nil then we've reached EOF,
     * and a NEXT actually tries to move to the LAST key (and PREVIOUS
     * moves to FIRST)
     *
     * TODO the btree-cursor has identical code which can be removed
     */
    if (cursor->is_nil(0)) {
        if (flags&HAM_CURSOR_NEXT) {
            flags&=~HAM_CURSOR_NEXT;
            if (cursor->is_first_use())
              flags|=HAM_CURSOR_FIRST;
            else
              flags|=HAM_CURSOR_LAST;
        }
        else if (flags&HAM_CURSOR_PREVIOUS) {
            flags&=~HAM_CURSOR_PREVIOUS;
            if (cursor->is_first_use())
              flags|=HAM_CURSOR_LAST;
            else
              flags|=HAM_CURSOR_FIRST;
        }
    }

    /* in non-transactional mode - just call the btree function and return */
    if (!(m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        st=cursor->get_btree_cursor()->move(key, record, flags);
        env->get_changeset().clear();
        if (st)
            return (st);

        /* run the record-level filters */
        return (__record_filters_after_find(m_db, record));
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (m_db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        local_txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        cursor->set_txn(local_txn);
    }

    /* everything else is handled by the cursor function */
    st=cursor->move(key, record, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    env->get_changeset().clear();

    /* run the record-level filters */
    if (st==0 && record)
        st=__record_filters_after_find(m_db, record);

    /* store the direction */
    if (flags&HAM_CURSOR_NEXT)
        cursor->set_lastop(HAM_CURSOR_NEXT);
    else if (flags&HAM_CURSOR_PREVIOUS)
        cursor->set_lastop(HAM_CURSOR_PREVIOUS);
    else
        cursor->set_lastop(0);

    if (st) {
        if (local_txn)
            local_txn->abort();
        if (st==HAM_KEY_ERASED_IN_TXN)
            st=HAM_KEY_NOT_FOUND;
        /* trigger a sync when the function is called again */
        cursor->set_lastop(0);
        return (st);
    }

    if (local_txn) {
        env->get_changeset().clear();
        return (local_txn->commit());
    }
    else
        return (st);
}

void
DatabaseImplementationLocal::cursor_close(Cursor *cursor)
{
    cursor->close();
}

static bool
db_close_callback(Page *page, Database *db, ham_u32_t flags)
{
    Environment *env=page->get_device()->get_env();

    if (page->get_db()==db && page!=env->get_header_page()) {
        (void)page->flush();
        (void)page->uncouple_all_cursors();

        /*
         * if this page has a header, and it's either a B-Tree root page or
         * a B-Tree index page: remove all extended keys from the cache,
         * and/or free their blobs
         *
         * TODO move BtreeBackend to backend
         */
        if (page->get_pers() &&
            (!(page->get_flags()&Page::NPERS_NO_HEADER)) &&
                (page->get_type()==Page::TYPE_B_ROOT ||
                    page->get_type()==Page::TYPE_B_INDEX)) {
            ham_assert(page->get_db());
            Backend *backend=page->get_db()->get_backend();
            BtreeBackend *be=dynamic_cast<BtreeBackend *>(backend);
            if (be)
                (void)be->free_page_extkeys(page, flags);
        }

        /* free the page */
        (void)page->free();
        return (true);
    }

    return (false);
}

ham_status_t
DatabaseImplementationLocal::close(ham_u32_t flags)
{
    Environment *env=m_db->get_env();
    ham_status_t st=HAM_SUCCESS;
    ham_status_t st2=HAM_SUCCESS;
    Backend *be;
    Database *newowner=0;
    ham_record_filter_t *record_head;

    /*
     * if this Database is the last database in the environment:
     * delete all environment-members
     */
    if (env) {
        Database *n=env->get_databases();
        while (n) {
            if (n!=m_db)
                break;
            n=n->get_next();
        }
    }

    be=m_db->get_backend();

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env
            && env->get_header_page()
            && !(env->get_flags()&HAM_IN_MEMORY)
            && env->get_device()
            && env->get_device()->is_open()
            && (!(m_db->get_rt_flags()&HAM_READ_ONLY))) {
        /* flush the database header, if it's dirty */
        if (env->is_dirty()) {
            st=env->get_header_page()->flush();
            if (st && st2==0)
                st2=st;
        }
    }

    /* get rid of the extkey-cache */
    if (m_db->get_extkey_cache()) {
        delete m_db->get_extkey_cache();
        m_db->set_extkey_cache(0);
    }

    /* in-memory-database: free all allocated blobs */
    if (be && be->is_active() && env->get_flags()&HAM_IN_MEMORY) {
        Transaction *txn;
        free_cb_context_t context = {0};
        context.db=m_db;
        txn = new Transaction(env, 0, HAM_TXN_TEMPORARY);
        (void)be->enumerate(__free_inmemory_blobs_cb, &context);
        (void)txn->commit();
    }

    /* clear the changeset */
    if (env)
        env->get_changeset().clear();

    /*
     * flush all pages of this database (but not the header page,
     * it's still required and will be flushed below)
     */
    if (env && env->get_cache())
        (void)env->get_cache()->visit(db_close_callback, m_db, 0);

    /* clean up the transaction tree */
    if (m_db->get_optree())
        txn_free_optree(m_db->get_optree());

    /* close the backend */
    if (be) {
        if (be->is_active())
            be->close();
        delete be;
        m_db->set_backend(0);
    }

    /* free cached memory */
    m_db->get_key_arena().clear();
    m_db->get_record_arena().clear();

    /*
     * environment: move the ownership to another database.
     * it's possible that there's no other database, then set the
     * ownership to 0
     */
    if (env) {
        Database *head=env->get_databases();
        while (head) {
            if (head!=m_db) {
                newowner=head;
                break;
            }
            head=head->get_next();
        }
    }
    if (env && env->get_header_page()) {
        ham_assert(env->get_header_page());
        env->get_header_page()->set_db(newowner);
    }

    /* close all record-level filters */
    record_head=m_db->get_record_filter();
    while (record_head) {
        ham_record_filter_t *next=record_head->_next;

        if (record_head->close_cb)
            record_head->close_cb((ham_db_t *)m_db, record_head);
        record_head=next;
    }
    m_db->set_record_filter(0);

    return (st2);
}

} // namespace ham
