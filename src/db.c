/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "error.h"
#include "cache.h"
#include "freelist.h"
#include "mem.h"
#include "os.h"
#include "db.h"
#include "btree.h"
#include "version.h"
#include "txn.h"
#include "blob.h"
#include "extkeys.h"
#include "cursor.h"
#include "btree_cursor.h"



ham_status_t
db_uncouple_all_cursors(ham_page_t *page)
{
    ham_status_t st;
    ham_cursor_t *n, *c=page_get_cursors(page);

    while (c) {
        n=cursor_get_next(c);
        st=bt_cursor_uncouple((ham_bt_cursor_t *)c, 0);
        if (st)
            return (st);
        cursor_set_next(c, 0);
        cursor_set_previous(c, 0);
        c=n;
    }

    page_set_cursors(page, 0);

    return (0);
}

int
db_default_prefix_compare(const ham_u8_t *lhs, ham_size_t lhs_length,
                   ham_size_t lhs_real_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length,
                   ham_size_t rhs_real_length)
{
    int m;
    ham_size_t min_length=lhs_length<rhs_length?lhs_length:rhs_length;

    m=memcmp(lhs, rhs, min_length);
    if (m<0)
        return (-1);
    if (m>0)
        return (+1);
    return (HAM_PREFIX_REQUEST_FULLKEY);
}

int
db_default_compare(const ham_u8_t *lhs, ham_size_t lhs_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int m;

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

ham_status_t
db_get_extended_key(ham_db_t *db, ham_u8_t *key_data,
                    ham_size_t key_length, ham_u32_t key_flags,
                    ham_u8_t **ext_key)
{
    ham_offset_t blobid;
    ham_status_t st;
    ham_size_t temp;
    ham_record_t record;
    ham_u8_t *ptr;

    *ext_key=0;

    ham_assert(key_flags&KEY_IS_EXTENDED,
            ("key is not extended"));

    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        if (!db_get_extkey_cache(db)) {
            extkey_cache_t *c=extkey_cache_new(db);
            if (db_get_env(db))
                env_set_extkey_cache(db_get_env(db), c);
            else
                db_set_extkey_cache(db, c);
            if (!db_get_extkey_cache(db))
                return (db_get_error(db));
        }
    }

    blobid=*(ham_offset_t *)(key_data+(db_get_keysize(db)-
            sizeof(ham_offset_t)));
    blobid=ham_db2h_offset(blobid);

    /* fetch from the cache */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=extkey_cache_fetch(db_get_extkey_cache(db), blobid,
                        &temp, &ptr);
        if (!st) {
            ham_assert(temp==key_length, ("invalid key length"));

            *ext_key=(ham_u8_t *)ham_mem_alloc(db, key_length);
            if (!*ext_key) {
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return (HAM_OUT_OF_MEMORY);
            }
            memcpy(*ext_key, ptr, key_length);
            return (0);
        }
        if (st!=HAM_KEY_NOT_FOUND) {
            db_set_error(db, st);
            return (st);
        }
    }

    /*
     * not cached - fetch from disk;
     * we allocate the memory here to avoid that the global record
     * pointer is overwritten
     */
    memset(&record, 0, sizeof(record));
    record.data=ham_mem_alloc(db, key_length+sizeof(ham_offset_t));
    if (!record.data)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    record.flags=HAM_RECORD_USER_ALLOC;

    st=blob_read(db, blobid, &record, 0);
    if (st) {
        ham_mem_free(db, record.data);
        return (db_set_error(db, st));
    }

    *ext_key=(ham_u8_t *)ham_mem_alloc(db, key_length);
    if (!*ext_key) {
        ham_mem_free(db, record.data);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }
    memcpy(*ext_key, key_data, db_get_keysize(db)-sizeof(ham_offset_t));
    memcpy(*ext_key+(db_get_keysize(db)-sizeof(ham_offset_t)),
               record.data, record.size);

    /* insert the FULL key in the cache */
    if (db_get_extkey_cache(db)) {
        (void)extkey_cache_insert(db_get_extkey_cache(db),
                blobid, key_length, *ext_key);
    }

    ham_mem_free(db, record.data);
    return (0);
}

/*
 * TODO too much duplicated code - use db_get_extended_key
 */
int
db_compare_keys(ham_db_t *db, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                long rhs_idx, ham_u32_t rhs_flags,
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int cmp=HAM_PREFIX_REQUEST_FULLKEY;
    ham_compare_func_t foo=db_get_compare_func(db);
    ham_prefix_compare_func_t prefoo=db_get_prefix_compare_func(db);
    ham_status_t st;
    ham_record_t lhs_record, rhs_record;
    ham_u8_t *plhs=0, *prhs=0;
    ham_size_t temp;
    ham_bool_t alloc1=HAM_FALSE, alloc2=HAM_FALSE;

    db_set_error(db, 0);

    /*
     * need prefix compare?
     */
    if (!(lhs_flags&KEY_IS_EXTENDED) && !(rhs_flags&KEY_IS_EXTENDED)) {
        /*
         * no!
         */
        return (foo(lhs, lhs_length, rhs, rhs_length));
    }

    /*
     * yes! - run prefix comparison, but only if we have a prefix
     * comparison function
     */
    if (prefoo) {
        ham_size_t lhsprefixlen, rhsprefixlen;

        if (lhs_flags&KEY_IS_EXTENDED)
            lhsprefixlen=db_get_keysize(db)-sizeof(ham_offset_t);
        else
            lhsprefixlen=lhs_length;

        if (rhs_flags&KEY_IS_EXTENDED)
            rhsprefixlen=db_get_keysize(db)-sizeof(ham_offset_t);
        else
            rhsprefixlen=rhs_length;

        cmp=prefoo(lhs, lhsprefixlen, lhs_length, rhs,
                rhsprefixlen, rhs_length);
        if (db_get_error(db))
            return (0);
    }

    if (cmp==HAM_PREFIX_REQUEST_FULLKEY) {
        /*
         * make sure that we have an extended key-cache
         *
         * in in-memory-db, the extkey-cache doesn't lead to performance
         * advantages; it only duplicates the data and wastes memory.
         * therefore we don't use it.
         */
        if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
            if (!db_get_extkey_cache(db)) {
                if (db_get_env(db))
                    env_set_extkey_cache(db_get_env(db), extkey_cache_new(db));
                else
                    db_set_extkey_cache(db, extkey_cache_new(db));
                if (!db_get_extkey_cache(db))
                    return (db_get_error(db));
            }
        }

        /*
         * 1. load the first key, if needed
         */
        if (lhs_flags&KEY_IS_EXTENDED) {
            ham_offset_t blobid;

            blobid=*(ham_offset_t *)(lhs+(db_get_keysize(db)-
                    sizeof(ham_offset_t)));
            blobid=ham_db2h_offset(blobid);

            /* fetch from the cache */
            if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
                st=extkey_cache_fetch(db_get_extkey_cache(db), blobid,
                        &temp, &plhs);
                if (!st)
                    ham_assert(temp==lhs_length, ("invalid key length"));
            }
            else
                st=HAM_KEY_NOT_FOUND;

            if (st) {
                if (st!=HAM_KEY_NOT_FOUND) {
                    db_set_error(db, st);
                    return (st);
                }
                /* not cached - fetch from disk */
                memset(&lhs_record, 0, sizeof(lhs_record));

                st=blob_read(db, blobid, &lhs_record, 0);
                if (st) {
                    db_set_error(db, st);
                    goto bail;
                }

                plhs=(ham_u8_t *)ham_mem_alloc(db, lhs_record.size+
                        db_get_keysize(db));
                if (!plhs) {
                    db_set_error(db, HAM_OUT_OF_MEMORY);
                    goto bail;
                }
                memcpy(plhs, lhs, db_get_keysize(db)-sizeof(ham_offset_t));
                memcpy(plhs+(db_get_keysize(db)-sizeof(ham_offset_t)),
                        lhs_record.data, lhs_record.size);

                /* insert the FULL key in the cache */
                if (db_get_extkey_cache(db)) {
                    (void)extkey_cache_insert(db_get_extkey_cache(db),
                            blobid, lhs_length, plhs);
                }
                alloc1=HAM_TRUE;
            }
        }

        /*
         * 2. load the second key, if needed
         */
        if (rhs_flags&KEY_IS_EXTENDED) {
            ham_offset_t blobid;

            blobid=*(ham_offset_t *)(rhs+(db_get_keysize(db)-
                    sizeof(ham_offset_t)));
            blobid=ham_db2h_offset(blobid);
			ham_assert(blobid, ("blobid is empty"));

            /* fetch from the cache */
            if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
                st=extkey_cache_fetch(db_get_extkey_cache(db), blobid,
                        &temp, &prhs);
                if (!st)
                    ham_assert(temp==rhs_length, ("invalid key length"));
            }
            else
                st=HAM_KEY_NOT_FOUND;

            if (st) {
                if (st!=HAM_KEY_NOT_FOUND) {
                    db_set_error(db, st);
                    return (st);
                }
                /* not cached - fetch from disk */
                memset(&rhs_record, 0, sizeof(rhs_record));

                st=blob_read(db, blobid, &rhs_record, 0);
                if (st) {
                    db_set_error(db, st);
                    goto bail;
                }

                prhs=(ham_u8_t *)ham_mem_alloc(db, rhs_record.size+
                        db_get_keysize(db));
                if (!prhs) {
                    db_set_error(db, HAM_OUT_OF_MEMORY);
                    goto bail;
                }

                memcpy(prhs, rhs, db_get_keysize(db)-sizeof(ham_offset_t));
                memcpy(prhs+(db_get_keysize(db)-sizeof(ham_offset_t)),
                        rhs_record.data, rhs_record.size);

                /* insert the FULL key in the cache */
                if (db_get_extkey_cache(db)) {
                    (void)extkey_cache_insert(db_get_extkey_cache(db),
                            blobid, rhs_length, prhs);
                }
                alloc2=HAM_TRUE;
            }
        }

        /*
         * 3. run the comparison function
         */
        cmp=foo(plhs ? plhs : lhs, lhs_length, prhs ? prhs : rhs, rhs_length);
    }

bail:
    if (alloc1 && plhs)
        ham_mem_free(db, plhs);
    if (alloc2 && prhs)
        ham_mem_free(db, prhs);

    return (cmp);
}

ham_backend_t *
db_create_backend(ham_db_t *db, ham_u32_t flags)
{
    ham_backend_t *be;
    ham_status_t st;

    /*
     * hash tables are not yet supported
    if (flags&HAM_USE_HASH) {
        ham_log(("hash indices are not yet supported"));
        return (0);
    }
     */

    /*
     * the default backend is the BTREE
     *
     * create a ham_backend_t with the size of a ham_btree_t
     */
    be=(ham_backend_t *)ham_mem_alloc(db, sizeof(ham_btree_t));
    if (!be) {
        ham_log(("out of memory"));
        return (0);
    }

    /* initialize the backend */
    st=btree_create((ham_btree_t *)be, db, flags);
    if (st) {
        ham_log(("failed to initialize backend: 0x%s", st));
        return (0);
    }

    return (be);
}

static ham_status_t
my_purge_cache(ham_db_t *db)
{
    ham_status_t st;
    ham_page_t *page;

    /*
     * first, try to delete unused pages from the cache
     */
    if (db_get_cache(db) && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        while (cache_too_big(db_get_cache(db))) {
            page=cache_get_unused_page(db_get_cache(db));
            if (!page) {
                if (db_get_rt_flags(db)&HAM_CACHE_STRICT) 
                    return (db_set_error(db, HAM_CACHE_FULL));
                else
                    break;
            }

            st=db_write_page_and_delete(page, 0);
            if (st)
                return (db_set_error(db, st));
        }
    }

    /*
     * then free unused extended keys. We don't do this too often to 
     * avoid a thrashing of the cache, and freeing unused extkeys
     * is more expensive (performance-wise) than freeing unused pages.
     */
    if (db_get_extkey_cache(db)) {
        if (db_get_txn_id(db)%10==0) {
            st=extkey_cache_purge(db_get_extkey_cache(db));
            if (st)
                return (db_set_error(db, st));
        }
    }

    return (HAM_SUCCESS);
}

ham_page_t *
db_page_alloc(ham_db_t *db)
{
    ham_status_t st;
    ham_page_t *page=0;

    /* purge cache, if necessary */
    st=my_purge_cache(db);
    if (st)
        return (0);

    /* try to get an unused page from the cache */
    if (db_get_cache(db))
        page=cache_get_unused_page(db_get_cache(db));

    if (page) {
        st=page_flush(page);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        st=db_uncouple_all_cursors(page);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        st=page_free(page);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        memset(page, 0, sizeof(ham_page_t));
        page_set_owner(page, db);
    }
    else {
        page=page_new(db);
        if (!page) {
            db_set_error(db, st);
            return (0);
        }
        
        st=page_alloc(page, db_get_pagesize(db));
        if (st) {
            db_set_error(db, st);
            return (0);
        }
    }

    if (page && db_get_cache(db)) {
        st=cache_put_page(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
    }

    return (page);
}

ham_status_t
db_free_page(ham_page_t *page)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);

    st=db_uncouple_all_cursors(page);
    if (st)
        return (st);

    if (db_get_cache(db)) {
        st=cache_remove_page(db_get_cache(db), page);
        if (st)
            return (st);
    }

    /*
     * if this page has a header, and it's either a B-Tree root page or 
     * a B-Tree index page: remove all extended keys from the cache, 
     * and/or free their blobs
     *
     * TODO move this to the backend!
     */
    if (page_get_pers(page) && 
	    (!(page_get_npers_flags(page)&PAGE_NPERS_NO_HEADER)) &&
         (page_get_type(page)==PAGE_TYPE_B_ROOT ||
          page_get_type(page)==PAGE_TYPE_B_INDEX)) {
        ham_size_t i;
        ham_offset_t blobid;
        int_key_t *bte;
        btree_node_t *node=ham_page_get_btree_node(page);
        extkey_cache_t *c=db_get_extkey_cache(page_get_owner(page));

        for (i=0; i<btree_node_get_count(node); i++) {
            bte=btree_node_get_key(db, node, i);
            if (key_get_flags(bte)&KEY_IS_EXTENDED) {
                blobid=key_get_extended_rid(db, bte);
                if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
                    /* delete the blobid to prevent that it's freed twice */
                    *(ham_offset_t *)(key_get_key(bte)+
                        (db_get_keysize(db)-sizeof(ham_offset_t)))=0;
                }
                (void)blob_free(db, blobid, 0);
                (void)extkey_cache_remove(c, blobid);
            }
        }
    }

    /*
     * free the page; this will automatically flush the page, if 
     * it's dirty
     */
    st=page_free(page);
    if (st)
        return (st);

    page_delete(page);
    return (HAM_SUCCESS);
}

ham_page_t *
db_alloc_page(ham_db_t *db, ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos=0;
    ham_page_t *page=0;

    /* purge the cache, if necessary */
    st=my_purge_cache(db);
    if (st)
        return (0);

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        tellpos=freel_alloc_page(db);
        if (tellpos) {
            ham_assert(tellpos%db_get_pagesize(db)==0,
                    ("page id %llu is not aligned", tellpos));
            /* try to fetch the page from the txn */
            if (db_get_txn(db)) {
                page=txn_get_page(db_get_txn(db), tellpos);
                if (page)
                    goto done;
            }
            /* try to fetch the page from the cache */
            if (db_get_cache(db)) {
                page=cache_get_page(db_get_cache(db), tellpos);
                if (page)
                    goto done;
            }
            /* allocate a new page structure */
            page=page_new(db);
            if (!page)
                return (0);
            page_set_self(page, tellpos);
            /* fetch the page from disk */
            st=page_fetch(page, db_get_pagesize(db));
            if (st) {
                page_delete(page);
                return (0);
            }
            goto done;
        }
    }

    if (!page) {
        page=page_new(db);
        if (!page)
            return (0);
    }

    st=page_alloc(page, db_get_pagesize(db));
    if (st)
        return (0);

done:
    page_set_type(page, type);
    page_set_dirty(page, 0);

    if (flags&PAGE_CLEAR_WITH_ZERO)
        memset(page_get_pers(page), 0, db_get_pagesize(db));

    if (db_get_txn(db)) {
        st=txn_add_page(db_get_txn(db), page);
        if (st) {
            db_set_error(db, st);
            /* TODO memleak? */
            return (0);
        }
    }

    if (db_get_cache(db)) {
        st=cache_put_page(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st);
            /* TODO memleak? */
            return (0);
        }
    }

    return (page);
}

ham_page_t *
db_fetch_page(ham_db_t *db, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page=0;
    ham_status_t st;

    if (db_get_txn(db)) {
        page=txn_get_page(db_get_txn(db), address);
        if (page)
            return (page);
    }

    if (db_get_cache(db)) {
        page=cache_get_page(db_get_cache(db), address);
        if (page) {
            if (db_get_txn(db)) {
                st=txn_add_page(db_get_txn(db), page);
                if (st) {
                    db_set_error(db, st);
                    return (0);
                }
            }
            st=cache_put_page(db_get_cache(db), page);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
            return (page);
        }
    }

    if (flags&DB_ONLY_FROM_CACHE)
        return (0);

    /* check if the cache allows us to allocate another page */
    st=my_purge_cache(db);
    if (st)
        return (0);

    page=page_new(db);
    if (!page)
        return (0);

    page_set_self(page, address);
    st=page_fetch(page, db_get_pagesize(db));
    if (st) {
        (void)page_delete(page);
        return (0);
    }

    if (db_get_txn(db)) {
        st=txn_add_page(db_get_txn(db), page);
        if (st) {
            db_set_error(db, st);
            (void)page_delete(page);
            return (0);
        }
    }

    if (db_get_cache(db)) {
        st=cache_put_page(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st);
            (void)page_delete(page);
            return (0);
        }
    }

    return (page);
}

ham_status_t
db_flush_page(ham_db_t *db, ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;

    /* write the page, if it's dirty and if write-through is enabled */
    if ((db_get_rt_flags(db)&HAM_WRITE_THROUGH) && page_is_dirty(page)) {
        st=page_flush(page);
        if (st)
            return (st);
    }

    if (db_get_cache(db))
        return (cache_put_page(db_get_cache(db), page));
    return (0);
}

ham_status_t
db_flush_all(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *head;

    if (!db_get_cache(db)) 
        return (0);

    head=cache_get_totallist(db_get_cache(db)); 
    while (head) {
        ham_page_t *next=page_get_next(head, PAGE_LIST_CACHED);

        /*
         * don't remove the page from the cache, if flag NODELETE
         * is set (this flag is used i.e. in ham_flush())
         */
        if (!(flags&DB_FLUSH_NODELETE)) {
            ham_assert(page_get_refcount(head)==0, 
                ("page is in use, but database is closing"));
            cache_set_totallist(db_get_cache(db), 
                page_list_remove(cache_get_totallist(db_get_cache(db)), 
                PAGE_LIST_CACHED, head));
            cache_set_cur_elements(db_get_cache(db), 
                cache_get_cur_elements(db_get_cache(db))-1);
        }

        st=db_write_page_and_delete(head, flags);
        if (st) 
            ham_log(("failed to flush page (%d) - ignoring error...", st));

        head=next;
    }

    return (HAM_SUCCESS);
}

ham_status_t
db_write_page_and_delete(ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);

    /*
     * write page to disk if it's dirty (and if we don't have 
     * an IN-MEMORY DB)
     */
    if (page_is_dirty(page) && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=page_flush(page);
        if (st)
            return (st);
    }

    /*
     * if the page is deleted, uncouple all cursors, then
     * free the memory of the page
     */
    if (!(flags&DB_FLUSH_NODELETE)) {
        st=db_uncouple_all_cursors(page);
        if (st)
            return (st);
        st=page_free(page);
        if (st)
            return (st);
        page_delete(page);
    }

    return (HAM_SUCCESS);
}

