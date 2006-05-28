/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/config.h>
#include "error.h"
#include "cache.h"
#include "freelist.h"
#include "mem.h"
#include "os.h"
#include "db.h"
#include "btree.h"
#include "version.h"
#include "txn.h"

static ham_status_t 
my_write_page(ham_db_t *db, ham_page_t *page)
{
    ham_status_t st;

    /*
     * !!!
     * one day, we'll have to protect these file IO-operations
     * with a mutex
     */
    ham_assert(!(db_get_flags(db)&HAM_IN_MEMORY_DB), 
            "can't fetch a page from in-memory-db", 0);
    ham_assert(page_get_pers(page)!=0, 
            "writing page 0x%llx, but page has no buffer", 
            page_get_self(page));

    st=os_seek(db_get_fd(db), page_get_self(page), HAM_OS_SEEK_SET);
    if (st) {
        ham_log("os_seek failed with status %d (%s)", st, ham_strerror(st));
        db_set_error(db, HAM_IO_ERROR);
        return (HAM_IO_ERROR);
    }
    st=os_write(db_get_fd(db), (ham_u8_t *)page_get_pers(page), 
            db_get_pagesize(db));
    if (st) {
        ham_log("os_write failed with status %d (%s)", st, ham_strerror(st));
        db_set_error(db, HAM_IO_ERROR);
        return (HAM_IO_ERROR);
    }

    page_set_dirty(page, 0);
    return (0);
}

static ham_status_t 
my_read_page(ham_db_t *db, ham_offset_t address, ham_page_t *page)
{
    ham_status_t st;

    /*
     * !!!
     * one day, we'll have to protect these file IO-operations
     * with a mutex
     */

    ham_assert(!(db_get_flags(db)&HAM_IN_MEMORY_DB), 
            "can't fetch a page from in-memory-db", 0);

    if (db_get_flags(db)&DB_USE_MMAP) {
        ham_u8_t *buffer;
        st=os_mmap(db_get_fd(db), address, db_get_pagesize(db),
            &buffer);
        if (st) {
            ham_log("os_mmap failed with status %d (%s)", st, ham_strerror(st));
            db_set_error(db, HAM_IO_ERROR);
            return (HAM_IO_ERROR);
        }
        page_set_pers(page, (union page_union_t *)buffer);
    }
    else {
        st=os_seek(db_get_fd(db), address, HAM_OS_SEEK_SET);
        if (st) {
            ham_log("os_seek failed with status %d (%s)", st, ham_strerror(st));
            db_set_error(db, HAM_IO_ERROR);
            return (HAM_IO_ERROR);
        }
        st=os_read(db_get_fd(db), (void *)page_get_pers(page), 
                db_get_pagesize(db));
        if (st) {
            ham_log("os_read failed with status %d (%s)", st, ham_strerror(st));
            db_set_error(db, HAM_IO_ERROR);
            return (HAM_IO_ERROR);
        }
    }

    return (0);
}

static ham_page_t *
my_alloc_page(ham_db_t *db)
{
    ham_page_t *page;
    ham_status_t st;

    page=cache_get_unused(db_get_cache(db));
    if (page) {
        if (page_is_dirty(page) && !(db_get_flags(db)&HAM_IN_MEMORY_DB)) { 
            st=my_write_page(db, page);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
        if (page_get_npers_flags(page)&PAGE_NPERS_MALLOC) {
            ham_mem_free(page_get_pers(page));
        }
        else {
            st=os_munmap(page_get_pers(page), db_get_pagesize(db));
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
        page_set_pers(page, 0);
    }
    /*
     * otherwise: allocate one page of memory 
     * TODO cache size is not checked!
     */
    else {
        page=(ham_page_t *)ham_mem_alloc(sizeof(ham_page_t));
        if (!page) {
            ham_log("page_new failed - out of memory", 0);
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }
    }

    memset(page, 0, sizeof(ham_page_t));
    page_set_owner(page, db);

    /* 
     * for in-memory-databases and if we use read(2) for I/O, we need 
     * a second page buffer for the file data
     */
    if (!(db_get_flags(db)&DB_USE_MMAP) && !page_get_pers(page)) {
        page_set_pers(page, (union pers_union_t *)ham_mem_alloc(
                    db_get_pagesize(db)));
        if (!page_get_pers(page)) {
            ham_log("page_new failed - out of memory", 0);
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }
        page_set_npers_flags(page, 
                page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
    }
    /*
     * TODO mmunmap??
     */

    /* TODO wenn wir in dieser funktion mit einem fehler rausgehen, 
     * haben wir memory leaks! */

    return (page);
}

int 
db_default_compare(const ham_u8_t *lhs, ham_size_t lhs_length, 
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int m;

    /* 
     * the default compare uses memcmp
     */
    if (lhs_length<rhs_length) {
        m=memcmp(lhs, rhs, lhs_length);
        if (m==0)
            return (-1);
        else
            return (0);
    }

    else if (rhs_length<lhs_length) {
        m=memcmp(lhs, rhs, rhs_length);
        if (m==0)
            return (1);
        else
            return (0);
    }

    return (memcmp(lhs, rhs, lhs_length));
}

int
db_compare_keys(ham_db_t *db, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags, const ham_u8_t *lhs, 
                ham_size_t lhs_length, ham_size_t lhs_real_length, 
                long rhs_idx, ham_u32_t rhs_flags, const ham_u8_t *rhs, 
                ham_size_t rhs_length, ham_size_t rhs_real_length)
{
#if 0 /* @@@ */
    ham_ext_key_t *ext;
    ham_size_t lhsprefixlen, rhsprefixlen;
    ham_prefix_compare_func_t prefoo=db_get_prefix_compare_func(db);
#endif

    int cmp=0;
    ham_compare_func_t foo=db_get_compare_func(db);
    db_set_error(db, 0);

    /*
     * need prefix compare? 
     */
    if (lhs_real_length<=lhs_length && rhs_real_length<=rhs_length) {
        /*
         * no!
         */
        return (foo(lhs, lhs_length, rhs, rhs_length));
    }

#if 0 /* @@@ */
    /*
     * yes! - run prefix comparison, but only if we have a prefix
     * comparison function
     */
    if (prefoo) {
        lhsprefixlen=lhs_length==lhs_real_length 
                ? lhs_length 
                : lhs_length-sizeof(ham_offset_t);
        rhsprefixlen=rhs_length==rhs_real_length 
                ? rhs_length 
                : rhs_length-sizeof(ham_offset_t);
        cmp=prefoo(lhs, lhsprefixlen, lhs_real_length, 
            rhs, rhsprefixlen, rhs_real_length);
        if (db_get_error(db))
            return (0);
    }
    if (!prefoo || cmp==HAM_PREFIX_REQUEST_FULLKEY) {
        /*
         * load the full key
         * 1. check if an extkeys-array is loaded (if not, allocate memory)
         */
        if (!page_get_extkeys(page)) {
            ext=(ham_ext_key_t *)ham_mem_alloc(db_get_maxkeys(db)*
                    sizeof(ham_ext_key_t));
            if (!ext) {
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return (0);
            }
            memset(ext, 0, db_get_maxkeys(db)*sizeof(ham_ext_key_t));
            page_set_extkeys(page, ext);
        }

        /*
         * 2. make sure that both keys are loaded; if not, load them from 
         * disk
         */
        ext=page_get_extkeys(page);
        if (lhs_length!=lhs_real_length && ext[lhs_idx].data==0) {
            ham_assert(lhs_idx!=-1, "invalid rhs_index -1", 0);
            if (!(lhs=my_load_key(db, &ext[lhs_idx], lhs, lhs_length,
                            lhs_real_length)))
                return (0);
            lhs_length=lhs_real_length;
        }
        if (rhs_length!=rhs_real_length && ext[rhs_idx].data==0) {
            ham_assert(rhs_idx!=-1, "invalid rhs_index -1", 0);
            if (!(rhs=my_load_key(db, &ext[rhs_idx], rhs, rhs_length, 
                            rhs_real_length)))
                return (0);
            rhs_length=rhs_real_length;
        }

        /*
         * 3. run the comparison function
         */
        return (foo(lhs, lhs_length, rhs, rhs_length));
    }
#endif

    return (cmp);
}

ham_backend_t *
db_create_backend(ham_db_t *db, ham_u32_t flags)
{
    ham_backend_t *be;
    ham_status_t st;

    /*
     * hash tables are not yet supported
     */
    if (flags&HAM_BE_HASH) {
        return (0);
    }

    /* 
     * the default backend is the BTREE
     *
     * create a ham_backend_t with the size of a ham_btree_t
     */
    be=(ham_backend_t *)ham_mem_alloc(sizeof(ham_btree_t));
    if (!be) {
        ham_log("out of memory", 0);
        return (0);
    }

    /* initialize the backend */
    st=btree_create((ham_btree_t *)be, db, flags);
    if (st) {
        ham_log("failed to initialize backend: 0x%s", st);
        return (0);
    }

    return (be);
}

ham_page_t *
db_fetch_page(ham_db_t *db, ham_txn_t *txn, ham_offset_t address, 
        ham_u32_t flags)
{
    ham_page_t *page;
    ham_status_t st;

    (void)flags;

    /*
     * first, check if the page is in the txn
     */
    if (txn) {
        page=txn_get_page(txn, address);
        if (page)
            return (page);
    }

    /*
     * if we have a cache: fetch the page from the cache
     */
    if (db_get_cache(db)) {
        page=cache_get(db_get_cache(db), address);
        if (page) {
            if (txn) {
                st=txn_add_page(txn, page);
                if (st) {
                    db_set_error(db, st);
                    return (0);
                }
            }
            return (page);
        }
    }

    /*
     * otherwise allocate memory for the page
     */
    page=my_alloc_page(db);
    if (!page)
        return (0);

    /*
     * and read the page, either with mmap or read
     */
    st=my_read_page(db, address, page);
    if (st) {
        db_set_error(db, st);
        (void)cache_move_to_garbage(db_get_cache(db), page);
        return (0);
    }
    page_set_self(page, address);

    /*
     * if a transaction is active: add the page to the transaction
     */
    if (txn) {
        st=txn_add_page(txn, page);
        if (st) {
            db_set_error(db, st);
            (void)cache_move_to_garbage(db_get_cache(db), page);
            return (0);
        }
    }

    /*
     * add the page to the cache
     */
    st=cache_put(db_get_cache(db), page);
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    return (page);
}

ham_status_t
db_flush_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page,
        ham_u32_t flags)
{
    ham_assert(txn==0, 
            "flush with txn not yet supported", 0);
    ham_assert(page_get_shadowpage(page)==0, 
            "flushing a page with a shadow-page", 0);

    /*
     * is this a shadow-page?
     * TODO we're not yet dealing with shadow pages
    if (page_get_shadoworig(page)) {
        ham_page_t *orig=page_get_shadoworig(page);
        page_set_shadowpage(orig, 0);
        page_set_shadoworig(page, 0);

        if ((flags & DB_REVERT_CHANGES) && page_is_dirty(page)) {
            st=cache_move_to_garbage(db_get_cache(db), page);
            if (st)
                return (st);
            return (cache_put(db_get_cache(db), orig));
        }
        else {
            st=cache_move_to_garbage(db_get_cache(db), orig);
            if (st)
                return (st);
            return (cache_put(db_get_cache(db), page));
        }
    }
     */

    return (cache_put(db_get_cache(db), page));
}

ham_status_t
db_flush_all(ham_db_t *db, ham_txn_t *txn)
{
    return (cache_flush_and_delete(db_get_cache(db)));
}

ham_page_t *
db_alloc_page(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags)
{
    ham_offset_t tellpos=0, resize=0;
    ham_status_t st;
    ham_page_t *page;

    /* allocate memory for the page */
    page=my_alloc_page(db);
    if (!page)
        return (0);

    /* 
     * if this is not an in-memory-db: set the memory to 0 and leave
     */
    if (db_get_flags(db)&HAM_IN_MEMORY_DB) {
        page_set_self(page, (ham_offset_t)page);
        /* store the page in the cache */
        st=cache_put(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st); /* TODO memleak! */
            return (0);
        }
        /* add the page to the transaction */
        if (txn) {
            st=txn_add_page(txn, page);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
        return (page);
    }

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        tellpos=freel_alloc_area(db, txn, db_get_pagesize(db),
                db_get_flags(db));
        if (tellpos) 
            page_set_self(page, tellpos);
    }

    /* otherwise: move to the end of the file */
    if (!tellpos) {
        st=os_seek(db_get_fd(db), 0, HAM_OS_SEEK_END);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        /* get the current file position */
        st=os_tell(db_get_fd(db), &tellpos);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        /* align the page, if necessary */
        if (!db_get_flags(db) & HAM_NO_PAGE_ALIGN) {
            if (tellpos%db_get_pagesize(db)) {
                resize=db_get_pagesize(db)-(tellpos%db_get_pagesize(db));
    
                /* add the file chunk to the freelist */
                if (!(flags & PAGE_IGNORE_FREELIST))
                    freel_add_area(page_get_owner(page), txn, tellpos,
                            db_get_pagesize(db)-(tellpos%db_get_pagesize(db)));
    
                /* adjust the position of the new page */
                tellpos+=db_get_pagesize(db)-(tellpos%db_get_pagesize(db));
            }
        }

        /* and write it to disk */
        st=os_truncate(db_get_fd(db), tellpos+resize+db_get_pagesize(db));
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        /*
         * if we're using MMAP: when allocating a new page, we need
         * memory for the persistent buffer
         */
        if ((db_get_flags(db)&DB_USE_MMAP) && !page_get_pers(page)) {
            page_set_pers(page, (union pers_union_t *)ham_mem_alloc(
                        db_get_pagesize(db)));
            if (!page_get_pers(page)) {
                ham_log("page_new failed - out of memory", 0);
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return (0);
            }
            /*
             * TODO memset is needed for valgrind
             */
            memset(page_get_pers(page), 0, db_get_pagesize(db));
            page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
        }
    }

    if (page_get_npers_flags(page)&PAGE_NPERS_MALLOC) 
        memset(page_get_pers(page), 0, db_get_pagesize(db));

    /* change the statistical information in the database header */
    /* TODO */
    db_set_dirty(db, HAM_TRUE);
    page_set_owner(page, db);
    page_set_self(page, tellpos);
    page_set_dirty(page, 0);

    /* add the page to the transaction */
    if (txn) {
        st=txn_add_page(txn, page);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
    }

    /* store the page in the cache */
    st=cache_put(db_get_cache(db), page);
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    if (txn)
        return (db_lock_page(db, txn, page, DB_READ_WRITE));
    else
        return (page);

    /* TODO avoid memory leak! auch nach anderen 
        aufrufen von my_alloc_page() */
}

ham_status_t
db_free_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags)
{
    /*
     * add the page to the freelist
     *
     * !!!
     * as soon as we have concurrency, this is no longer safe - if txn A
     * deletes a page, but is not yet committed, then txn B deletes a 
     * page, B is locked

    return (freel_add_area(db, txn, page_get_self(page), db_get_pagesize(db)));
     */

    /*
     * make sure that the page is locked
     */
    (void)db_lock_page(db, txn, page, PAGE_NPERS_LOCK_WRITE);

    return (0);

    /*
    - page_npers_flags |= DELETE_PENDING (nur origpage!)
    - unlock (eine gelöschte page braucht keinen unlock)
    - beim txn_commit: wenn page_npers_flags & DELETE_PENDING: 
        - freel_add_area()
        - raus aus dem cache
        - in die garbagelist
    - beim txn_abort: wenn page_flags & DELETE_PENDING: 
        - flag löschen

    freel_add_area() sollte die txn nicht benutzen, sonst bekommen wir 
        ne menge schwierigkeiten. 
    freel_get_area() kann die txn benutzen - das bedeutet dass die page
        dirty wird; versucht ein anderer thread/txn, die page zu locken,
        wird er geblockt - er kann dann einfach eine neue page allokieren,
        ohne die freelist zu benutzen.
    */
}

ham_page_t *
db_lock_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags)
{
    ham_status_t st;

    /*
     * a transaction which already holds the lock should be allowed
     * to lock the page a second time.
     *
     * ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_LOCKED), 
            "trying to lock a locked page", 0);
     */

    if (flags & DB_READ_ONLY) {
        if (page_get_npers_flags(page)&PAGE_NPERS_LOCK_READ)
            return (page);
        page_set_npers_flags(page, 
                page_get_npers_flags(page)|PAGE_NPERS_LOCK_READ); 
        return (page);
    }

    if (page_get_npers_flags(page)&PAGE_NPERS_LOCK_WRITE)
        return (page);

    page_set_npers_flags(page, 
            page_get_npers_flags(page)|PAGE_NPERS_LOCK_WRITE); 

    ham_assert(!(txn_get_flags(txn)&TXN_READ_ONLY), 
            "trying to lock a page for write access, but the txn "
            "is read-only", 0);

    /*
     * if the page is dirty: create a shadow-page
     * TODO 
     * right now we flush it
     */
    if (page_is_dirty(page) && !(db_get_flags(db)&HAM_IN_MEMORY_DB)) { 
        st=my_write_page(db, page);
        if (st) 
            db_set_error(db, st);
            /* fall through */
    }

    return (page);
}

ham_status_t
db_unlock_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags)
{
    ham_assert(page_get_npers_flags(page)&PAGE_NPERS_LOCKED, 
            "trying to unlock an unlocked page", 0);

    /* 
     * !!
     * this is a nop
     */
    page_set_npers_flags(page, page_get_npers_flags(page)&(~PAGE_NPERS_LOCKED)); 
    return (0);
}

ham_status_t 
db_write_page_and_delete(ham_db_t *db, ham_page_t *page)
{
    /*
     * write page to disk
     */
    if (page_is_dirty(page) && !(db_get_flags(db)&HAM_IN_MEMORY_DB))
        (void)my_write_page(db, page);

    /* 
     * for in-memory-databases and if we use read(2) for I/O, we need 
     * to free the persistent data 
     */
    if (page_get_pers(page)) {
        if (page_get_npers_flags(page)&PAGE_NPERS_MALLOC) 
            ham_mem_free(page_get_pers(page));
        else 
            (void)os_munmap(page_get_pers(page), db_get_pagesize(db));
    }

    ham_mem_free(page);

    return (0);
}
