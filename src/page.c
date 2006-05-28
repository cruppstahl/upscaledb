/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "error.h"
#include "db.h"
#include "page.h"
#include "mem.h"
#include "os.h"

static ham_bool_t
my_is_in_list(ham_page_t *p, int which)
{
    return (p->_npers._next[which] || p->_npers._prev[which]);
}

static void
my_validage_page(ham_page_t *p)
{
    ham_size_t i;

    /*
     * make sure that we've no circular references
     */
    for (i=0; i<MAX_PAGE_LISTS; i++) {
        if (p->_npers._prev[i])
            ham_assert(p->_npers._prev[i]!=p->_npers._next[i],
                "circular reference in list %d", i);
        ham_assert(p->_npers._prev[i]!=p,
            "circular reference in list %d", i);
        ham_assert(p->_npers._next[i]!=p,
            "circular reference in list %d", i);
    }

    /*
     * not allowed: dirty and in garbage bin
     */
    ham_assert(!(page_is_dirty(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "dirty and in garbage bin", 0);

    /*
     * not allowed: shadowpage and in garbage bin
     */
    ham_assert(!(page_get_shadowpage(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "shadowpage and in garbage bin", 0);

    /*
     * not allowed: shadowpage and in garbage bin
     */
    ham_assert(!(page_get_shadowpage(p) && my_is_in_list(p, PAGE_LIST_UNREF)),
            "shadowpage and in unref-list", 0);

    /*
     * not allowed: referenced and in garbage bin
    ham_assert(!(page_ref_get(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "referenced and in garbage bin", 0);
     */

    /*
     * not allowed: shadowpage and in garbage bin
     */
    ham_assert(!(page_get_shadowpage(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "shadow-page and in garbage bin", 0);

    /*
     * not allowed: in transaction and in garbage bin
    ham_assert(!(my_is_in_list(p, PAGE_LIST_TXN) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "in txn and in garbage bin", 0);
     */

    /*
     * not allowed: cached and in garbage bin
     */
    ham_assert(!(my_is_in_list(p, PAGE_LIST_BUCKET) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "cached and in garbage bin", 0);

    /*
     * not allowed: referenced and in unref-list
     */
    ham_assert(!(page_ref_get(p) && my_is_in_list(p, PAGE_LIST_UNREF)),
            "referenced and in unref-list", 0);
}

ham_page_t *
page_get_next(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._next[which];
    my_validage_page(page);
    if (p)
        my_validage_page(p);
    return (p);
}

void
page_set_next(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._next[which]=other;
    my_validage_page(page);
    if (other)
        my_validage_page(other);
}

ham_page_t *
page_get_previous(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._prev[which];
    my_validage_page(page);
    if (p)
        my_validage_page(p);
    return (p);
}

void
page_set_previous(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._prev[which]=other;
    my_validage_page(page);
    if (other)
        my_validage_page(other);
}

ham_page_t *
page_new(ham_db_t *db)
{
    ham_page_t *page;
    ham_size_t size;

    /*
     * allocate one page of memory
     */
    size=db_get_pagesize(db)+sizeof(page->_npers);
    page=(ham_page_t *)ham_mem_alloc(size);
    if (!page)
        ham_log("page_new failed - out of memory", 0);
    else {
        memset(page, 0, size);
        page_set_owner(page, db);
    }

    return (page);
}

void
page_delete_ext_keys(ham_page_t *page)
{
    ham_size_t i;
    ham_ext_key_t *extkeys;
    
    extkeys=page_get_extkeys(page);
    if (!extkeys) 
        return;

    for (i=0; i<db_get_maxkeys(page_get_owner(page)); i++) {
        if (extkeys[i].data) {
            ham_mem_free(extkeys[i].data);
            extkeys[i].data=0;
            extkeys[i].size=0;
        }
    }
    ham_mem_free(extkeys);
    page_set_extkeys(page, 0);
}

void
page_delete(ham_page_t *page)
{
    page_delete_ext_keys(page);
    ham_mem_free(page);
}

void
page_ref_inc_impl(ham_page_t *page, const char *file, int line)
{
    (void)file;
    (void)line;
    ++page->_npers._refcount;
}

ham_size_t
page_ref_dec_impl(ham_page_t *page, const char *file, int line)
{
    (void)file;
    (void)line;
    return (--page->_npers._refcount);
}

ham_status_t
page_io_read(ham_page_t *page, ham_offset_t address)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);

    /* move to the position of the page */
    st=os_seek(db_get_fd(db), address, HAM_OS_SEEK_SET);
    if (st) {
        ham_log("os_seek failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }

    /* initialize the page */
    page_set_self(page, address);
    page_set_owner(page, db);

    /* read the page */
    st=os_read(db_get_fd(db), (ham_u8_t *)&page->_pers, 
            db_get_pagesize(db));
    if (st) {
        ham_log("os_read failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }

    return (HAM_SUCCESS);
}

ham_status_t
page_io_write(ham_page_t *page)
{
    ham_status_t st;

    /* move to the position of the page */
    st=os_seek(db_get_fd(page_get_owner(page)), page_get_self(page), 
            HAM_OS_SEEK_SET);
    if (st) {
        ham_log("os_seek failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }

    /* write the file */
    st=os_write(db_get_fd(page_get_owner(page)), (ham_u8_t *)&page->_pers, 
            db_get_pagesize(page_get_owner(page)));
    if (st) {
        ham_log("os_write failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }
    
    /* delete the "dirty"-flag */
    page_set_dirty(page, 0);

    return (HAM_SUCCESS);
}

ham_status_t
page_io_alloc(ham_page_t *page, ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos, resize=0;
    ham_db_t *db=page_get_owner(page);
    
    /* first, we ask the freelist for a page */
    if (!(flags & PAGE_IGNORE_FREELIST)) {
        tellpos=freel_alloc_area(db, txn, db_get_pagesize(db), 
                ham_get_flags(db));
        if (tellpos) {
            page_set_self(page, tellpos);
            return (0);
        }
    }

    /* move to the end of the file */
    st=os_seek(db_get_fd(db), 0, HAM_OS_SEEK_END);
    if (st) {
        ham_log("os_seek failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }

    /* get the current file position */
    st=os_tell(db_get_fd(db), &tellpos);
    if (st) {
        ham_log("os_tell failed with status %d (%s)", st, ham_strerror(st));
        return (st);
    }

    /* align the page, if necessary */
    if (!ham_get_flags(db) & HAM_NO_PAGE_ALIGN) {
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

    /* initialize the page */
    memset(page->_pers._payload, 0, db_get_pagesize(db));
    page_set_self(page, tellpos);

    /* and write it to disk */
    st=os_truncate(db_get_fd(db), tellpos+resize+db_get_pagesize(db));
    if (st) {
        ham_log("os_truncate failed with status %d (%s)", st, ham_strerror(st));
        return st;
    }

    /* change the statistical information in the database header */
    db_set_dirty(db, HAM_TRUE);

    return (HAM_SUCCESS);
}

ham_status_t
page_io_free(ham_txn_t *txn, ham_page_t *page)
{
    ham_db_t *db=page_get_owner(page);
    db_set_dirty(db, HAM_TRUE);

    return (freel_add_area(page_get_owner(page), txn, page_get_self(page), 
                db_get_pagesize(page_get_owner(page))));
}

ham_page_t *
page_list_insert(ham_page_t *head, int which, ham_page_t *page)
{
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);

    if (!head)
        return (page);

    page_set_next(page, which, head);
    page_set_previous(head, which, page);
    return (page);
}

ham_page_t *
page_list_insert_ring(ham_page_t *head, int which, ham_page_t *page)
{
   ham_page_t *last;

    if (!head) {
        page_set_next(page, which, page);
        page_set_previous(page, which, page);
        return (page);
    }

    last=page_get_previous(head, which);
    page_set_previous(page, which, last);
    page_set_previous(head, which, page);
    page_set_next(page, which, head);
    page_set_next(last, which, page);
    return (page);
}

ham_page_t *
page_list_remove(ham_page_t *head, int which, ham_page_t *page)
{
    ham_page_t *n, *p;

    if (page==head) {
        n=page_get_next(page, which);
        if (n)
            page_set_previous(n, which, 0);
        page_set_next(page, which, 0);
        page_set_previous(page, which, 0);
        return (n);
    }

    n=page_get_next(page, which);
    p=page_get_previous(page, which);
    if (p)
        page_set_next(p, which, n);
    if (n)
        page_set_previous(n, which, p);
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);
    return (head);
}
