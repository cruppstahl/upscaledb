/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"

static ham_offset_t
my_alloc_in_list(ham_db_t *db, freel_entry_t *list, ham_size_t elements, 
        ham_size_t junksize, ham_u32_t flags)
{
    ham_offset_t result;
    ham_size_t i, newsize, best=elements;

    /*
     * search the freelist for an unused entry; we prefer entries 
     * which are the same size as requested
     */
    for (i=0; i<elements; i++) {
        /* check alignment of the page */
        if (flags & HAM_NO_PAGE_ALIGN) {
            if (freel_get_address(&list[i])%db_get_pagesize(db))
                continue;
        }

        if (freel_get_size(&list[i])==junksize) {
            result=freel_get_address(&list[i]);
            freel_set_size(&list[i], 0);
            freel_set_address(&list[i], 0);
            return (result);
        }

        if (freel_get_size(&list[i])>junksize)
            best=i;
    }

    /* 
     * we haven't found a perfect match, but an entry which is big enough
     */
    if (best<elements) {
        ham_offset_t addr=freel_get_address(&list[best]);
        newsize=freel_get_size(&list[best])-junksize;
        freel_set_size(&list[best], newsize);
        freel_set_address(&list[best], addr+junksize);
        return (addr);
    }

    return (0);
}

static ham_bool_t
my_add_area(freel_entry_t *list, ham_size_t elements, 
        ham_offset_t address, ham_size_t size)
{
    ham_size_t i;

    for (i=0; i<elements; i++) {
        if (freel_get_address(&list[i])==0) {
            freel_set_address(&list[i], address);
            freel_set_size(&list[i], size);
            return (HAM_TRUE);
        }
    }

    return (HAM_FALSE);
}

ham_size_t
freel_get_max_elements(ham_db_t *db)
{
    /*
     * a freelist overflow page has one overflow pointer of type 
     * ham_size_t at the very beginning
     */
    return ((db_get_pagesize(db)-sizeof(ham_u16_t)-sizeof(ham_offset_t))
            /sizeof(freel_entry_t));
}

ham_offset_t
freel_alloc_area(ham_db_t *db, ham_txn_t *txn, ham_size_t size, ham_u32_t flags)
{
    ham_offset_t address, overflow, result=0;
    ham_page_t *page;
    ham_size_t max=freel_get_max_elements(db);
    freel_payload_t *fp;

    /*
     * get the first freelist page - if there's no freelist page, return.
     */
    address=db_get_freelist(db);
    if (!address) 
        return (0);
    page=db_fetch_page(db, txn, address, 0);
    if (!page)
        return (db_get_error(db));

    /* 
     * search the page for a freelist entry 
     */
    fp=page_get_freel_payload(page);
    result=my_alloc_in_list(db, freel_page_get_entries(fp), max, size, flags);
    if (result) {
        db_set_dirty(db, HAM_TRUE);
        return (result);
    }

    /* 
     * continue with overflow pages 
     */
    overflow=freel_page_get_overflow(fp);
    max=freel_get_max_elements(db);

    while (overflow) {
        /* allocate the overflow page */
        page=db_fetch_page(db, txn, overflow, 0);
        if (!page)
            return (db_get_error(db));

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(page);

        /* first member is the overflow pointer */
        overflow=freel_page_get_overflow(fp);

        /* search for an empty entry */
        result=my_alloc_in_list(db, freel_page_get_entries(fp), 
                max, size, flags);
        if (result) {
            page_set_dirty(page, HAM_TRUE);
            return (result);
        }
    }

    /* no success at all... */
    return (0);
}

ham_status_t 
freel_add_area(ham_db_t *db, ham_txn_t *txn, ham_offset_t address, 
        ham_size_t size)
{
    ham_page_t *page=0, *newp;
    ham_offset_t overflow;
    freel_payload_t *fp;
    ham_size_t max=freel_get_max_elements(db);

    /* 
     * first, we get the address of the freelist page; if there's no 
     * such page yet, we allocate a new one
     */ 
    address=db_get_freelist(db);
    if (!address) {
        page=db_alloc_page(db, txn, PAGE_IGNORE_FREELIST);
        if (!page)
            return (db_get_error(db));
        /* set the whole page to zero */
        memset(page->_pers->_s._payload, 0, db_get_pagesize(db));
        /* store the page address in the database header */
        db_set_freelist(db, page_get_self(page));
        db_set_dirty(db, HAM_TRUE);
    }
    else {
        page=db_fetch_page(db, txn, address, 0);
        if (!page)
            return (db_get_error(db));
    }

    /* try to add the entry to the new freelist page */
    fp=page_get_freel_payload(page);
    if (my_add_area(freel_page_get_entries(fp), max, address, size)) {
        page_set_dirty(page, HAM_TRUE);
        return (0);
    }

    /* 
     * if there freelist page is full: continue with the overflow page
     */
    max=freel_get_max_elements(db);
    overflow=freel_page_get_overflow(fp);

    while (overflow) {
        /* read the overflow page */
        page=db_fetch_page(db, txn, overflow, 0);
        if (!page) 
            return (db_get_error(db));

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(page);

        /* get the overflow pointer */
        overflow=freel_page_get_overflow(fp); 

        /* try to add the entry */
        if (my_add_area(freel_page_get_entries(fp), max, address, size)) {
            page_set_dirty(page, HAM_TRUE);
            return (0);
        }
    }

    /* 
     * all overflow pages are full - add a new one! page is still a valid
     * page pointer 
     * we allocate a page on disk for the page WITHOUT accessing
     * the freelist, because right now the freelist is totally full
     * and every access would result in problems. 
     */
    newp=db_alloc_page(db, txn, PAGE_IGNORE_FREELIST);

    /* set the whole page to zero */
    memset(newp->_pers->_s._payload, 0, db_get_pagesize(db));

    /* 
     * now update the overflow pointer of the previous freelist page,
     * and set the dirty-flag 
     */
    freel_page_set_overflow(page_get_freel_payload(page), 
            page_get_self(newp));
    page_set_dirty(page, 1);

    /* try to add the entry to the new freelist page */
    fp=page_get_freel_payload(newp);
    if (my_add_area(freel_page_get_entries(fp), max, address, size)) {
        page_set_dirty(newp, HAM_TRUE);
        return (0);
    }

    /* 
     * we're still here - this means we got a strange error. this 
     * shouldn't happen! 
     */
    ham_assert(!"shouldn't be here...", 0, 0);
    return (HAM_INTERNAL_ERROR);
}
