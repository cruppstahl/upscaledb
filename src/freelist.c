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
freel_get_max_header_elements(ham_db_t *db)
{
    unsigned long pfl=(unsigned long)&db->_u._pers._freelist;
    unsigned long pst=(unsigned long)db;
    return ((pst+SIZEOF_PERS_HEADER)-pfl)/sizeof(freel_entry_t);
}

ham_size_t
freel_get_max_overflow_elements(ham_db_t *db)
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
    ham_offset_t overflow, result=0;
    ham_size_t max=freel_get_max_header_elements(db);

    /* search the header page for a freelist entry */
    result=my_alloc_in_list(db, 
            freel_page_get_entries(&db->_u._pers._freelist), 
            max, size, flags);
    if (result) {
        db_set_dirty(db, HAM_TRUE);
        return (result);
    }

    /* continue with overflow pages */
    overflow=freel_page_get_overflow(&db->_u._pers._freelist); 
    max=freel_get_max_overflow_elements(db);

    while (overflow) {
        ham_page_t *p;
        freel_payload_t *fp;
        
        /* allocate the overflow page */
        p=txn_fetch_page(txn, overflow, 0);
        if (!p) /* TODO error!! */
            return (0);

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(p);

        /* first member is the overflow pointer */
        overflow=freel_page_get_overflow(fp);

        /* search for an empty entry */
        result=my_alloc_in_list(db, freel_page_get_entries(fp), 
                max, size, flags);
        if (result) {
            page_set_dirty(p, HAM_TRUE);
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
    ham_page_t *p=0, *newp;
    ham_offset_t overflow;
    freel_payload_t *fp;
    ham_size_t max=freel_get_max_header_elements(db);

    /* first we try to add the new area to the header page */
    if (my_add_area(freel_page_get_entries(&db->_u._pers._freelist), 
                max, address, size)) {
        db_set_dirty(db, HAM_TRUE);
        return (0);
    }

    /* 
     * continue with overflow pages
     */
    max=freel_get_max_overflow_elements(db);
    overflow=freel_page_get_overflow(&db->_u._pers._freelist);

    while (overflow) {
        /* read the overflow page */
        p=txn_fetch_page(txn, overflow, 0);
        if (!p) /* TODO fatal error!! */
            return (0);

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(p);

        /* get the overflow pointer */
        overflow=freel_page_get_overflow(fp); 

        /* try to add the entry */
        if (my_add_area(freel_page_get_entries(fp), max, address, size)) {
            page_set_dirty(p, HAM_TRUE);
            return (0);
        }
    }

    /* 
     * all overflow pages are full - add a new one! p is still a valid
     * page pointer 
     * we allocate a page on disk for the page WITHOUT accessing
     * the freelist, because right now the freelist is totally full
     * and every access would result in problems. 
     */
    newp=txn_alloc_page(txn, PAGE_IGNORE_FREELIST);

    /* set the whole page to zero */
    memset(newp->_pers._payload, 0, db_get_pagesize(db));

    /* 
     * now update the overflow pointer of the previous freelist page,
     * and set the dirty-flag 
     */
    if (p) {
        freel_page_set_overflow(page_get_freel_payload(p), 
                page_get_self(newp));
        page_set_dirty(p, 1);
    }
    else {
        db_set_dirty(db, HAM_TRUE);
        freel_page_set_overflow(&db->_u._pers._freelist, page_get_self(newp));
    }

    /* try to add the entry to the new freelist page */
    fp=page_get_freel_payload(newp);
    if (my_add_area(freel_page_get_entries(fp), max, address, size)) {
        page_set_dirty(newp, HAM_TRUE);
        return (0);
    }

    /* TODO we're still here - this means we got a strange error. this 
     * shouldn't happen! */
    return (-1); /* TODO */
}
