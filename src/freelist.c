/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"

#define OFFSET_OF(obj, field) ((char *)(&((obj).field))-(char *)&(obj))

static ham_bool_t
my_add_area(ham_db_t *db, freel_payload_t *fp, 
        ham_offset_t address, ham_size_t size)
{
    ham_size_t i;
    freel_entry_t *list=freel_payload_get_entries(fp); 

    ham_assert(freel_payload_get_count(fp)<=freel_payload_get_maxsize(fp), 
            ("invalid freelist object"));

    /*
     * try to append the item to an existing entry
     */
    if (db_get_rt_flags(db)&HAM_OPTIMIZE_SIZE) {
        for (i=0; i<freel_payload_get_count(fp); i++) {
            /*
             * if we can append the item, remove the item from the 
             * freelist and re-insert it
             */
            if (freel_get_address(&list[i])+freel_get_size(&list[i])==address) {
                size  +=freel_get_size(&list[i]);
                address=freel_get_address(&list[i]);
                if (i<(ham_size_t)freel_payload_get_count(fp)-1) 
                    memmove(&list[i], &list[i+1], 
                        (freel_payload_get_count(fp)-i-1)*sizeof(list[i]));
                freel_payload_set_count(fp, freel_payload_get_count(fp)-1);
                return (my_add_area(db, fp, address, size));
            }
            /*
             * same if we can prepend the item
             */
            if (address+size==freel_get_address(&list[i])) {
                size   +=freel_get_size(&list[i]);
                if (i<(ham_size_t)freel_payload_get_count(fp)-1) 
                    memmove(&list[i], &list[i+1], 
                        (freel_payload_get_count(fp)-i-1)*sizeof(list[i]));
                freel_payload_set_count(fp, freel_payload_get_count(fp)-1);
                return (my_add_area(db, fp, address, size));
            }
        }
    }

    /* 
     * did not found room for this chunk?
     */
    if (freel_payload_get_count(fp)==freel_payload_get_maxsize(fp))
        return (HAM_FALSE);

    /*
     * we were not able to append the item to an existing entry - insert
     * it in the list
     */
    for (i=0; i<freel_payload_get_count(fp); i++) {
        if (size>=freel_get_size(&list[i])) {
            memmove(&list[i+1], &list[i], 
                    (freel_payload_get_count(fp)-i)*sizeof(list[i]));
            freel_set_size(&list[i], size);
            freel_set_address(&list[i], address);
            freel_payload_set_count(fp, freel_payload_get_count(fp)+1);
            return (HAM_TRUE);
        }
    }

    /*
     * append at the end of the list
     */
    freel_set_size(&list[i], size);
    freel_set_address(&list[i], address);
    freel_payload_set_count(fp, freel_payload_get_count(fp)+1);

    return (HAM_TRUE);
}

static ham_offset_t
my_alloc_in_list(ham_db_t *db, freel_payload_t *fp, 
        ham_size_t chunksize, ham_u32_t flags)
{
    ham_offset_t newoffs=0;
    ham_size_t i, best=freel_payload_get_count(fp);
    freel_entry_t *list=freel_payload_get_entries(fp);

    /*
     * search the freelist for an unused entry; we prefer entries 
     * which are the same size as requested
     */
    for (i=0; i<freel_payload_get_count(fp); i++) {

        /*
         * blob is bigger then the requested blob?
         */
        if (freel_get_size(&list[i])>chunksize) {
            /*
             * need aligned page in an unaligned blob?
             */
            if (!(flags&FREEL_DONT_ALIGN) && chunksize==db_get_pagesize(db)) {
                newoffs=(freel_get_address(&list[i])/db_get_pagesize(db))*
                    db_get_pagesize(db)+db_get_pagesize(db);
                if (freel_get_size(&list[i])-
                        (newoffs-freel_get_address(&list[i]))>=chunksize)
                    best=i;
            }
            else
                best=i;
        }
        /*
         * blob is the same size as requested?
         */
        else if (freel_get_size(&list[i])==chunksize) {
            /*
             * need aligned chunk, and the chunk is aligned
             * OR need no aligned chunk: use this chunk
             */
            if (flags&FREEL_DONT_ALIGN) {
                best=i;
                break;
            }
            else if (freel_get_address(&list[i])%db_get_pagesize(db)==0) {
                best=i;
                break;
            }
        }
        /*
         * otherwise, if the current chunk is smaller then the requested 
         * chunk size, there's no need to continue, because all following
         * chunks are even smaller.
         */
        else {
            break;
        }
    }

    /*
     * nothing found - return to caller
     */
    if (best==freel_payload_get_count(fp)) 
        return (0);

    /*
     * otherwise remove the chunk
     */
    if (freel_get_size(&list[best])>chunksize) {

        if (flags&FREEL_DONT_ALIGN) {
            ham_size_t   diff=freel_get_size(&list[best])-chunksize;
            ham_offset_t offs=freel_get_address(&list[best]);

            if (best<(ham_size_t)freel_payload_get_count(fp)-1) 
                memmove(&list[best], &list[best+1], 
                        (freel_payload_get_count(fp)-best-1)*sizeof(list[i]));
            freel_payload_set_count(fp, freel_payload_get_count(fp)-1);

            (void)my_add_area(db, fp, offs+chunksize, diff);
            return (offs);
        }
        return (0);
#if 0
        TODO implement this...

        /*ham_assert(!"shouldn't be here...", 0, 0);*/
        else {
            ham_offset_t offs1, offs2, newoffs;
            ham_size_t   size1, size2;
            
            newoffs=(freel_get_address(&list[best])/db_get_pagesize(db))*
                        db_get_pagesize(db)+db_get_pagesize(db);
            offs1  =freel_get_address(&list[best]);
            size1  =newoffs-freel_get_address(&list[best]);
            offs2  =newoffs+chunksize;
            size2  =(freel_get_address(&list[best])+
                        freel_get_size(&list[best]))-(newoffs+chunksize);

            if (best<freel_payload_get_count(fp)-1) 
                memmove(&list[best], &list[best+1], 
                        (freel_payload_get_count(fp)-best-1)*sizeof(list[i]));
            freel_payload_set_count(fp, freel_payload_get_count(fp)-1);

            (void)my_add_area(db, fp, offs1, size1);
            (void)my_add_area(db, fp, offs2, size2);
            return (newoffs);
        }
#endif
    }
    else {
        ham_offset_t newoffs=freel_get_address(&list[best]);

        if (best<(ham_size_t)freel_payload_get_count(fp)-1) 
            memmove(&list[best], &list[best+1], 
                    (freel_payload_get_count(fp)-best-1)*sizeof(list[i]));
        freel_payload_set_count(fp, freel_payload_get_count(fp)-1);

        return (newoffs);
    }

    return (0);
}

static ham_size_t
my_get_max_elements(ham_db_t *db)
{
    static struct page_union_header_t h;
    /*
     * a freelist overflow page has one overflow pointer of type 
     * ham_size_t at the very beginning
     */
    return ((db_get_usable_pagesize(db)-(int)(OFFSET_OF(h, _payload))-
                sizeof(ham_u16_t)-sizeof(ham_offset_t))/sizeof(freel_entry_t));
}

static ham_page_t *
my_fetch_page(ham_db_t *db, ham_offset_t address)
{
    ham_page_t *page;
    ham_status_t st;

    /*
     * check if the page is in the list
     */
    page=db_get_freelist_cache(db);
    while (page) {
        if (page_get_self(page)==address)
            return (page);
        page=page_get_next(page, PAGE_LIST_TXN);
    }

    /*
     * allocate a new page structure
     */
    page=page_new(db);
    if (!page)
        return (0);

    /* 
     * fetch the page from the device 
     */
    page_set_self(page, address);
    st=page_fetch(page);
    if (st) {
        page_delete(page);
        return (0);
    }

    /*
     * insert the page in our local cache and return the page
     */
    db_set_freelist_cache(db, 
            page_list_insert(db_get_freelist_cache(db), PAGE_LIST_TXN, page));

    return (page);
}

static ham_page_t *
my_alloc_page(ham_db_t *db)
{
    ham_page_t *page;

    /*
     * allocate a new page, if the cache is not yet full enough - although
     * the freelist pages are not managed by the cache, we try to respect
     * the maximum cache size
     */
    if (!cache_can_add_page(db_get_cache(db))) {
        ham_trace(("cache is full! resize the cache"));
        db_set_error(db, HAM_CACHE_FULL);
        return (0);
    }

    page=db_alloc_page(db, PAGE_TYPE_FREELIST, 
                PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO);
    if (!page) 
        return (0);

    page_add_ref(page);

    /*
     * insert the page in our local cache and return the page
     */
    db_set_freelist_cache(db, 
            page_list_insert(db_get_freelist_cache(db), PAGE_LIST_TXN, page));

    return (page);
}

ham_status_t
freel_create(ham_db_t *db)
{
    (void)db;
    return (0);
}

ham_offset_t
freel_alloc_area(ham_db_t *db, ham_size_t size, ham_u32_t flags)
{
    ham_offset_t overflow, result=0;
    ham_page_t *page;
    ham_status_t st;
    freel_payload_t *fp;
    db_header_t *hdr;

    /* 
     * get the database header page, and its freelist payload 
     */ 
    page=db_get_header_page(db);
    hdr=(db_header_t *)page_get_payload(page);
    fp=&hdr->_freelist;

    /* 
     * search the page for a freelist entry 
     */
    result=my_alloc_in_list(db, fp, size, flags);
    if (result) {
        page_set_dirty(page, HAM_TRUE);
        if (!(db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH)) {
            st=page_flush(page);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
        return (result);
    }

    /* 
     * continue with overflow pages 
     */
    overflow=freel_payload_get_overflow(fp);

    while (overflow) {
        /* allocate the overflow page */
        page=my_fetch_page(db, overflow);
        if (!page)
            return (db_get_error(db));

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(page);

        /* first member is the overflow pointer */
        overflow=freel_payload_get_overflow(fp);

        /* search for an empty entry */
        result=my_alloc_in_list(db, fp, size, flags);
        if (result) {
            /* write the page to disk */
            page_set_dirty(page, HAM_TRUE);
            if (!(db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH)) {
                st=page_flush(page);
                if (st) {
                    db_set_error(db, st);
                    return (0);
                }
            }
            return (result);
        }
    }

    /* no success at all... */
    return (0);
}

ham_status_t 
freel_add_area(ham_db_t *db, ham_offset_t address, ham_size_t size)
{
    ham_page_t *page=0, *newp;
    ham_offset_t overflow;
    freel_payload_t *fp;
    db_header_t *hdr;

    /* 
     * get the database header page, and its freelist payload 
     */ 
    page=db_get_header_page(db);
    hdr=(db_header_t *)page_get_payload(page);

    /* try to add the entry to the freelist */
    fp=&hdr->_freelist;
    if (my_add_area(db, fp, address, size)) {
        page_set_dirty(page, HAM_TRUE);
        return (0);
    }

    /* 
     * if there freelist page is full: continue with the overflow page
     */
    overflow=freel_payload_get_overflow(fp);

    while (overflow) {
        /* read the overflow page */
        page=my_fetch_page(db, overflow);
        if (!page) 
            return (db_get_error(db));

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(page);

        /* get the overflow pointer */
        overflow=freel_payload_get_overflow(fp); 

        /* try to add the entry */
        if (my_add_area(db, fp, address, size)) {
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
    newp=my_alloc_page(db);
    if (!newp) 
        return (0);

    /* set the whole page to zero */
    memset(newp->_pers->_s._payload, 0, db_get_usable_pagesize(db));

    /* 
     * now update the overflow pointer of the previous freelist page,
     * and set the dirty-flag 
     *
     * TODO flush the modified page?? 
     *
     */
    if (page==db_get_header_page(db)) {
        fp=&hdr->_freelist;
        freel_payload_set_overflow(fp, page_get_self(newp));
        db_set_dirty(db, 1);
        page_set_dirty(page, 1);
    }
    else {
        freel_payload_set_overflow(page_get_freel_payload(page), 
                page_get_self(newp));
        page_set_dirty(page, 1);
    }

    fp=page_get_freel_payload(newp);
    freel_payload_set_maxsize(fp, my_get_max_elements(db));

    /* 
     * try to add the entry to the new freelist page 
     */
    if (my_add_area(db, fp, address, size)) {
        page_set_dirty(newp, HAM_TRUE);
        return (0);
    }

    /* 
     * we're still here - this means we got a strange error. this 
     * shouldn't happen! 
     */
    ham_assert(!"shouldn't be here...", (""));
    return (HAM_INTERNAL_ERROR);
}

ham_status_t
freel_shutdown(ham_db_t *db)
{
    ham_page_t *page, *next;

    /*
     * write all pages to the device
     */
    page=db_get_freelist_cache(db);
    while (page) {
        next=page_get_next(page, PAGE_LIST_TXN);
        page_release_ref(page);
        page_flush(page);
        /*(void)db_write_page_and_delete(page, 0); TODO löscht die 
         *  page; die page ist aber noch in der totallist -> beim
         *  cache_flush_and_delete wird auf die page wieder zugegriffen */
        page=next;
    }

    db_set_freelist_cache(db, 0);

    return (0);
}
