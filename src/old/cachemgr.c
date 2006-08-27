/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * implementation of the cache manager
 *
 */

#include <string.h>
#include "cachemgr.h"
#include "mem.h"
#include "db.h"
#include "page.h"
#include "error.h"

#define my_calc_hash(cm, o)                                            \
    (cm_get_cachesize(cm)==0                                           \
        ? 0                                                            \
        : (((o-SIZEOF_PERS_HEADER)/db_get_pagesize(cm_get_owner(cm))) \
            %cm_get_bucketsize(cm)))


/*
 * allocate a new memory page
 * TODO wo wird die cachesize gecheckt??
 */
static ham_page_t *
my_page_new(ham_cachemgr_t *cm)
{
    ham_page_t *p;

    /*
     * first check the garbage-list
     */
    if (cm_get_garbagelist(cm)) {
        p=cm_get_garbagelist(cm);
        cm_set_garbagelist(cm, page_list_remove(cm_get_garbagelist(cm), 
            PAGE_LIST_GARBAGE, p));
        return (p);
    }

    /*
     * otherwise allocate the memory
     */
    p=page_new(cm_get_owner(cm));
    if (p)
        cm_set_usedsize(cm, 
            cm_get_usedsize(cm)+db_get_pagesize(cm_get_owner(cm)));
    return (p);
}

/*
 * get a shadow-page of a page
 */
static ham_page_t *
my_get_shadowpage(ham_cachemgr_t *cm, ham_page_t *page)
{
    ham_size_t i;
    ham_page_t *sp=0;

    ham_assert(page_get_shadowpage(page)==0, 
            "invalid shadow-page pointer of page 0x%lx", page_get_self(page));
    ham_assert(page_get_orig_page(page)==0, 
            "invalid original-page pointer of page 0x%lx", page_get_self(page));

    /*
     * allocate memory for a new page
     */
    sp=my_page_new(cm);
    if (!sp) {
        db_set_error(page_get_owner(page), HAM_OUT_OF_MEMORY);
        return (0);
    }

    /*
     * copy the page contents to the shadow page and set up the links
     * between the pages
     */
    memcpy(sp, page, sizeof(page->_npers)+db_get_pagesize(cm_get_owner(cm)));
    page_set_shadowpage(page, sp);
    page_set_orig_page(sp, page);

    /*
     * remove the page from all linked lists
     */
    for (i=0; i<MAX_PAGE_LISTS; i++) {
        sp->_npers._prev[i]=0;
        sp->_npers._next[i]=0;
    }

    /*
     * a shadow-page is not dirty
     */
    page_set_dirty(sp, 0);

    return (sp);
}

/*
 * remove a page from the cache
 */
static ham_status_t
my_remove_page(ham_cachemgr_t *cm, ham_page_t *p)
{
    ham_size_t hash=my_calc_hash(cm, page_get_self(p));

    cm_get_bucket(cm, hash)=page_list_remove(cm_get_bucket(cm, hash), 
            PAGE_LIST_BUCKET, p);

    return (0);
}

/*
 * flush and delete a page
 */
static ham_status_t
my_flush_and_delete(ham_cachemgr_t *cm, ham_page_t *p)
{
    ham_status_t st=0;

    ham_assert(page_ref_get(p)==0,
            "page 0x%llx has reference count of %d, flushing\n", 
            page_get_self(p), page_ref_get(p));

    if (page_is_dirty(p))
        st=page_io_write(p);
    if (st)
        db_set_error(page_get_owner(p), st);
    page_delete(p);
    cm_set_usedsize(cm, cm_get_usedsize(cm)-db_get_pagesize(cm_get_owner(cm)));
    return (st);
}

/*
 * delete pages from the list of unreferenced pages, till either the
 * list is empty or the memory restrictions are ok
 */
static ham_status_t 
my_flush_unreferenced(ham_cachemgr_t *cm)
{
    ham_page_t *p;
    ham_status_t st;

    /*
     * do we have garbage pages?
     */
    while (cm_get_usedsize(cm)>cm_get_cachesize(cm)) {
        /*
         * get the head of the list
         */
        p=cm_get_garbagelist(cm);
        if (!p)
            break;

        /*
         * discard the next page
         */
        cm_set_garbagelist(cm, 
                page_list_remove(cm_get_garbagelist(cm), PAGE_LIST_GARBAGE, p));
        st=my_flush_and_delete(cm, p);
        if (st)
            return (st);
    }

    /*
     * check if we need to free pages
     */
    while (cm_get_usedsize(cm)>cm_get_cachesize(cm)) {
        /*
         * get the head of the list
         */
        p=cm_get_unreflist(cm);
        if (!p)
            break;
    
        /*
         * the list of unreferenced pages is a ring; move to the last element
         */
        p=page_get_previous(p, PAGE_LIST_UNREF);
        if (!p)
            break;
    
        /*
         * remove p from the list
         */
        cm_set_unreflist(cm, 
                page_list_remove(cm_get_unreflist(cm), PAGE_LIST_UNREF, p));
    
        /* 
         * remove the page from the cache
         */
        st=my_remove_page(cm, p);
        if (st)
            return (st);
    
        /*
         * flush (if necessary), and delete the memory
         */
        st=my_flush_and_delete(cm, p);
        if (st)
            return (st);
    }

    return (0);
}

/*
 * insert a page in the cache
 */
static ham_status_t
my_insert_page(ham_cachemgr_t *cm, ham_page_t *p)
{
    ham_size_t hash=my_calc_hash(cm, page_get_self(p));

    cm_get_bucket(cm, hash)=page_list_insert(cm_get_bucket(cm, hash), 
            PAGE_LIST_BUCKET, p);

    return (0);
}

/*
 * search a page in the hash
 */
static ham_page_t *
my_find_page(ham_cachemgr_t *cm, ham_offset_t offset)
{
    ham_size_t hash=my_calc_hash(cm, offset);
    ham_page_t *head;

    head=cm_get_bucket(cm, hash);
    while (head) {
        if (page_get_self(head)==offset)
            return (head);
        head=page_get_next(head, PAGE_LIST_BUCKET);
    }
    return (0);
}

ham_cachemgr_t *
cm_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize)
{
    ham_cachemgr_t *cm;
    ham_size_t mem, buckets;

    buckets=(cachesize/db_get_pagesize(db))/4;
    if (!buckets)
        buckets=1;
    mem=sizeof(ham_cachemgr_t)+(buckets-1)*sizeof(void *);

    cm=ham_mem_alloc(mem);
    if (!cm) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }
    memset(cm, 0, mem);
    cm_set_owner(cm, db);
    cm_set_flags(cm, flags);
    cm_set_cachesize(cm, cachesize);
    cm_set_bucketsize(cm, buckets);
    return (cm);
}

void
cm_delete(ham_cachemgr_t *cm)
{
    ham_mem_free(cm);
}

ham_page_t *
cm_fetch(ham_cachemgr_t *cm, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *p;
    ham_status_t st;

    /*
     * first try to fetch the page from the cache
     */
    p=my_find_page(cm, address);

    /*
     * found it?
     */
    if (p) {
        /*
         * get rid of extended keys - TODO warum?
         */
        page_delete_ext_keys(p);

        /*
         * if it's not referenced so far, remove it from the list
         * of unreferenced pages.
         */
        if (page_ref_get(p)==0) 
            cm_set_unreflist(cm, page_list_remove(cm_get_unreflist(cm), 
                    PAGE_LIST_UNREF, p));

        /*
         * if the page is dirty, and we want to write in that
         * page: try to return a shadowpage
         */
        if (page_is_dirty(p) && !(flags & CM_READ_ONLY)) {
            ham_page_t *sp=0;

            /*
             * get a shadowpage of p; if this page already has a 
             * shadow-page, we take the shadow-page (we have to rewrite
             * this as soon as we have concurrency)
             */
            if (page_get_shadowpage(p)) 
                sp=page_get_shadowpage(p);
            else 
                sp=my_get_shadowpage(cm, p);

            /*
             * if we can't get a shadow page (i.e. because we 
             * already consumed too much memory) we write the page to 
             * disk, reset the dirty-bit and return the current page
             */
            if (!sp) {
                st=page_io_write(p);
                if (st) /* TODO log */
                    return (0);
                page_set_dirty(p, 0);
                /* fall through */
            }
            /*
             * otherwise return the shadowpage
             */
            else {
                p=sp;
            }
        }

        ham_assert(page_get_shadowpage(p)==0, 
                "page 0x%lx has invalid shadowpage", page_get_self(p));

        /*
         * increase the reference counter and finally return the page
         */
        page_ref_inc(p, 0);
        return (p);
    }

    /*
     * page is not in the cache: fetch it from disk
     */
    p=my_page_new(cm);
    if (!p) {
        db_set_error(cm_get_owner(cm), HAM_OUT_OF_MEMORY);
        return (0);
    }
    page_set_self(p, address);
    st=page_io_read(p, address);
    db_set_error(cm_get_owner(cm), st);
    if (st) { /* TODO log */
        page_delete(p);
        return (0);
    }

    /*
     * increase the used size 
     */
    cm_set_usedsize(cm, 
            cm_get_usedsize(cm)+db_get_pagesize(cm_get_owner(cm)));

    /*
     * increase the reference counter, store the page in the cache 
     * and return it
     */
    page_ref_inc(p, 0);
    my_insert_page(cm, p);

    /*
     * try to not eat too much memory
     */
    (void)my_flush_unreferenced(cm);

    return (p);
}

ham_status_t 
cm_flush(ham_cachemgr_t *cm, ham_page_t *page, ham_u32_t flags)
{
    /*
     * decrease reference counter
     */
    page_ref_dec(page, 0);

    /*
     * is this page a shadowpage?
     */
    if (page_get_orig_page(page)) {
        ham_page_t *orig=page_get_orig_page(page);

        ham_assert(page_get_shadowpage(orig)==page, 
                "invalid shadowpage links of page 0x%lx and 0x%lx",
                page_get_self(orig), page_get_self(page));

        /*
         * sever the links between original page and shadow page
         */
        page_set_orig_page(page, 0);
        page_set_shadowpage(orig, 0);

        /*
         * if the page is dirty and valid (i.e. no HAM_CM_REVERT_CHANGES 
         * is set), this shadowpage replaces the original page.
         */
        if (page_is_dirty(page) && !(flags & HAM_CM_REVERT_CHANGES)) {
            /*
             * remove the original page from the cache and insert it
             * into the garbagelist
             */
            page_set_dirty(orig, 0);
            (void)my_remove_page(cm, orig);
            cm_get_garbagelist(cm)=page_list_insert(cm_get_garbagelist(cm), 
                PAGE_LIST_GARBAGE, orig);

            /*
             * store the shadow page in the cache
             */
            (void)my_insert_page(cm, page);
        }
        /*
         * otherwise (not dirty or HAM_CM_REVERT_CHANGES is set) we
         * add the shadow page to the garbage list
         */
        else {
            /*
             * move the shadow page to the garbage list
             */
            page_set_dirty(page, 0);
            (void)my_remove_page(cm, page);
            cm_get_garbagelist(cm)=page_list_insert(cm_get_garbagelist(cm), 
                PAGE_LIST_GARBAGE, page);

            /*
             * continue working with the original page
             */
            page=orig;
        }
    }

    /*
     * if the page is no longer referenced: insert it to the unreflist
     */
    if (page_ref_get(page)==0) 
        cm_set_unreflist(cm, page_list_insert(cm_get_unreflist(cm), 
                PAGE_LIST_UNREF, page));

    (void)my_flush_unreferenced(cm);
    return (0);
}

ham_page_t *
cm_alloc_page(ham_cachemgr_t *cm, ham_txn_t *txn, ham_u32_t flags)
{
    ham_page_t *p;
    ham_status_t st;
    ham_db_t *db=cm_get_owner(cm);

    /*
     * get memory for a page
     */
    p=my_page_new(cm);
    if (!p)
        return (0);
    
    /*
     * the freelist is checked in page_io_alloc()
     */
    st=page_io_alloc(p, txn, flags);
    if (st) {
        db_set_error(cm_get_owner(cm), st);
        page_delete(p);
        return (0);
    }

    /*
     * insert the page in the cache
     */
    st=my_insert_page(cm, p);
    if (st) {
        db_set_error(db, st);
        page_delete(p);
        return (0);
    }

    /*
     * increase the reference counter and return the page
     */
    page_ref_inc(p, 0);
    cm_set_usedsize(cm, cm_get_usedsize(cm)+db_get_pagesize(db));

    return (p);
}

ham_status_t 
cm_flush_all(ham_cachemgr_t *cm, ham_u32_t flags)
{
    ham_size_t i;
    ham_page_t *head;

    /* 
     * for each bucket in the hash table
     */
    for (i=0; i<cm_get_bucketsize(cm); i++) {
        /*
         * get the head of the bucket
         */
        head=cm_get_bucket(cm, i);

        /*
         * for each page in the bucket
         */
        while (head) {
            ham_page_t *n=page_get_next(head, PAGE_LIST_BUCKET);

            /*
             * flush the page
             */
            (void)my_flush_and_delete(cm, head);

            head=n;
        }

        cm_get_bucket(cm, i)=0;
    }

    /*
     * clear the list of unreferenced pages
     */
    cm_set_unreflist(cm, 0);

    /*
     * delete the pages in the garbage-list
     */
    head=cm_get_garbagelist(cm);
    while (head) {
        ham_page_t *n=page_get_next(head, PAGE_LIST_GARBAGE);
        page_delete(head);
        head=n;
    }

    return (0);
}

ham_status_t
cm_move_to_garbage(ham_cachemgr_t *cm, ham_page_t *page)
{
    ham_status_t st;

    ham_assert(page_ref_get(page)==1, "refcount of page 0x%lx is %d\n", 
            page_get_self(page), page_ref_get(page));

    /*
     * decrease reference counter
     */
    page_ref_dec(page, 0);

    st=my_remove_page(cm, page);
    if (st)
        return (st);
    cm_get_garbagelist(cm)=page_list_insert(cm_get_garbagelist(cm), 
            PAGE_LIST_GARBAGE, page);
    return (0);
}
