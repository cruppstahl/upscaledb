/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief the cache manager
 *
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#include <vector>

#include "internal_fwd_decl.h"
#include "env.h"


/** CACHE_BUCKET_SIZE should be a prime number or similar, as it is used in 
 * a MODULO hash scheme */
#define CACHE_BUCKET_SIZE    10317


/**
 * the cache manager
 */
class Cache
{
  public:
    /** don't remove the page from the cache */
    static const int NOREMOVE=1;

    /** the default constructor
     * @remark max_size is in bytes!
     */
    Cache(Environment *env, 
                ham_u64_t capacity_bytes=HAM_DEFAULT_CACHESIZE);

    /**
     * the destructor closes and destroys the cache manager object
     * @remark this will NOT flush the cache!
     */
    ~Cache() {
    }
    
    /**
     * get an unused page (or an unreferenced page, if no unused page
     * was available
     *
     * @remark if the page is dirty, it's the caller's responsibility to 
     * write it to disk!
     *
     * @remark the page is removed from the cache
     */
    Page *get_unused_page(void) {
        Page *page;
        Page *oldest;

        /* get the chronologically oldest page */
        oldest=m_totallist_tail;
        if (!oldest)
            return (0);

        /* now iterate through all pages, starting from the oldest
         * (which is the tail of the "totallist", the list of ALL cached 
         * pages) */
        page=oldest;
        do {
            /* pick the first unused page (not in a changeset) */
            if (!page_is_in_list(m_env->get_changeset().get_head(), 
                        page, Page::LIST_CHANGESET))
                break;
        
            page=page->get_previous(Page::LIST_CACHED);
            ham_assert(page!=oldest, (0));
        } while (page && page!=oldest);
    
        if (!page)
            return (0);

        /* remove the page from the cache and return it */
        remove_page(page);

        return (page);
    }

    /**
     * get a page from the cache
     *
     * @remark the page is removed from the cache
     *
     * @return 0 if the page was not cached
     */
    Page *get_page(ham_offset_t address, ham_u32_t flags=0) {
        Page *page;
        ham_u64_t hash=calc_hash(address);

        page=m_buckets[hash];
        while (page) {
            if (page->get_self()==address)
                break;
            page=page->get_next(Page::LIST_BUCKET);
        }

        /* not found? then return */
        if (!page)
            return (0);

        /* otherwise remove the page from the cache */
        remove_page(page);

        /* if the flag NOREMOVE is set, then re-insert the page. 
         *
         * The remove/insert trick causes the page to be inserted at the
         * head of the "totallist", and therefore it will automatically move
         * far away from the tail. And the pages at the tail are highest 
         * candidates to be deleted when the cache is purged. */
        if (flags&Cache::NOREMOVE)
            put_page(page);

        return (page);
    }

    /**
     * store a page in the cache
     */
    void put_page(Page *page) {
        ham_u64_t hash=calc_hash(page->get_self());

        ham_assert(page->get_pers(), (""));

        /* first remove the page from the cache, if it's already cached
         *
         * we re-insert the page because we want to make sure that the 
         * cache->_totallist_tail pointer is updated and that the page
         * is inserted at the HEAD of the list
         */
        if (page_is_in_list(m_totallist, page, Page::LIST_CACHED))
            remove_page(page);

        /* now (re-)insert into the list of all cached pages, and increment
         * the counter */
        ham_assert(!page_is_in_list(m_totallist, page, Page::LIST_CACHED), (0));
        m_totallist=page_list_insert(m_totallist, Page::LIST_CACHED, page);

        m_cur_elements++;

        /*
         * insert it in the cache buckets
         * !!!
         * to avoid inserting the page twice, we first remove it from the 
         * bucket
         */
        if (page_is_in_list(m_buckets[hash], page, Page::LIST_BUCKET))
            m_buckets[hash]=page_list_remove(m_buckets[hash], 
                            Page::LIST_BUCKET, page);
        ham_assert(!page_is_in_list(m_buckets[hash], page, 
                    Page::LIST_BUCKET), (0));
        m_buckets[hash]=page_list_insert(m_buckets[hash], 
                    Page::LIST_BUCKET, page);

        /* is this the chronologically oldest page? then set the pointer */
        if (!m_totallist_tail)
            m_totallist_tail=page;

        ham_assert(check_integrity()==0, (""));
    }
    
    /**
     * remove a page from the cache
     */
    void remove_page(Page *page) {
        ham_bool_t removed = HAM_FALSE;

        /* are we removing the chronologically oldest page? then 
         * update the pointer with the next oldest page */
        if (m_totallist_tail==page)
            m_totallist_tail=page->get_previous(Page::LIST_CACHED);

        /* remove the page from the cache buckets */
        if (page->get_self()) {
            ham_u64_t hash=calc_hash(page->get_self());
            if (page_is_in_list(m_buckets[hash], page, 
                    Page::LIST_BUCKET)) {
                m_buckets[hash]=page_list_remove(m_buckets[hash], 
                        Page::LIST_BUCKET, page);
            }
        }

        /* remove it from the list of all cached pages */
        if (page_is_in_list(m_totallist, page, Page::LIST_CACHED)) {
            m_totallist=page_list_remove(m_totallist, Page::LIST_CACHED, page);
            removed = HAM_TRUE;
        }

        /* decrease the number of cached elements */
        if (removed)
            m_cur_elements--;

        ham_assert(check_integrity()==0, (""));
    }

    /**
     * returns true if the caller should purge the cache
     */
    bool is_too_big(void) {
        return (m_cur_elements*m_env->get_pagesize()>m_capacity);
    }

    /**
     * get number of currently stored pages
     */
    ham_u64_t get_cur_elements(void) {
        return (m_cur_elements);
    }

    /**
     * set the HEAD of the global page list
     */
    void set_totallist(Page *l) { 
        m_totallist=l; 
    }

    /**
     * retrieve the HEAD of the global page list
     */
    Page *get_totallist(void) { 
        return (m_totallist); 
    }

    /**
     * decrease number of current elements
     * TODO this should be private, but db.c needs it
     */
    void dec_cur_elements() { 
        m_cur_elements--; 
    }

    /**
     * get the capacity (in bytes)
     */
    ham_u64_t get_capacity(void) { 
        return (m_capacity); 
    }

    /**
     * check the cache integrity
     */
    ham_status_t check_integrity(void);

  private:
    ham_u64_t calc_hash(ham_offset_t o) {
        return (o%CACHE_BUCKET_SIZE);
    }

    /** the current Environment */
    Environment *m_env;

    /** the capacity (in bytes) */
    ham_u64_t m_capacity;

    /** the current number of cached elements */
    ham_u64_t m_cur_elements;

    /** linked list of ALL cached pages */
    Page *m_totallist;

    /** the tail of the linked "totallist" - this is the oldest element,
     * and therefore the highest candidate for a flush */
    Page *m_totallist_tail;

    /** the buckets - a linked list of Page pointers */
    std::vector<Page *> m_buckets;
};

#endif /* HAM_CACHE_H__ */
