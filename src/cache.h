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
    static const int NOREMOVE = 1;

    /** the default constructor
     * @remark max_size is in bytes!
     */
    Cache(Environment *env, ham_u64_t capacity_bytes=HAM_DEFAULT_CACHESIZE);

    /**
     * get a page from the cache
     *
     * @remark the page is removed from the cache
     *
     * @return 0 if the page was not cached
     */
    Page *get_page(ham_offset_t address, ham_u32_t flags=0) {
      ScopedLock lock(m_mutex);

      ham_u64_t hash = calc_hash(address);
      Page *page = m_buckets[hash];
      while (page) {
        if (page->get_self() == address)
          break;
        page = page->get_next(Page::LIST_BUCKET);
      }

      /* not found? then return */
      if (!page)
        return (0);

      /* otherwise remove the page from the cache */
      remove_page_nolock(page);

      /* if the flag NOREMOVE is set, then re-insert the page. 
       *
       * The remove/insert trick causes the page to be inserted at the
       * head of the "totallist", and therefore it will automatically move
       * far away from the tail. And the pages at the tail are highest 
       * candidates to be deleted when the cache is purged. */
      if (flags&Cache::NOREMOVE)
          put_page_nolock(page);
      return (page);
    }

    /** store a page in the cache */
    void put_page(Page *page) {
      ScopedLock lock(m_mutex);
      put_page_nolock(page);
    }

    /** remove a page from the cache */
    void remove_page(Page *page) {
      ScopedLock lock(m_mutex);
      remove_page_nolock(page);
    }

    typedef ham_status_t (*PurgeCallback)(Page *page);

    /** purges the cache; the callback is called for every page that needs
     * to be purged */
    ham_status_t purge(PurgeCallback cb, bool strict) {
      ham_status_t st;
      ScopedLock lock(m_mutex);
      do {
        st = purge_max20(cb, strict);
        if (st && st != HAM_LIMITS_REACHED)
          return (st);
      } while (st == HAM_LIMITS_REACHED && is_too_big_nolock());

      return (0);
    }

    /** the visitor callback returns true if the page should be removed from
     * the cache and deleted */
    typedef bool (*VisitCallback)(Page *page, Database *db, ham_u32_t flags);

    /** visits all pages in the "totallist"; this is used by the Environment
     * to flush (and delete) pages */
    ham_status_t visit(VisitCallback cb, Database *db, ham_u32_t flags) {
      ScopedLock lock(m_mutex);
      Page *head = m_totallist;
      while (head) {
        Page *next = head->get_next(Page::LIST_CACHED);

        if (cb(head, db, flags)) {
          remove_page_nolock(head);
          delete head;
        }
        head = next;
      }
      return (0);
    }

    /** returns true if the caller should purge the cache */
    bool is_too_big() {
      ScopedLock lock(m_mutex);
      return (is_too_big_nolock());
    }

    /** get number of currently stored pages */
    ham_u64_t get_cur_elements() {
      ScopedLock lock(m_mutex);
      return (m_cur_elements);
    }

    /** get the capacity (in bytes) */
    ham_u64_t get_capacity() { 
      ScopedLock lock(m_mutex);
      return (m_capacity); 
    }

    /** check the cache integrity */
    ham_status_t check_integrity() {
      ScopedLock lock(m_mutex);
      return (check_integrity_nolock()); 
    }

  private:
    /**
     * get an unused page (or an unreferenced page, if no unused page
     * was available (w/o mutex)
     */
    Page *get_unused_page_nolock() {
      /* get the chronologically oldest page */
      Page *oldest = m_totallist_tail;
      if (!oldest)
          return (0);

      /* now iterate through all pages, starting from the oldest
       * (which is the tail of the "totallist", the list of ALL cached 
       * pages) */
      Page *page = oldest;
      do {
        /* pick the first unused page (not in a changeset) */
        if (!m_env->get_changeset().contains(page))
            break;
        
        page = page->get_previous(Page::LIST_CACHED);
        ham_assert(page != oldest);
      } while (page && page != oldest);
    
      if (!page)
        return (0);

      /* remove the page from the cache and return it */
      remove_page_nolock(page);
      return (page);
    }

    /** returns true if the caller should purge the cache (w/o mutex) */
    bool is_too_big_nolock() {
      return (m_cur_elements * m_env->get_pagesize() > m_capacity);
    }

    /** check the cache integrity (w/o mutex) */
    ham_status_t check_integrity_nolock();

    /** remove a page from the cache (w/o mutex) */
    void remove_page_nolock(Page *page) {
      bool removed = false;

      /* are we removing the chronologically oldest page? then 
       * update the pointer with the next oldest page */
      if (m_totallist_tail == page)
        m_totallist_tail = page->get_previous(Page::LIST_CACHED);

      /* remove the page from the cache buckets */
      if (page->get_self()) {
        ham_u64_t hash = calc_hash(page->get_self());
        if (page->is_in_list(m_buckets[hash], Page::LIST_BUCKET)) {
          m_buckets[hash] = page->list_remove(m_buckets[hash], 
                        Page::LIST_BUCKET);
        }
      }

      /* remove it from the list of all cached pages */
      if (page->is_in_list(m_totallist, Page::LIST_CACHED)) {
        m_totallist = page->list_remove(m_totallist, Page::LIST_CACHED);
        removed = true;
      }
      /* decrease the number of cached elements */
      if (removed)
        m_cur_elements--;

      ham_assert(check_integrity_nolock() == 0);
    }

    /** store a page in the cache (w/o mutex lock) */
    void put_page_nolock(Page *page) {
      ham_u64_t hash = calc_hash(page->get_self());

      ham_assert(page->get_pers());

      /* first remove the page from the cache, if it's already cached
       *
       * we re-insert the page because we want to make sure that the 
       * cache->_totallist_tail pointer is updated and that the page
       * is inserted at the HEAD of the list
       */
      if (page->is_in_list(m_totallist, Page::LIST_CACHED))
        remove_page_nolock(page);

      /* now (re-)insert into the list of all cached pages, and increment
       * the counter */
      ham_assert(!page->is_in_list(m_totallist, Page::LIST_CACHED));
      m_totallist = page->list_insert(m_totallist, Page::LIST_CACHED);

      m_cur_elements++;

      /*
       * insert it in the cache buckets
       * !!!
       * to avoid inserting the page twice, we first remove it from the 
       * bucket
       */
      if (page->is_in_list(m_buckets[hash], Page::LIST_BUCKET))
        m_buckets[hash] = page->list_remove(m_buckets[hash], Page::LIST_BUCKET);
      ham_assert(!page->is_in_list(m_buckets[hash], Page::LIST_BUCKET));
      m_buckets[hash] = page->list_insert(m_buckets[hash], Page::LIST_BUCKET);

      /* is this the chronologically oldest page? then set the pointer */
      if (!m_totallist_tail)
        m_totallist_tail = page;

      ham_assert(check_integrity_nolock() == 0);
    }
    
    /** purges max. 20 pages (and not more to avoid I/O spikes) */
    ham_status_t purge_max20(PurgeCallback cb, bool strict) {
      ham_status_t st;
      Page *page;
      unsigned i, max_pages = (unsigned)m_cur_elements;

      if (!is_too_big_nolock())
        return (0);

      /* 
       * max_pages specifies how many pages we try to flush in case the
       * cache is full. some benchmarks showed that 10% is a good value. 
       *
       * if STRICT cache limits are enabled then purge as much as we can
       */
      if (!strict) {
        max_pages /= 10;
        if (max_pages == 0)
          max_pages = 1;
        /* but still we set an upper limit to avoid IO spikes */
        else if (max_pages > 20)
          max_pages = 20;
      }

      /* now free those pages */
      for (i = 0; i < max_pages; i++) {
        page = get_unused_page_nolock();
        if (!page) {
          if (i == 0 && strict)
            return (HAM_CACHE_FULL);
          else
            break;
        }

        st = cb(page);
        if (st)
          return (st);
      }

      if (i == max_pages && max_pages != 0)
        return (HAM_LIMITS_REACHED);
      return (0);
    }

    /** calculate the hash of a page address */
    ham_u64_t calc_hash(ham_offset_t o) {
      return (o % CACHE_BUCKET_SIZE);
    }

    /** set the HEAD of the global page list */
    void set_totallist(Page *l) { 
      m_totallist = l; 
    }

    /** a mutex for this Environment */
    Mutex m_mutex;

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
