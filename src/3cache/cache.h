/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * The Cache Manager
 *
 * Stores pages in a non-intrusive hash table (each Page instance keeps
 * next/previous pointers for the overflow bucket). Can efficiently purge
 * unused pages, because all pages are also stored in a (non-intrusive)
 * linked list, and whenever a page is accessed it is removed and re-inserted
 * at the head. The tail therefore points to the page which was not used
 * in a long time, and is the primary candidate for purging.
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_CACHE_H
#define HAM_CACHE_H

#include "0root/root.h"

#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LocalEnvironment;
class PageManager;

/*
 * The Cache Manager
 */
class Cache
{
    enum {
      // The number of buckets should be a prime number or similar, as it
      // is used in a MODULO hash scheme
      kBucketSize = 10317
    };

  public:
    // The default constructor
    Cache(LocalEnvironment *env,
            uint64_t capacity_bytes = HAM_DEFAULT_CACHE_SIZE)
      : m_env(env), m_capacity(capacity_bytes), m_cur_elements(0),
        m_alloc_elements(0), m_totallist(Page::kListCache),
        m_buckets(kBucketSize, PageCollection(Page::kListBucket)),
        m_cache_hits(0), m_cache_misses(0) {
      ham_assert(m_capacity > 0);
    }

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get_page(uint64_t address, uint32_t flags = 0) {
      size_t hash = calc_hash(address);
      Page *page = m_buckets[hash].get(address);;

      /* not found? then return */
      if (!page) {
        m_cache_misses++;
        return (0);
      }

      // Now re-insert the page at the head of the "totallist", and
      // thus move far away from the tail. The pages at the tail are highest
      // candidates to be deleted when the cache is purged.
      m_totallist.remove(page);
      m_totallist.add(page);

      m_cache_hits++;

      return (page);
    }

    // Stores a page in the cache
    void put_page(Page *page) {
      size_t hash = calc_hash(page->get_address());

      ham_assert(page->get_data());

      /* First remove the page from the cache, if it's already cached
       *
       * Then re-insert the page at the head of the list. The tail will
       * point to the least recently used page.
       */
      m_totallist.remove(page);
      m_totallist.add(page);

      m_cur_elements++;
      if (page->is_allocated())
        m_alloc_elements++;

      /*
       * insert it in the cache buckets
       * !!!
       * to avoid inserting the page twice, we first remove it from the
       * bucket
       */
      m_buckets[hash].remove(page->get_address());
      m_buckets[hash].add(page);
    }

    // Removes a page from the cache
    void remove_page(Page *page) {
      ham_assert(page->get_address() != 0);

      /* remove the page from the cache buckets */
      size_t hash = calc_hash(page->get_address());
      m_buckets[hash].remove(page->get_address());

      /* remove it from the list of all cached pages */
      if (m_totallist.remove(page)) {
        m_cur_elements--;
        if (page->is_allocated())
          m_alloc_elements--;
      }
    }

    typedef void (*PurgeCallback)(Page *page, PageManager *pm);

    // Purges the cache; the callback is called for every page that needs
    // to be purged
    //
    // TODO
    // rewrite w/ extract()
    void purge(PurgeCallback cb, PageManager *pm, unsigned limit) {
      unsigned i = 0;

      /* get the chronologically oldest page */
      Page *oldest = m_totallist.tail();
      if (!oldest)
        return;

      /* now iterate through all pages, starting from the oldest
       * (which is the tail of the "totallist", the list of ALL cached
       * pages) */
      Page *page = oldest;
      do {
        /* pick the first page that can be purged */
        if (can_purge_page(page)) {
          Page *prev = page->get_previous(Page::kListCache);
          remove_page(page);
          cb(page, pm);
          i++;
          page = prev;
        }
        else
          page = page->get_previous(Page::kListCache);
        ham_assert(page != oldest);
      } while (i < limit && page && page != oldest);
    }

    // the visitor callback returns true if the page should be removed from
    // the cache and deleted
    typedef bool (*VisitCallback)(Page *page, LocalEnvironment *env,
            LocalDatabase *db, uint32_t flags);

    // Visits all pages in the "totallist"; this is used by the Environment
    // to flush (and delete) pages
    //
    // TODO use extract_if()
    void visit(VisitCallback cb, LocalEnvironment *env, LocalDatabase *db,
            uint32_t flags) {
      Page *head = m_totallist.head();
      while (head) {
        Page *next = head->get_next(Page::kListCache);

        if (cb(head, env, db, flags)) {
          remove_page(head);
          delete head;
        }
        head = next;
      }
    }

    // Returns the capacity (in bytes)
    size_t get_capacity() const {
      return (m_capacity);
    }

    // Returns the number of currently cached elements
    size_t get_current_elements() const {
      return (m_cur_elements);
    }

    // Returns the number of currently cached elements (excluding those that
    // are mmapped)
    size_t get_allocated_elements() const {
      return (m_alloc_elements);
    }

    // Fills in the current metrics
    void get_metrics(ham_env_metrics_t *metrics) const {
      metrics->cache_hits = m_cache_hits;
      metrics->cache_misses = m_cache_misses;
    }

  private:
    // Returns true if the page can be purged: page must use allocated
    // memory instead of an mmapped pointer; page must not be in use (= in
    // a changeset) and not have cursors attached
    bool can_purge_page(Page *page) {
      if (page->is_allocated() == false)
        return (false);
      if (m_env->get_changeset().contains(page))
        return (false);
      if (page->get_cursor_list() != 0)
        return (false);
      return (true);
    }

    // Calculates the hash of a page address
    size_t calc_hash(uint64_t o) const {
      return ((size_t)(o % kBucketSize));
    }

    // the current Environment
    LocalEnvironment *m_env;

    // the capacity (in bytes)
    uint64_t m_capacity;

    // the current number of cached elements
    size_t m_cur_elements;

    // the current number of cached elements that were allocated (and not
    // mapped)
    size_t m_alloc_elements;

    // linked list of ALL cached pages
    PageCollection m_totallist;

    // The hash table buckets - a linked list of Page pointers
    std::vector<PageCollection> m_buckets;

    // counts the cache hits
    uint64_t m_cache_hits;

    // counts the cache misses
    uint64_t m_cache_misses;
};

} // namespace hamsterdb

#endif /* HAM_CACHE_H */
