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
 * @thread_safe: yes
 */

#ifndef HAM_CACHE_H
#define HAM_CACHE_H

#include "0root/root.h"

#include <vector>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/spinlock.h"
#include "2page/page_collection.h"

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
      : m_env(env), m_capacity(capacity_bytes), m_alloc_elements(0),
        m_totallist(Page::kListCache),
        m_buckets(kBucketSize, PageCollection(Page::kListBucket)),
        m_cache_hits(0), m_cache_misses(0) {
      ham_assert(m_capacity > 0);
    }

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get_page(uint64_t address, uint32_t flags = 0) {
      size_t hash = calc_hash(address);

      ScopedSpinlock lock(m_mutex);

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

      ScopedSpinlock lock(m_mutex);

      /* First remove the page from the cache, if it's already cached
       *
       * Then re-insert the page at the head of the list. The tail will
       * point to the least recently used page.
       */
      m_totallist.remove(page);
      m_totallist.add(page);

      if (page->is_allocated())
        m_alloc_elements++;

      m_buckets[hash].add(page);
    }

    // Removes a page from the cache
    void remove_page(Page *page) {
      ScopedSpinlock lock(m_mutex);
      remove_page_unlocked(page);
    }

    typedef void (*PurgeCallback)(Page *page, PageManager *pm);

    // Purges the cache; the callback is called for every page that needs
    // to be purged
    void purge(PurgeCallback cb, PageManager *pm);

    // The callback returns true if the page should be removed from
    // the cache and deleted
    typedef bool (*PurgeIfCallback)(Page *page, LocalEnvironment *env,
            LocalDatabase *db, uint32_t flags);

    // Visits all pages in the "totallist". If |cb| returns true then the
    // page is removed and deleted. This is used by the Environment
    // to flush (and delete) pages.
    void purge_if(PurgeIfCallback cb, LocalEnvironment *env, LocalDatabase *db,
                    uint32_t flags);

    // Returns the capacity (in bytes)
    size_t get_capacity() const {
      return (m_capacity);
    }

    // Returns the number of currently cached elements
    size_t get_current_elements() const {
      return (m_totallist.size());
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
    friend struct PageCollectionPurgeIfCallback;

    void remove_page_unlocked(Page *page) {
      ham_assert(page->get_address() != 0);
      size_t hash = calc_hash(page->get_address());
      /* remove the page from the cache buckets */
      m_buckets[hash].remove(page);

      /* remove it from the list of all cached pages */
      if (m_totallist.remove(page) && page->is_allocated())
        m_alloc_elements--;
    }

    // Calculates the hash of a page address
    size_t calc_hash(uint64_t o) const {
      return ((size_t)(o % kBucketSize));
    }

    // the current Environment
    LocalEnvironment *m_env;

    // A fast spinlock
    Spinlock m_mutex;

    // the capacity (in bytes)
    uint64_t m_capacity;

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
