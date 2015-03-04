/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "2page/page.h"
#include "2page/page_collection.h"
#include "2config/env_config.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Cache
{
    enum {
      // The number of buckets should be a prime number or similar, as it
      // is used in a MODULO hash scheme
      kBucketSize = 10317,
    };

    template<typename Purger>
    struct PurgeIfSelector
    {
      PurgeIfSelector(Cache *cache, Purger &purger)
        : m_cache(cache), m_purger(purger) {
      }

      bool operator()(Page *page) {
        if (m_purger(page)) {
          m_cache->del(page);
          delete page;
        }
        // don't remove page from list; it was already removed above
        return (false);
      }

      Cache *m_cache;
      Purger &m_purger;
    };

  public:
    // The default constructor
    Cache(const EnvironmentConfiguration &config)
      : m_capacity_bytes(config.flags & HAM_CACHE_UNLIMITED
                            ? 0xffffffffffffffffull
                            : config.cache_size_bytes),
        m_page_size_bytes(config.page_size_bytes),
        m_alloc_elements(0), m_totallist(Page::kListCache),
        m_buckets(kBucketSize, PageCollection(Page::kListBucket)),
        m_cache_hits(0), m_cache_misses(0) {
      ham_assert(m_capacity_bytes > 0);
    }

    // Fills in the current metrics
    void fill_metrics(ham_env_metrics_t *metrics) const {
      metrics->cache_hits = m_cache_hits;
      metrics->cache_misses = m_cache_misses;
    }

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get(uint64_t address) {
      size_t hash = calc_hash(address);

      Page *page = m_buckets[hash].get(address);;
      if (!page) {
        m_cache_misses++;
        return (0);
      }

      // Now re-insert the page at the head of the "totallist", and
      // thus move far away from the tail. The pages at the tail are highest
      // candidates to be deleted when the cache is purged.
      m_totallist.del(page);
      m_totallist.put(page);
      m_cache_hits++;
      return (page);
    }

    // Stores a page in the cache
    void put(Page *page) {
      size_t hash = calc_hash(page->get_address());
      ham_assert(page->get_data());

      /* First remove the page from the cache, if it's already cached
       *
       * Then re-insert the page at the head of the list. The tail will
       * point to the least recently used page.
       */
      m_totallist.del(page);
      m_totallist.put(page);

      if (page->is_allocated())
        m_alloc_elements++;
      m_buckets[hash].put(page);
    }

    // Removes a page from the cache
    void del(Page *page) {
      ham_assert(page->get_address() != 0);
      size_t hash = calc_hash(page->get_address());
      /* remove the page from the cache buckets */
      m_buckets[hash].del(page);

      /* remove it from the list of all cached pages */
      if (m_totallist.del(page) && page->is_allocated())
        m_alloc_elements--;
    }

    // Purges the cache. Implements a LRU eviction algorithm. Dirty pages are
    // forwarded to the |processor()| for flushing.
    //
    // Tries to purge at least 20 pages. In benchmarks this has proven to
    // be a good limit.
    template<typename Processor>
    void purge(Processor &processor, Page *ignore_page) {
      int limit = int(current_elements()
                        - (m_capacity_bytes / m_page_size_bytes));

      Page *page = m_totallist.tail();
      for (int i = 0; i < limit && page != 0; i++) {
        Page *next = page->get_previous(Page::kListCache);

        // dirty pages are flushed by the worker thread
        if (page->is_dirty()) {
          processor(page);
          page = next;
          continue;
        }
        // non-dirty pages are deleted if possible
        if (!page->is_dirty()
                && page->cursor_list() == 0
                && page != ignore_page
                && page->mutex().try_lock()) {
          del(page);
          page->mutex().unlock();
          delete page;
        }

        page = next;
      }
    }

    // Visits all pages in the "totallist". If |cb| returns true then the
    // page is removed and deleted. This is used by the Environment
    // to flush (and delete) pages.
    template<typename Purger>
    void purge_if(Purger &purger) {
      PurgeIfSelector<Purger> selector(this, purger);
      m_totallist.extract(selector);
    }

    // Returns true if the capacity limits are exceeded
    bool is_cache_full() const {
      return (current_elements() * m_page_size_bytes
                    > m_capacity_bytes);
    }

    // Returns the capacity (in bytes)
    uint64_t capacity() const {
      return (m_capacity_bytes);
    }

    // Returns the number of currently cached elements
    size_t current_elements() const {
      return (m_totallist.size());
    }

    // Returns the number of currently cached elements (excluding those that
    // are mmapped)
    size_t allocated_elements() const {
      return (m_alloc_elements);
    }

  private:
    // Calculates the hash of a page address
    size_t calc_hash(uint64_t value) const {
      return ((size_t)(value % Cache::kBucketSize));
    }

    // the capacity (in bytes)
    uint64_t m_capacity_bytes;

    // the current page size (in bytes)
    uint64_t m_page_size_bytes;

    // the current number of cached elements that were allocated (and not
    // mapped)
    size_t m_alloc_elements;

    // linked list of ALL cached pages
    PageCollection m_totallist;

    // The hash table buckets - each is a linked list of Page pointers
    std::vector<PageCollection> m_buckets;

    // counts the cache hits
    uint64_t m_cache_hits;

    // counts the cache misses
    uint64_t m_cache_misses;
};

} // namespace hamsterdb

#endif /* HAM_CACHE_H */
