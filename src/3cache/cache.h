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
#include "3cache/cache_state.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Cache
{
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
      : m_state(config) {
    }

    // Fills in the current metrics
    void fill_metrics(ham_env_metrics_t *metrics) const;

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get(uint64_t address);

    // Stores a page in the cache
    void put(Page *page);

    // Removes a page from the cache
    void del(Page *page);

    // Purges the cache. Implements a LRU eviction algorithm. Dirty pages are
    // forwarded to the |processor()| for flushing.
    //
    // Tries to purge at least 20 pages. In benchmarks this has proven to
    // be a good limit.
    template<typename Processor>
    void purge(Processor &processor, Page *ignore_page) {
      int limit = current_elements()
                - (m_state.capacity_bytes / m_state.page_size_bytes);

      Page *page = m_state.totallist.tail();
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
      m_state.totallist.extract(selector);
    }

    // Returns the capacity (in bytes)
    size_t capacity() const;

    // Returns the number of currently cached elements
    size_t current_elements();

    // Returns the number of currently cached elements (excluding those that
    // are mmapped)
    size_t allocated_elements() const;

  private:
    // The mutable state
    CacheState m_state;
};

} // namespace hamsterdb

#endif /* HAM_CACHE_H */
