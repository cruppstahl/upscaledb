/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
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

#ifndef UPS_CACHE_H
#define UPS_CACHE_H

#include "0root/root.h"

#include <vector>

#include "ups/upscaledb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "2page/page_collection.h"
#include "2config/env_config.h"
#include "3cache/cache_state.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

namespace Impl {
// Calculates the hash of a page address
static inline size_t
calc_hash(uint64_t value)
{
  return (size_t)(value % CacheState::kBucketSize);
}
} // namespace Impl

struct Cache
{
  template<typename Purger>
  struct PurgeIfSelector {
    PurgeIfSelector(Cache *cache, Purger &purger)
      : cache_(cache), purger_(purger) {
    }

    bool operator()(Page *page) {
      if (purger_(page))
        cache_->del(page);
      // don't remove page from list; it was already removed above
      return false;
    }

    Cache *cache_;
    Purger &purger_;
  };

  // The default constructor
  Cache(const EnvConfig &config)
    : state(config) {
  }

  // Fills in the current metrics
  void fill_metrics(ups_env_metrics_t *metrics) const {
    metrics->cache_hits = state.cache_hits;
    metrics->cache_misses = state.cache_misses;
  }

  // Retrieves a page from the cache, also removes the page from the cache
  // and re-inserts it at the front. Returns null if the page was not cached.
  Page *get(uint64_t address) {
    size_t hash = Impl::calc_hash(address);

    Page *page = state.buckets[hash].get(address);
    if (!page) {
      state.cache_misses++;
      return 0;
    }

    // Now re-insert the page at the head of the "totallist", and
    // thus move far away from the tail. The pages at the tail are highest
    // candidates to be deleted when the cache is purged.
    state.totallist.del(page);
    state.totallist.put(page);
    state.cache_hits++;
    return page;
  }

  // Stores a page in the cache
  void put(Page *page) {
    size_t hash = Impl::calc_hash(page->address());

    /* First remove the page from the cache, if it's already cached
     *
     * Then re-insert the page at the head of the list. The tail will
     * point to the least recently used page.
     */
    state.totallist.del(page);
    state.totallist.put(page);
    if (page->is_allocated())
      state.alloc_elements++;

    state.buckets[hash].put(page);
  }

  // Removes a page from the cache
  void del(Page *page) {
    assert(page->address() != 0);

    /* remove it from the list of all cached pages */
    if (state.totallist.del(page) && page->is_allocated())
      state.alloc_elements--;

    /* remove the page from the cache buckets */
    size_t hash = Impl::calc_hash(page->address());
    state.buckets[hash].del(page);
  }

  // Purges the cache. Implements a LRU eviction algorithm. Dirty pages are
  // forwarded to the |processor()| for flushing.
  // The |ignore_page| is passed by the caller; this page will not be purged
  // under any circumstance. This is used by the PageManager to make sure
  // that the "last blob page" is not evicted by the cache.
  void purge_candidates(std::vector<uint64_t> &candidates,
                  std::vector<Page *> &garbage,
                  Page *ignore_page) {
    int limit = (int)(current_elements()
                      - (state.capacity_bytes / state.page_size_bytes));

    Page *page = state.totallist.tail();
    for (int i = 0; i < limit && page != 0; i++) {
      if (page->mutex().try_lock()) {
        if (page->cursor_list.size() == 0
              && page != ignore_page
              && page->type() != Page::kTypeBroot) {
          if (page->is_dirty())
            candidates.push_back(page->address());
          else
            garbage.push_back(page);
        }
        page->mutex().unlock();
      }

      page = page->previous(Page::kListCache);
    }
  }

  // Visits all pages in the "totallist". If |cb| returns true then the
  // page is removed and deleted. This is used by the Environment
  // to flush (and delete) pages.
  template<typename Purger>
  void purge_if(Purger &purger) {
    PurgeIfSelector<Purger> selector(this, purger);
    state.totallist.extract(selector);
  }

  // Returns true if the capacity limits are exceeded
  bool is_cache_full() const {
    return state.totallist.size() * state.page_size_bytes
            > state.capacity_bytes;
  }

  // Returns the capacity (in bytes)
  uint64_t capacity() const {
    return state.capacity_bytes;
  }

  // Returns the number of currently cached elements
  size_t current_elements() const {
    return state.totallist.size();
  }

  // Returns the number of currently cached elements (excluding those that
  // are mmapped)
  size_t allocated_elements() const {
    return state.alloc_elements;
  }

  CacheState state;
};

} // namespace upscaledb

#endif /* UPS_CACHE_H */
