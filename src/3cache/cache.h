/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class Cache
{
    typedef PageCollection<Page::kListBucket> CacheLine;

    enum {
      // The number of buckets should be a prime number or similar, as it
      // is used in a MODULO hash scheme
      kBucketSize = 10317,
    };

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

  public:
    // The default constructor
    Cache(const EnvConfig &config)
      : capacity_bytes(isset(config.flags, UPS_CACHE_UNLIMITED)
                            ? std::numeric_limits<uint64_t>::max()
                            : config.cache_size_bytes),
        page_size_bytes(config.page_size_bytes), alloc_elements(0),
        buckets(kBucketSize), cache_hits(0), cache_misses(0) {
      assert(capacity_bytes > 0);
    }

    // Fills in the current metrics
    void fill_metrics(ups_env_metrics_t *metrics) const {
      metrics->cache_hits = cache_hits;
      metrics->cache_misses = cache_misses;
    }

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get(uint64_t address) {
      size_t hash = calc_hash(address);

      Page *page = buckets[hash].get(address);
      if (!page) {
        cache_misses++;
        return 0;
      }

      // Now re-insert the page at the head of the "totallist", and
      // thus move far away from the tail. The pages at the tail are highest
      // candidates to be deleted when the cache is purged.
      totallist.del(page);
      totallist.put(page);
      cache_hits++;
      return page;
    }

    // Stores a page in the cache
    void put(Page *page) {
      size_t hash = calc_hash(page->address());

      /* First remove the page from the cache, if it's already cached
       *
       * Then re-insert the page at the head of the list. The tail will
       * point to the least recently used page.
       */
      totallist.del(page);
      totallist.put(page);
      if (page->is_allocated())
        alloc_elements++;

      buckets[hash].put(page);
    }

    // Removes a page from the cache
    void del(Page *page) {
      assert(page->address() != 0);

      /* remove it from the list of all cached pages */
      if (totallist.del(page) && page->is_allocated())
        alloc_elements--;

      /* remove the page from the cache buckets */
      size_t hash = calc_hash(page->address());
      buckets[hash].del(page);
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
                        - (capacity_bytes / page_size_bytes));

      Page *page = totallist.tail();
      for (int i = 0; i < limit && page != 0; i++) {
        if (page->mutex().try_lock()) {
          if (page->cursor_list() == 0 && page != ignore_page) {
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
      totallist.extract(selector);
    }

    // Returns true if the capacity limits are exceeded
    bool is_cache_full() const {
      return totallist.size() * page_size_bytes > capacity_bytes;
    }

    // Returns the capacity (in bytes)
    uint64_t capacity() const {
      return capacity_bytes;
    }

    // Returns the number of currently cached elements
    size_t current_elements() const {
      return totallist.size();
    }

    // Returns the number of currently cached elements (excluding those that
    // are mmapped)
    size_t allocated_elements() const {
      return alloc_elements;
    }

  private:
    // Calculates the hash of a page address
    size_t calc_hash(uint64_t value) const {
      return (size_t)(value % Cache::kBucketSize);
    }

    // the capacity (in bytes)
    uint64_t capacity_bytes;

    // the current page size (in bytes)
    uint64_t page_size_bytes;

    // the current number of cached elements that were allocated (and not
    // mapped)
    size_t alloc_elements;

    // linked list of ALL cached pages
    PageCollection<Page::kListCache> totallist;

    // The hash table buckets - each is a linked list of Page pointers
    std::vector<CacheLine> buckets;

    // counts the cache hits
    uint64_t cache_hits;

    // counts the cache misses
    uint64_t cache_misses;
};

} // namespace upscaledb

#endif /* UPS_CACHE_H */
