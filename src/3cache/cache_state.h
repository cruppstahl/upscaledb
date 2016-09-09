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

#ifndef UPS_CACHE_STATE_H
#define UPS_CACHE_STATE_H

#include "0root/root.h"

#include <vector>

#include "ups/types.h"

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "2page/page_collection.h"
#include "2config/env_config.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct CacheState
{
  typedef PageCollection<Page::kListBucket> CacheLine;

  enum {
    // The number of buckets should be a prime number or similar, as it
    // is used in a MODULO hash scheme
    kBucketSize = 10317,
  };

  CacheState(const EnvConfig &config)
    : capacity_bytes(ISSET(config.flags, UPS_CACHE_UNLIMITED)
                            ? std::numeric_limits<uint64_t>::max()
                            : config.cache_size_bytes),
      page_size_bytes(config.page_size_bytes), alloc_elements(0),
      buckets(kBucketSize), cache_hits(0), cache_misses(0) {
    assert(capacity_bytes > 0);
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

#endif /* UPS_CACHE_STATE_H */
