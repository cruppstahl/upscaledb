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
 * The Freelist manages the list of currently unused (free) pages.
 *
 * @exception_safe: basic
 * @thread_safe: no
 */

#ifndef UPS_FREELIST_H
#define UPS_FREELIST_H

#include "0root/root.h"

#include <map>

// Always verify that a file of level N does not include headers > N!
#include "2config/env_config.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;

struct Freelist {
  // The freelist maps page-id to number of free pages (usually 1)
  typedef std::map<uint64_t, size_t> FreeMap;

  // Constructor
  Freelist(const EnvConfig &config_)
    : config(config_) {
    clear();
  }

  // Clears the internal state
  void clear() {
    freelist_hits = 0;
    freelist_misses = 0;
    free_pages.clear();
  }

  // Returns true if the freelist is empty
  bool empty() const {
    return (free_pages.empty());
  }

  // Encodes the freelist's state in |data|. Returns a bool which is set to
  // true if there is additional data, or false if the whole state was
  // encoded.
  // Set |cont.first| to false for the first call.
  std::pair<bool, Freelist::FreeMap::const_iterator>
                    encode_state(
                        std::pair<bool, Freelist::FreeMap::const_iterator> cont,
                        uint8_t *data, size_t data_size);

  // Decodes the freelist's state from raw data and adds it to the internal
  // map
  void decode_state(uint8_t *data);

  // Allocates |num_pages| sequential pages from the freelist; returns the
  // page id of the first page, or 0 if not successfull
  uint64_t alloc(size_t num_pages);

  // Stores a page in the freelist
  void put(uint64_t page_id, size_t page_count);

  // Returns true if a page is in the freelist
  bool has(uint64_t page_id) const;

  // Tries to truncate the file by counting how many pages at the file's end
  // are unused. Returns the address of the last unused page, or |file_size|
  // if there are no unused pages at the end.
  uint64_t truncate(uint64_t file_size);

  // Copy of the Environment's configuration
  const EnvConfig &config;

  // The map with free pages
  FreeMap free_pages;

  // number of successful freelist hits
  uint64_t freelist_hits;

  // number of freelist misses
  uint64_t freelist_misses;
};

} // namespace upscaledb

#endif /* UPS_FREELIST_H */
