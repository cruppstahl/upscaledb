/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/pickle.h"
#include "3page_manager/freelist.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

std::pair<bool, Freelist::FreeMap::const_iterator>
Freelist::encode_state(std::pair<bool, Freelist::FreeMap::const_iterator> cont,
                uint8_t *data, size_t data_size)
{
  uint32_t page_size = config.page_size_bytes;
  Freelist::FreeMap::const_iterator it = cont.second;
  if (cont.first == false)
    it = free_pages.begin();
  else
    ups_assert(it != free_pages.end());
  
  uint32_t counter = 0;
  uint8_t *p = data;
  p += 8;   // leave room for the pointer to the next page
  p += 4;   // leave room for the counter

  while (it != free_pages.end()) {
    // 9 bytes is the maximum amount of storage that we will need for a
    // new entry; if it does not fit then break
    if ((p + 9) - data >= (ptrdiff_t)data_size)
      break;

    // check if the next entry (and the following) are adjacent; if yes then
    // they are merged. Up to 16 pages can be merged.
    uint32_t page_counter = 1;
    uint64_t base = it->first;
    ups_assert(base % page_size == 0);
    uint64_t current = it->first;

    // move to the next entry, then merge all adjacent pages
    for (it++; it != free_pages.end() && page_counter < 16 - 1; it++) {
      if (it->first != current + page_size)
        break;
      current += page_size;
      page_counter++;
    }

    // now |base| is the start of a sequence of free pages, and the
    // sequence has |page_counter| pages
    //
    // This is encoded as
    // - 1 byte header
    //   - 4 bits for |page_counter|
    //   - 4 bits for the number of bytes following ("n")
    // - n byte page-id (div page_size)
    ups_assert(page_counter < 16);
    int num_bytes = Pickle::encode_u64(p + 1, base / page_size);
    *p = (page_counter << 4) | num_bytes;
    p += 1 + num_bytes;

    counter++;
  }

  // now store the counter
  *(uint32_t *)(data + 8) = counter;

  std::pair<bool, Freelist::FreeMap::const_iterator> retval;
  retval.first = (it != free_pages.end());
  retval.second = it;
  return (retval);
}

void
Freelist::decode_state(uint8_t *data)
{
  uint32_t page_size = config.page_size_bytes;

  // get the number of stored elements
  uint32_t counter = *(uint32_t *)data;
  data += 4;

  // now read all pages
  for (uint32_t i = 0; i < counter; i++) {
    // 4 bits page_counter, 4 bits for number of following bytes
    int page_counter = (*data & 0xf0) >> 4;
    int num_bytes = *data & 0x0f;
    ups_assert(page_counter > 0);
    ups_assert(num_bytes <= 8);
    data += 1;

    uint64_t id = Pickle::decode_u64(num_bytes, data);
    data += num_bytes;

    free_pages[id * page_size] = page_counter;
  }
}

uint64_t
Freelist::alloc(size_t num_pages)
{
  uint64_t address = 0;
  uint32_t page_size = config.page_size_bytes;

  for (FreeMap::iterator it = free_pages.begin();
            it != free_pages.end();
            it++) {
    if (it->second == num_pages) {
      address = it->first;
      free_pages.erase(it);
      break;
    }
    if (it->second > num_pages) {
      address = it->first;
      free_pages[it->first + num_pages * page_size] = it->second - num_pages;
      free_pages.erase(it);
      break;
    }
  }

  if (address != 0)
    freelist_hits++;
  else
    freelist_misses++;

  return (address);
}

void
Freelist::put(uint64_t page_id, size_t page_count)
{
  free_pages[page_id] = page_count;
}

bool
Freelist::has(uint64_t page_id) const
{
  return (free_pages.find(page_id) != free_pages.end());
}

uint64_t
Freelist::truncate(uint64_t file_size)
{
  uint32_t page_size = config.page_size_bytes;
  uint64_t lower_bound = file_size;

  for (FreeMap::reverse_iterator it = free_pages.rbegin();
            it != free_pages.rend();
            it++) {
    if (it->first + it->second * page_size == lower_bound)
      lower_bound = it->first;
  }

  return (lower_bound);
}

} // namespace upscaledb
