/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "error.h"
#include "page_manager.h"
#include "reduced_freelist.h"
#include "blob.h"

using namespace hamsterdb;

ham_status_t
ReducedFreelist::alloc_page(ham_u64_t *paddress)
{
  ham_assert(0 == check_integrity());
  *paddress = alloc(m_env->get_pagesize(), true);
  return (0);
}

ham_status_t
ReducedFreelist::free_page(Page *page)
{
  ham_assert(0 == check_integrity());

  // change page type to TYPE_FREELIST to mark this page as free
  if (page->get_type() != Page::TYPE_FREELIST) {
    page->set_type(Page::TYPE_FREELIST);
    page->set_dirty(true);
  }

  ham_size_t page_size = m_env->get_pagesize();

  // make sure to remove all blobs from this page from the freelist
  // if they were already added
  for (size_t i = 0; i < m_entries.size(); /* nop */) {
    EntryVec::iterator it = m_entries.begin() + i;
    // blob is in this page - remove the blob
    if (it->first >= page->get_self()
        && it->first + it->second <= page->get_self() + page_size) {
      m_entries.erase(it);
      continue;
    }
    // partial blob is in this page - only remove the partial part
    else if (it->first >= page->get_self()
          && it->first <= page->get_self() + page_size) {
      ham_size_t cutoff = page->get_self() + page_size - it->first;
      it->first += cutoff;
      it->second -= cutoff;
    }

    i++;
  }

  // now add this page to the freelist
  return (free_area(page->get_self(), page_size));
}

ham_status_t
ReducedFreelist::free_area(ham_u64_t address, ham_size_t size)
{
  ham_size_t page_size = m_env->get_pagesize();

  ham_assert(0 == check_integrity());

  // make sure that we can fit the PBlobHeader into the first page
  ham_u64_t page_id = address - (address % page_size);
  if (page_id + page_size - address < sizeof(PBlobHeader)) {
    if (size > sizeof(PBlobHeader)) {
      size -= sizeof(PBlobHeader);
      address += sizeof(PBlobHeader);
    }
    else
      return (0);
  }

  // we're only interested in blobs which can fit a PBlobHeader
  if (size < sizeof(PBlobHeader))
    return (0);

  // if this blob is too small, and we already have many small blobs in the
  // list: just discard it
  if (size < SMALL_SIZE_THRESHOLD) {
    if (m_small_blobs > SMALL_BLOB_THRESHOLD)
      return (0);
    m_small_blobs++;
  }

  // if the vector is full then delete the smallest blob
  if (m_entries.size() == MAX_ENTRIES) {
    EntryVec::iterator it, smallest = m_entries.end();
    ham_size_t s = 0;
    for (it = m_entries.begin(); it != m_entries.end(); it++) {
      if (it->second < s) {
        smallest = it;
        s = it->first;
      }
      // make sure this blob is not yet stored in the freelist
      ham_assert(it->first != address);
    }
    m_entries.erase(smallest);
  }

  // then simply append
  m_entries.push_back(Entry(address, size));

  ham_assert(0 == check_integrity());
  return (0);
}

ham_u64_t
ReducedFreelist::alloc(ham_size_t size, bool aligned)
{
  ham_u64_t address = 0;

  ham_assert(0 == check_integrity());

  EntryVec::iterator it, best = m_entries.end();
  ham_size_t b = (ham_size_t)-1;
  for (it = m_entries.begin(); it != m_entries.end(); it++) {
    // search for an aligned address?
    if (aligned && it->first % size != 0)
      continue;

    // exact match?
    if (it->second == size) {
      address = it->first;
      m_entries.erase(it);
      break;
    }

    if (it->second > size && it->second < b) {
      b = it->second;
      best = it;
    }
  }

  // no exact match, but we found a slot that is big enough? then
  // overwrite this slot with the remainder
  if (address == 0 && b < (ham_size_t)-1) {
    ham_assert(best != m_entries.end());
    address = best->first;
    best->first += size;
    best->second -= size;
  }

  ham_assert(0 == check_integrity());

  return (address);
}

static bool
compare(const ReducedFreelist::Entry &lhs, const ReducedFreelist::Entry &rhs)
{
  return (lhs.first < rhs.first);
}

ham_status_t
ReducedFreelist::check_integrity()
{
  ham_assert(m_entries.size() <= MAX_ENTRIES);

  // sort by address
  std::sort(m_entries.begin(), m_entries.end(), compare);

  ham_u64_t address = 0;

  for (EntryVec::iterator it = m_entries.begin(); it != m_entries.end(); it++) {
    if (it->first == 0 || it->second == 0) {
      ham_log(("Invalid freelist entry %llu/%u", it->first, it->second));
      return (HAM_INTEGRITY_VIOLATED);
    }
    if (it->first < address) {
      ham_log(("Invalid freelist address %llu/%u < %llu",
                  it->first, it->second, address));
      return (HAM_INTEGRITY_VIOLATED);
    }
    address = it->first + it->second;
  }

  return (0);
}
