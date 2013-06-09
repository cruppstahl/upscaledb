/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief freelist structures, functions and macros
 *
 */

#ifndef HAM_REDUCED_FREELIST_H__
#define HAM_REDUCED_FREELIST_H__

#include <vector>
#include <utility> // for std::pair

#include "internal_fwd_decl.h"
#include "page.h"
#include "env.h"
#include "freelist.h"

namespace hamsterdb {

class Database;

/**
 * The freelist class structure
 */
class ReducedFreelist : public Freelist
{
  public:
    typedef std::pair<ham_u64_t, ham_size_t> Entry;
    typedef std::vector<Entry> EntryVec;

    enum {
      /** if a blob is < this threshold then it can be discarded if there
       * are too many of them */
      kSmallSizeThreshold = 32,

      /** if there are more small blobs than this threshold then they can
       * be discarded */
      kSmallBlobThreshold = 100,

      /** Number of entries that we can store */
      kMaxEntries = 512,

      /** required alignment for all blobs - none */
      kBlobAlignment = 1
    };

    /** constructor */
    ReducedFreelist(Environment *env)
      : Freelist(env), m_small_blobs(0), m_last(-1) {
      m_entries.reserve(kMaxEntries);
    }

    /** tries to allocate a page from the freelist */
    virtual ham_status_t alloc_page(ham_u64_t *paddress) {
      ham_assert(0 == check_integrity());
      *paddress = alloc(m_env->get_pagesize(), true);
      return (0);
    }

    /** tries to allocate a blob area from the freelist */
    virtual ham_status_t alloc_area(ham_size_t size, ham_u64_t *paddress) {
      ham_assert(size > 0);

      ham_assert(0 == check_integrity());
      ham_u64_t rv = alloc(size, false);
      if (m_small_blobs > 0 && rv != 0 && size < kSmallSizeThreshold)
        m_small_blobs--;
      *paddress = rv;
      return (0);
    }

    /** adds an unused page to the freelist */
    virtual ham_status_t free_page(Page *page) {
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

    /** adds an unused area to the freelist */
    virtual ham_status_t free_area(ham_u64_t address, ham_size_t size) {
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
      if (size < kSmallSizeThreshold) {
        if (m_small_blobs > kSmallBlobThreshold)
          return (0);
        m_small_blobs++;
      }

      // if the vector is empty then simply insert and return
      if (m_entries.empty()) {
        m_entries.push_back(Entry(address, size));
        m_last = 0;
        return (0);
      }

      // if the vector is full then delete the smallest blob
      if (m_entries.size() == kMaxEntries) {
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
      m_last = m_entries.size() - 1;
    
      ham_assert(0 == check_integrity());
      return (0);
    }

    /** returns the alignment for blobs */
    virtual int get_blob_alignment() const {
      return (kBlobAlignment);
    }

    /** verifies integrity of the freelist */
    virtual ham_status_t check_integrity();

    /** retrieves entries; only for testing! */
    const EntryVec &get_entries() const {
      return (m_entries);
    }

  private:
    /** allocates an arbitrary-sized chunk; if aligned is true then the
     * returned address (if any) is pagesize-aligned
     */
    ham_u64_t alloc(ham_size_t size, bool aligned) {
      ham_u64_t address = 0;

      ham_assert(0 == check_integrity());

      EntryVec::iterator it, best = m_entries.end();

      if (m_last > -1) {
        it = m_entries.begin() + m_last;
        if ((aligned && it->first % size == 0)
            || it->second == size) {
          address = it->first;
          m_entries.erase(it);
          m_last = -1;
          return (address);
        }
        else if (it->second > size) {
          address = it->first;
          it->second -= size;
          it->first += size;
          return (address);
        }
      }

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

    /** Number of small blobs (< kSmallSizeThreshold) currently in the list */
    ham_size_t m_small_blobs;

    /** the list of currently free entries */
    EntryVec m_entries;

    /** index of the last inserted entry */
    int m_last;
};

} // namespace hamsterdb

#endif /* HAM_REDUCED_FREELIST_H__ */
