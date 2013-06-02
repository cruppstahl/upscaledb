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

#ifndef HAM_SIMPLE_FREELIST_H__
#define HAM_SIMPLE_FREELIST_H__

#include <vector>
#include <utility> // for std::pair

#include "internal_fwd_decl.h"
#include "page.h"
#include "env.h"

namespace hamsterdb {

class Database;

/**
 * The freelist class structure
 */
class ReducedFreelist
{
  public:
    typedef std::pair<ham_u64_t, ham_size_t> Entry;
    typedef std::vector<Entry> EntryVec;

    enum {
      /** if a blob is < this threshold then it can be discarded if there
       * are too many of them */
      SMALL_SIZE_THRESHOLD = 32,

      /** if there are more small blobs than this threshold then they can
       * be discarded */
      SMALL_BLOB_THRESHOLD = 1000,

      /** Number of entries that we can store */
      MAX_ENTRIES = 32
    };

    /** constructor */
    ReducedFreelist(Environment *env)
      : m_env(env), m_small_blobs(0) {
      m_entries.reserve(MAX_ENTRIES);
    }

    /** tries to allocate a page from the freelist */
    ham_u64_t alloc_page();

    /** tries to allocate a blob area from the freelist */
    ham_u64_t alloc_blob(ham_size_t size) {
      ham_assert(size > 0);

      ham_assert(0 == check_integrity());
      ham_u64_t rv = alloc(size, false);
      if (m_small_blobs > 0 && rv != 0 && size < SMALL_SIZE_THRESHOLD)
        m_small_blobs--;
      return (rv);
    }

    /** adds an unused page to the freelist */
    void free_page(Page *page);

    /** adds an unused area to the freelist */
    void free_area(ham_u64_t address, ham_size_t size);

    /** verifies integrity of the freelist */
    ham_status_t check_integrity();

    /** retrieves entries; only for testing! */
    const EntryVec &get_entries() const {
      return (m_entries);
    }

  private:
    /** allocates an arbitrary-sized chunk; if aligned is true then the
     * returned address (if any) is pagesize-aligned
     */
    ham_u64_t alloc(ham_size_t size, bool aligned);

    /** the Environment */
    Environment *m_env;

    /** Number of small blobs (< SMALL_SIZE_THRESHOLD) currently in the list */
    ham_size_t m_small_blobs;

    /** the list of currently free entries */
    EntryVec m_entries;
};

} // namespace hamsterdb

#endif /* HAM_SIMPLE_FREELIST_H__ */
