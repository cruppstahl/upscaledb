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

#ifndef HAM_FREELIST_H__
#define HAM_FREELIST_H__

#include <vector>

#include "internal_fwd_decl.h"
#include "freelist.h"
#include "freelist_stats.h"

namespace hamsterdb {

#define HAM_DAM_RANDOM_WRITE            1
#define HAM_DAM_SEQUENTIAL_INSERT       2

/**
 * an entry in the freelist cache
 */
struct FreelistEntry {
  /** the start address of this freelist page */
  ham_u64_t start_address;

  /** maximum bits in this page */
  ham_size_t max_bits;

  /** allocated bits in this page */
  ham_size_t allocated_bits;

  /** the page ID */
  ham_u64_t page_id;

  /**
   * freelist algorithm specific run-time data
   *
   * This is done as a union as it will reduce code complexity
   * significantly in the common freelist processing areas.
   */
  PFreelistPageStatistics perf_data;
};


/**
 * the freelist class structure
 */
class Freelist
{
  public:
    enum {
      kBlobAlignment = 32
    };

    /** Constructor */
    Freelist(Environment *env)
      : m_env(env), m_count_hits(0), m_count_misses(0) {
    }

    /** mark a page in the file as "free" */
    ham_status_t free_page(Page *page);

    /**
     * mark an area in the file as "free"
     * @note will assert that address and size are aligned!
     */
    ham_status_t free_area(ham_u64_t address, ham_size_t size);

    /**
     * try to allocate (possibly aligned) space from the freelist,
     * where the allocated space should be positioned at or beyond
     * the given address.
     *
     * @note will assert that size is properly aligned!
     */
    ham_status_t alloc_area(ham_size_t size, ham_u64_t *paddress) {
      return (alloc_area_impl(size, paddress, false, 0));
    }

    /** try to allocate an (aligned) page from the freelist */
    ham_status_t alloc_page(ham_u64_t *paddr);

    // Fills in the collected metrics and usage statistics
    void get_metrics(ham_env_metrics_t *metrics) const;

    /** get a pointer to the environment (reqd for freelist_stats) */
    // TODO remove this
    Environment *get_env() {
      return (m_env);
    }

  private:
    friend class FreelistStatistics;

    /** @note The lower_bound_address is assumed to be aligned. */
    ham_status_t alloc_area_impl(ham_size_t size, ham_u64_t *paddr,
                bool aligned, ham_u64_t lower_bound_address);

    /** Retrieves the first freelist entry */
    FreelistEntry *get_entries() {
      return (m_entries.size() ? &m_entries[0] : 0);
    }

    /** Retrieves the number of freelist entries */
    // TODO make private or remove it
    size_t get_count() const {
      return (m_entries.size());
    }

  private:
    /** lazily initializes the freelist structure */
    ham_status_t initialize();

    /** get a reference to the DB FILE (global) statistics */
    GlobalStatistics *get_global_statistics() {
      return (&m_perf_data);
    }

    /** retrieves the FreelistEntry which manages a specific
     * file address */
    FreelistEntry *get_entry_for_address(ham_u64_t address);

    /** returns maximum bits that fit in a regular page */
    ham_size_t get_entry_maxspan();

    /** adds 'new_count' Entries */
    void resize(ham_size_t new_count);

    /** allocates a page for a given entry */
    ham_status_t alloc_freelist_page(Page **ppage, FreelistEntry *entry);

    /** sets (or resets) all bits in a given range */
    ham_size_t set_bits(FreelistEntry *entry, PFreelistPayload *fp,
                    ham_size_t start_bit, ham_size_t size_bits,
                    bool set, FreelistStatistics::Hints *hints);

    /** searches for a free bit array in the whole list */
    ham_s32_t search_bits(FreelistEntry *entry, PFreelistPayload *f,
                    ham_size_t size_bits, FreelistStatistics::Hints *hints);

    /**
     * Report if the requested size can be obtained from the given freelist
     * page.
     *
     * Always make use of the collected statistics, but act upon it in
     * different ways, depending on our current 'mgt_mode' setting.
     *
     * Note: the answer is an ESTIMATE, _not_ a guarantee.
     *
     * Return the first cache entry index from now (start_index) where you
     * have a chance of finding a free slot.
     *
     * Note: the initial round with have start_index == -1 incoming.
     *
     * Return -1 to signal there's no chance at all.
     */
    ham_s32_t locate_sufficient_free_space(FreelistStatistics::Hints *dst,
                    FreelistStatistics::GlobalHints *hints,
                    ham_s32_t start_index);

    /**
     * replacement for env->set_dirty() and page->set_dirty(); will dirty page
     * (or env) and also add the page (or header page) to the changeset
     */
    void mark_dirty(Page *page);

    /** Environment which owns this Freelist */
    Environment *m_env;

    /** the cached freelist entries */
    std::vector<FreelistEntry> m_entries;

    /** count the freelist hits */
    ham_u64_t m_count_hits;

    /** count the freelist misses */
    ham_u64_t m_count_misses;

    /** some freelist algorithm specific run-time data */
    GlobalStatistics m_perf_data;
};

#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a Page
 */
HAM_PACK_0 struct HAM_PACK_1 PFreelistPayload
{
  /** "real" address of the first bit */
  ham_u64_t _start_address;

  /** address of the next freelist page */
  ham_u64_t _overflow;

  /**
   * 'zero': must be 0; serves as a doublecheck we're not
   * processing an old-style 16-bit freelist page, where this
   * spot would have the ham_u16_t _max_bits, which would
   * always != 0 ...
   */
  ham_u16_t _zero;

  ham_u16_t _reserved;

  /** maximum number of bits for this page */
  ham_u32_t _max_bits;

  /** number of already allocated bits in the page */
  ham_u32_t _allocated_bits;

  /**
   * The persisted statistics.
   *
   * Note that a copy is held in the nonpermanent section of
   * each freelist entry; after all, it's ludicrous to keep
   * the cache clogged with freelist pages which our
   * statistics show are useless given our usage patterns
   * (determined at run-time; this is meant to help many-insert,
   * few-delete usage patterns the most, while many-delete usage
   * patterns will benefit most from a good cache page aging system
   * (see elsewhere in the code) as that will ensure relevant
   * freelist pages stay in the cache for as long as we need
   * them. Meanwhile, we've complicated things a little here
   * as we need to flush statistics to the persistent page
   * memory when flushing a cached page.
   */
  PFreelistPageStatistics _statistics;

  /** the algorithm-specific payload starts here.  */
  ham_u8_t _bitmap[1];

} HAM_PACK_2;

#include "packstop.h"

/** get the size of the persistent freelist header (new style) */
#define freel_get_bitmap_offset()  (OFFSETOF(PFreelistPayload, _bitmap))

/** get the address of the first bitmap-entry of this page */
#define freel_get_start_address(fl)    (ham_db2h64((fl)->_start_address))

/** set the start-address */
#define freel_set_start_address(fl, s)   (fl)->_start_address=ham_h2db64(s)

/** get the maximum number of bits which are handled by this bitmap */
#define freel_get_max_bits(fl)       (ham_db2h32((fl)->_max_bits))

/** set the maximum number of bits which are handled by this bitmap */
#define freel_set_max_bits(fl, m)    (fl)->_max_bits=ham_h2db32(m)

/** get the number of currently used bits which are handled by this bitmap */
#define freel_get_allocated_bits(fl)   (ham_db2h32((fl)->_allocated_bits))

/** set the number of currently used bits which are handled by this bitmap */
#define freel_set_allocated_bits(fl, u)  (fl)->_allocated_bits=ham_h2db32(u)

/** get the address of the next overflow page */
#define freel_get_overflow(fl)       (ham_db2h_offset((fl)->_overflow))

/** set the address of the next overflow page */
#define freel_set_overflow(fl, o)    (fl)->_overflow=ham_h2db_offset(o)

/** get a PFreelistPayload from a Page */
#define page_get_freelist(p)      ((PFreelistPayload *)p->get_payload())

/** get the bitmap of the freelist */
#define freel_get_bitmap(fl)       (fl)->_bitmap

/** get the v1.1.0+ persisted entry performance statistics */
#define freel_get_statistics(fl)     &((fl)->_statistics)

} // namespace hamsterdb

#endif /* HAM_FREELIST_H__ */
