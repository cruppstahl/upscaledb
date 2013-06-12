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

#ifndef HAM_BITMAP_FREELIST_H__
#define HAM_BITMAP_FREELIST_H__

#include <vector>

#include "internal_fwd_decl.h"
#include "statistics.h"
#include "freelist.h"

namespace hamsterdb {

#define HAM_DAM_RANDOM_WRITE            1
#define HAM_DAM_SEQUENTIAL_INSERT       2

/**
 * The upper bound value which will trigger a statistics data rescale operation
 * to be initiated in order to prevent integer overflow in the statistics data
 * elements.
 */
#define HAM_STATISTICS_HIGH_WATER_MARK  0x7FFFFFFF /* could be 0xFFFFFFFF */

/* 
 * As we [can] support record sizes up to 4Gb, at least theoretically,
 * we can express this size range as a spanning aligned size range:
 * 1..N, where N = log2(4Gb) - log2(alignment). As we happen to know
 * alignment == 32, at least for all regular hamsterdb builds, our
 * biggest power-of-2 for the freelist slot count ~ 32-5 = 27, where 0
 * represents slot size = 1 alignment, 1 represents size of 2 * 32,
 * 2 ~ 4 * 32, and so on.
 *
 * EDIT:
 * In order to cut down on statistics management cost due to overhead
 * caused by having to keep up with the latest for VERY large sizes, we
 * cut this number down to support sizes up to a maximum size of 64Kb ~
 * 2^16, meaning any requests for more than 64Kb/CHUNKSIZE bytes is
 * sharing their statistics.
 *
 */
#define HAM_FREELIST_SLOT_SPREAD   (16-5+1) /* 1 chunk .. 2^(SPREAD-1) chunks */

/**
 * global freelist algorithm specific run-time info
 */
struct GlobalStatistics
{
  GlobalStatistics() {
    memset(this, 0, sizeof(*this));
  }

  ham_u32_t first_page_with_free_space[HAM_FREELIST_SLOT_SPREAD];
};

#include "packstart.h"

/**
 * We keep track of VERY first free slot index + free slot index
 * pointing at last (~ supposed largest) free range + 'utilization' of the
 * range between FIRST and LAST as a ratio of number of free slots in
 * there vs. total number of slots in that range (giving us a 'fill'
 * ratio) + a fragmentation indication, determined by counting the number
 * of freelist slot searches that FAILed vs. SUCCEEDed within the
 * first..last range, when the search begun at the 'first' position
 * (a FAIL here meaning the freelist scan did not deliver a free slot
 * WITHIN the first..last range, i.e. it has scanned this entire range
 * without finding anything suitably large).
 *
 * Note that the free_fill in here is AN ESTIMATE.
 */
typedef HAM_PACK_0 struct HAM_PACK_1 PFreelistSlotsizeStats
{
  ham_u32_t first_start;
  /* reserved: */
  ham_u32_t free_fill;
  ham_u32_t epic_fail_midrange;
  ham_u32_t epic_win_midrange;

  /** number of scans per size range */
  ham_u32_t scan_count;
  ham_u32_t ok_scan_count;

  /** summed cost ('duration') of all scans per size range.  */
  ham_u32_t scan_cost;
  ham_u32_t ok_scan_cost;

} HAM_PACK_2 PFreelistSlotsizeStats;

#include "packstop.h"


#include "packstart.h"

/**
 * freelist statistics as they are persisted on disc.
 *
 * Stats are kept with each freelist entry record, but we also keep
 * some derived data in the nonpermanent space with each freelist:
 * it's not required to keep a freelist page in cache just so the
 * statistics + our operational mode combined can tell us it's a waste
 * of time to go there.
 */
typedef HAM_PACK_0 struct HAM_PACK_1 PFreelistPageStatistics
{
  /**
   * k-way statistics which stores requested space slot size related data.
   *
   * The data is stored in @ref HAM_FREELIST_SLOT_SPREAD different buckets
   * which partition the statistical info across the entire space request
   * range by using a logarithmic partitioning function.
   *
   * That way, very accurate, independent statistics can be stores for both
   * small, medium and large sized space requests, so that the freelist hinter
   * can deliver a high quality search hint for various requests.
   */
  PFreelistSlotsizeStats per_size[HAM_FREELIST_SLOT_SPREAD];

  /**
   * (bit) offset which tells us which free slot is the EVER LAST
   * created one; after all, freelistpage:maxbits is a scandalously
   * optimistic lie: all it tells us is how large the freelist page
   * _itself_ can grow, NOT how many free slots we actually have
   * _alive_ in there.
   *
   * 0: special case, meaning: not yet initialized...
   */
  ham_u32_t last_start;

  /**
   * total number of available bits in the page ~ all the chunks which
   * actually represent a chunk in the DB storage space.
   *
   * (Note that a freelist can be larger (_max_bits) than the actual
   * number of storage pages currently sitting in the database file.)
   *
   * The number of chunks already in use in the database therefore ~
   * persisted_bits - _allocated_bits.
   */
  ham_u32_t persisted_bits;

  /**
   * count the number of insert operations where this freelist page
   * played a role
   */
  ham_u32_t insert_count;
  /**
   * count the number of delete operations where this freelist page
   * played a role
   */
  ham_u32_t delete_count;
  /**
   * count the number of times the freelist size was adjusted as new storage
   * space was added to the database.
   *
   * This can occur in two situations: either when a new page is allocated and
   * a part of it is marked as 'free' as it is not used up in its entirety, or
   * when a page is released (freed) which was previously allocated
   * without involvement of the freelist manager (this can happen when new
   * HUGE BOBs are inserted, then erased again).
   */
  ham_u32_t extend_count;

  /**
   * count the number of times a freelist free space search (alloc
   * operation) failed to find any suitably large free space in this
   * freelist page.
   */
  ham_u32_t fail_count;

  /**
   * count the number of find operations where this freelist page
   * played a role
   */
  ham_u32_t search_count;

  /**
   * Tracks the ascent of the various statistical counters in here in order
   * to prevent integer overflow.
   *
   * This is accomplished by tracking the summed hinting costs over time in
   * this variable and when that number surpasses a predetermined 'high
   * water mark', all statistics counters are 'rescaled', which scales
   * down all counters and related data so that new data can be added again
   * multiple times before the risk of integer overflow may occur again.
   *
   * The goal here is to balance usable statistical numerical data while
   * assuring integer overflows @e never happen for @e any of the statistics
   * items.
   */
  ham_u32_t rescale_monitor;

} HAM_PACK_2 PFreelistPageStatistics;

#include "packstop.h"


/**
 * an entry in the freelist cache
 */
struct FullFreelistEntry {
  /** the start address of this freelist page */
  ham_u64_t start_address;

  /** maximum bits in this page */
  ham_size_t max_bits;

  /** allocated bits in this page */
  ham_size_t allocated_bits;

  /** the page ID */
  ham_u64_t page_id;

  /** freelist algorithm specific run-time data */
  PFreelistPageStatistics perf_data;
};

class FullFreelistStatisticsHints;
class FullFreelistStatisticsGlobalHints;

/**
 * the freelist class structure
 */
class FullFreelist : public Freelist
{
  enum {
    kBlobAlignment = 32
  };

  public:
    /** Constructor */
    FullFreelist(Environment *env)
      : Freelist(env) {
    }

    /** mark a page in the file as "free" */
    virtual ham_status_t free_page(Page *page);

    /**
     * mark an area in the file as "free"
     * @note will assert that address and size are aligned!
     */
    virtual ham_status_t free_area(ham_u64_t address, ham_size_t size);

    /**
     * try to allocate (possibly aligned) space from the freelist,
     * where the allocated space should be positioned at or beyond
     * the given address.
     *
     * @note will assert that size is properly aligned!
     */
    virtual ham_status_t alloc_area(ham_size_t size, ham_u64_t *paddress) {
      return (alloc_area_impl(size, paddress, false, 0));
    }

    /** try to allocate an (aligned) page from the freelist */
    virtual ham_status_t alloc_page(ham_u64_t *paddr);

    /** returns the alignment for blobs */
    virtual int get_blob_alignment() const {
      return (kBlobAlignment);
    }

    /** get a pointer to the environment (reqd for full_freelist_stats) */
    // TODO remove this
    Environment *get_env() {
      return (m_env);
    }

  private:
    friend class FullFreelistStatistics;

    /** @note The lower_bound_address is assumed to be aligned. */
    ham_status_t alloc_area_impl(ham_size_t size, ham_u64_t *paddr,
                bool aligned, ham_u64_t lower_bound_address);

    /** Retrieves the first freelist entry */
    FullFreelistEntry *get_entries() {
      return (m_entries.size() ? &m_entries[0] : 0);
    }

    /** Retrieves the number of freelist entries */
    // TODO make thisprivate or remove it
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

    /** retrieves the FullFreelistEntry which manages a specific
     * file address */
    FullFreelistEntry *get_entry_for_address(ham_u64_t address);

    /** returns maximum bits that fit in a regular page */
    ham_size_t get_entry_maxspan();

    /** adds 'new_count' Entries */
    void resize(ham_size_t new_count);

    /** allocates a page for a given entry */
    ham_status_t alloc_freelist_page(Page **ppage, FullFreelistEntry *entry);

    /** sets (or resets) all bits in a given range */
    ham_size_t set_bits(FullFreelistEntry *entry, PFullFreelistPayload *fp,
                    ham_size_t start_bit, ham_size_t size_bits,
                    bool set, FullFreelistStatisticsHints *hints);

    /** searches for a free bit array in the whole list */
    ham_s32_t search_bits(FullFreelistEntry *entry, PFullFreelistPayload *f,
                    ham_size_t size_bits, FullFreelistStatisticsHints *hints);

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
    ham_s32_t locate_sufficient_free_space(FullFreelistStatisticsHints *dst,
                    FullFreelistStatisticsGlobalHints *hints,
                    ham_s32_t start_index);

    /**
     * replacement for env->set_dirty() and page->set_dirty(); will dirty page
     * (or env) and also add the page (or header page) to the changeset
     */
    void mark_dirty(Page *page);

    /** the cached freelist entries */
    std::vector<FullFreelistEntry> m_entries;

    /** some freelist algorithm specific run-time data */
    GlobalStatistics m_perf_data;
};

#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a Page
 */
HAM_PACK_0 struct HAM_PACK_1 PFullFreelistPayload
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
#define freel_get_bitmap_offset()  (OFFSETOF(PFullFreelistPayload, _bitmap))

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

/** get a PFullFreelistPayload from a Page */
#define page_get_freelist(p)      ((PFullFreelistPayload *)p->get_payload())

/** get the bitmap of the freelist */
#define freel_get_bitmap(fl)       (fl)->_bitmap

/** get the v1.1.0+ persisted entry performance statistics */
#define freel_get_statistics(fl)     &((fl)->_statistics)

} // namespace hamsterdb

#endif /* HAM_BITMAP_FREELIST_H__ */
