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

#ifndef HAM_FREELIST_H__
#define HAM_FREELIST_H__

#include <vector>

#include "ham/hamsterdb_int.h"

#include "endianswap.h"
#include "freelist_stats.h"
#include "page.h"

namespace hamsterdb {

class LocalEnvironment;

/*
 * An entry in the Freelist cache
 *
 * The freelist is spread over many pages in the Environment. The freelist
 * therefore caches the meta-information about those pages. This structure
 * is a single cache entry describing one freelist page.
 */
struct FreelistEntry {
  // the start address of this freelist page
  ham_u64_t start_address;

  // maximum bits in this page
  ham_size_t max_bits;

  // free bits in this page
  ham_size_t free_bits;

  // the page ID
  ham_u64_t pageid;

  // freelist specific run-time data and usage statistics
  PFreelistPageStatistics perf_data;
};


/*
 * The Freelist class
 */
class Freelist
{
  public:
    enum {
      // Every blob (and page) is aligned to this:
      kBlobAlignment        = 32,

      // The (deprecated) Data Access Mode optimizing for random writes
      kDamRandomWrite       = 1,

      // ... and for sequential inserts
      kDamSequentialInsert  = 2
    };

    // Constructor
    Freelist(LocalEnvironment *env)
      : m_env(env), m_count_hits(0), m_count_misses(0) {
    }

    // Adds a page to the freelist
    ham_status_t free_page(Page *page);

    // Adds an arbitrary file area to the freelist
    // Asserts that address and size are aligned to |kBlobAlignment|!
    ham_status_t free_area(ham_u64_t address, ham_size_t size);

    // Tries to allocate an (aligned) page from the freelist.
    // Returns 0 in |paddress| if there was not enough free space to satisfy
    // the request.
    ham_status_t alloc_page(ham_u64_t *paddress);

    // Tries to allocate (possibly aligned) space from the freelist.
    // Returns 0 in |paddress| if there was not enough free space to satisfy
    // the request.
    // Asserts that size is aligned to |kBlobAlignment|.
    ham_status_t alloc_area(ham_size_t size, ham_u64_t *paddress) {
      return (alloc_area_impl(size, paddress, false, 0));
    }

    // Truncates the page at the given |address| and removes it
    // from the freelist.
    // Asserts that |address| is pagesize-aligned.
    ham_status_t truncate_page(ham_u64_t address);

    // Returns true if the page at |address| is free, otherwise false
    // Asserts that |address| is pagesize-aligned.
    bool is_page_free(ham_u64_t address);

    // Fills in the collected metrics and usage statistics
    void get_metrics(ham_env_metrics_t *metrics) const;

  private:
    friend class FreelistStatistics;

    // Returns a pointer to the environment (reqd for freelist_stats)
    LocalEnvironment *get_env() {
      return (m_env);
    }

    // Actual implementation for the alloc*-functions.
    // The lower_bound_address is assumed to be aligned.
    ham_status_t alloc_area_impl(ham_size_t size, ham_u64_t *paddr,
                bool aligned, ham_u64_t lower_bound_address);

    // Returns the first freelist entry
    // TODO required?
    FreelistEntry *get_entries() {
      return (m_entries.size() ? &m_entries[0] : 0);
    }

    // Returns the number of freelist entries
    // TODO merge with get_entries(), return std::vector<>
    size_t get_count() const {
      return (m_entries.size());
    }

  private:
    // Lazily initializes the freelist structure by reading the linked list
    // of freelist pages and filling the cache
    ham_status_t initialize();

    // Returns the performance usage statistics
    GlobalStatistics *get_global_statistics() {
      return (&m_perf_data);
    }

    // Returns the FreelistEntry which manages a specific file address
    FreelistEntry *get_entry_for_address(ham_u64_t address);

    // Returns the maximum bits that fit in a regular page
    ham_size_t get_entry_maxspan();

    // Resizes the cache and adds |new_count| entries
    void resize(ham_size_t new_count);

    // Allocates a freelist page for the specified |entry|
    ham_status_t alloc_freelist_page(Page **ppage, FreelistEntry *entry);

    // Sets (or resets) all bits in a given range, depending on |set|
    ham_size_t set_bits(FreelistEntry *entry, PFreelistPayload *fp,
                    ham_size_t start_bit, ham_size_t size_bits,
                    bool set, FreelistStatistics::Hints *hints);

    // Checks if the specified bits are set; if not, returns -1. Otherwise
    // returns the number of checked bits (= |size_bits|)
    ham_size_t check_bits(FreelistEntry *entry, PFreelistPayload *fp,
                    ham_size_t start_bit, ham_size_t size_bits);

    // Searches for a free bit array in the whole list
    ham_s32_t search_bits(FreelistEntry *entry, PFreelistPayload *f,
                    ham_size_t size_bits, FreelistStatistics::Hints *hints);

    // Report if the requested size can be obtained from the given freelist
    // page.
    //
    // Always make use of the collected statistics, but act upon it in
    // different ways, depending on our current 'mgt_mode' setting.
    //
    // Note: the answer is an ESTIMATE, _not_ a guarantee.
    //
    // Return the first cache entry index from now (start_index) where you
    // have a chance of finding a free slot.
    //
    // Note: the initial round with have start_index == -1 incoming.
    //
    // Return -1 to signal there's no chance at all.
    ham_s32_t locate_sufficient_free_space(FreelistStatistics::Hints *dst,
                    FreelistStatistics::GlobalHints *hints,
                    ham_s32_t start_index);

    // Replacement for env->set_dirty() and page->set_dirty(); will dirty page
    // (or env) and also add the page (or header page) to the changeset
    void mark_dirty(Page *page);

    // Environment which owns this Freelist
    LocalEnvironment *m_env;

    // the cached freelist entries
    std::vector<FreelistEntry> m_entries;

    // count the freelist hits
    ham_u64_t m_count_hits;

    // count the freelist misses
    ham_u64_t m_count_misses;

    // freelist specific run-time and performance data
    GlobalStatistics m_perf_data;
};

#include "packstart.h"

/*
 * a freelist-payload; it spans the persistent part of a Page
 */
HAM_PACK_0 class HAM_PACK_1 PFreelistPayload
{
  public:
    // Returns a PFreelistPayload from a Page
    static PFreelistPayload *from_page(Page *page) {
      return ((PFreelistPayload *)page->get_payload());
    }

    // Returns the offset of the persistent freelist header
    static ham_size_t get_bitmap_offset() {
      return (OFFSETOF(PFreelistPayload, m_bitmap));
    }

    // Returns the "real" address (in the database file) of the
    // first bit in the bitmap
    ham_u64_t get_start_address() const {
      return (ham_db2h64(m_start_address));
    }

    // Sets the start address
    void set_start_address(ham_u64_t address) {
      m_start_address = ham_h2db64(address);
    }

    // Returns the address of the next overflow page
    ham_u64_t get_overflow() const {
      return (ham_db2h_offset(m_overflow));
    }

    // Sets the address of the next overflow page
    void set_overflow(ham_u64_t address) {
      m_overflow = ham_h2db_offset(address);
    }

    // Returns the maximum number of bits which are stored in this bitmap
    ham_u32_t get_max_bits() const {
      return (ham_db2h32(m_max_bits));
    }

    // Sets the maximum number of bits which are stored in this bitmap
    void set_max_bits(ham_u32_t bits) {
      m_max_bits = ham_h2db32(bits);
    }

    // Returns the number of currently used bits which are stored in this bitmap
    ham_u32_t get_free_bits() const {
      return (ham_db2h32(m_free_bits));
    }

    // Sets the number of currently used bits which are strored in this bitmap
    void set_free_bits(ham_u32_t bits) {
      m_free_bits = ham_h2db32(bits);
    }

    // Returns the persisted performance statistics
    PFreelistPageStatistics *get_statistics() {
      return (&m_statistics);
    }

    // Returns the bitmap data
    ham_u8_t *get_bitmap() {
      return (m_bitmap);
    }

  private:
    // The "real" address of the first bit in the file mapping
    ham_u64_t m_start_address;

    // address of the next freelist page
    ham_u64_t m_overflow;

    // 'zero': must be 0; serves as a doublecheck we're not
    // processing an old-style 16-bit freelist page, where this
    // spot would have the ham_u16_t _max_bits, which would
    // always != 0 ...
    // TODO remove this with the next file format upgrade
    ham_u16_t m_zero;

    // Reserved value, inserted for padding
    ham_u16_t m_reserved;

    // Maximum number of bits for this page
    ham_u32_t m_max_bits;

    // Number of free bits in the page
    ham_u32_t m_free_bits;

    // The persisted statistics for this page
    // TODO double-check if this can be removed?
    PFreelistPageStatistics m_statistics;

    // The freelist bitmap, 1 bit corresponds to Freelist::kBlobAlignment bytes
    ham_u8_t m_bitmap[1];

} HAM_PACK_2;

#include "packstop.h"


} // namespace hamsterdb

#endif /* HAM_FREELIST_H__ */
