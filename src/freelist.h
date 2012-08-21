/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "freelist_statistics.h"

namespace ham {

/**
 * an entry in the freelist cache
 */
struct FreelistEntry {
    /** the start address of this freelist page */
    ham_offset_t start_address;

    /** maximum bits in this page */
    ham_size_t max_bits;

    /** allocated bits in this page */
    ham_size_t allocated_bits;

    /** the page ID */
    ham_offset_t page_id;

    /**
     * freelist algorithm specific run-time data
     *
     * This is done as a union as it will reduce code complexity
     * significantly in the common freelist processing areas.
     */
    runtime_statistics_pagedata_t perf_data;
};


/**
 * the freelist class structure
 */
class Freelist
{
  public:

    /** constructor */
    Freelist(Environment *env)
      : m_env(env) { }

    /** destructor */
    ~Freelist() { flush_statistics(); }

    /**
     * mark an area in the file as "free"
     *
     * if 'overwrite' is true, will not assert that the bits are all
     * set to zero
     *
     * @note will assert that address and size are DB_CHUNKSIZE-aligned!
     */
    ham_status_t mark_free(Database *db, ham_offset_t address, ham_size_t size,
                    ham_bool_t overwrite);

    /**
     * try to allocate (possibly aligned) space from the freelist,
     * where the allocated space should be positioned at or beyond
     * the given address.
     *
     * @note will assert that size is DB_CHUNKSIZE-aligned!
     *
     * @note The lower_bound_address is assumed to be on a DB_CHUNKSIZE
     * boundary at least. @a aligned space will end up at a
     * @ref DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes boundary.
     * Regardless, the lower address bound check will be performed
     * on a DB_CHUNKSIZE boundary level anyhow.
     */
    ham_status_t alloc_area(ham_offset_t *addr_ref,
                        Database *db, ham_size_t size, bool aligned=false,
                        ham_offset_t lower_bound_address=0);

    /**
     * try to allocate an (aligned) page from the freelist
     */
    ham_status_t alloc_page(ham_offset_t *addr_ref, Database *db);

    /** get the number of freelist entries */
    size_t get_count() {
        return m_entries.size();
    }

    /** get the first freelist entry */
    FreelistEntry *get_entries() {
        return m_entries.size() ? &m_entries[0] : 0;
    }

    /** retrieve the Environment */
    Environment *get_env() {
        return m_env;
    }

  private:
    /**
     * setup / initialize the proper performance data for this
     * freelist page.
     *
     * Yes, this data will (very probably) be lost once the page is
     * removed from the in-memory cache, unless the currently active
     * freelist algorithm persists this data to disc.
     */
    void init_perf_data(FreelistEntry *entry, FreelistPayload *payload);

    /** flush page statistics */
    ham_status_t flush_statistics();

    /** lazily initializes the freelist structure */
    ham_status_t initialize();

    /** retrieves the FreelistEntry which manages a specific address */
    ham_status_t get_entry(FreelistEntry **entry_ref, ham_offset_t address);

    /** returns maximum bits that fit in a page */
    ham_size_t get_entry_maxspan();

    /** adds 'new_count' Entries */
    ham_status_t resize(ham_size_t new_count);

    /** allocates a page for a given entry */
    ham_status_t alloc_page(Page **page_ref, FreelistEntry *entry);

    /** sets (or resets) all bits in a given range */
    ham_size_t set_bits(FreelistEntry *entry, FreelistPayload *fp,
                bool overwrite, ham_size_t start_bit, ham_size_t size_bits,
                bool set, freelist_hints_t *hints);

    /** searches for a free bit array in the whole list */
    ham_s32_t search_bits(FreelistEntry *entry, FreelistPayload *f,
                ham_size_t size_bits, freelist_hints_t *hints);

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
    ham_s32_t locate_sufficient_free_space(freelist_hints_t *dst,
                    freelist_global_hints_t *hints, ham_s32_t start_index);

  private:
    /** the Environment */
    Environment *m_env;

    /** the cached freelist entries */
    std::vector<FreelistEntry> m_entries;
};

#define freel_cache_get_count(fc)   fc->get_count()
#define freel_cache_get_entries(fc) fc->get_entries()

#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a Page
 */
HAM_PACK_0 struct HAM_PACK_1 FreelistPayload
{
    /** "real" address of the first bit */
    ham_u64_t _start_address;

    /** address of the next freelist page */
    ham_offset_t _overflow;

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
    freelist_page_statistics_t _statistics;

    /** the algorithm-specific payload starts here.  */
    ham_u8_t _bitmap[1];
} HAM_PACK_2;

#include "packstop.h"

/** get the size of the persistent freelist header (new style) */
#define db_get_freelist_header_size()    (OFFSETOF(FreelistPayload, _bitmap))

/** get the address of the first bitmap-entry of this page */
#define freel_get_start_address(fl)      (ham_db2h64((fl)->_start_address))

/** set the start-address */
#define freel_set_start_address(fl, s)   (fl)->_start_address=ham_h2db64(s)

/** get the maximum number of bits which are handled by this bitmap */
#define freel_get_max_bits(fl)           (ham_db2h32((fl)->_max_bits))

/** set the maximum number of bits which are handled by this bitmap */
#define freel_set_max_bits(fl, m)        (fl)->_max_bits=ham_h2db32(m)

/** get the number of currently used bits which are handled by this bitmap */
#define freel_get_allocated_bits(fl)     (ham_db2h32((fl)->_allocated_bits))

/** set the number of currently used bits which are handled by this bitmap */
#define freel_set_allocated_bits(fl, u)  (fl)->_allocated_bits=ham_h2db32(u)

/** get the address of the next overflow page */
#define freel_get_overflow(fl)           (ham_db2h_offset((fl)->_overflow))

/** set the address of the next overflow page */
#define freel_set_overflow(fl, o)        (fl)->_overflow=ham_h2db_offset(o)

/** get a FreelistPayload from a Page */
#define page_get_freelist(p)            ((FreelistPayload *)p->get_payload())

/** get the bitmap of the freelist */
#define freel_get_bitmap(fl)             (fl)->_bitmap

/** get the v1.1.0+ persisted entry performance statistics */
#define freel_get_statistics(fl)         &((fl)->_statistics)

} // namespace ham

#endif /* HAM_FREELIST_H__ */
