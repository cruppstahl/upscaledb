/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief freelist statistics structures, functions and macros
 *
 */

#ifndef HAM_FREELIST_STATISTICS_H__
#define HAM_FREELIST_STATISTICS_H__

#include "internal_fwd_decl.h"

#include <ham/hamsterdb_stats.h>




#ifdef __cplusplus
extern "C" {
#endif 



#include "packstart.h"

/**
We keep track of VERY first free slot index + free slot index 
pointing at last (~ supposed largest) free range + 'utilization' of the
range between FIRST and LAST as a ratio of number of free slots in
there vs. total number of slots in that range (giving us a 'fill'
ratio) + a fragmentation indication, determined by counting the number
of freelist slot searches that FAILed vs. SUCCEEDed within the
first..last range, when the search begun at the 'first' position
(a FAIL here meaning the freelist scan did not deliver a free slot
WITHIN the first..last range, i.e. it has scanned this entire range
without finding anything suitably large).

Note that the free_fill in here is AN ESTIMATE.
 */
typedef HAM_PACK_0 struct HAM_PACK_1 freelist_slotsize_stats_t 
{
	ham_u32_t first_start;
	/* reserved: */
	ham_u32_t free_fill;
	ham_u32_t epic_fail_midrange;
	ham_u32_t epic_win_midrange;

	/**
	 * number of scans per size range
	 */
	ham_u32_t scan_count;
	ham_u32_t ok_scan_count;
	
    /**
     * summed cost ('duration') of all scans per size range.
     */
	ham_u32_t scan_cost;
	ham_u32_t ok_scan_cost;

} HAM_PACK_2 freelist_slotsize_stats_t;

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
typedef HAM_PACK_0 struct HAM_PACK_1 freelist_page_statistics_t
{
	/**
	k-way statistics store which stores requested space slot size related data.

	The data is stored in @ref HAM_FREELIST_SLOT_SPREAD different buckets
	which partition the statistical info across the entire space request range
	by using a logarithmic partitioning function.

	That way, very accurate, independent statistics can be stores for both 
	small, medium and large sized space requests, so that the freelist hinter
	can deliver a high quality search hint for various requests.
	*/
	freelist_slotsize_stats_t per_size[HAM_FREELIST_SLOT_SPREAD]; 

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
     * count the number of times the freelist size was adjusted as new storage space
	 * was added to the database.
	 *
	 * This can occur in two situations: either when a new page is allocated and
	 * a part of it is marked as 'free' as it is not used up in its entirety, or
	 * when a page is released (freed) which was previously allocated without involvement
	 * of the freelist manager (this can happen when new HUGE BOBs are inserted, then
	 * erased again).
     */
	ham_u32_t extend_count;
    /**
     * count the number of times a freelist free space search (alloc operation) failed
	 * to find any suitably large free space in this freelist page.
     */
	ham_u32_t fail_count;
    /**
     * count the number of find operations where this freelist page
     * played a role
     */
	ham_u32_t search_count;

	/**
	Tracks the ascent of the various statistical counters in here in order to prevent 
	integer overflow.

	This is accomplished by tracking the summed hinting costs over time in this variable
	and when that number surpasses a predetermined 'high water mark', all statistics
	counters are 'rescaled', which scales down all counters and related data so that new
	data can be added again multiple times before the risk of integer overflow may occur
	again.

	The goal here is to balance usable statistical numerical data while assuring integer
	overflows @e never happen for @e any of the statistics items.
	*/
	ham_u32_t rescale_monitor;

} HAM_PACK_2 freelist_page_statistics_t;

#include "packstop.h"



/**
 * freelist algorithm specific run-time info per freelist entry (page)
 */
struct runtime_statistics_pagedata_t
{
	freelist_page_statistics_t _persisted_stats;

	ham_bool_t _dirty;
};








struct freelist_hints_t
{
	/** [in/out] INCLUSIVE bound: where free slots start */
	ham_u32_t startpos; 
	/** [in/out] EXCLUSIVE bound: where free slots end */
	ham_u32_t endpos;	
	/** [in/out] suggested search/skip probe distance */
	ham_u32_t skip_distance;
	/** [in/out] suggested DAM mgt_mode for the remainder of this request */
	ham_u16_t mgt_mode;
	/** [input] whether or not we are looking for aligned storage */
	ham_bool_t aligned;
    /** [input] the lower bound address of the slot we're looking for. Usually zero(0). */
    ham_offset_t lower_bound_address;
	/** [input] the size of the slot we're looking for */
	ham_size_t size_bits;
	/** [input] the size of a freelist page (in chunks) */
	ham_size_t freelist_pagesize_bits;
    /** 
	[input] the number of (rounded up) pages we need to fulfill the request; 1 for
			 'regular' (non-huge) requests. 
			 
			 Cannot be 0, as that is only correct for a zero-length request.
	*/
    ham_size_t page_span_width;

	/** [feedback] cost tracking for our statistics */
	ham_size_t cost; 
};


struct freelist_global_hints_t
{
    /**
	[in/out] INCLUSIVE bound: at which freelist page entry to start
             looking 
	*/
    ham_u32_t start_entry;

    /**
	[in/out] how many entries to skip
     *
     * You'd expect this to be 1 all the time, but in some modes it is
     * expected that a 'semi-random' scan will yield better results;
     * especially when we combine that approach with a limited number of
     * rounds before we switch to SEQUENTIAL+FAST mode.
     *
     * By varying the offset (start) for each operation we then are
     * assured that all freelist pages will be perused once in a while,
     * while we still cut down on freelist entry scanning quite a bit.
     */
    ham_u32_t skip_step;

    /** [in/out] and the accompanying start offset for the SRNG */
    ham_u32_t skip_init_offset;

    /**
	[in/out] upper bound on number of rounds ~ entries to scan: when
     to stop looking 
	 */
    ham_u32_t max_rounds;

	/** [in/out] suggested DAM mgt_mode for the remainder of this request */
    ham_u16_t mgt_mode;

    /** 
	[output] whether or not we are looking for a chunk of storage 
	         spanning multiple pages ('huge blobs'): lists the number 
			 of (rounded up) pages we need to fulfill the request; 1 for
			 'regular' (non-huge) requests. 
			 
			 Cannot be 0, as that is only correct for a zero-length request.
	*/
    ham_size_t page_span_width;

    /** [input] whether or not we are looking for aligned storage */
    ham_bool_t aligned;

    /** [input] the lower bound address of the slot we're looking for. Usually zero(0). */
    ham_offset_t lower_bound_address;

    /** [input] the size of the slot we're looking for */
    ham_size_t size_bits;

	/** [input] the size of a freelist page (in chunks) */
	ham_size_t freelist_pagesize_bits;
};













extern void
db_update_freelist_globalhints_no_hit(ham_device_t *dev, ham_env_t *env, 
					freelist_entry_t *entry, freelist_hints_t *hints);



extern void
db_update_freelist_stats_edit(ham_device_t *dev, ham_env_t *env, freelist_entry_t *entry, 
					freelist_payload_t *f, 
					ham_u32_t position, 
					ham_size_t size_bits, 
					ham_bool_t free_these, 
					freelist_hints_t *hints);

extern void
db_update_freelist_stats_fail(ham_device_t *dev, ham_env_t *env, freelist_entry_t *entry,
					freelist_payload_t *f, 
					freelist_hints_t *hints);

extern void
db_update_freelist_stats(ham_device_t *dev, ham_env_t *env, freelist_entry_t *entry,
					freelist_payload_t *f, 
					ham_u32_t position, 
					freelist_hints_t *hints);

extern void
db_get_freelist_entry_hints(freelist_hints_t *dst, ham_device_t *dev, ham_env_t *env, 
						freelist_entry_t *entry);

extern void
db_get_global_freelist_hints(freelist_global_hints_t *dst, ham_device_t *dev, ham_env_t *env);




extern ham_status_t
stats_fill_freel_statistics_t(ham_env_t *env, ham_statistics_t *dst);






#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_FREELIST_H__ */
