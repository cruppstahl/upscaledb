/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * freelist structures, functions and macros
 *
 */

#ifndef HAM_STATISTICS_H__
#define HAM_STATISTICS_H__

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
	ham_u32_t delete_count;
	ham_u32_t extend_count;
	ham_u32_t fail_count;
	ham_u32_t search_count;

	ham_u32_t rescale_monitor;

} HAM_PACK_2 freelist_page_statistics_t;

#include "packstop.h"



/**
 * freelist algorithm specific run-time info per freelist entry (page)
 */
typedef struct runtime_statistics_pagedata_t
{
	freelist_page_statistics_t _persisted_stats;

	ham_bool_t _dirty;
} runtime_statistics_pagedata_t;





/**
get a reference to the statistics data of the given operation 
*/
#define db_get_op_perf_data(db, o)				((db_get_db_perf_data(db))->op + (o))









#define stats_memmove_cost(size)			(((size) + 512 - 1) / 512)




typedef struct
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

} freelist_hints_t;


typedef struct
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

    /** [input] the size of the slot we're looking for */
    ham_size_t size_bits;

	/** [input] the size of a freelist page (in chunks) */
	ham_size_t freelist_pagesize_bits;

} freelist_global_hints_t;









/* forward references */
struct freelist_entry_t;
struct freelist_payload_t;
struct ham_page_t;






typedef struct
{
	ham_u32_t original_flags; /* [in] insert flags */
	ham_u32_t flags; /* [in/out] find flags */

	ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
	ham_bool_t key_is_out_of_bounds; /* [out] */
	ham_bool_t try_fast_track;	/* [out] check specified btree leaf node page first */

	ham_size_t cost; /* [feedback] cost tracking for our statistics */

} find_hints_t;

struct ham_bt_cursor_t;

typedef struct
{
	ham_u32_t original_flags; /* [in] insert flags */
	ham_u32_t flags; /* [in/out] insert flags; may be modified while performing the insert */
	struct ham_bt_cursor_t *cursor; /* [in] */

	ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
	ham_bool_t try_fast_track;	/* [out] check specified btree leaf node page first */
	ham_bool_t force_append;	/* [out] not (yet) part of the hints, but a result from it: informs insert_nosplit() that the insertion position (slot) is already known */
	ham_bool_t force_prepend;	/* [out] not (yet) part of the hints, but a result from it: informs insert_nosplit() that the insertion position (slot) is already known */
	ham_size_t cost; /* [feedback] cost tracking for our statistics */
	struct ham_page_t *processed_leaf_page; /* [feedback] the btree leaf page which received the inserted key */
	ham_s32_t processed_slot;	/* [feedback] >=0: entry slot index of the key within the btree leaf node; -1: failure condition */

} insert_hints_t;

typedef struct
{
	ham_u32_t original_flags; /* [in] insert flags */
	ham_u32_t flags; /* [in/out] insert flags; may be modified while performing the insert */
	struct ham_bt_cursor_t *cursor; /* [in] */

	ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
	ham_bool_t key_is_out_of_bounds; /* [out] */
	ham_bool_t try_fast_track;	/* [out] check specified btree leaf node page first */
	ham_size_t cost; /* [feedback] cost tracking for our statistics */
	struct ham_page_t *processed_leaf_page; /* [feedback] the btree leaf page which received the inserted key */
	ham_s32_t processed_slot;	/* [feedback] >=0: entry slot index of the key within the btree leaf node; -1: failure condition */

} erase_hints_t;





/**
 * statistics gathering calls for insert / erase (delete) / find
 */
extern void
db_update_global_stats_find_query(ham_db_t *db, ham_size_t key_size);
extern void
db_update_global_stats_insert_query(ham_db_t *db, ham_size_t key_size, ham_size_t record_size);
extern void
db_update_global_stats_erase_query(ham_db_t *db, ham_size_t key_size);

extern void
db_update_freelist_globalhints_no_hit(ham_db_t *db, struct freelist_entry_t *entry, freelist_hints_t *hints);





extern void
db_update_freelist_stats_edit(ham_db_t *db, struct freelist_entry_t *entry, 
					struct freelist_payload_t *f, 
					ham_u32_t position, 
					ham_size_t size_bits, 
					ham_bool_t free_these, 
					ham_u16_t mgt_mode);

extern void
db_update_freelist_stats_fail(ham_db_t *db, struct freelist_entry_t *entry,
					struct freelist_payload_t *f, 
					freelist_hints_t *hints);

extern void
db_update_freelist_stats(ham_db_t *db, struct freelist_entry_t *entry,
					struct freelist_payload_t *f, 
					ham_u32_t position, 
					freelist_hints_t *hints);

extern void
db_get_freelist_entry_hints(freelist_hints_t *dst, ham_db_t *db, 
						struct freelist_entry_t *entry);

extern void
db_get_global_freelist_hints(freelist_global_hints_t *dst, ham_db_t *db);




extern void 
stats_update_fail_oob(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)
#define stats_update_erase_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)


extern void 
stats_update_fail(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)
#define stats_update_insert_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_INSERT, db, (hints)->cost, (hints)->try_fast_track)
#define stats_update_erase_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)


extern void 
stats_update(int op, ham_db_t *db, struct ham_page_t *page, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find(db, page, hints)	stats_update(HAM_OPERATION_STATS_FIND, db, page, (hints)->cost, (hints)->try_fast_track)
#define stats_update_insert(db, page, hints)	stats_update(HAM_OPERATION_STATS_INSERT, db, page, (hints)->cost, (hints)->try_fast_track)
#define stats_update_erase(db, page, hints)	stats_update(HAM_OPERATION_STATS_ERASE, db, page, (hints)->cost, (hints)->try_fast_track)

extern void 
stats_page_is_nuked(ham_db_t *db, struct ham_page_t *page, ham_bool_t split);

extern void 
stats_update_any_bound(ham_db_t *db, struct ham_page_t *page, ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot);


extern void 
btree_find_get_hints(find_hints_t *hints, ham_db_t *db, ham_key_t *key);
extern void 
btree_insert_get_hints(insert_hints_t *hints, ham_db_t *db, ham_key_t *key);
extern void 
btree_erase_get_hints(erase_hints_t *hints, ham_db_t *db, ham_key_t *key);

extern void
stats_init_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);
extern void
stats_flush_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);
extern void
stats_trash_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);

extern void
stats_init_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata);
extern void
stats_flush_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata, ham_bool_t last_in_env);
extern void
stats_trash_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata);


extern ham_status_t
stats_fill_ham_statistics_t(ham_env_t *env, ham_db_t *db, ham_statistics_t *dst);





#if HAM_DEBUG
extern void 
cache_report_history(ham_db_t *db);

extern void 
cache_check_history(ham_db_t *db, struct ham_page_t *page, int state);
#endif

/**
internal monitoring:
*/
extern void 
cache_push_history(struct ham_page_t *page, int state);



#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_FREELIST_H__ */
