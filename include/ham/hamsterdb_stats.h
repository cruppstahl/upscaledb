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
 * \file hamsterdb_stats.h
 * \brief Internal hamsterdb Embedded Storage statistics gathering and 
 * hinting functions.
 * \author Ger Hobbelt, ger@hobbelt.com
 *
 */

#ifndef HAM_HAMSTERDB_STATS_H__
#define HAM_HAMSTERDB_STATS_H__

#include <ham/hamsterdb.h>

#ifdef __cplusplus
extern "C" {
#endif 

struct ham_statistics_t;


/**
 * function prototype for the hamsterdb-specified @ref ham_statistics_t cleanup
 * function.
 *
 * @sa HAM_PARAM_GET_STATISTICS
 * @sa HAM_statistics_t
 * @sa ham_clean_statistics_datarec
 */
typedef void ham_free_statistics_func_t(struct ham_statistics_t *self);

/**
 * The upper bound value which will trigger a statistics data rescale operation
 * to be initiated in order to prevent integer overflow in the statistics data 
 * elements. 
 */
#define HAM_STATISTICS_HIGH_WATER_MARK                0x7FFFFFFF /* could be 0xFFFFFFFF */

/**
 * As we [can] support record sizes up to 4Gb, at least theoretically,
 * we can express this size range as a spanning DB_CHUNKSIZE size range:
 * 1..N, where N = log2(4Gb) - log2(DB_CHUNKSIZE). As we happen to know
 * DB_CHUNKSIZE == 32, at least for all regular hamsterdb builds, our
 * biggest power-of-2 for the freelist slot count ~ 32-5 = 27, where 0
 * represents slot size = 1 DB_CHUNKSIZE, 1 represents size of 2
 * DB_CHUNKSIZEs, 2 ~ 4 DB_CHUNKSIZEs, and so on.
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

/* -- equivalents of the statistics.h internal PERSISTED data structures -- */

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
typedef struct ham_freelist_slotsize_stats_t 
{
    ham_u32_t first_start;

    /* reserved: */
    ham_u32_t free_fill;
    ham_u32_t epic_fail_midrange;
    ham_u32_t epic_win_midrange;

    /** number of scans per size range */
    ham_u32_t scan_count;

    ham_u32_t ok_scan_count;
    
    /** summed cost ('duration') of all scans per size range */
    ham_u32_t scan_cost;
    ham_u32_t ok_scan_cost;

} ham_freelist_slotsize_stats_t;

/**
 * freelist statistics as they are persisted on disc.
 *
 * Stats are kept with each freelist entry record, but we also keep
 * some derived data in the nonpermanent space with each freelist:
 * it's not required to keep a freelist page in cache just so the
 * statistics + our operational mode combined can tell us it's a waste
 * of time to go there.
 */
typedef struct ham_freelist_page_statistics_t
{
    ham_freelist_slotsize_stats_t per_size[HAM_FREELIST_SLOT_SPREAD]; 

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

} ham_freelist_page_statistics_t;

/* -- end of equivalents of the statistics.h internal PERSISTED data 
 * structures -- */

/**
 * global freelist algorithm specific run-time info: per cache
 */
typedef struct ham_runtime_statistics_globdata_t
{
    /** number of scans per size range */
    ham_u32_t scan_count[HAM_FREELIST_SLOT_SPREAD];
    ham_u32_t ok_scan_count[HAM_FREELIST_SLOT_SPREAD];
    
    /** summed cost ('duration') of all scans per size range */
    ham_u32_t scan_cost[HAM_FREELIST_SLOT_SPREAD];
    ham_u32_t ok_scan_cost[HAM_FREELIST_SLOT_SPREAD];

    /** count the number of insert operations for this DB */
    ham_u32_t insert_count;
    ham_u32_t delete_count;
    ham_u32_t extend_count;
    ham_u32_t fail_count;
    ham_u32_t search_count;

    ham_u32_t insert_query_count;
    ham_u32_t erase_query_count;
    ham_u32_t query_count;

    ham_u32_t first_page_with_free_space[HAM_FREELIST_SLOT_SPREAD];

    /**
     *  Note: counter/statistics value overflow management:
     *
     * As the 'cost' numbers will be the fastest growing numbers of
     * them all, it is sufficient to check cost against a suitable
     * high water mark, and once it reaches that mark, to rescale
     * all statistics.
     *
     * Of course, we could have done without the rescaling by using
     * 64-bit integers for all statistics elements, but 64-bit
     * integers are not native to all platforms and incurr a (minor)
     * run-time penalty when used. It is felt that slower machines,
     * which are often 32-bit only, benefit from a compare plus
     * once-in-a-while rescale, as this overhead can be amortized
     * over a large multitude of statistics updates.
     *
     * How does the rescaling work?
     *
     * The statistics all are meant to represent relative numbers,
     * so uniformly scaling these numbers will not produce worse
     * results from the hinters -- as long as the scaling does not produce 
     * edge values (0 or 1) which destroy the significance of the numbers 
     * gathered thus far.
     *
     * I believe a rescale by a factor of 256 (2^8) is quite safe
     * when the high water mark is near the maxint (2^32) edge, even
     * when the cost number can be 100 times as large as the other
     * numbers in some regular use cases. Meanwhile, a division by
     * 256 will reduce the collected numeric values so much that
     * there is ample headroom again for the next 100K+ operations;
     * at an average monitored cost increase of 10-20 per
     * insert/delete trial and, for very large databases using an
     * overly conservative freelist management setting, ~50-200 trials 
     * per insert/delete API invocation (which should be a hint to the 
     * user that another DAM mode is preferred; after all, 'classical' 
     * is only there for backwards compatibility, and in the old
     * days, hamsterdb was a snail when you'd be storing 1M+ records
     * in a single DB table), the resulting statistics additive step
     * is a nominal worst case of 20 * 200 = 4000 cost points per
     * insert/delete.
     *
     * Assuming a high water mark for signed int, i.e. 2^31 ~ 2.14
     * billion, dividing ('rescaling') that number down to 2^(31-8) ~ 8M 
     * produces a headroom of ~ 2.13 billion points, which,
     * assuming the nominal worst case of a cost addition of 4000
     * points per insert/delete, implies new headroom for ~ 500K
     * insert/delete API operations.
     *
     * Which, in my book, is ample space. This also means that the
     * costs incurred by the rescaling can be amortized over 500K+
     * operations, resulting in an - on average - negligible overhead.
     *
     * So we can use 32-bits for all statistics counters quite
     * safely. Assuming our 'cost is the fastest riser' position holds for 
     * all use cases, that is.
     *
     * A quick analysis shows this to be probably true, even for
     * fringe cases (a mathematical proff would be nicer here, but
     * alas):
     * let's assume worst case, where we have a lot of trials
     * (testing each freelist page entry in a very long freelist,
     * i.e. a huge database table) which all fail. 'Cost' is
     * calculated EVERY TIME the innermost freelist search method is
     * invoked, i.e. when the freelist bitarray is inspected, and
     * both fail and success costs are immediately fed into the
     * statistics, so our worst case for the 'cost-is-fastest' lemma
     * would be a long trace of fail trials, which do NOT test the
     * freelist bitarrays, i.e. fails which are discarded in the outer layers, 
     * thanks to the hinters (global and per-entry) kicking in and preventing
     * those freelist bitarray scans. Assume then that all counters
     * have the same value, which would mean that the number of
     * fails has to be higher that the final cost, repetitively.
     *
     * This _can_ happen when the number of fail trials at the
     * per-entry outer level is higher than the cost of the final
     * (and only) freelist bitarray scan, which clocks in at a
     * nominal 4-10 points for success cases. However, those _outer_
     * fail trials are NOT counted and fed to the statistics, so
     * this case will only register a single, successful or failing,
     * trial - with cost.
     *
     * As long as the code is not changed to count those
     * hinter-induced fast rounds in the outer layers when searching
     * for a slot in the freelist, the lemma 'cost grows fastest'
     * holds, as any other possible 'worst case' will either
     * succeed quite quickly or fail through a bitarray scan, which
     * results in such fail rounds having associated non-zero, 1+
     * costs associated with them.
     *
     * To be on the safe side of it all, we accumulate all costs in
     * a special statistics counter, which is specifically designed
     * to be used for the high water mark minotring and subsequent
     * decision to rescale: rescale_monitor.
     */
    ham_u32_t rescale_monitor;

} ham_runtime_statistics_globdata_t;


/**
 * @defgroup ham_operation_types hamsterdb Database Operation Types
 * @{
 *
 * Indices into find/insert/erase specific statistics
 * 
 * @sa ham_statistics_t
 * @sa ham_runtime_statistics_opdbdata_t
 */
#define HAM_OPERATION_STATS_FIND           0
#define HAM_OPERATION_STATS_INSERT         1
#define HAM_OPERATION_STATS_ERASE          2

/** The number of operations defined for the statistics gathering process */
#define HAM_OPERATION_STATS_MAX            3

/**
 * @}
 */


/**
 * Statistics gathered specific per operation (find, insert, erase)
 */
typedef struct ham_runtime_statistics_opdbdata_t
{
    ham_u32_t btree_count;
    ham_u32_t btree_fail_count;
    ham_u32_t btree_cost;
    ham_u32_t btree_fail_cost;
        
    ham_offset_t btree_last_page_addr;

    /** 
     * number of consecutive times that this last page was produced as 
     * an answer ('sequential hits') 
     */
    ham_u32_t btree_last_page_sq_hits; 

    ham_u32_t query_count;

    ham_u32_t btree_hinting_fail_count;
    ham_u32_t btree_hinting_count;

    ham_u32_t aging_tracker;

} ham_runtime_statistics_opdbdata_t;

typedef struct ham_runtime_statistics_dbdata_t
{
    /* find/insert/erase */
    ham_runtime_statistics_opdbdata_t op[HAM_OPERATION_STATS_MAX]; 

    /**
     * common rescale tracker as the rescaling is done on all operations data
     * at once, so these remain 'balanced'.
     * 
     * Fringe case consideration: when there's, say, a lot of FIND going
     * on with a few ERASE operations in between, is it A Bad Thing that
     * the ERASE stats risc getting rescaled to almost nil then? Answer: NO.
     * Because there's a high probability that the last ERASE btree leaf
     * node isn't in cache anymore anyway -- unless it's the same one
     * as used by FIND.
     *
     * The reason we keep track of 3 different leaf nodes is only so we 
     * can supply good hinting in scanerios where FIND, INSERT and/or
     * ERASE are mixed in reasonable ratios; keeping track of only a single
     * btree leaf would deny us some good hinting for the other operations.
     */
    ham_u32_t rescale_tracker;

    /**
     * Remember the upper and lower bound kays for this database; update them 
     * when we insert a new key, maybe even update them when we delete/erase 
     * a key.
     * 
     * These bounds are collected on the fly while searching (find()): they are
     * stored in here as soon as a find() operation hits either the lower or 
     * upper bound of the key range stored in the database.
     *
     * The purpose of storing these bounds is to speed up out-of-bounds 
     * key searches significantly: by comparing incoming keys with these 
     * bounds, we can immediately tell whether a key will have a change of 
     * being found or not, thus precluding the need to traverse the btree - 
     * which would produce the same answer in the end anyhow.
     *
     * WARNING: having these key (copies) in here means we'll need to 
     * clean them up when we close the database connection, or we'll risk 
     * leaking memory in the key->data here.
     *
     * NOTE #1: this is the humble beginning of what in a more sophisticated 
     * database server system would be called a 'histogram' (Oracle, etc.). 
     * Here we don't spend the effort to collect data for a full histogram, 
     * but merely collect info about the extremes of our stored key range.
     * 
     * NOTE #2: I'm pondering whether this piece of statistics gathering 
     * should be allowed to be turned off by the user 'because he knows best' 
     * where premium run-time performance requirements are at stake. 
     * Yet... The overhead here is a maximum of two key comparisons plus 
     * 2 key copies (which can be significant when we're talking about 
     * extended keys!) when we're producing find/insert/erase results which
     * access a btree leaf node which is positioned at the upper/lower edge of 
     * the btree key range.
     *
     * Hence, worst case happens for sure with tiny databases, as those will 
     * have ONE btree page only (root=leaf!) and the worst case is reduced 
     * to 1 key comparison + 1 key copy for any larger database, which spans 
     * two btree pages or more.
     *
     * To further reduce the worst case overhead, we also store the 
     * within-btree-node index of the upper/lower bound key: when this does 
     * not change, there is no need to compare the key - unless the key is 
     * overwritten, which is a special case of the insert operation.
     * 
     * @warning
     * The @a key data is allocated using the @ref ham_db_t allocator and the 
     * key data must be freed before the related @ref ham_db_t handle 
     * is closed or deleted.
     */
    ham_key_t lower_bound;
    ham_u32_t lower_bound_index;
    ham_offset_t lower_bound_page_address;
    ham_bool_t lower_bound_set;
    ham_key_t upper_bound;
    ham_u32_t upper_bound_index;
    ham_offset_t upper_bound_page_address;
    ham_bool_t upper_bound_set;

} ham_runtime_statistics_dbdata_t;

/**
 * This structure is a @e READ-ONLY data structure returned through invoking 
 * @ref ham_env_get_parameters or @ref ham_get_parameters with a 
 * @ref HAM_PARAM_GET_STATISTICS @ref ham_parameter_t entry.
 *
 * @warning
 * The content of this structure will be subject to change with each hamsterdb 
 * release; having it available in the public interface does @e not mean one 
 * can assume the data layout and/or content of the @ref ham_statistics_t 
 * structure to remain constant over multiple release version updates of 
 * hamsterdb.
 *
 * Also note that the data is exported to aid very advanced uses of hamsterdb 
 * only and is to be accessed in an exclusively @e read-only fashion.
 *
 * The structure includes a function pointer which will optionally be set 
 * by hamsterdb upon invoking @ref ham_env_get_parameters or 
 * @ref ham_get_parameters and this function should be invoked 
 * by the caller to release all memory allocated by hamsterdb in the 
 * @ref ham_statistics_t structure, and this action @e MUST be performed 
 * @e before the related @a env and/or @a db handles are either closed 
 * or deleted, whichever of these comes first in your application run-time flow.
 *
 * The easiest way to invoke this @ref ham_clean_statistics_datarec function 
 * (when it is set), is to use the provided @ref ham_free_statistics() macro.
 *
 * @sa HAM_PARAM_GET_STATISTICS
 * @sa ham_clean_statistics_datarec
 * @sa ham_get_parameters
 * @sa ham_env_get_parameters
 */
typedef struct ham_statistics_t
{
    /** Number of freelist pages (and statistics records) known to hamsterdb */
    ham_size_t freelist_record_count;

    /** Number of freelist statistics records allocated in this structure */
    ham_size_t freelist_stats_maxalloc;

    /** The @a freelist_stats_maxalloc freelist statistics records */
    ham_freelist_page_statistics_t *freelist_stats;

    /** The @ref ham_db_t specific statistics */
    ham_runtime_statistics_dbdata_t db_stats;

    /** The @ref ham_env_t statistics, a.k.a. 'global statistics' */
    ham_runtime_statistics_globdata_t global_stats;

    /**
     * [input] Whether the freelist statistics should be gathered (this is 
     * a relatively costly operation)
     * [output] will be reset when the freelist statistics have been gathered
     */
    unsigned dont_collect_freelist_stats: 1;

    /**
     * [input] Whether the @ref ham_db_t specific statistics should be gathered
     * [output] will be reset when the db specific statistics have been gathered
     */
    unsigned dont_collect_db_stats: 1;

    /**
     * [input] Whether the @ref ham_env_t statistics (a.k.a. 'global 
     * statistics') should be gathered
     * [output] will be reset when the global statistics have been gathered
     */
    unsigned dont_collect_global_stats: 1;

    /**
     * A reference to a hamsterdb-specified @e optional data cleanup function.
     *
     * @warning
     * The user @e MUST call this cleanup function when it is set by 
     * hamsterdb, preferrably through invoking 
     * @ref ham_clean_statistics_datarec() as that function will check if
     * this callback has been set or not before invoking it.
     * 
     * @sa ham_clean_statistics_datarec
     */
    ham_free_statistics_func_t *_free_func;

    /* 
     * internal use: this element is set by hamsterdb and to be used by the 
     * @a _free_func callback.
     */
    void *_free_func_internal_arg;

} ham_statistics_t;

/**
 * Invoke the optional @ref ham_statistics_t content cleanup function. 
 * 
 * This function will check whether the @ref ham_statistics_t free/cleanup 
 * callback has been set or not before invoking it.
 *
 * @param s A pointer to a valid @ref ham_statistics_t data structure. 'Valid' 
 * means you must call this @ref ham_free_statistics() macro @e after having 
 * called @ref ham_env_get_parameters or @ref ham_get_parameters with 
 * a @ref HAM_PARAM_GET_STATISTICS @ref ham_parameter_t entry which had this 
 * @ref ham_statistics_t reference @a s attached and @e before either the 
 * related @ref ham_db_t or @ref ham_env_t handles are closed (@ref 
 * ham_env_close/@ref ham_close) or deleted (@ref ham_env_delete/@ref 
 * ham_delete).
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a s pointer is NULL
 *
 * @sa HAM_PARAM_GET_STATISTICS
 * @sa ham_clean_statistics_datarec
 * @sa ham_get_parameters
 * @sa ham_env_get_parameters
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_clean_statistics_datarec(ham_statistics_t *stats);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_STATS_H__ */

