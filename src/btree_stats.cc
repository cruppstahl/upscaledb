/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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
#include <stdio.h>

#include <ham/hamsterdb_stats.h>

#include "btree.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "endianswap.h"
#include "env.h"
#include "error.h"
#include "freelist_statistics.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "util.h"

namespace ham {

/*
 *  TODO statistics gatherer/hinter:
 *
 * keep track of two areas' 'utilization':
 *
 * 1) for fast/uberfast mode, keep track of the LAST free zone, i.e.
 * the free zone at the end;
 * ONLY move the start marker for that BACKWARDS when we get a freeing
 * op just before it OR when we specifically scan backwards to find the
 * adjusted start after lots of fragmented delete ops and we're nog in
 * turbo-fast mode: this would save space.
 *
 * 2) keep track of the marker where the FIRST free chunk just was,
 * i.e. before which' point there definitely is NO free space. Use this
 * marker as the start for a free-space-search when in
 * space-saving/classic mode; use the other 'start of free space at end
 * of the page' marker as the starting point for (uber-)fast searches.
 *
 * 'utilization': keep track of the number of free chunks and allocated
 * chunks in the middle zone ~ the zone between FIRST and LST marker:
 * the ratio is a measure of the chance we expect to have when searching
 * this zone for a free spot - by not coding/designing to cover a
 * specific pathological case (add+delete @ start & end of store and
 * repeating this cycle over and over causing the DB to 'jump all over
 * the place' in classic mode; half of the free slot searches would be
 * 'full scans' of the freelist then. Anyway, we do not wish to code for
 * this specific pathological case, as such code will certainly
 * introduce another pathological case instead, which should be fixed,
 * resulting in expanded code and yet another pathological case fit for
 * the new code situation, etc.etc. ad nauseam. Instead, we use
 * statistical measures to express an estimate, i.e. the chance that we
 * might need to scan a large portion of the freelist when we
 * run in classic spacesaving insert mode, and apply that statistical
 * data to the hinter, using the current mode.
 *
 * -- YES, that also means we are able to switch freelist scanning
 * mode, and thus speed- versus storage consumption hints, on a per-insert basis: a single
 * database can mix slow but spacesaving record inserts for those times / tables when we do not need the extra oemph, while other inserts can
 * be told (using the flags in the API calls) to act optimized for
 *  - none (classic) --> ~ storage space saving
 *  - storage space saving
 *  - insertion speed By using 2 bits: 1 for speed and one for
 * uber/turbo or regular, we can have 3 or 4 modes, where a 'speedy
 * space saving' mode might imply we're using those freelist stats to
 * decide whether to start the scan at the end or near the start of the
 * freelist, in order to arrive at a 'reasonable space utilization'
 * while keeping up the speed, at least when determined over multiple
 * inserts.
 *
 * And mode 4 can be used to enforce full scan or something like that:
 * this can be used to improve the statistics as those are not persisted
 * on disc.
 *
 *
 * The stats gatherer is delivering the most oomph, especially for tiny
 * keys and records, where Boyer-Moore is not really effective (or even
 * counter productive); gathering stats about the free slots and
 * occupied slots helps us speeding up multiple inserts, even while the
 * data is only alive for 1 run-time open-close period of time.
 *
 *
 * Make the cache counter code indirect, so we can switch and test
 * various cache aging systems quickly.
 *
 *
 *
 * When loading a freelist page, we can use sampling to get an idea of
 * where the LAST zone starts and ends (2 bsearches: one assuming the
 * freelist is sorted in descending order --> last 1 bit, one assuming the freelist is sorted in ascending
 * order (now that we 'know' the last free bit, this will scan the range 0..last-1-bit to
 * find the first 1 bit in there.
 *
 * Making sure we limit our # of samples irrespective of freelist page
 * size, so we can use the same stats gather for classic and modern
 * modes.
 *
 *
 * perform such sampling using semi-random intervals: prevent being
 * sensitive to a particular pathologic case this way.
 */



#define rescale_256(val)                            \
    val = ((val + 256 - 1) >> 8) /* make sure non-zero numbers remain non-zero: roundup(x) */

#define rescale_2(val)                            \
    val = ((val + 2 - 1) >> 1) /* make sure non-zero numbers remain non-zero: roundup(x) */



/**
 * inline function (must be fast!) which calculates the smallest
 * encompassing power-of-2 for the given value. The integer equivalent
 * of roundup(log2(value)).
 *
 * Returned value range: 0..64
 */
static __inline ham_u16_t ham_log2(ham_u64_t v)
{
    // which would be faster? Duff style unrolled loop or (CPU cached) loop?
#if 0

    register ham_u64_t value = v;
    register ham_u16_t power = !!value;

#if 0
#define HAM_LOG2_ONE_STAGE(value, power)            \
    value >>= 1;                                    \
    power+=!!value;   /* no branching required; extra cost: always same \
                       * # of rounds --> quad+ amount of extra rounds --> \
                       * much slower! */
#else
#define HAM_LOG2_ONE_STAGE(value, power)            \
    value >>= 1;                                    \
    if (!value) break;                              \
    power++;
#endif

#define HAM_LOG2_16_STAGES(value, power)            \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
                                                    \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
                                                    \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
                                                    \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power);               \
    HAM_LOG2_ONE_STAGE(value, power)

    do
    {
        HAM_LOG2_16_STAGES(value, power);
#if 0
        HAM_LOG2_16_STAGES(value, power);
        HAM_LOG2_16_STAGES(value, power);
        HAM_LOG2_16_STAGES(value, power);
#endif
    } while (value);

    return power;

#else /* if 0 */

    if (v)
    {
        register ham_u16_t power = 64;
        register ham_s64_t value = (ham_s64_t)v;

        /*
         * test top bit by checking two's complement sign.
         *
         * This LOG2 is crafted to spend the least number of
         * rounds inside the BM freelist bitarray scans.
         */
        while (!(value < 0))
        {
            power--;
            value <<= 1;
        }
        return power;
    }
    return 0;

#endif /* if 0 */
}

/**
 * inline function (must be fast!) which calculates the smallest
 * encompassing power-of-16 for the given value. The integer equivalent
 * of roundup(log16(value)).
 *
 * Returned value range: 0..16
 */
static __inline ham_u16_t ham_log16(ham_size_t v)
{
    register ham_size_t value = v;
    register ham_u16_t power = !!value;

    if (value)
    {
        do
        {
            power++;
        } while (value >>= 4);
    }

    return power;
}

static __inline ham_u16_t ham_bitcount2bucket_index(ham_size_t size)
{
    ham_u16_t bucket = ham_log2(size);
    if (bucket >= HAM_FREELIST_SLOT_SPREAD)
    {
        bucket = HAM_FREELIST_SLOT_SPREAD - 1;
    }
    return bucket;
}

/**
 * inline function (must be fast!) which calculates the inverse of the
 * ham_log2() above:
 *
 * converting a bucket index number to the maximum possible size for
 * that bucket.
 */
static __inline ham_size_t
ham_bucket_index2bitcount(ham_u16_t bucket)
{
    return (1U << (bucket * 1)) - 1;
}

void
db_update_global_stats_find_query(Database *db, ham_size_t key_size)
{
    Environment *env = db->get_env();

    if (!(env->get_flags()&HAM_IN_MEMORY_DB)) {
        ham_runtime_statistics_globdata_t *globalstats = env->get_global_perf_data();
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);

#ifdef HAM_DEBUG
        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);
#endif

        globalstats->query_count++;

        opstats->query_count++;
    }
}

void
db_update_global_stats_insert_query(Database *db, ham_size_t key_size, ham_size_t record_size)
{
    Environment *env = db->get_env();

    if (!(env->get_flags()&HAM_IN_MEMORY_DB))
    {
        ham_runtime_statistics_globdata_t *globalstats = env->get_global_perf_data();
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_INSERT);

#ifdef HAM_DEBUG
        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);
#endif

        globalstats->insert_query_count++;

        opstats->query_count++;
    }
}

void
db_update_global_stats_erase_query(Database *db, ham_size_t key_size)
{
    Environment *env = db->get_env();

    if (!(env->get_flags()&HAM_IN_MEMORY_DB))
    {
        ham_runtime_statistics_globdata_t *globalstats = env->get_global_perf_data();
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_ERASE);

#ifdef HAM_DEBUG
        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);
#endif

        globalstats->erase_query_count++;

        opstats->query_count++;
    }
}

static void
rescale_db_stats(ham_runtime_statistics_dbdata_t *dbstats)
{
    ham_runtime_statistics_opdbdata_t *opstats;

    rescale_256(dbstats->rescale_tracker);

    opstats = dbstats->op + HAM_OPERATION_STATS_FIND;

    rescale_256(opstats->btree_count);
    rescale_256(opstats->btree_fail_count);
    rescale_256(opstats->btree_cost);
    rescale_256(opstats->btree_fail_cost);

    // opstats->btree_last_page_addr;
    rescale_256(opstats->btree_last_page_sq_hits);

    rescale_256(opstats->query_count);


    opstats = dbstats->op + HAM_OPERATION_STATS_INSERT;

    rescale_256(opstats->btree_count);
    rescale_256(opstats->btree_fail_count);
    rescale_256(opstats->btree_cost);
    rescale_256(opstats->btree_fail_cost);

    // opstats->btree_last_page_addr;
    rescale_256(opstats->btree_last_page_sq_hits);

    rescale_256(opstats->query_count);


    opstats = dbstats->op + HAM_OPERATION_STATS_ERASE;

    rescale_256(opstats->btree_count);
    rescale_256(opstats->btree_fail_count);
    rescale_256(opstats->btree_cost);
    rescale_256(opstats->btree_fail_cost);

    // opstats->btree_last_page_addr;
    rescale_256(opstats->btree_last_page_sq_hits);

    rescale_256(opstats->query_count);
}

/**
 * update statistics following a followed up out-of-bound hint
 */
void
stats_update_fail_oob(int op, Database *db, ham_size_t cost,
                    ham_bool_t try_fast_track)
{
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_ERASE);

    opstats->btree_last_page_sq_hits = 0; /* reset */
}

void
stats_update_fail(int op, Database *db, ham_size_t cost,
                    ham_bool_t try_fast_track)
{
    ham_runtime_statistics_dbdata_t *dbstats = db->get_perf_data();
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_INSERT
                || op == HAM_OPERATION_STATS_ERASE);

    /*
     * Again, cost is the fastest riser, so we check that one against a high
     * water mark to decide whether to rescale or not
     */
    if (dbstats->rescale_tracker >= HAM_STATISTICS_HIGH_WATER_MARK - cost) {
        rescale_db_stats(dbstats);
    }
    dbstats->rescale_tracker += cost;

    opstats->btree_count++;
    opstats->btree_fail_count++;
    opstats->btree_cost += cost;
    opstats->btree_fail_cost += cost;

    //opstats->btree_last_page_addr = 0; -- keep page from previous match around!
    opstats->btree_last_page_sq_hits = 0; /* reset */

    if (try_fast_track)
    {
        opstats->btree_hinting_fail_count++;
        opstats->btree_hinting_count++;
    }
}

void
stats_update(int op, Database *db, Page *page, ham_size_t cost,
                    ham_bool_t try_fast_track)
{
    ham_runtime_statistics_dbdata_t *dbstats = db->get_perf_data();
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_INSERT
                || op == HAM_OPERATION_STATS_ERASE);
    ham_assert(page);

    /*
     * Again, cost is the fastest riser, so we check that one against a high water mark
     * to decide whether to rescale or not
     */
    if (dbstats->rescale_tracker >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
    {
        rescale_db_stats(dbstats);
    }
    dbstats->rescale_tracker += cost;

    opstats->btree_count++;
    //opstats->btree_fail_count++;
    opstats->btree_cost += cost;
    //opstats->btree_fail_cost += cost;

    /*
     * when we got a hint, account for it's success/failure
     */
    if (try_fast_track)
    {
        if (opstats->btree_last_page_addr != page->get_self())
        {
            opstats->btree_hinting_fail_count++;
        }
        opstats->btree_hinting_count++;
    }

    if (opstats->btree_last_page_addr
        && opstats->btree_last_page_addr == page->get_self())
    {
        opstats->btree_last_page_sq_hits++;
    }
    else
    {
        opstats->btree_last_page_addr = page->get_self();
    }
}

/*
 * when the last hit leaf node is split or shrunk, blow it away for all operations!
 *
 * Also blow away a page when a transaction aborts which has modified this page. We'd rather
 * reconstruct our critical statistics then carry the wrong bounds, etc. around.
 *
 * This is done to prevent the hinter from hinting/pointing at an (by now)
 * INVALID btree node later on!
 */
void
btree_stats_page_is_nuked(Database *db, Page *page,
                    ham_bool_t split)
{
    ham_runtime_statistics_dbdata_t *dbdata = db->get_perf_data();
    Environment *env = db->get_env();
    int i;

    for (i = 0; i <= 2; i++)
    {
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, i);

        ham_assert(i == HAM_OPERATION_STATS_FIND
                    || i == HAM_OPERATION_STATS_INSERT
                    || i == HAM_OPERATION_STATS_ERASE);

        if (opstats->btree_last_page_addr == page->get_self())
        {
            opstats->btree_last_page_addr = 0;
            opstats->btree_last_page_sq_hits = 0;
        }
    }

    if (dbdata->lower_bound_page_address == page->get_self()) {
        if (dbdata->lower_bound.data) {
            ham_assert(env->get_allocator() != 0);
            env->get_allocator()->free(dbdata->lower_bound.data);
        }
        memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
        dbdata->lower_bound_index = 0;
        dbdata->lower_bound_page_address = 0;
        dbdata->lower_bound_set = HAM_FALSE;
    }

    if (dbdata->upper_bound_page_address == page->get_self()) {
        if (dbdata->upper_bound.data) {
            ham_assert(env->get_allocator() != 0);
            env->get_allocator()->free(dbdata->upper_bound.data);
        }
        memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
        dbdata->upper_bound_index = 0;
        dbdata->upper_bound_page_address = 0;
        dbdata->upper_bound_set = HAM_FALSE;
    }
}

void
btree_stats_update_any_bound(int op, Database *db, Page *page,
                    ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot)
{
    ham_status_t st;
    ham_runtime_statistics_dbdata_t *dbdata = db->get_perf_data();
    Environment *env = db->get_env();
    btree_node_t *node = page_get_btree_node(page);

    /* reset both flags - they will be set if lower_bound or
     * upper_bound are modified */
    dbdata->last_insert_was_prepend=0;
    dbdata->last_insert_was_append=0;

    ham_assert(env->get_allocator() != 0);
    ham_assert(btree_node_is_leaf(node));
    if (!btree_node_get_left(node)) {
        /* this is the leaf page which carries the lower bound key */
        ham_assert(btree_node_get_count(node) == 0 ? !btree_node_get_right(node) : 1);
        if (btree_node_get_count(node) == 0)
        {
            /* range is empty
             *
             * do not set the lower/upper boundary; otherwise we may trigger
             * a key comparison with an empty key, and the comparison function
             * could not be fit to handle this.

             EDIT: the code should be able to handle that particular situation
                   as this was tested a while ago. Besides, the settings here
                   are a signal for the hinter the table is currently
                   completely empty and no btree traversal whatsoever is
                   needed before we find, insert or erase.

             EDIT #2: custom compare routines may b0rk on the data NULL pointers
                   (the monster test comparison function does, for example),
                   so the smarter thing to do here is NOT set the bounds here.

                   The trouble with that approach is that the hinter no longer
                   'knows about' an empty table, but is that so bad? An empty
                   table would constitute only a btree root node anyway, so the
                   regular traversal would be quick anyhow.
             */
            if (dbdata->lower_bound_index != 1
                || dbdata->upper_bound_index != 0)
            {
                /* only set when not done already */
                if (dbdata->lower_bound.data)
                    env->get_allocator()->free(dbdata->lower_bound.data);
                if (dbdata->upper_bound.data)
                    env->get_allocator()->free(dbdata->upper_bound.data);
                memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
                memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
                dbdata->lower_bound_index = 1; /* impossible value for lower bound index */
                dbdata->upper_bound_index = 0;
                dbdata->lower_bound_page_address = page->get_self();
                dbdata->upper_bound_page_address = 0; /* page->get_self(); */
                dbdata->lower_bound_set = HAM_TRUE;
                dbdata->upper_bound_set = HAM_FALSE; /* cannot be TRUE or subsequent updates for single record carrying tables may fail */
                //ham_assert(dbdata->lower_bound.data != NULL);
                ham_assert(dbdata->lower_bound_page_address != 0);
            }
        }
        else
        {
            /*
            lower bound key is always located at index [0]

            update our key info when either our current data is undefined (startup condition)
            or the first key was edited in some way (slot == 0). This 'copy anyway' approach
            saves us one costly key comparison.
            */
            if (dbdata->lower_bound_index != 0
                || dbdata->lower_bound_page_address != page->get_self()
                || slot == 0)
            {
                /* only set when not done already */
                dbdata->lower_bound_set = HAM_TRUE;
                dbdata->lower_bound_index = 0;
                dbdata->lower_bound_page_address = page->get_self();

                if (dbdata->lower_bound.data) {
                    env->get_allocator()->free(dbdata->lower_bound.data);
                    dbdata->lower_bound.data=0;
                    dbdata->lower_bound.size=0;
                }

                st = btree_copy_key_int2pub(db,
                    btree_node_get_key(db, node, dbdata->lower_bound_index),
                    &dbdata->lower_bound);
                if (st)
                {
                    /* panic! is case of failure, just drop the lower bound
                     * entirely. */
                    if (dbdata->lower_bound.data)
                        env->get_allocator()->free(dbdata->lower_bound.data);
                    memset(&dbdata->lower_bound, 0,
                            sizeof(dbdata->lower_bound));
                    dbdata->lower_bound_index = 0;
                    dbdata->lower_bound_page_address = 0;
                    dbdata->lower_bound_set = HAM_FALSE;
                }
                else
                {
                    ham_assert(dbdata->lower_bound.data == NULL ?
                        dbdata->lower_bound.size == 0 :
                        dbdata->lower_bound.size > 0);
                    ham_assert(dbdata->lower_bound_page_address != 0);
                }
                if (op==HAM_OPERATION_STATS_INSERT)
                    dbdata->last_insert_was_prepend=1;
            }
        }
    }

    if (!btree_node_get_right(node)) {
        /* this is the leaf page which carries the upper bound key */
        ham_assert(btree_node_get_count(node) == 0
                ? !btree_node_get_left(node)
                : 1);
        if (btree_node_get_count(node) != 0) {
            /*
             * range is non-empty; the other case has already been handled
             * above upper bound key is always located at index [size-1]
             * update our key info when either our current data is
             * undefined (startup condition) or the last key was edited in
             * some way (slot == size-1). This 'copy anyway' approach
             * saves us one costly key comparison.
             */
            if (dbdata->upper_bound_index !=
                        (ham_u32_t)btree_node_get_count(node)-1
                    || dbdata->upper_bound_page_address!=page->get_self()
                    || (ham_u16_t)slot==btree_node_get_count(node)-1) {
                /* only set when not done already */
                dbdata->upper_bound_set = HAM_TRUE;
                dbdata->upper_bound_index = btree_node_get_count(node) - 1;
                dbdata->upper_bound_page_address = page->get_self();

                if (dbdata->upper_bound.data) {
                    env->get_allocator()->free(dbdata->upper_bound.data);
                    dbdata->upper_bound.data=0;
                    dbdata->upper_bound.size=0;
                }

                st = btree_copy_key_int2pub(db,
                    btree_node_get_key(db, node, dbdata->upper_bound_index),
                    &dbdata->upper_bound);
                if (st) {
                    /* panic! is case of failure, just drop the upper bound
                     * entirely. */
                    if (dbdata->upper_bound.data)
                        env->get_allocator()->free(dbdata->upper_bound.data);
                    memset(&dbdata->upper_bound, 0,
                            sizeof(dbdata->upper_bound));
                    dbdata->upper_bound_index = 0;
                    dbdata->upper_bound_page_address = 0;
                    dbdata->upper_bound_set = HAM_FALSE;
                }
                if (op==HAM_OPERATION_STATS_INSERT)
                    dbdata->last_insert_was_append=1;
            }
        }
    }
}

/*
 * NOTE:
 *
 * The current statistics collectors recognize scenarios where insert &
 * delete mix with find, as both insert and delete (pardon, erase)
 * can split/merge/rebalance the B-tree and thus completely INVALIDATE
 * btree leaf nodes, the address of which is kept in DB-wide statistics
 * storage. The current way of doing things is to keep the statistics simple,
 * i.e. btree leaf node pointers are nuked when an insert operation splits
 * them or an erase operation merges or erases such pages. I don't think
 * it's really useful to have complex leaf-node tracking in here to improve
 * hinting in such mixed use cases.
 */
void
btree_find_get_hints(find_hints_t *hints, Database *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db->get_perf_data();
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);
    ham_u32_t flags = hints->flags;

    ham_assert(hints->key_is_out_of_bounds == HAM_FALSE);
    ham_assert(hints->try_fast_track == HAM_FALSE);

    /*
    we can only give some possibly helpful hints, when we
    know the tree leaf node (page) we can direct find() to...
    */
    if (opstats->btree_last_page_addr != 0)
    {
        /*
        When we're in SEQUENTIAL mode, we'll advise to check the previously used leaf.
        When the FAIL ratio increases above a certain number, we STOP hinting as we
        clearly hinted WRONG before. We'll try again later, though.

        Note also that we 'age' the HINT FAIL info collected during FIND statistics gathering,
        so that things will be attempted again after while.
        */
        if (flags & (HAM_HINT_APPEND | HAM_HINT_PREPEND))
        {
            /* find specific: APPEND / PREPEND --> SEQUENTIAL */
            flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);
            flags |= HAM_HINT_SEQUENTIAL;
        }

        if ((flags & HAM_HINTS_MASK) == 0) {
            /* no local preference specified; go with the DB-wide DAM config */
            switch (db->get_data_access_mode()) {
            default:
                break;

            case HAM_DAM_SEQUENTIAL_INSERT:
                flags = HAM_HINT_SEQUENTIAL;
                break;
            }
        }

        switch (flags & HAM_HINTS_MASK)
        {
        default:
        case HAM_HINT_RANDOM_ACCESS:
            /* do not provide any hints for the fast track */
            break;

        case HAM_HINT_SEQUENTIAL:
            /*
            when we have more than 4 hits on the same page already, we'll assume this one
            will end up there as well. As this counter will reset itself on the first FAIL,
            there's no harm in acting this quick. In pathological cases, the worst what
            can happen is that in 20% of cases there will be performed an extra check on
            a cached btree leaf node, which is still minimal overhead then.
            */
            if (opstats->btree_last_page_sq_hits >= 3)
            {
                hints->leaf_page_addr = opstats->btree_last_page_addr;
                hints->try_fast_track = HAM_TRUE;
                break;
            }
            /* fall through! */
            if (0)
            {
        case HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS:
                /* same as above, but now act as fast as possible on this info */
                if (opstats->btree_last_page_sq_hits >= 1)
                {
                    hints->leaf_page_addr = opstats->btree_last_page_addr;
                    hints->try_fast_track = HAM_TRUE;
                    break;
                }
            }
            {
                /*
                we assume this request is located near the previous request, so we check
                if there's anything in the statistics that can help out.

                Note #1: since the hinting counts are 'aged' down to a value of 0..1K (with 2K peak),
                we don't need to use a 64-bit integer for the ratio calculation here.

                Note #2: the ratio is only 'trustworthy' when the base count is about 4 or higher.
                This is because the aging rounds up while scaling down, which means one single FAIL
                can get you a ratio as large as 50% when total count is 1 as well, due to
                either startup or aging rescale; without this minimum size check, the ratio + aging
                would effectively stop the hinter from working after either an aging step or when
                a few FAILs happen during the initial few FIND operations (startup condition).

                EDIT: the above bit about the hinter stopping due to too much FAIL at start or after
                rescale does NOT apply any more as the hinter now also includes checks which trigger
                when a (small) series of hits on the same page are found, which acts as a restarter
                for this as well.
                */
                ham_u32_t ratio = opstats->btree_hinting_fail_count;

                ratio = ratio * 1000 / (1 + opstats->btree_hinting_count);
                if (ratio < 200)
                {
                    hints->leaf_page_addr = opstats->btree_last_page_addr;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
            break;
        }
    }

    /*
    age the hinting statistics

    This is different from the need to rescale the statistics data, as the latter is due to
    the risk of integer overflow when accounting for a zillion operations.

    Instead, the hinting costs are 'aged' to reduce the influence of older hinting
    results on subsequent hinter output.

    The way this aging happens here results in hinting_count traveling asymptotically towards
    1K with an upper bound of the count of 2K, while fail_count will be equal or lower to
    these numbers.

    And, yes, this also means the hinting counters will NOT be rescaled by the DB rescaler;
    hinting counts act independently.
    */
    opstats->aging_tracker++;
    if (opstats->aging_tracker >= 1000)
    {
        rescale_2(opstats->btree_hinting_fail_count);
        rescale_2(opstats->btree_hinting_count);

        opstats->aging_tracker = 0;
    }

    /*
    and lastly check whether the key is out of range: when the adequate LE/GE search flags
    are not set in such a case, we can quickly decide right here that a match won't be
    forthcoming: KEY_NOT_FOUND will be your thanks.

    One might want to add this extra 2 key comparison overhead only for
    'large' databases, i.e. databases which consist of more than 1 btree page
    (--> lower bound page address != upper bound page address) in order to keep this overhead
    to the bare minimum under all circumstances.

    THOUGHT: However, even with a tiny, single btree page database,
    it takes the in-page binary search log2(N) key comparisons to find out we've hit an out of
    bounds key, where N is the number of keys currently stored in the btree page, so we MAY already
    benefit from this when there's a large number of keys stored in this single btree page database...

    Say we allow a 5% overhead --> 2 key comparisons ~ 5% --> minimum key count in page = 2^40 keys.
    Which we'll never store in a single page as it is limited to 2^16 keys.

    Conclusion: only do this out-of-bounds check for multipage databases.

    And when the previous section of the hinter already produced some hints about where we might
    expect to hit (btree leaf page), we'll take that hint into account, assuming it's correct.
    And if it is not, there's nothing bad happening, except that the bounds check has
    been skipped so btree_find() will take the long (classic) route towards finding out that
    a lower or upper bound was hit.
    */
    ham_assert(!(key->_flags & KEY_IS_EXTENDED));
    key->_flags &= ~KEY_IS_EXTENDED;

    if (!dam_is_set(flags, HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)
        && dbdata->lower_bound_page_address != dbdata->upper_bound_page_address
        && (hints->try_fast_track
        ? (dbdata->lower_bound_page_address == hints->leaf_page_addr
            || dbdata->upper_bound_page_address == hints->leaf_page_addr)
            : HAM_TRUE)) {
        if (dbdata->lower_bound_set
                && !dam_is_set(flags, HAM_FIND_GT_MATCH)) {
            if (dbdata->lower_bound_index == 1) {
                /*
                impossible index: this is a marker to signal the table
                is completely empty
                */
                hints->key_is_out_of_bounds = HAM_TRUE;
                hints->try_fast_track = HAM_TRUE;
            }
            else {
                int cmp;

                ham_assert(dbdata->lower_bound_index == 0);
                ham_assert(dbdata->lower_bound.data == NULL ?
                    dbdata->lower_bound.size == 0 :
                    dbdata->lower_bound.size > 0);
                ham_assert(dbdata->lower_bound_page_address != 0);
                cmp = db->compare_keys(key, &dbdata->lower_bound);

                if (cmp < 0) {
                    hints->key_is_out_of_bounds = HAM_TRUE;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
        }

        if (dbdata->upper_bound_set
                && !dam_is_set(flags, HAM_FIND_LT_MATCH)) {
            int cmp;

            ham_assert(dbdata->upper_bound_index >= 0);
            ham_assert(dbdata->upper_bound.data == NULL ?
                dbdata->upper_bound.size == 0 :
                dbdata->upper_bound.size > 0);
            ham_assert(dbdata->upper_bound_page_address != 0);
            cmp = db->compare_keys(key, &dbdata->upper_bound);

            if (cmp > 0)
            {
                hints->key_is_out_of_bounds = HAM_TRUE;
                hints->try_fast_track = HAM_TRUE;
            }
        }
    }
}

void
btree_insert_get_hints(insert_hints_t *hints, Database *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db->get_perf_data();
    btree_cursor_t *cursor = hints->cursor ?
        ((Cursor *)(hints->cursor))->get_btree_cursor() : 0;

    ham_assert(hints->force_append == HAM_FALSE);
    ham_assert(hints->force_prepend == HAM_FALSE);
    ham_assert(hints->try_fast_track == HAM_FALSE);

    /* if the previous insert-operation replaced the upper_bound (or
     * lower_bound) key then it was actually an append (or prepend) operation.
     * in this case there's some probability that the next operation is also
     * appending/prepending.
     */
    if (dbdata->last_insert_was_append)
        hints->flags|=HAM_HINT_APPEND;
    else if (dbdata->last_insert_was_prepend)
        hints->flags|=HAM_HINT_PREPEND;

    if ((hints->flags & HAM_HINT_APPEND) && (cursor)) {
        if (!((Cursor *)hints->cursor)->is_nil(0)) {
            ham_assert(db == btree_cursor_get_db(cursor));

            /*
             fetch the page of the cursor. We deem the cost of an uncoupled cursor
             too high as that implies calling a full-fledged key search on the
             given key - which can be rather costly - so we rather wait for the
             statistical cavalry a little later on in this program then.
             */
            if (btree_cursor_is_coupled(cursor)) {
                Page *page = btree_cursor_get_coupled_page(cursor);
                btree_node_t *node = page_get_btree_node(page);
                ham_assert(btree_node_is_leaf(node));
                /*
                 * if cursor is not coupled to the LAST (right-most) leaf
                 * in the Database, it does not make sense to append
                 */
                if (btree_node_get_right(node)) {
                    hints->force_append = HAM_FALSE;
                    hints->try_fast_track = HAM_FALSE;
                }
                else {
                    hints->leaf_page_addr = page->get_self();
                    hints->force_append = HAM_TRUE;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
        }
    }
    else if ((hints->flags & HAM_HINT_PREPEND) && (cursor))
    {
        if (!((Cursor *)hints->cursor)->is_nil(0)) {
            ham_assert(db == btree_cursor_get_db(cursor));

            /*
             fetch the page of the cursor. We deem the cost of an uncoupled cursor
             too high as that implies calling a full-fledged key search on the
             given key - which can be rather costly - so we rather wait for the
             statistical cavalry a little later on in this program then.
             */
            if (btree_cursor_is_coupled(cursor)) {
                Page *page = btree_cursor_get_coupled_page(cursor);
                btree_node_t *node = page_get_btree_node(page);
                ham_assert(btree_node_is_leaf(node));
                /*
                 * if cursor is not coupled to the FIRST (left-most) leaf
                 * in the Database, it does not make sense to prepend
                 */
                if (btree_node_get_left(node)) {
                    hints->force_prepend = HAM_FALSE;
                    hints->try_fast_track = HAM_FALSE;
                }
                else {
                    hints->leaf_page_addr = page->get_self();
                    hints->force_prepend = HAM_TRUE;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
        }
    }
    //hints->flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);

    /*
    The statistical cavalry:

    - when the given key is positioned beyond the end, hint 'append' anyway.

    - When the given key is positioned before the start, hint 'prepend' anyway.

    NOTE: This 'auto-detect' mechanism (thanks to the key bounds being collected through
    the statistics gathering calls) renders the manual option HAM_HINT_APPEND/_PREPEND
    somewhat obsolete, really.

    The only advantage of manually specifying HAM_HINT_APPEND/_PREPEND is that it can save you
    two key comparisons in here.
    */
    ham_assert(!(key->_flags & KEY_IS_EXTENDED));
    key->_flags &= ~KEY_IS_EXTENDED;

    if (!hints->try_fast_track)
    {
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_INSERT);

        ham_assert(opstats != NULL);

        if (hints->flags & (HAM_HINT_APPEND | HAM_HINT_PREPEND))
        {
            /* find specific: APPEND / PREPEND --> SEQUENTIAL */
            hints->flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);
            hints->flags |= HAM_HINT_SEQUENTIAL;
        }

        if ((hints->flags & HAM_HINTS_MASK) == 0)
        {
            /* no local preference specified; go with the DB-wide DAM config */
            switch (db->get_data_access_mode()) {
            default:
                break;

            case HAM_DAM_SEQUENTIAL_INSERT:
                hints->flags |= HAM_HINT_SEQUENTIAL;
                break;
            }
        }

        switch (hints->flags & HAM_HINTS_MASK)
        {
        default:
        case HAM_HINT_RANDOM_ACCESS:
            /* do not provide any hints for the fast track */
            break;

        case HAM_HINT_SEQUENTIAL:
            /*
            when we have more than 4 hits on the same page already, we'll assume this one
            will end up there as well. As this counter will reset itself on the first FAIL,
            there's no harm in acting this quick. In pathological cases, the worst what
            can happen is that in 20% of cases there will be performed an extra check on
            a cached btree leaf node, which is still minimal overhead then.
            */
            if (opstats->btree_last_page_sq_hits >= 3)
            {
                hints->leaf_page_addr = opstats->btree_last_page_addr;
                hints->try_fast_track = HAM_TRUE;
                break;
            }
            /* fall through! */
            if (0)
            {
        case HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS:
            /* same as above, but now act as fast as possible on this info */
            if (opstats->btree_last_page_sq_hits >= 1)
            {
                hints->leaf_page_addr = opstats->btree_last_page_addr;
                hints->try_fast_track = HAM_TRUE;
                break;
            }
            }
            {
                /*
                we assume this request is located near the previous request, so we check
                if there's anything in the statistics that can help out.

                Note #1: since the hinting counts are 'aged' down to a value of 0..1K (with 2K peak),
                we don't need to use a 64-bit integer for the ratio calculation here.

                Note #2: the ratio is only 'trustworthy' when the base count is about 4 or higher.
                This is because the aging rounds up while scaling down, which means one single FAIL
                can get you a ratio as large as 50% when total count is 1 as well, due to
                either startup or aging rescale; without this minimum size check, the ratio + aging
                would effectively stop the hinter from working after either an aging step or when
                a few FAILs happen during the initial few FIND operations (startup condition).

                EDIT: the above bit about the hinter stopping due to too much FAIL at start or after
                rescale does NOT apply any more as the hinter now also includes checks which trigger
                when a (small) series of hits on the same page are found, which acts as a restarter
                for this as well.
                */
                ham_u32_t ratio = opstats->btree_hinting_fail_count;

                ratio = ratio * 1000 / (1 + opstats->btree_hinting_count);
                if (ratio < 200)
                {
                    hints->leaf_page_addr = opstats->btree_last_page_addr;
                    hints->try_fast_track = HAM_TRUE;
                    hints->force_append = HAM_TRUE;
                }
            }

            if (dbdata->lower_bound_set)
            {
                if (dbdata->lower_bound_index == 1)
                {
                    /*
                    impossible index: this is a marker to signal the table
                    is completely empty
                    */
                    //hints->flags |= HAM_HINT_PREPEND;
                    hints->force_prepend = HAM_TRUE;
                    hints->leaf_page_addr = dbdata->lower_bound_page_address;
                    hints->try_fast_track = HAM_TRUE;
                }
                else
                {
                    int cmp;

                    ham_assert(dbdata->lower_bound_index == 0);
                    ham_assert(dbdata->lower_bound.data == NULL ?
                        dbdata->lower_bound.size == 0 :
                        dbdata->lower_bound.size > 0);
                    ham_assert(dbdata->lower_bound_page_address != 0);
                    cmp = db->compare_keys(key, &dbdata->lower_bound);

                    if (cmp < 0)
                    {
                        //hints->flags |= HAM_HINT_PREPEND;
                        hints->force_prepend = HAM_TRUE;
                        hints->leaf_page_addr = dbdata->lower_bound_page_address;
                        hints->try_fast_track = HAM_TRUE;
                    }
                }
            }

            if (dbdata->upper_bound_set)
            {
                int cmp;

                ham_assert(dbdata->upper_bound_index >= 0);
                ham_assert(dbdata->upper_bound.data == NULL ?
                    dbdata->upper_bound.size == 0 :
                    dbdata->upper_bound.size > 0);
                ham_assert(dbdata->upper_bound_page_address != 0);
                cmp = db->compare_keys(key, &dbdata->upper_bound);

                if (cmp > 0)
                {
                    //hints->flags |= HAM_HINT_APPEND;
                    hints->force_append = HAM_TRUE;
                    hints->leaf_page_addr = dbdata->upper_bound_page_address;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
            break;
        }
    }

    /*
    we don't yet hint about jumping to the last accessed leaf node immediately (yet)

    EDIT:

    now we do: see the flags + dam code above: this happens when neither PREPEND
    nor APPEND hints are specified
    */
}

void
btree_erase_get_hints(erase_hints_t *hints, Database *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db->get_perf_data();

    ham_assert(hints->key_is_out_of_bounds == HAM_FALSE);
    ham_assert(hints->try_fast_track == HAM_FALSE);

    ham_assert(!(key->_flags & KEY_IS_EXTENDED));
    key->_flags &= ~KEY_IS_EXTENDED;

    /* forget about deleting a key when it's out of bounds */
    if (dbdata->lower_bound_set)
    {
        if (dbdata->lower_bound_index == 1)
        {
            /*
            impossible index: this is a marker to signal the table
            is completely empty
            */
            hints->key_is_out_of_bounds = HAM_TRUE;
            hints->try_fast_track = HAM_TRUE;
        }
        else
        {
            int cmp;

            ham_assert(dbdata->lower_bound_index == 0);
            ham_assert(dbdata->lower_bound.data == NULL ?
                dbdata->lower_bound.size == 0 :
                dbdata->lower_bound.size > 0);
            ham_assert(dbdata->lower_bound_page_address != 0);
            cmp = db->compare_keys(key, &dbdata->lower_bound);

            if (cmp < 0)
            {
                hints->key_is_out_of_bounds = HAM_TRUE;
                hints->try_fast_track = HAM_TRUE;
            }
        }
    }

    if (dbdata->upper_bound_set)
    {
        int cmp;

        ham_assert(dbdata->upper_bound_index >= 0);
        ham_assert(dbdata->upper_bound.data == NULL ?
            dbdata->upper_bound.size == 0 :
            dbdata->upper_bound.size > 0);
        ham_assert(dbdata->upper_bound_page_address != 0);
        cmp = db->compare_keys(key, &dbdata->upper_bound);

        if (cmp > 0)
        {
            hints->key_is_out_of_bounds = HAM_TRUE;
            hints->try_fast_track = HAM_TRUE;
        }
    }
}

void
btree_stats_init_globdata(Environment *env,
                    ham_runtime_statistics_globdata_t *globdata)
{
    memset(globdata, 0, sizeof(*globdata));
}

void
btree_stats_trash_globdata(Environment *env,
                    ham_runtime_statistics_globdata_t *globdata)
{
    /* nothing to trash */
    memset(globdata, 0, sizeof(*globdata));
}

void
btree_stats_init_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata)
{
    memset(dbdata, 0, sizeof(*dbdata));
}

void
btree_stats_flush_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata,
                    ham_bool_t last_in_env)
{
    /*
     * the freelist statistics are persisted through the freelist destructor,
     * everything else is currently not persistet
     */
}

void
btree_stats_trash_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata)
{
    Environment *env = db->get_env();

    /* trash the upper/lower bound keys, when set: */
    if (dbdata->upper_bound.data) {
        ham_assert(env->get_allocator() != 0);
        env->get_allocator()->free(dbdata->upper_bound.data);
    }
    if (dbdata->lower_bound.data) {
        ham_assert(env->get_allocator() != 0);
        env->get_allocator()->free(dbdata->lower_bound.data);
    }
    memset(dbdata, 0, sizeof(*dbdata));
}

ham_status_t
btree_stats_fill_ham_statistics_t(Environment *env, Database *db,
                    ham_statistics_t *dst)
{
    ham_status_t st = 0;
    ham_bool_t collect_globdata;
    ham_bool_t collect_dbdata;

    ham_assert(dst);

    /* copy the user-specified selectors before we zero the whole darn thing */
    collect_globdata = (!dst->dont_collect_global_stats && env);
    collect_dbdata = (!dst->dont_collect_db_stats && db);

    /* now zero the entire structure to begin with */
    memset(dst, 0, sizeof(*dst));

    /* then see if we can / should collect env/global and db-specific stats in there */
    if (collect_globdata) {
        ham_runtime_statistics_globdata_t *globalstats;

        ham_assert(env);
        globalstats = env->get_global_perf_data();
        ham_assert(globalstats);

        dst->global_stats = *globalstats;
    }
    if (collect_dbdata) {
        ham_runtime_statistics_dbdata_t *dbdata;

        ham_assert(db);
        dbdata = db->get_perf_data();
        ham_assert(dbdata);

        dst->db_stats = *dbdata;
    }

    dst->dont_collect_freelist_stats = !0;

    /* now the tougher part: see if we should report the freelist statistics */
    if (env->get_freelist())
        st = freelist_fill_statistics_t(env->get_freelist(), dst);

    /* and finally mark which sections have actually been fetched */
    dst->dont_collect_global_stats = !collect_globdata;
    dst->dont_collect_db_stats = !collect_dbdata;

    return st;
}

} // namespace ham
