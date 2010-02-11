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
#include "cache.h"
#include "db.h"
#include "endian.h"
#include "env.h"
#include "error.h"
#include "freelist_statistics.h"
#include "mem.h"
#include "page.h"
#include "statistics.h"
#include "util.h"



 
#if HAM_DEBUG
static void cache_init_history(void);
#else
#define cache_init_history() /**/
#endif




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
static __inline ham_size_t ham_bucket_index2bitcount(ham_u16_t bucket)
{
    return (1U << (bucket * 1)) - 1;
}











void
db_update_global_stats_find_query(ham_db_t *db, ham_size_t key_size)
{
    ham_env_t *env = db_get_env(db);

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
    {
        ham_runtime_statistics_globdata_t *globalstats = env_get_global_perf_data(env);
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);

        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
        //ham_assert(device_get_freelist_cache(dev), (0));

        globalstats->query_count++;

        opstats->query_count++;
    }
}


void
db_update_global_stats_insert_query(ham_db_t *db, ham_size_t key_size, ham_size_t record_size)
{
    ham_env_t *env = db_get_env(db);

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
    {
        ham_runtime_statistics_globdata_t *globalstats = env_get_global_perf_data(env);
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_INSERT);

        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
        //ham_assert(device_get_freelist_cache(dev), (0));

        globalstats->insert_query_count++;

        opstats->query_count++;
    }
}


void
db_update_global_stats_erase_query(ham_db_t *db, ham_size_t key_size)
{
    ham_env_t *env = db_get_env(db);

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
    {
        ham_runtime_statistics_globdata_t *globalstats = env_get_global_perf_data(env);
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_ERASE);

        ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
        //ham_assert(device_get_freelist_cache(dev), (0));

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
update statistics following a followed up out-of-bound hint 
*/
void stats_update_fail_oob(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track)
{
    //ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
    //ham_runtime_statistics_dbdata_t *dbstats = db_get_db_perf_data(db);
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_ERASE, (0));


    //opstats->btree_last_page_addr = 0; -- keep page from previous match around!
    opstats->btree_last_page_sq_hits = 0; /* reset */

    // this is a different type of hinting: don't count it
#if 0
    if (try_fast_track)
    {
        opstats->btree_hinting_fail_count++;
        opstats->btree_hinting_count++;
    }
#endif
}

void stats_update_fail(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track)
{
    //ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
    ham_runtime_statistics_dbdata_t *dbstats = db_get_db_perf_data(db);
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_INSERT
                || op == HAM_OPERATION_STATS_ERASE, (0));

    /*
    Again, cost is the fastest riser, so we check that one against a high water mark
    to decide whether to rescale or not
    */
    if (dbstats->rescale_tracker >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
    {
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

void stats_update(int op, ham_db_t *db, ham_page_t *page, ham_size_t cost, ham_bool_t try_fast_track)
{
    ham_runtime_statistics_dbdata_t *dbstats = db_get_db_perf_data(db);
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, op);

    ham_assert(op == HAM_OPERATION_STATS_FIND
                || op == HAM_OPERATION_STATS_INSERT
                || op == HAM_OPERATION_STATS_ERASE, (0));
    ham_assert(page, (0));

    /*
    Again, cost is the fastest riser, so we check that one against a high water mark
    to decide whether to rescale or not
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
    when we got a hint, account for it's success/failure
    */
    if (try_fast_track)
    {
        if (opstats->btree_last_page_addr != page_get_self(page))
        {
            opstats->btree_hinting_fail_count++;
        }
        opstats->btree_hinting_count++;
    }
    
    if (opstats->btree_last_page_addr
        && opstats->btree_last_page_addr == page_get_self(page))
    {
        opstats->btree_last_page_sq_hits++;
    }
    else
    {
        opstats->btree_last_page_addr = page_get_self(page);
    }
}

/*
when the last hit leaf node is split or shrunk, blow it away for all operations!

Also blow away a page when a transaction aborts which has modified this page. We'd rather
reconstruct our critical statistics then carry the wrong bounds, etc. around.

This is done to prevent the hinter from hinting/pointing at an (by now)
INVALID btree node later on!
*/
void stats_page_is_nuked(ham_db_t *db, struct ham_page_t *page, ham_bool_t split)
{
    ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
    ham_env_t *env = db_get_env(db);
    int i;

    for (i = 0; i <= 2; i++)
    {
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, i);

        ham_assert(i == HAM_OPERATION_STATS_FIND
                    || i == HAM_OPERATION_STATS_INSERT
                    || i == HAM_OPERATION_STATS_ERASE, (0));

        if (opstats->btree_last_page_addr == page_get_self(page))
        {
            opstats->btree_last_page_addr = 0;
            opstats->btree_last_page_sq_hits = 0;
        }
    }

    if (dbdata->lower_bound_page_address == page_get_self(page))
    {
        if (dbdata->lower_bound.data)
        {
            ham_assert(env_get_allocator(env) != 0, (0));
            allocator_free(env_get_allocator(env), dbdata->lower_bound.data);
        }
        memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
        dbdata->lower_bound_index = 0;
        dbdata->lower_bound_page_address = 0;
        dbdata->lower_bound_set = HAM_FALSE;
    }
    if (dbdata->upper_bound_page_address == page_get_self(page))
    {
        if (dbdata->upper_bound.data)
        {
            ham_assert(env_get_allocator(env) != 0, (0));
            allocator_free(env_get_allocator(env), dbdata->upper_bound.data);
        }
        memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
        dbdata->upper_bound_index = 0;
        dbdata->upper_bound_page_address = 0;
        dbdata->upper_bound_set = HAM_FALSE;
    }
}


void stats_update_any_bound(ham_db_t *db, struct ham_page_t *page, ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot)
{
    ham_status_t st;
    ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
    ham_env_t *env = db_get_env(db);
    btree_node_t *node = ham_page_get_btree_node(page);

    ham_assert(env_get_allocator(env) != 0, (0));
    ham_assert(btree_node_is_leaf(node), (0));
    if (!btree_node_get_left(node))
    {
        /* this is the leaf page which carries the lower bound key */
        ham_assert(btree_node_get_count(node) == 0 ? !btree_node_get_right(node) : 1, (0));
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
                    allocator_free(env_get_allocator(env), dbdata->lower_bound.data);
                if (dbdata->upper_bound.data)
                    allocator_free(env_get_allocator(env), dbdata->upper_bound.data);
                memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
                memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
                dbdata->lower_bound_index = 1; /* impossible value for lower bound index */
                dbdata->upper_bound_index = 0;
                dbdata->lower_bound_page_address = page_get_self(page);
                dbdata->upper_bound_page_address = 0; /* page_get_self(page); */
                dbdata->lower_bound_set = HAM_TRUE;
                dbdata->upper_bound_set = HAM_FALSE; /* cannot be TRUE or subsequent updates for single record carrying tables may fail */
                //ham_assert(dbdata->lower_bound.data != NULL, (0));
                ham_assert(dbdata->lower_bound_page_address != 0, (0));
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
                || dbdata->lower_bound_page_address != page_get_self(page)
                || slot == 0)
            {
                page_add_ref(page);

                /* only set when not done already */
                dbdata->lower_bound_set = HAM_TRUE;
                dbdata->lower_bound_index = 0;
                dbdata->lower_bound_page_address = page_get_self(page);

                if (dbdata->lower_bound.data) {
                    allocator_free(env_get_allocator(env), dbdata->lower_bound.data);
                    dbdata->lower_bound.data=0;
                    dbdata->lower_bound.size=0;
                }

                st = util_copy_key_int2pub(db, 
                    btree_node_get_key(db, node, dbdata->lower_bound_index),
                    &dbdata->lower_bound);
                if (st) 
                {
                    /* panic! is case of failure, just drop the lower bound 
                     * entirely. */
                    if (dbdata->lower_bound.data)
                        allocator_free(env_get_allocator(env), dbdata->lower_bound.data);
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
                        dbdata->lower_bound.size > 0, (0));
                    ham_assert(dbdata->lower_bound_page_address != 0, (0));
                }
                page_release_ref(page);
            }
        }
    }

    if (!btree_node_get_right(node)) 
    {
        /* this is the leaf page which carries the upper bound key */
        ham_assert(btree_node_get_count(node) == 0 
                ? !btree_node_get_left(node) 
                : 1, (0));
        if (btree_node_get_count(node) != 0) 
        {
            /* 
             * range is non-empty; the other case has already been handled 
             * above upper bound key is always located at index [size-1] 
             * update our key info when either our current data is 
             * undefined (startup condition) or the last key was edited in 
             * some way (slot == size-1). This 'copy anyway' approach 
             * saves us one costly key comparison.
             */
            if (dbdata->upper_bound_index != btree_node_get_count(node) - 1
                    || dbdata->upper_bound_page_address != page_get_self(page)
                    || slot == btree_node_get_count(node) - 1) 
            {
                page_add_ref(page);

                /* only set when not done already */
                dbdata->upper_bound_set = HAM_TRUE;
                dbdata->upper_bound_index = btree_node_get_count(node) - 1;
                dbdata->upper_bound_page_address = page_get_self(page);

                if (dbdata->upper_bound.data) {
                    allocator_free(env_get_allocator(env), dbdata->upper_bound.data);
                    dbdata->upper_bound.data=0;
                    dbdata->upper_bound.size=0;
                }

                st = util_copy_key_int2pub(db, 
                    btree_node_get_key(db, node, dbdata->upper_bound_index),
                    &dbdata->upper_bound);
                if (st) 
                {
                    /* panic! is case of failure, just drop the upper bound 
                     * entirely. */
                    if (dbdata->upper_bound.data)
                        allocator_free(env_get_allocator(env), dbdata->upper_bound.data);
                    memset(&dbdata->upper_bound, 0, 
                            sizeof(dbdata->upper_bound));
                    dbdata->upper_bound_index = 0;
                    dbdata->upper_bound_page_address = 0;
                    dbdata->upper_bound_set = HAM_FALSE;
                }
                page_release_ref(page);
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
btree_find_get_hints(find_hints_t *hints, ham_db_t *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
    ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);
    ham_u32_t flags = hints->flags;

    ham_assert(hints->key_is_out_of_bounds == HAM_FALSE, (0));
    ham_assert(hints->try_fast_track == HAM_FALSE, (0));

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

        if ((flags & HAM_HINTS_MASK) == 0)
        {
            /* no local preference specified; go with the DB-wide DAM config */
            switch (db_get_data_access_mode(db) & ~HAM_DAM_ENFORCE_PRE110_FORMAT)
            {
            default:
                break;

            case HAM_DAM_SEQUENTIAL_INSERT:
                flags = HAM_HINT_SEQUENTIAL;
                break;

            case HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT:
                flags = HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS;
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
    ham_assert(!(key->_flags & KEY_IS_EXTENDED), (0));
    key->_flags &= ~KEY_IS_EXTENDED;

    if (!db_is_mgt_mode_set(flags, HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)
        && dbdata->lower_bound_page_address != dbdata->upper_bound_page_address
        && (hints->try_fast_track 
        ? (dbdata->lower_bound_page_address == hints->leaf_page_addr
            || dbdata->upper_bound_page_address == hints->leaf_page_addr)
            : HAM_TRUE))
    {
        if (dbdata->lower_bound_set
            && !db_is_mgt_mode_set(flags, HAM_FIND_GT_MATCH))
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
            
                ham_assert(dbdata->lower_bound_index == 0, (0));
                ham_assert(dbdata->lower_bound.data == NULL ?
                    dbdata->lower_bound.size == 0 : 
                    dbdata->lower_bound.size > 0, (0));
                ham_assert(dbdata->lower_bound_page_address != 0, (0));
                cmp = db_compare_keys(db, key, &dbdata->lower_bound);

                if (cmp < 0)
                {
                    hints->key_is_out_of_bounds = HAM_TRUE;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
        }

        if (dbdata->upper_bound_set
            && !db_is_mgt_mode_set(flags, HAM_FIND_LT_MATCH))
        {
            int cmp;
            
            ham_assert(dbdata->upper_bound_index >= 0, (0));
            ham_assert(dbdata->upper_bound.data == NULL ?
                dbdata->upper_bound.size == 0 : 
                dbdata->upper_bound.size > 0, (0));
            ham_assert(dbdata->upper_bound_page_address != 0, (0));
            cmp = db_compare_keys(db, key, &dbdata->upper_bound);

            if (cmp > 0)
            {
                hints->key_is_out_of_bounds = HAM_TRUE;
                hints->try_fast_track = HAM_TRUE;
            }
        }
    }
}




void 
btree_insert_get_hints(insert_hints_t *hints, ham_db_t *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
    ham_bt_cursor_t *cursor = (ham_bt_cursor_t *)hints->cursor;

    ham_assert(hints->force_append == HAM_FALSE, (0));
    ham_assert(hints->force_prepend == HAM_FALSE, (0));
    ham_assert(hints->try_fast_track == HAM_FALSE, (0));

    if ((hints->flags & HAM_HINT_APPEND) && (cursor))
    {
        if (!bt_cursor_is_nil(cursor))
        {
            ham_assert(bt_cursor_is_nil(cursor)==0, ("cursor must not be nil"));
            ham_assert(db == bt_cursor_get_db(cursor), (0));

            /*
             fetch the page of the cursor. We deem the cost of an uncoupled cursor 
             too high as that implies calling a full-fledged key search on the
             given key - which can be rather costly - so we rather wait for the
             statistical cavalry a little later on in this program then.
             */
            if (bt_cursor_get_flags(cursor) & BT_CURSOR_FLAG_COUPLED) 
            {
                ham_page_t *page = bt_cursor_get_coupled_page(cursor);
                btree_node_t *node = ham_page_get_btree_node(page);
                ham_assert(btree_node_is_leaf(node), 
                            ("cursor points to internal node"));
                //ham_assert(!btree_node_get_right(node), ("cursor points to leaf node which is NOT the uppermost/last one"));
                /*
                 * if cursor is not coupled to the LAST (right-most) leaf
                 * in the Database, it does not make sense to append
                 */
                if (btree_node_get_right(node)) {
                    hints->force_append = HAM_FALSE;
                    hints->try_fast_track = HAM_FALSE;
                }
                else {
                    hints->leaf_page_addr = page_get_self(page);
                    hints->force_append = HAM_TRUE;
                    hints->try_fast_track = HAM_TRUE;
                }
            }
        }
    }
    else if ((hints->flags & HAM_HINT_PREPEND) && (cursor))
    {
        if (!bt_cursor_is_nil(cursor))
        {
            ham_assert(bt_cursor_is_nil(cursor)==0, ("cursor must not be nil"));
            ham_assert(db == bt_cursor_get_db(cursor), (0));

            /*
             fetch the page of the cursor. We deem the cost of an uncoupled cursor 
             too high as that implies calling a full-fledged key search on the
             given key - which can be rather costly - so we rather wait for the
             statistical cavalry a little later on in this program then.
             */
            if (bt_cursor_get_flags(cursor) & BT_CURSOR_FLAG_COUPLED) 
            {
                ham_page_t *page = bt_cursor_get_coupled_page(cursor);
                btree_node_t *node = ham_page_get_btree_node(page);
                ham_assert(btree_node_is_leaf(node), 
                        ("cursor points to internal node"));
                //ham_assert(!btree_node_get_left(node), ("cursor points to leaf node which is NOT the lowest/first one"));
                /*
                 * if cursor is not coupled to the FIRST (left-most) leaf
                 * in the Database, it does not make sense to prepend
                 */
                if (btree_node_get_left(node)) {
                    hints->force_prepend = HAM_FALSE;
                    hints->try_fast_track = HAM_FALSE;
                }
                else {
                    hints->leaf_page_addr = page_get_self(page);
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
    ham_assert(!(key->_flags & KEY_IS_EXTENDED), (0));
    key->_flags &= ~KEY_IS_EXTENDED;

    if (!hints->try_fast_track)
    {
        ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_INSERT);

        ham_assert(opstats != NULL, (0));

        if (hints->flags & (HAM_HINT_APPEND | HAM_HINT_PREPEND))
        {
            /* find specific: APPEND / PREPEND --> SEQUENTIAL */
            hints->flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND); 
            hints->flags |= HAM_HINT_SEQUENTIAL;
        }

        if ((hints->flags & HAM_HINTS_MASK) == 0)
        {
            /* no local preference specified; go with the DB-wide DAM config */
            switch (db_get_data_access_mode(db) & ~HAM_DAM_ENFORCE_PRE110_FORMAT)
            {
            default:
                break;

            case HAM_DAM_SEQUENTIAL_INSERT:
                hints->flags |= HAM_HINT_SEQUENTIAL;
                break;

            case HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT:
                hints->flags |= HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS;
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
                    
                    ham_assert(dbdata->lower_bound_index == 0, (0));
                    ham_assert(dbdata->lower_bound.data == NULL ?
                        dbdata->lower_bound.size == 0 : 
                        dbdata->lower_bound.size > 0, (0));
                    ham_assert(dbdata->lower_bound_page_address != 0, (0));
                    cmp = db_compare_keys(db, key, &dbdata->lower_bound);

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
                
                ham_assert(dbdata->upper_bound_index >= 0, (0));
                ham_assert(dbdata->upper_bound.data == NULL ?
                    dbdata->upper_bound.size == 0 : 
                    dbdata->upper_bound.size > 0, (0));
                ham_assert(dbdata->upper_bound_page_address != 0, (0));
                cmp = db_compare_keys(db, key, &dbdata->upper_bound);

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
btree_erase_get_hints(erase_hints_t *hints, ham_db_t *db, ham_key_t *key)
{
    ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);

    ham_assert(hints->key_is_out_of_bounds == HAM_FALSE, (0));
    ham_assert(hints->try_fast_track == HAM_FALSE, (0));

    ham_assert(!(key->_flags & KEY_IS_EXTENDED), (0));
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
            
            ham_assert(dbdata->lower_bound_index == 0, (0));
            ham_assert(dbdata->lower_bound.data == NULL ?
                dbdata->lower_bound.size == 0 : 
                dbdata->lower_bound.size > 0, (0));
            ham_assert(dbdata->lower_bound_page_address != 0, (0));
            cmp = db_compare_keys(db, key, &dbdata->lower_bound);

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
        
        ham_assert(dbdata->upper_bound_index >= 0, (0));
        ham_assert(dbdata->upper_bound.data == NULL ?
            dbdata->upper_bound.size == 0 : 
            dbdata->upper_bound.size > 0, (0));
        ham_assert(dbdata->upper_bound_page_address != 0, (0));
        cmp = db_compare_keys(db, key, &dbdata->upper_bound);

        if (cmp > 0)
        {
            hints->key_is_out_of_bounds = HAM_TRUE;
            hints->try_fast_track = HAM_TRUE;
        }
    }
}








void
stats_init_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata)
{
    memset(globdata, 0, sizeof(*globdata));
    cache_init_history();
}

void
stats_flush_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata)
{
    /* nothing to persist? */
}

void
stats_trash_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata)
{
    /* nothing to trash */
    memset(globdata, 0, sizeof(*globdata));
}

void
stats_init_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata)
{
    memset(dbdata, 0, sizeof(*dbdata));
}

void
stats_flush_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata, ham_bool_t last_in_env)
{
    /* 
    the freelist statistics are persisted through the freelist destructor,
    which is invoked elsewhere, so all we need to worry about here are the
    'global' db/env oriented find/insert/erase statistics.
    
    TODO: 
    
    persist those in the db header, that is IFF we're a v1.1.0+ DB
    and we're the last one in the environment (or running solo).
    */
    if (last_in_env)
    {
        /* do we have the new freelist statistics persisting format or are we using an older DB format? */
        if (!db_is_mgt_mode_set(db_get_data_access_mode(db), HAM_DAM_ENFORCE_PRE110_FORMAT))
        {

        }
    }
}

void
stats_trash_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata)
{
    ham_env_t *env = db_get_env(db);

    /* trash the upper/lower bound keys, when set: */
    if (dbdata->upper_bound.data) {
        ham_assert(env_get_allocator(env) != 0, (0));
        allocator_free(env_get_allocator(env), dbdata->upper_bound.data);
    }
    if (dbdata->lower_bound.data) {
        ham_assert(env_get_allocator(env) != 0, (0));
        allocator_free(env_get_allocator(env), dbdata->lower_bound.data);
    }
    memset(dbdata, 0, sizeof(*dbdata));
}

ham_status_t
stats_fill_ham_statistics_t(ham_env_t *env, ham_db_t *db, ham_statistics_t *dst)
{
    ham_status_t st;
    ham_bool_t collect_globdata;
    ham_bool_t collect_dbdata;

    ham_assert(dst, (0));

    /* copy the user-specified selectors before we zero the whole darn thing */
    collect_globdata = (!dst->dont_collect_global_stats && env);
    collect_dbdata = (!dst->dont_collect_db_stats && db);

    /* now zero the entire structure to begin with */
    memset(dst, 0, sizeof(*dst));

    /* then see if we can / should collect env/global and db-specific stats in there */
    if (collect_globdata)
    {
        ham_runtime_statistics_globdata_t *globalstats;

        ham_assert(env, (0));
        globalstats = env_get_global_perf_data(env);
        ham_assert(globalstats, (0));

        dst->global_stats = *globalstats;
    }
    if (collect_dbdata)
    {
        ham_runtime_statistics_dbdata_t *dbdata;

        ham_assert(db, (0));
        dbdata = db_get_db_perf_data(db);
        ham_assert(dbdata, (0));

        dst->db_stats = *dbdata;
    }

    dst->dont_collect_freelist_stats = !0;

    /* now the tougher part: see if we should report the freelist statistics */
    st = stats_fill_freel_statistics_t(env, dst);

    /* and finally mark which sections have actually been fetched */
    dst->dont_collect_global_stats = !collect_globdata;
    dst->dont_collect_db_stats = !collect_dbdata;
    
    return st;
}



























#if HAM_DEBUG


#define CACHE_HISTORY_SIZE    512


typedef struct
{
    unsigned int next; // 0: End Of List
    unsigned int prev;

    ham_u64_t age_id;            

    ham_offset_t addr;
    ham_u64_t count;

    ham_u64_t recall;            
    ham_offset_t recall_dist;
    ham_u64_t refetch;            /* still in history, but already gone from cache and now retrieved again */
    ham_offset_t refetch_dist;

    ham_u64_t alloc;
    ham_u64_t fetch;

    ham_u64_t remove;
    ham_u64_t insert;
    ham_u64_t purge;

    ham_u32_t cache_cntr;
    ham_u32_t refcount;

} cache_history_t;

cache_history_t cache_history[CACHE_HISTORY_SIZE] = {{0}};

unsigned int history_start_position = 0;
unsigned int history_fill = 0;
ham_u64_t age_id = 0;            


static void 
cache_init_history(void)
{
    memset(cache_history, 0, sizeof(cache_history));
    history_start_position = 0;
    history_fill = 0;
    age_id = 0;
}


static cache_history_t *
cache_history_locate_entry(ham_page_t *page, int state)
{
    unsigned int i;
    int l;
    int r;
    int pos;

    for (i = 0; i < history_fill; i++)
    {
        ham_assert(cache_history[cache_history[i].prev].next == i, (0));
        ham_assert(cache_history[cache_history[i].next].prev == i, (0));
        ham_assert(history_fill > cache_history[i].prev, (0));
        ham_assert(history_fill > cache_history[i].next, (0));
    }

    age_id++;

    l = pos = 0;
    r = history_fill - 1;
    while (l <= r)
    {
        pos = (l + r + 1) / 2;
        if (cache_history[pos].addr == page_get_self(page))
        {
            l = r = pos;
            break;
        }
        else if (cache_history[pos].addr < page_get_self(page))
        {
            l = pos + 1;
            pos++;
        }
        else
        {
            r = pos - 1;
        }
    }
    /*
    when we get here, pos points at the matching node OR at the spot
    where we need to insert our new entry
    */
    if (l != r)
    {
        cache_history_t tmp = {0};
        
        tmp.addr = page_get_self(page);

        ham_assert(l > r, ("the insertion point"));

        if (pos >= CACHE_HISTORY_SIZE || history_fill == CACHE_HISTORY_SIZE)
        {
            ham_assert(history_fill == CACHE_HISTORY_SIZE, (0));

            /* no free slot: pick the oldest and discard that one */
            l = cache_history[history_start_position].prev;
            cache_history[cache_history[l].prev].next = cache_history[l].next;
            cache_history[cache_history[l].next].prev = cache_history[l].prev;
            
            /* remove node from the list? */
            if (l + 1 != history_fill)
            {
                ham_assert(l < history_fill, (0));

                /* move everyone -1 */
                memmove(&cache_history[l], &cache_history[l + 1], (history_fill - (l + 1)) * sizeof(cache_history[0]));
                history_fill--;
                /* and adjust indices! */
                for (i = 0; i < history_fill; i++)
                {
                    if (cache_history[i].prev >= l)
                        cache_history[i].prev--;
                    if (cache_history[i].next >= l)
                        cache_history[i].next--;
                    ham_assert(history_fill > cache_history[i].prev, (0));
                    ham_assert(history_fill > cache_history[i].next, (0));
                }
                for (i = 0; i < history_fill; i++)
                {
                    ham_assert(cache_history[cache_history[i].prev].next == i, (0));
                    ham_assert(cache_history[cache_history[i].next].prev == i, (0));
                    ham_assert(history_fill >= cache_history[i].prev, (0));
                    ham_assert(history_fill >= cache_history[i].next, (0));
                }
            }
            else
            {
                history_fill--;
                for (i = 0; i < history_fill; i++)
                {
                    ham_assert(cache_history[cache_history[i].prev].next == i, (0));
                    ham_assert(cache_history[cache_history[i].next].prev == i, (0));
                    ham_assert(history_fill >= cache_history[i].prev, (0));
                    ham_assert(history_fill >= cache_history[i].next, (0));
                }
            }
            if (history_start_position >= l)
                history_start_position--;
            ham_assert(history_fill > history_start_position, (0));

            if (pos > history_fill) 
                pos = history_fill;
        }

        ham_assert(history_fill < CACHE_HISTORY_SIZE, (0));

        /* add / insert node to the list? */
        if (pos != history_fill)
        {
            ham_assert(pos < history_fill, (0));

            /* move everyone +1 */
            memmove(&cache_history[pos + 1], &cache_history[pos], (history_fill - pos) * sizeof(cache_history[0]));
            history_fill++;
            /* and adjust indices! */
            for (i = 0; i < history_fill; i++)
            {
                if (cache_history[i].prev >= pos)
                    cache_history[i].prev++;
                if (cache_history[i].next >= pos)
                    cache_history[i].next++;
                ham_assert(history_fill > cache_history[i].prev, (0));
                ham_assert(history_fill > cache_history[i].next, (0));
            }
            for (i = 0; i < history_fill; i++)
            {
                if (i != pos)
                {
                    ham_assert(cache_history[cache_history[i].prev].next == i, (0));
                    ham_assert(cache_history[cache_history[i].next].prev == i, (0));
                    ham_assert(history_fill > cache_history[i].prev, (0));
                    ham_assert(history_fill > cache_history[i].next, (0));
                }
            }
            if (history_start_position >= pos)
                history_start_position++;
            ham_assert(history_fill >= history_start_position, (0));
        }
        else
        {
            history_fill++;
        }

        tmp.next = history_start_position;
        tmp.prev = cache_history[history_start_position].prev;
        cache_history[history_start_position].prev = pos;
        cache_history[tmp.prev].next = pos;

        cache_history[pos] = tmp;

        for (i = 0; i < history_fill; i++)
        {
            ham_assert(cache_history[cache_history[i].prev].next == i, (0));
            ham_assert(cache_history[cache_history[i].next].prev == i, (0));
            ham_assert(history_fill > cache_history[i].prev, (0));
            ham_assert(history_fill > cache_history[i].next, (0));
        }
    }
    else 
    {
        ham_u64_t distance;            

        ham_assert(l == pos, ("a direct match"));

        distance = age_id - cache_history[pos].age_id;
/*
state:
cache_get_unused_page() --> -2, -3
cache_get_page(!CACHE_REMOVE) --> -4

cache_put_page() --> 0 (+10 for new page)

cache_remove_page() --> -1
my_purge_cache() --> -100
db_flush_all() --> -6

db_alloc_page() -> -99
db_fetch_page() --> 1, 2, 3 (1,2 = from cache, 3 = from device)
*/
        switch (state)
        {
        case 3:
            cache_history[pos].refetch_dist += distance;
            cache_history[pos].refetch++;
            break;

        case 1:
        case 2:
        case -2:
        case -3:
        case -4:
            cache_history[pos].recall_dist += distance;
            cache_history[pos].recall++;
            break;
        }

        if (pos != history_start_position)
        {
            /* reshuffle the position order: remove + re-insert node 'pos' */
            l = cache_history[pos].prev;
            r = cache_history[pos].next;
            cache_history[l].next = r;
            cache_history[r].prev = l;

            r = history_start_position;
            l = cache_history[r].prev;
            cache_history[pos].next = r;
            cache_history[pos].prev = l;
            cache_history[l].next = pos;
            cache_history[r].prev = pos;
        }

        for (i = 0; i < history_fill; i++)
        {
            ham_assert(cache_history[cache_history[i].prev].next == i, (0));
            ham_assert(cache_history[cache_history[i].next].prev == i, (0));
            ham_assert(history_fill > cache_history[i].prev, (0));
            ham_assert(history_fill > cache_history[i].next, (0));
        }

        /* record access 'time' */
        cache_history[pos].age_id = age_id;
    }

    history_start_position = pos;
    ham_assert(history_fill > history_start_position, (0));

    cache_history[pos].cache_cntr = page_get_cache_cntr(page);
    cache_history[pos].refcount = page_get_refcount(page);

    return &cache_history[pos];
}

static ham_page_t *
cache_get_live_page(ham_cache_t *cache, ham_offset_t addr, char *af, int aflen)
{
    ham_page_t *head;

    head=cache_get_totallist(cache);
    while (head) 
    {
        if (page_get_self(head) == addr)
        {
            if (aflen > 0)
                af[0] = 0;
            return head;
        }
        af++;
        aflen--;
        head=page_get_next(head, PAGE_LIST_CACHED);
    }
    head=cache_get_garbagelist(cache);
    while (head) 
    {
        if (page_get_self(head) == addr)
        {
            if (aflen > 0)
                af[0] = 0;
            return head;
        }
        af++;
        aflen--;
        head=page_get_next(head, PAGE_LIST_GARBAGE);
    }

    return NULL;
}




#if 0
static void cache_report_history(ham_env_t *env)
{
    ham_cache_t *cache = env_get_cache(env);
    ham_u32_t i;
    char af[2048];
    int c;
    ham_u64_t minc;
    ham_u64_t refetch;
    unsigned int pos;
    static int print_perf_data = 0;

    if (print_perf_data)
    {
    printf("\n\ncache history:\n");

    printf("Format:\n"
        "address[index]: rc#:<recall count>, rc:<recall_distance>/<recall count>,\n"
        "                rf#:<refetch count>, rf:<refetch_distance>/<refresh count>,\n"
        "                age: <cache_cntr>(<refcount>)\n");
    }

    memset(af, 0, sizeof(af));
    for (i = 0 ; i < (int)sizeof(af) && i < cache->_cur_elements; i++)
    {
        af[i] = 1;
    }

    minc = 0;
    refetch = 0;

    pos = history_start_position;
    for (i = 0; history_fill > 0; i++)
    {
        ham_page_t *p;

        p = cache_get_live_page(cache, cache_history[pos].addr, af, sizeof(af));

        ham_assert(cache_history[pos].addr, (0));
        ham_assert(0 < (cache_history[pos].count + cache_history[pos].alloc + cache_history[pos].fetch), (0));
        
    if (print_perf_data)
    {
        printf("%p[%3d]: rc#:%4lld, rc:%6lld, rf#:%4lld, rf:%6lld, age: %d(%d)\n",
            (void *)cache_history[pos].addr, i,
            (long long int)cache_history[pos].recall,
            (long long int)(cache_history[pos].recall ? cache_history[pos].recall_dist / cache_history[pos].recall : 0),
            (long long int)cache_history[pos].refetch,
            (long long int)(cache_history[pos].refetch ? cache_history[pos].refetch_dist / cache_history[pos].refetch : 0),
            cache_history[pos].cache_cntr,
            cache_history[pos].refcount);
    }

        minc += cache_history[pos].count;
        refetch += cache_history[pos].refetch;

        pos = cache_history[pos].next;
        if (pos == history_start_position)
            break;
    }

    printf("\n");
    
    if (i != 0)
        minc /= i;
    printf("AVG. COUNT: %lld     REFETCH TOTAL: %lld\n", (long long int)minc, (long long int)refetch);

    if (print_perf_data)
    {
    printf("\nFormat:\n"
            "[index]: f/a:<fetch>/<alloc>, a:<alloc>, c:<count>, i:<insert>, r:<remove>,\n"
            "         p:<purge>, age: <cache_cntr>(<refcount>)\n");
    }

    pos = history_start_position;
    for (i = 0; history_fill > 0; i++)
    {
        ham_page_t *p;

        p = cache_get_live_page(cache, cache_history[pos].addr, af, sizeof(af));

        if (print_perf_data)
        {
            printf("[%3d]: f/a:%6lld/%lld, a:%lld, c:%6lld, i:%2lld, r:%2lld, p:%2lld, age: %d(%d)/",
                i,
                (long long int)cache_history[pos].fetch, (long long int)cache_history[pos].alloc,
                (long long int)cache_history[pos].alloc,
                (long long int)cache_history[pos].count,
                (long long int)cache_history[pos].insert,
                (long long int)cache_history[pos].remove,
                (long long int)cache_history[pos].purge,
                cache_history[pos].cache_cntr,
                cache_history[pos].refcount);
            if (p)
            {
                printf("%d(%d)\n",
                    page_get_cache_cntr(p),
                    page_get_refcount(p));
            }
            else
            {
                printf("**GONE**\n");
            }
        }

        pos = cache_history[pos].next;
        if (pos == history_start_position)
            break;
    }

    c = 0;
    for (i = 0; i < sizeof(af); i++)
    {
        if (af[i] && i != 0)       // totallist->head is never in history...
            c++;
    }
    if (c > 0)
    {
        ham_page_t *head;

        printf("\n*** CACHED PAGES UNACCOUNTED FOR: %d ***\n\n", c);

        i = 0;
        head=cache_get_totallist(cache);
        while (head) 
        {
            if (i < sizeof(af) && af[i])
            {
                printf("%p[%2d]: type: $%x (%s), age: %d(%d)\n",
                    (void *)page_get_self(head), i,
                    page_get_npers_flags(head), "CACHED",
                    page_get_cache_cntr(head),
                    page_get_refcount(head));
            }
            head=page_get_next(head, PAGE_LIST_CACHED);
            i++;
        }
        head=cache_get_garbagelist(cache);
        while (head) 
        {
            if (i < sizeof(af) && af[i])
            {
                printf("%p[%2d]: type: $%x (%s), age: %d(%d)\n",
                    (void *)page_get_self(head), i,
                    page_get_npers_flags(head), "GARBAGE",
                    page_get_cache_cntr(head),
                    page_get_refcount(head));
            }
            head=page_get_next(head, PAGE_LIST_GARBAGE);
            i++;
        }
    }

    if (print_perf_data)
    {
        printf("\n\n");
    }
}
#endif

/*
state:
cache_get_unused_page() --> -2, -3
cache_get_page(!CACHE_REMOVE) --> -4

cache_put_page() --> 0 (+10 for new page)

cache_remove_page() --> -1
my_purge_cache() --> -100
db_flush_all() --> -6
*/
void cache_push_history(ham_page_t *page, int state)
{
    cache_history_t *ref = cache_history_locate_entry(page, state);

    ham_assert(ref->addr == page_get_self(page), (0));

    ref->count++;
    if (state > 0)
    {
        ref->insert++;
    }
    else if (state < 0)
    {
        ref->remove++;
        if (state <= -100)
        {
            ref->purge++;
        }
    }
}


/*
state:
db_alloc_page() -> -99
db_fetch_page() --> 1, 2, 3 (1,2 = from cache, 3 = from device)
*/
void cache_check_history(ham_env_t *env, ham_page_t *page, int state)
{
    cache_history_t *ref = cache_history_locate_entry(page, state);

    ham_assert(ref->addr == page_get_self(page), (0));

    if (state > 0)
    {
        ref->fetch++;
    }
    else 
    {
        ref->alloc++;
    }

#if 0
    {
        static int c = 0;

        c++;
        if (c % 500000 == 100000)
        {
            cache_report_history(env);
        }
    }
#endif
}

#endif




