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

#include "btree.h"
#include "btree_cursor.h"
#include "db.h"
#include "device.h"
#include "endianswap.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "freelist_statistics.h"
#include "mem.h"
#include "util.h"

namespace ham {

/**
 * statistics gatherer/hinter:
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
 * mode, and thus speed- versus storage consumption hints, on a per-insert
 * basis: a single
 * database can mix slow but spacesaving record inserts for those times /
 * tables when we do not need the extra oemph, while other inserts can
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
 * freelist is sorted in descending order --> last 1 bit, one assuming the
 * freelist is sorted in ascending
 * order (now that we 'know' the last free bit, this will scan the range
 * 0..last-1-bit to find the first 1 bit in there.
 *
 * Making sure we limit our # of samples irrespective of freelist page
 * size, so we can use the same stats gather for classic and modern
 * modes.
 *
 * perform such sampling using semi-random intervals: prevent being
 * sensitive to a particular pathologic case this way.
 */

/** scale @a val down by a factor of 256.
 * Make sure non-zero numbers remain non-zero: roundup(x)
 */
#define rescale_256(val)                            \
    val = ((val + 256 - 1) >> 8)

/**
 * inline function (must be fast!) which calculates the smallest
 * encompassing power-of-2 for the given value. The integer equivalent
 * of roundup(log2(value)).
 *
 * Returned value range: 0..64
 */
static __inline ham_u16_t
ham_log2(ham_u64_t v)
{
    if (v) {
        register ham_u16_t power = 64;
        register ham_s64_t value = (ham_s64_t)v;

        /*
         * test top bit by checking two's complement sign.
         *
         * This LOG2 is crafted to spend the least number of
         * rounds inside the BM freelist bitarray scans.
         */
        while (!(value < 0)) {
            power--;
            value <<= 1;
        }
        return power;
    }
    return 0;
}

static __inline ham_u16_t
ham_bitcount2bucket_index(ham_size_t size)
{
    ham_u16_t bucket = ham_log2(size);
    if (bucket >= HAM_FREELIST_SLOT_SPREAD)
        bucket = HAM_FREELIST_SLOT_SPREAD - 1;
    return bucket;
}

/**
 * inline function (must be fast!) which calculates the inverse of the
 * ham_log2() above:

 * converting a bucket index number to the maximum possible size for
 * that bucket.
 */
static __inline ham_size_t
ham_bucket_index2bitcount(ham_u16_t bucket)
{
    return (1U << (bucket * 1)) - 1;
}

static void
rescale_global_statistics(Environment *env)
{
    ham_runtime_statistics_globdata_t *globalstats = env->get_global_perf_data();
    ham_u16_t b;

    for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
        rescale_256(globalstats->scan_count[b]);
        rescale_256(globalstats->ok_scan_count[b]);
        rescale_256(globalstats->scan_cost[b]);
        rescale_256(globalstats->ok_scan_cost[b]);
        //rescale_256(globalstats->first_page_with_free_space[b]);
    }

    rescale_256(globalstats->insert_count);
    rescale_256(globalstats->delete_count);
    rescale_256(globalstats->extend_count);
    rescale_256(globalstats->fail_count);
    rescale_256(globalstats->search_count);
    rescale_256(globalstats->insert_query_count);
    rescale_256(globalstats->erase_query_count);
    rescale_256(globalstats->query_count);
    rescale_256(globalstats->rescale_monitor);
}

static void
rescale_freelist_page_stats(Freelist *cache, FreelistEntry *entry)
{
    ham_u16_t b;
    freelist_page_statistics_t *entrystats = &entry->perf_data._persisted_stats;

    for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
        //rescale_256(entrystats->per_size[b].first_start);
        //rescale_256(entrystats->per_size[b].free_fill);
        rescale_256(entrystats->per_size[b].epic_fail_midrange);
        rescale_256(entrystats->per_size[b].epic_win_midrange);
        rescale_256(entrystats->per_size[b].scan_count);
        rescale_256(entrystats->per_size[b].ok_scan_count);
        rescale_256(entrystats->per_size[b].scan_cost);
        rescale_256(entrystats->per_size[b].ok_scan_cost);
    }

    //rescale_256(entrystats->last_start);
    //rescale_256(entrystats->persisted_bits);
    rescale_256(entrystats->insert_count);
    rescale_256(entrystats->delete_count);
    rescale_256(entrystats->extend_count);
    rescale_256(entrystats->fail_count);
    rescale_256(entrystats->search_count);
    rescale_256(entrystats->rescale_monitor);

    entry->perf_data._dirty = HAM_TRUE;
}

void
freelist_stats_fail(Freelist *fl, FreelistEntry *entry, FreelistPayload *f,
                freelist_hints_t *hints)
{
    /*
     * freelist scans with a non-zero lower bound address are SPECIAL searches,
     * which should NOT corrupt our statistics in any way.
     */
    if (hints->lower_bound_address == 0) {
        ham_runtime_statistics_globdata_t *globalstats
                    = fl->get_env()->get_global_perf_data();
        freelist_page_statistics_t *entrystats
                    = &entry->perf_data._persisted_stats;
        ham_size_t cost = hints->cost;

        ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
        ham_u32_t position = entrystats->persisted_bits;

        // should NOT use freel_get_max_bitsXX(f) here!
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);

        entry->perf_data._dirty = HAM_TRUE;

        if (globalstats->rescale_monitor
                >= HAM_STATISTICS_HIGH_WATER_MARK - cost) {
            /* rescale cache numbers! */
            rescale_global_statistics(fl->get_env());
        }
        globalstats->rescale_monitor += cost;

        globalstats->fail_count++;
        globalstats->search_count++;
        globalstats->scan_cost[bucket] += cost;
        globalstats->scan_count[bucket]++;

        if (entrystats->rescale_monitor >=
                HAM_STATISTICS_HIGH_WATER_MARK - cost) {
            /* rescale cache numbers! */
            rescale_freelist_page_stats(fl, entry);
        }
        entrystats->rescale_monitor += cost;

        /* we _did_ look in the midrange, but clearly we were not lucky there */
        if (hints->startpos < entrystats->last_start)
            entrystats->per_size[bucket].epic_fail_midrange++;
        entrystats->fail_count++;
        entrystats->search_count++;
        entrystats->per_size[bucket].scan_cost += cost;
        entrystats->per_size[bucket].scan_count++;

        /*
         * only upgrade the fail-based start position to the very edge of
         * the freelist page's occupied zone, when the edge is known
         * (initialized).
         */
        if (!hints->aligned && position) {
            ham_u16_t b;
            /*
             * adjust the position to point at a free slot within the
             * occupied zone, which would produce such an outcome by having
             * too few free slots still in there following such a position.
             *
             * Hence we're saying there _is_ space (even when there may be
             * none at all) but we also say this free space is not large
             * enough to suit us.
             *
             * Why this weird juggling? Because, when the freelist is
             * expanded as new (free) pages become registered, we will then
             * have (a) sufficient free space (duh!) and most importantly,
             * we'll have (b) made sure the next search for available slots
             * by then does NOT skip/ignore those last few free bits we
             * still _may_ have in this preceding zone, which is a WIN when
             * we're into saving disc space.
             */
            ham_u32_t offset = entry->allocated_bits;
            if (offset > hints->size_bits)
                offset = hints->size_bits;
            if (position > offset - 1)
                position -= offset - 1;
            /*
             * now we are at the first position within the freelist page
             * where the reported FAIL for the given size_bits would happen,
             * guaranteed.
             */
            for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                if (entrystats->per_size[b].first_start < position)
                    entrystats->per_size[b].first_start = position;
                /* also update buckets for larger chunks at the same time */
            }

            if (entrystats->last_start < position)
                entrystats->last_start = position;
            for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                ham_assert(entrystats->last_start
                    >= entrystats->per_size[b].first_start);
            }
        }
    }
}

void
freelist_stats_update(Freelist *fl, FreelistEntry *entry, FreelistPayload *f,
                ham_u32_t position, freelist_hints_t *hints)
{
    /*
     * freelist scans with a non-zero lower bound address are SPECIAL searches,
     * which should NOT corrupt our statistics in any way.
     */
    if (hints->lower_bound_address == 0) {
        ham_u16_t b;
        ham_size_t cost = hints->cost;

        ham_runtime_statistics_globdata_t *globalstats =
                    fl->get_env()->get_global_perf_data();
        freelist_page_statistics_t *entrystats =
                    &entry->perf_data._persisted_stats;

        ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);

        entry->perf_data._dirty = HAM_TRUE;

        if (globalstats->rescale_monitor
                >= HAM_STATISTICS_HIGH_WATER_MARK - cost) {
            /* rescale cache numbers! */
            rescale_global_statistics(fl->get_env());
        }
        globalstats->rescale_monitor += cost;

        globalstats->search_count++;
        globalstats->ok_scan_cost[bucket] += cost;
        globalstats->scan_cost[bucket] += cost;
        globalstats->ok_scan_count[bucket]++;
        globalstats->scan_count[bucket]++;

        if (entrystats->rescale_monitor >=
                HAM_STATISTICS_HIGH_WATER_MARK - cost) {
            /* rescale cache numbers! */
            rescale_freelist_page_stats(fl, entry);
        }
        entrystats->rescale_monitor += cost;

        if (hints->startpos < entrystats->last_start) {
            if (position < entrystats->last_start) {
                /* we _did_ look in the midrange, but clearly we were not lucky there */
                entrystats->per_size[bucket].epic_fail_midrange++;
            }
            else {
                entrystats->per_size[bucket].epic_win_midrange++;
            }
        }
        entrystats->search_count++;
        entrystats->per_size[bucket].ok_scan_cost += cost;
        entrystats->per_size[bucket].scan_cost += cost;
        entrystats->per_size[bucket].ok_scan_count++;
        entrystats->per_size[bucket].scan_count++;

        /*
         * since we get called here when we just found a suitably large
         * free slot, that slot will be _gone_ for the next search, so we
         * bump up our 'free slots to be found starting here'
         * offset by size_bits, skipping the current space.
         */
        position += hints->size_bits;

        for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++) {
            if (entrystats->per_size[b].first_start < position)
                entrystats->per_size[b].first_start = position;
            /* also update buckets for larger chunks at the same time */
        }

        if (entrystats->last_start < position)
            entrystats->last_start = position;
        for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
            ham_assert(entrystats->last_start
                    >= entrystats->per_size[b].first_start);
        }

        if (entrystats->persisted_bits < position) {
            /* overflow? reset this marker! */
            ham_assert(entrystats->persisted_bits == 0);
            if (hints->size_bits > entry->allocated_bits)
                entrystats->persisted_bits = position;
            else
                /* extra HACKY safety margin */
                entrystats->persisted_bits = position
                            - hints->size_bits + entry->allocated_bits;
        }
    }
}

/*
 * No need to check for rescaling in here; see the notes that go with
 * 'cost_monitor' to know that these counter increments will always
 * remain below the current high water mark and hence do not risk
 * introducing integer overflow here.
 *
 * This applies to the edit, no_hit, and query stat update routines
 * below.
 */
void
freelist_stats_edit(Freelist *fl, FreelistEntry *entry, FreelistPayload *f,
                ham_u32_t position, ham_size_t size_bits, ham_bool_t free_these,
                freelist_hints_t *hints)
{
    /*
     * freelist scans with a non-zero lower bound address are SPECIAL searches,
     * which should NOT corrupt our statistics in any way.
     * In short: we are not (yet) capable of processing these runs into the
     * overall staistics gathering.
    */
    if (hints->lower_bound_address == 0) {
        ham_runtime_statistics_globdata_t *globalstats
                    = fl->get_env()->get_global_perf_data();
        freelist_page_statistics_t *entrystats
                    = &entry->perf_data._persisted_stats;

        ham_u16_t bucket = ham_bitcount2bucket_index(size_bits);
        ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);

        entry->perf_data._dirty = HAM_TRUE;

        if (free_these) {
            /*
             * addition of free slots: delete, transaction abort or DB
             * extend operation
             *
             * differentiate between them by checking if the new free zone
             * is an entirely fresh addition or sitting somewhere in already
             * used (recorded) space: extend or not?
             */
            ham_u16_t b;

            ham_assert(entrystats->last_start
                    >= entrystats->per_size[bucket].first_start);
            for (b = 0; b <= bucket; b++) {
                if (entrystats->per_size[b].first_start > position)
                    entrystats->per_size[b].first_start = position;
                /* also update buckets for smaller chunks at the same time */
            }

            /* if we just freed the chunk just BEFORE the 'last_free', why
             * not merge them, eh? */
            if (entrystats->last_start == position + size_bits) {
                entrystats->last_start = position;

                /* when we can adjust the last chunk, we should also adjust
                 *the start for bigger chunks... */
                for (b = bucket + 1; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                    if (entrystats->per_size[b].first_start > position)
                        entrystats->per_size[b].first_start = position;
                    /* also update buckets for smaller chunks at the same
                     * time */
                }
            }
            for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                ham_assert(entrystats->last_start
                        >= entrystats->per_size[b].first_start);
            }

            position += size_bits;

            /* if this is a 'free' for a newly created page, we'd need to
             * adjust the outer edge */
            if (entrystats->persisted_bits < position) {
                globalstats->extend_count++;

                ham_assert(entrystats->last_start < position);
                entrystats->persisted_bits = position;
            }
            else {
                //ham_assert(entrystats->last_start >= position);
                globalstats->delete_count++;
            }

            ham_assert(entrystats->persisted_bits >= position);

            {
                ham_u32_t entry_index = (ham_u32_t)(entry - fl->get_entries());

                ham_assert(entry_index >= 0);
                ham_assert(entry_index < freel_cache_get_count(fl));

                for (b = 0; b <= bucket; b++) {
                    if (globalstats->first_page_with_free_space[b] > entry_index)
                        globalstats->first_page_with_free_space[b] = entry_index;
                    /* also update buckets for smaller chunks at the same
                     * time */
                }
            }
        }
        else {
            ham_u16_t b;

            /*
             *  occupation of free slots: insert or similar operation
             */
            position += size_bits;

            for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                if (entrystats->per_size[b].first_start < position)
                    entrystats->per_size[b].first_start = position;
                /* also update buckets for larger chunks at the same time */
            }

            globalstats->insert_count++;

            if (entrystats->last_start < position)
                entrystats->last_start = position;
            for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                ham_assert(entrystats->last_start
                        >= entrystats->per_size[b].first_start);
            }

            if (entrystats->persisted_bits < position) {
                /*
                 * the next is really a HACKY HACKY stop-gap measure:
                 * we see that the last_ever_seen offset has not been
                 * initialized (or incorrectly initialized) up to now, so we
                 * guestimate where it is, guessing on the safe side: we
                 * assume all free bits are situated past the current
                 * location, and shift the last_ever_seen position up
                 * accordingly
                 */
                //globalstats->extend_count++;

                ham_assert(entrystats->persisted_bits == 0);
                entrystats->persisted_bits = position +
                    size_bits + entry->allocated_bits;
            }

            /*
             * maxsize within given bucket must still fit in the page, or
             * it's useless checking this page again.
             */
            if (ham_bucket_index2bitcount(bucket) > entry->allocated_bits) {
                ham_u32_t entry_index = (ham_u32_t)(entry - fl->get_entries());

                ham_assert(entry_index >= 0);
                ham_assert(entry_index < freel_cache_get_count(fl));

                /*
                 * We can update this number ONLY WHEN we have an
                 * allocation in the edge page;
                 * this is because we have modes where the freelist is
                 * checked in random and blindly updating the lower bound
                 * here would jeopardize the utilization of the DB.
                 *
                 * This applies to INCREMENTING the lower bound like we do
                 * here; we can ALWAYS DECREMENT the lower bound, as we do
                 * in the 'free_these' branch above.
                 */
                if (globalstats->first_page_with_free_space[bucket] == entry_index)
                {
                    for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++) {
                        if (globalstats->first_page_with_free_space[b] <= entry_index)
                            globalstats->first_page_with_free_space[b] = entry_index + 1;
                        /* also update buckets for smaller chunks at the
                         * same time */
                    }
                }
            }
        }
    }
}

void
freelist_globalhints_no_hit(Freelist *fl, FreelistEntry *entry,
                freelist_hints_t *hints)
{
    ham_runtime_statistics_globdata_t *globalstats = fl->get_env()->get_global_perf_data();

    ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
    ham_u32_t entry_index = (ham_u32_t)(entry - fl->get_entries());

    ham_assert(entry_index >= 0);
    ham_assert(entry_index < freel_cache_get_count(fl));

    ham_assert(hints->page_span_width >= 1);

    /*
     * We can update this number ONLY WHEN we have an allocation in the
     * edge page; this is because we have modes where the freelist is checked in
     * random and blindly updating the lower bound here would jeopardize
     * the utilization of the DB.
     */
    if (globalstats->first_page_with_free_space[bucket] == entry_index) {
        ham_u16_t b;

        for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++) {
            if (globalstats->first_page_with_free_space[b] <= entry_index)
                globalstats->first_page_with_free_space[b] = entry_index + hints->page_span_width;
            /* also update buckets for smaller chunks at the same time */
        }
    }
}

/**
 * This call assumes the 'dst' hint values have already been filled
 * with some sane values before; this routine will update those values
 * where it deems necessary.
 *
 * This function is called once for each operation that requires the
 * use of the freelist: it gives hints about where in the ENTIRE
 * FREELIST you'd wish to start searching; this means this hinter
 * differs from the 'per entry' hinter gelow in that it provides
 * freelist page indices instead of offsets: that last bit is the job of
 * the 'per entry hinter'; our job here is to cut down on the number of
 * freelist pages visited.
 */
void
freelist_get_global_hints(Freelist *fl, freelist_global_hints_t *dst)
{
    ham_runtime_statistics_globdata_t *globalstats = fl->get_env()->get_global_perf_data();

    ham_u32_t offset;
    ham_size_t pos;
    ham_u16_t bucket = ham_bitcount2bucket_index(dst->size_bits);
    ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);
    ham_assert(dst);
    ham_assert(dst->skip_init_offset == 0);
    ham_assert(dst->skip_step == 1);

#if 0 /* disabled printing of statistics */
    {
        static int c = 0;
        c++;
        if (c % 100000 == 999) {
            /*
             * what is our ratio fail vs. search?
             *
             * Since we know search >= fail, we'll calculate the
             * reciprocal in integer arithmetic, as that one will be >= 1.0
             */
            if (globalstats->fail_count) {
                ham_u64_t fail_reciprocal_ratio = globalstats->search_count;
                fail_reciprocal_ratio *= 1000;
                fail_reciprocal_ratio /= globalstats->fail_count;

                ham_trace(("GLOBAL FAIL/SEARCH ratio: %f",
                            1000.0/fail_reciprocal_ratio));
            }
            /*
             * and how about our scan cost per scan? and per good scan?
             */
            if (globalstats->scan_count[bucket]) {
                ham_u64_t cost_per_scan = globalstats->scan_cost[bucket];
                cost_per_scan *= 1000;
                cost_per_scan /= globalstats->scan_count[bucket];

                ham_trace(("GLOBAL COST/SCAN ratio: %f",
                            cost_per_scan/1000.0));
            }
            if (globalstats->ok_scan_count[bucket]) {
                ham_u64_t ok_cost_per_scan = globalstats->ok_scan_cost[bucket];
                ok_cost_per_scan *= 1000;
                ok_cost_per_scan /= globalstats->ok_scan_count[bucket];

                ham_trace(("GLOBAL 'OK' COST/SCAN ratio: %f",
                            ok_cost_per_scan/1000.0));
            }
            if (globalstats->erase_query_count
                    + globalstats->insert_query_count) {
                ham_u64_t trials_per_query = 0;
                int i;

                for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++)
                    trials_per_query += globalstats->scan_count[i];
                trials_per_query *= 1000;
                trials_per_query /= globalstats->erase_query_count
                                    + globalstats->insert_query_count;

                ham_trace(("GLOBAL TRIALS/QUERY (INSERT + DELETE) ratio: %f",
                            trials_per_query/1000.0));
            }
        }
    }
#endif

    /*
    determine where the search range starts; usually this is at the first
    freelist page.
    */
    ham_assert(HAM_MAX_U32 >= dst->lower_bound_address / (DB_CHUNKSIZE * dst->freelist_pagesize_bits));
    pos = (ham_size_t)(dst->lower_bound_address / (DB_CHUNKSIZE * dst->freelist_pagesize_bits));
    if (dst->start_entry < pos)
        dst->start_entry = pos;

    /*
     * improve our start position, when we know there's nothing to be
     * found before a given minimum offset
     */
    offset = globalstats->first_page_with_free_space[bucket];
    if (dst->start_entry < offset)
        dst->start_entry = offset;

    /*
     * if we are looking for space for a 'huge blob', i.e. a size which
     * spans multiple pages, we should let the caller know: round up the
     * number of full pages that we'll need for this one.
     */
    dst->page_span_width =
        (dst->size_bits + dst->freelist_pagesize_bits - 1)
            / dst->freelist_pagesize_bits;
    ham_assert(dst->page_span_width >= 1);

    /*
     * NOW that we have the range and everything to say things we are
     * certain about, we can further improve things by introducing a bit
     * of heuristics a.k.a. statistical mumbojumbo:
     *
     * when we're in UBER/FAST mode and SEQUENTIAL to boot, we only
     * wish to look at the last chunk of free space and ignore the rest.
     *
     * When we're in UBER/FAST mode, CLASSIC style, we don't feel like
     * wading through an entire freelist every time when we know already
     * that utilization is such that our chances at finding a match are
     * low, which means we'd rather turn this thing into SEQUENTIAL
     * mode, maybe even SEQUENTIAL+UBER/FAST, for as long as the
     * utilization is such that our chance at finding a match is still
     * rather low.
     */
    switch (dst->mgt_mode & (HAM_DAM_SEQUENTIAL_INSERT
                            | HAM_DAM_RANDOM_WRITE))
    {
        /* SEQ+RANDOM_ACCESS: impossible mode; nasty trick for testing
         * to help Overflow4 unittest pass: disables global hinting,
         * but does do reverse scan for a bit of speed */
    case HAM_DAM_RANDOM_WRITE | HAM_DAM_SEQUENTIAL_INSERT:
        dst->max_rounds = freel_cache_get_count(fl);
        dst->mgt_mode &= ~HAM_DAM_RANDOM_WRITE;
        if (0)
        {
    default:
            /* dst->max_rounds = freel_cache_get_count(fl); */
            dst->max_rounds = 32; /* speed up 'classic' for LARGE
                                     databases anyhow! */
        }
        if (0)
        {
        /*
         * here's where we get fancy:
         *
         * We allow ourselves a bit of magick: for larger freelists, we
         * cut down on the number of pages we'll probe during each
         * operation, thus cutting down on freelist scanning/hinting
         * work out there.
         *
         * The 'sensible' heuristic here is ...
         *
         * for 'non-UBER/FAST' modes: a limit of 8 freelist pages,
         *
         * for 'UBER/FAST' modes: a limit of 3 freelist pages tops.
         */
    case HAM_DAM_SEQUENTIAL_INSERT:
    case HAM_DAM_RANDOM_WRITE:
            dst->max_rounds = 8;
        }
        if (dst->max_rounds >= freel_cache_get_count(fl)) {
            /* dst->max_rounds = freel_cache_get_count(cache); */
        }
        else {
            /*
             * and to facilitate an 'even distribution' of the freelist
             * entries being scanned, we hint the scanner should use a
             * SRNG (semi random number generator) approach by using the
             * principle of a prime-modulo SRNG, where the next value is
             * calculated using a multiplier which is mutual prime with
             * the freelist entry count, followed by a modulo operation.
             *
             * _WE_ need to tweak that a bit as looking at any freelist
             * entries before the starting index there is useless as we
             * already know those entries don't carry sufficient free
             * space anyhow. Nevertheless we don't need to be very
             * mindful about it; we'll be using a large real number for
             * the semi-random generation of the next freelist entry
             * index, so all we got to do is make sure we've got our
             * 'size' MODULO correct when we use this hinting data.
             *
             * 295075153: we happen to have this large prime which
             * we'll assume will be larger than any sane freelist entry
             * list will ever get in this millenium ;-) so using it for
             * the mutual-prime multiplier in here will be fine.
             * (Incidentally, we say 'multiplier', but we use it really
             * as an adder, which is perfectly fine as any (A+B) MOD C
             * operation will have a cycle of B when the B is mutual
             * prime to C assuming a constant A; this also means that, as we
             * apply this operation multiple times in sequence, the resulting
             * numbers have a cycle of B and will therefore deliver a
             * rather flat distribution over C when B is suitably large
             * compared to C. (That last bit is not mandatory, but it generally
             * makes for a more semi-random skipping pattern.)
             */
            dst->skip_step=295075153;
            /*
             * The init_offset is just a number to break repetitiveness
             * of the generated pattern; in SRNG terms, this is the
             * seed.
             *
             * We re-use the statistics counts here as a 'noisy source'
             * for our seed. Note that we use the fail_count only as all
             * this randomization is fine and dandy, but we don't want
             * it to help thrash the page cache, so the freelist page
             * entry probe pattern should remian the same until a probe
             * FAILs; only then do we really need to change the pattern.
             */
            dst->skip_init_offset=globalstats->fail_count;
        }
        break;
    }

    /*
     * and it's no use trying more times (and freelist entries) then we
     * know we have available within the designated search range.
     */
    if (dst->max_rounds > freel_cache_get_count(fl) - dst->start_entry)
        dst->max_rounds = freel_cache_get_count(fl) - dst->start_entry;

    /*
     * To accommodate multi-freelist-entry spanning 'huge blob' free space
     * searches, we set up the init and step here to match that of a
     * Boyer-Moore search method.
     *
     * Yes, this means this code has intimate knowledge of the 'huge blob free
     * space search' caller, i.e. the algorithm used when
     *
     *   dst->page_span_width > 1
     *
     * and I agree it's nasty, but this way the outer call's code is more
     * straight-forward in handling both the regular, BM-assisted full scan
     * of the freelist AND the faster 'skipping' mode(s) possible here (e.g.
     * the UBER-FAST search mode where only part of the freelist will be
     * sampled for each request).
     */
    if (dst->skip_step < dst->page_span_width) {
        /*
         * set up for BM: init = 1 step ahead minus 1, as we check the LAST
         * entry instead of the FIRST, and skip=span so we jump over the
         * freelist according to the BM plan: no hit on the sample means the
         * next possible spot will include sample current+span.
         */
        dst->skip_init_offset = dst->page_span_width - 1;
        dst->skip_step = dst->page_span_width;
    }
}

/**
 * This call assumes the 'dst' hint values have already been filled
 * with some sane values before; this routine will update those values
 * where it deems necessary.
 */
void
freelist_get_entry_hints(Freelist *fl, FreelistEntry *entry,
                freelist_hints_t *dst)
{
    freelist_page_statistics_t *entrystats = &entry->perf_data._persisted_stats;

    ham_u32_t offset;
    ham_u16_t bucket = ham_bitcount2bucket_index(dst->size_bits);
    ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD);
    ham_assert(dst);

    /*
     * we can decide to 'up' the skip/probe_step size in the hints when
     * we find out we're running into a lot of fragmentation, i.e.
     * lots of free slot hints which don't lead to a perfect hit.
     *
     * By bumping up the probestep distance, we can also 'upgrade' our
     * start offset to come from the next bucket: the one meant for the
     * bigger boys out there.
     */

#if 0 /* disabled printing of statistics */
    {
    static int c = 0;
    ham_runtime_statistics_globdata_t *globalstats = env->get_global_perf_data();
    c++;
    if (c % 100000 == 999) {
        /*
         * what is our ratio fail vs. search?
         *
         * Since we know search >= fail, we'll calculate the
         * reciprocal in integer arithmetic, as that one will be >= 1.0
         */
        if (globalstats->fail_count) {
            ham_u64_t fail_reciprocal_ratio = globalstats->search_count;
            fail_reciprocal_ratio *= 1000;
            fail_reciprocal_ratio /= globalstats->fail_count;

            ham_trace(("FAIL/SEARCH ratio: %f", 1000.0/fail_reciprocal_ratio));
        }
        /*
         * and how about our scan cost per scan? and per good scan?
         */
        if (globalstats->scan_count[bucket]) {
            ham_u64_t cost_per_scan = globalstats->scan_cost[bucket];
            cost_per_scan *= 1000;
            cost_per_scan /= globalstats->scan_count[bucket];

            ham_trace(("COST/SCAN ratio: %f", cost_per_scan/1000.0));
        }
        if (globalstats->ok_scan_count[bucket]) {
            ham_u64_t ok_cost_per_scan = globalstats->ok_scan_cost[bucket];
            ok_cost_per_scan *= 1000;
            ok_cost_per_scan /= globalstats->ok_scan_count[bucket];

            ham_trace(("'OK' COST/SCAN ratio: %f", ok_cost_per_scan/1000.0));
        }
        if (globalstats->erase_query_count
            + globalstats->insert_query_count) {
            ham_u64_t trials_per_query = 0;
            int i;

            for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++)
            {
                trials_per_query += globalstats->scan_count[i];
            }
            trials_per_query *= 1000;
            trials_per_query /= globalstats->erase_query_count
                                + globalstats->insert_query_count;

            ham_trace(("TRIALS/QUERY (INSERT + DELETE) ratio: %f",
                        trials_per_query/1000.0));
        }

        /*
         * what is our FREELIST PAGE's ratio fail vs. search?
         *
         * Since we know search >= fail, we'll calculate the
         * reciprocal in integer arithmetic, as that one will be >= 1.0
         */
        if (entrystats->fail_count) {
            ham_u64_t fail_reciprocal_ratio = entrystats->search_count;
            fail_reciprocal_ratio *= 1000;
            fail_reciprocal_ratio /= entrystats->fail_count;

            ham_trace(("PAGE FAIL/SEARCH ratio: %f",
                        1000.0/fail_reciprocal_ratio));
        }
        /*
         * and how about our scan cost per scan? and per good scan?
         */
        if (entrystats->per_size[bucket].scan_count) {
            ham_u64_t cost_per_scan = entrystats->per_size[bucket].scan_cost;
            cost_per_scan *= 1000;
            cost_per_scan /= entrystats->per_size[bucket].scan_count;

            ham_trace(("PAGE COST/SCAN ratio: %f", cost_per_scan/1000.0));
        }
        if (entrystats->per_size[bucket].ok_scan_count) {
            ham_u64_t ok_cost_per_scan =
                entrystats->per_size[bucket].ok_scan_cost;
            ok_cost_per_scan *= 1000;
            ok_cost_per_scan /= entrystats->per_size[bucket].ok_scan_count;

            ham_trace(("PAGE 'OK' COST/SCAN ratio: %f",
                       ok_cost_per_scan/1000.0));
        }
    }
    }
#endif

    ham_assert(entrystats->last_start
            >= entrystats->per_size[bucket].first_start);
    ham_assert(entrystats->persisted_bits
            >= entrystats->last_start);

    /*
     * improve our start position, when we know there's nothing to be
     * found before a given minimum offset
     */
    offset = entrystats->per_size[bucket].first_start;
    if (dst->startpos < offset)
        dst->startpos = offset;

    offset = entrystats->persisted_bits;
    if (offset == 0) {
        /*
         * we need to init this one; take the allocated_bits size as a
         * heuristically sound (ahem) probe_step value and backtrack
         * from the end of the freelist page towards occupied country,
         * praying we find a free slot.
         *
         * We can help ourselves by limiting such a scan down to the
         * topmost start position reported for the freelist page, thus
         * cutting down on scanning overhead.
         *
         * Another improvement would be to forget about initializing it
         * here, instead waiting for our next stats update to come in:
         * there, we'll certainly have a starting offset to gawk at.
         *
         * And that leaves us the last bit of knowledge we can abuse in
         * this backtracking scan process in search for that elusive
         * last-ever free slot: freelist pages are sometimes created
         * from entire disc pages, sometimes they are put in with other
         * bits, occupying the surplus of a disc page. As such, we
         * cannot assume that a freelist page's first bit starts at a
         * discpage boundary, but we CAN  be certain that the ever-last
         * added free slot out there will be sitting at the very end of
         * some discpage. And that's what we can scan for: as long as
         * there's a free slot to have, it's got to be at such a spot!
         */

        /* TODO */
    }
    else {
        /*
        reduce the search range to span only the really available
        free slots
        */
        if (dst->endpos > offset)
            dst->endpos = offset;

        /* take alignment into account as well! */
        if (dst->aligned) {
            ham_u32_t alignment = fl->get_env()->get_pagesize() / DB_CHUNKSIZE;
            dst->startpos += alignment - 1;
            dst->startpos -= dst->startpos % alignment;
        }
    }
}

/**
 * copy one internal format freelist statistics record to a public format
 * record for the same.
 *
 * Can't use memcpy() because of alignment issues we don't want the hamsterdb
 * API user to bother about -- let alone forcing him/her to include the
 * packstart.h and packstop.h header files too.
 */
static void
copy_freelist_page_stat2api_rec(ham_freelist_page_statistics_t *dst,
                freelist_page_statistics_t *src)
{
    int i;

    for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++) {
        ham_freelist_slotsize_stats_t *d = dst->per_size + i;
        freelist_slotsize_stats_t *s = src->per_size + i;

        d->first_start = s->first_start;
        d->free_fill = s->free_fill;
        d->epic_fail_midrange = s->epic_fail_midrange;
        d->epic_win_midrange = s->epic_win_midrange;
        d->scan_count = s->scan_count;
        d->ok_scan_count = s->ok_scan_count;
        d->scan_cost = s->scan_cost;
        d->ok_scan_cost = s->ok_scan_cost;
    }

    dst->last_start = src->last_start;
    dst->persisted_bits = src->persisted_bits;
    dst->insert_count = src->insert_count;
    dst->delete_count = src->delete_count;
    dst->extend_count = src->extend_count;
    dst->fail_count = src->fail_count;
    dst->search_count = src->search_count;
    dst->rescale_monitor = src->rescale_monitor;
}

/**
 * The @ref ham_statistics_t cleanup/free callback function: this one is
 * needed as we must use the same system to free any allocated heap storage as
 * we used for allocating such storage in the first place, i.e. our freelist
 * stats array.
 */
static void
cleanup_ham_statistics_t(ham_statistics_t *dst)
{
    Allocator *a = (Allocator *)dst->_free_func_internal_arg;

    /* cleanup is simple: when it was allocated, free the freelist stats array */
    if (dst->freelist_stats) {
        a->free(dst->freelist_stats);
        dst->freelist_stats = NULL;
    }
    dst->freelist_stats_maxalloc = 0;

    /* and blow ourselves away from dst, while keeping the other data in
     * dst intact: */
    dst->_free_func = 0;
    dst->_free_func_internal_arg = NULL;
}

ham_status_t
freelist_fill_statistics_t(Freelist *fl, ham_statistics_t *dst)
{
    ham_bool_t collect_freelistdata;

    ham_assert(dst);

    /* copy the user-specified selectors before we zero the whole darn thing */
    collect_freelistdata = (!dst->dont_collect_freelist_stats && fl->get_env());

    /* now the tougher part: see if we should report the freelist statistics */
    if (collect_freelistdata) {
        Allocator *allocator = fl->get_env()->get_allocator();

        if (!fl || !allocator || !fl->get_entries())
            collect_freelistdata = HAM_FALSE;
        else {
            ham_size_t count = freel_cache_get_count(fl);

            if (count > 0) {
                ham_freelist_page_statistics_t *d;
                ham_size_t i;

                dst->_free_func = cleanup_ham_statistics_t;
                dst->_free_func_internal_arg = (void *)allocator;

                d = dst->freelist_stats = (ham_freelist_page_statistics_t *)
                            allocator->alloc(count
                                    * sizeof(dst->freelist_stats[0]));
                if (!d)
                    return (HAM_OUT_OF_MEMORY);
                memset(d, 0, count * sizeof(dst->freelist_stats[0]));

                /* now fill those API freelist records from the regular
                 * (internal) ones: */
                for (i = 0; i < count; i++) {
                    FreelistEntry *entry = fl->get_entries() + i;

                    copy_freelist_page_stat2api_rec(d + i,
                            &entry->perf_data._persisted_stats);
                }
            }

            dst->freelist_stats_maxalloc = count;
            dst->freelist_record_count = count;
        }
    }

    /* and finally mark which sections have actually been fetched */
    dst->dont_collect_freelist_stats = !collect_freelistdata;

    return 0;
}

} // namespace ham
