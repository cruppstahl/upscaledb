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
 */

#include "config.h"

#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/hamsterdb_stats.h>
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"
#include "btree_cursor.h"
#include "btree.h"
#include "util.h"
#include "statistics.h"






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
 * space-saving/classic mode; use the other 'start of free spcae at end
 * of the page'
 * marker as the starting point for (uber-)fast searches.
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
 *  resulting in expanded code and yet another pathological case fit for
 * the new code situation, etc.etc. ad nauseam. Instead, we use
 * statistical measures to express an estimate, i.e. the chance that we
 * might need to scan a large portion of the freelist when we
 *  run in classic spacesaving insert mode, and apply that statistical
 * data to the hinter, using the current mode.
 *
 *  -- YES, that also means we are able to switch freelist scanning
 * mode, and thus speed-
 *  versus storage consumption hints, on a per-insert basis: a single
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
 *  And mode 4 can be used to enforce full scan or something like that:
 * this can be used to improve the statistics as those are not persisted
 * on disc.
 *
 *
 *  The stats gatherer is delivering the most oomph, especially for tiny
 * keys and records, where Boyer-Moore is not really effective (or even
 * counter productive); gathering stats about the free slots and
 * occupied slots helps us speeding up multiple inserts, even while the
 * data is only alive for 1 run-time open-close period of time.
 *
 *
 *  Make the cache counter code indirect, so we can switch and test
 * various cache aging systems quickly.
 *
 *
 *
 *  When loading a freelist page, we can use sampling to get an idea of
 * where the LAST zone starts and ends (2 bsearches: one assuming the
 * freelist is sorted in descending order
 *  --> last 1 bit, one assuming the freelist is sorted in ascending
 * order (now that we
 *  'know' the last free bit, this will scan the range 0..last-1-bit to
 * find the first 1 bit in there.
 *
 *  Making sure we limit our # of samples irrespective of freelist page
 * size, so we can use the same stats gather for classic and modern
 * modes.
 *
 *
 *  perform such sampling using semi-random intervals: prevent being
 * sensitive to a particular pathologic case this way.
 */



#define rescale_256(val)							\
	val = ((val + 256 - 1) >> 8) /* make sure non-zero numbers remain non-zero: roundup(x) */

#define rescale_2(val)							\
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
 *  converting a bucket index number to the maximum possible size for
 * that bucket.
 */
static __inline ham_size_t ham_bucket_index2bitcount(ham_u16_t bucket)
{
	return (1U << (bucket * 1)) - 1;
}






static void
rescale_global_statistics(ham_db_t *db)
{
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	ham_u16_t b;

	for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
	{
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
rescale_freelist_page_stats(freelist_cache_t *cache, freelist_entry_t *entry)
{
	ham_u16_t b;
	freelist_page_statistics_t *entrystats = freel_entry_get_statistics(entry);

	for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
	{
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

	freel_entry_statistics_set_dirty(entry);
}

void
db_update_freelist_stats_fail(ham_db_t *db, freelist_entry_t *entry,
					freelist_payload_t *f, 
					freelist_hints_t *hints)
{
    freelist_cache_t *cache = db_get_freelist_cache(db);
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	freelist_page_statistics_t *entrystats = freel_entry_get_statistics(entry);
	ham_size_t cost = hints->cost;

	ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
	ham_u32_t position = entrystats->persisted_bits;

	// should NOT use freel_get_max_bitsXX(f) here!
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	freel_entry_statistics_set_dirty(entry);

	if (globalstats->rescale_monitor >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
	{
		/* rescale cache numbers! */
		rescale_global_statistics(db);
	}
	globalstats->rescale_monitor += cost;

	globalstats->fail_count++;
	globalstats->search_count++;
	globalstats->scan_cost[bucket] += cost;
	globalstats->scan_count[bucket]++;

	if (entrystats->rescale_monitor >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
	{
		/* rescale cache numbers! */
		rescale_freelist_page_stats(cache, entry);
	}
	entrystats->rescale_monitor += cost;

	if (hints->startpos < entrystats->last_start)
	{
		/* we _did_ look in the midrange, but clearly we were not lucky there */
		entrystats->per_size[bucket].epic_fail_midrange++;
	}
	entrystats->fail_count++;
	entrystats->search_count++;
	entrystats->per_size[bucket].scan_cost += cost;
	entrystats->per_size[bucket].scan_count++;

    /*
     * only upgrade the fail-based start position to the very edge of
     * the freelist page's occupied zone, when the edge is known
     * (initialized).
     */
	if (!hints->aligned && position)
	{
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
		ham_u32_t offset = entry->_allocated_bits;
		if (offset > hints->size_bits)
		{
			offset = hints->size_bits;
		}
		if (position > offset - 1)
		{
			position -= offset - 1;
		}
        /*
         * now we are at the first position within the freelist page
         * where the reported FAIL for the given size_bits would happen,
         * guaranteed.
         */
		for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			if (entrystats->per_size[b].first_start < position)
			{
				entrystats->per_size[b].first_start = position;
			}
			/* also update buckets for larger chunks at the same time */
		}

		if (entrystats->last_start < position)
		{
			entrystats->last_start = position;
		}
		for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			ham_assert(entrystats->last_start >= entrystats->per_size[b].first_start, (0));
		}
	}
}


void
db_update_freelist_stats(ham_db_t *db, freelist_entry_t *entry,
					freelist_payload_t *f, 
					ham_u32_t position, 
					freelist_hints_t *hints)
{
	ham_u16_t b;
	ham_size_t cost = hints->cost;

    freelist_cache_t *cache = db_get_freelist_cache(db);
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	freelist_page_statistics_t *entrystats = freel_entry_get_statistics(entry);

	ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	freel_entry_statistics_set_dirty(entry);

	if (globalstats->rescale_monitor >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
	{
		/* rescale cache numbers! */
		rescale_global_statistics(db);
	}
	globalstats->rescale_monitor += cost;

	globalstats->search_count++;
	globalstats->ok_scan_cost[bucket] += cost;
	globalstats->scan_cost[bucket] += cost;
	globalstats->ok_scan_count[bucket]++;
	globalstats->scan_count[bucket]++;

	if (entrystats->rescale_monitor >= HAM_STATISTICS_HIGH_WATER_MARK - cost)
	{
		/* rescale cache numbers! */
		rescale_freelist_page_stats(cache, entry);
	}
	entrystats->rescale_monitor += cost;

	if (hints->startpos < entrystats->last_start)
	{
		if (position < entrystats->last_start)
		{
			/* we _did_ look in the midrange, but clearly we were not lucky there */
			entrystats->per_size[bucket].epic_fail_midrange++;
		}
		else
		{
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
     *  offset by size_bits, skipping the current space.
     */
	position += hints->size_bits;
	
	for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++)
	{
		if (entrystats->per_size[b].first_start < position)
		{
			entrystats->per_size[b].first_start = position;
		}
		/* also update buckets for larger chunks at the same time */
	}

	if (entrystats->last_start < position)
	{
		entrystats->last_start = position;
	}
	for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
	{
		ham_assert(entrystats->last_start >= entrystats->per_size[b].first_start, (0));
	}

	if (entrystats->persisted_bits < position)
	{
		/* overflow? reset this marker! */
		ham_assert(entrystats->persisted_bits == 0, ("Should not get here when not invoked from the [unit]tests!"));
		if (hints->size_bits > entry->_allocated_bits)
		{
			entrystats->persisted_bits = position;
		}
		else
		{
			/* extra HACKY safety margin */ 
			entrystats->persisted_bits = position - hints->size_bits + entry->_allocated_bits;
		}
	}
}


/*
 * No need to check for rescaling in here; see the notes that go with
 * 'cost_monitor' to know that these counter increments will always
 *  remain below the current high water mark and hence do not risk
 * introducing integer overflow here.
 *
 * This applies to the edit, no_hit, and query stat update routines
 * below.
 */

void
db_update_freelist_stats_edit(ham_db_t *db, freelist_entry_t *entry, 
					freelist_payload_t *f, 
					ham_u32_t position, 
					ham_size_t size_bits, 
					ham_bool_t free_these, 
					ham_u16_t mgt_mode)
{
    freelist_cache_t *cache = db_get_freelist_cache(db);
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	freelist_page_statistics_t *entrystats = freel_entry_get_statistics(entry);

	ham_u16_t bucket = ham_bitcount2bucket_index(size_bits);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	freel_entry_statistics_set_dirty(entry);

	if (free_these)
	{
        /*
         * addition of free slots: delete, transaction abort or DB
         * extend operation
         *
         * differentiate between them by checking if the new free zone
         * is an entirely fresh addition or sitting somewhere in already
         * used (recorded) space: extend or not?
         */
		ham_u16_t b;

		ham_assert(entrystats->last_start >= entrystats->per_size[bucket].first_start, (0));
		for (b = 0; b <= bucket; b++)
		{
			if (entrystats->per_size[b].first_start > position)
			{
				entrystats->per_size[b].first_start = position;
			}
			/* also update buckets for smaller chunks at the same time */
		}

        /* if we just freed the chunk just BEFORE the 'last_free', why
         * not merge them, eh? */
		if (entrystats->last_start == position + size_bits)
		{
			entrystats->last_start = position;

            /* when we can adjust the last chunk, we should also adjust
             *the start for bigger chunks... */
			for (b = bucket + 1; b < HAM_FREELIST_SLOT_SPREAD; b++)
			{
				if (entrystats->per_size[b].first_start > position)
				{
					entrystats->per_size[b].first_start = position;
				}
                /* also update buckets for smaller chunks at the same
                 * time */
			}
		}
		for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			ham_assert(entrystats->last_start >= entrystats->per_size[b].first_start, (0));
		}

		position += size_bits;

        /* if this is a 'free' for a newly created page, we'd need to
         * adjust the outer edge */
		if (entrystats->persisted_bits < position)
		{
			globalstats->extend_count++;

			ham_assert(entrystats->last_start < position, (0));
			entrystats->persisted_bits = position;
		}
		else
		{
			//ham_assert(entrystats->last_start >= position, (0));
			globalstats->delete_count++;
		}

		ham_assert(entrystats->persisted_bits >= position, (0));

		{
			ham_u32_t entry_index = (ham_u32_t)(entry - freel_cache_get_entries(cache));

			ham_assert(entry_index >= 0, (0));
			ham_assert(entry_index < freel_cache_get_count(cache), (0));

			for (b = 0; b <= bucket; b++)
			{
				if (globalstats->first_page_with_free_space[b] > entry_index)
				{
					globalstats->first_page_with_free_space[b] = entry_index;
				}
                /* also update buckets for smaller chunks at the same
                 * time */
			}
		}
	}
	else
	{
		ham_u16_t b;

        /*
         *  occupation of free slots: insert or similar operation
         */
		position += size_bits;

		for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			if (entrystats->per_size[b].first_start < position)
			{
				entrystats->per_size[b].first_start = position;
			}
			/* also update buckets for larger chunks at the same time */
		}

		globalstats->insert_count++;

		if (entrystats->last_start < position)
		{
			entrystats->last_start = position;
		}
		for (b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			ham_assert(entrystats->last_start >= entrystats->per_size[b].first_start, (0));
		}

		if (entrystats->persisted_bits < position)
		{
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

			ham_assert(entrystats->persisted_bits == 0, ("Should not get here when not invoked from the [unit]tests!"));
			entrystats->persisted_bits = position + size_bits + entry->_allocated_bits;
		}

        /*
         * maxsize within given bucket must still fit in the page, or
         * it's useless checking this page again.
         */
		if (ham_bucket_index2bitcount(bucket) > freel_entry_get_allocated_bits(entry))
		{
			ham_u32_t entry_index = (ham_u32_t)(entry - freel_cache_get_entries(cache));

			ham_assert(entry_index >= 0, (0));
			ham_assert(entry_index < freel_cache_get_count(cache), (0));

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
				for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++)
				{
					if (globalstats->first_page_with_free_space[b] <= entry_index)
					{
						globalstats->first_page_with_free_space[b] = entry_index + 1;
					}
                    /* also update buckets for smaller chunks at the
                     * same time */
				}
			}
		}
	}
}




void
db_update_freelist_globalhints_no_hit(ham_db_t *db, freelist_entry_t *entry, freelist_hints_t *hints)
{
    freelist_cache_t *cache = db_get_freelist_cache(db);
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);

	ham_u16_t bucket = ham_bitcount2bucket_index(hints->size_bits);
	ham_u32_t entry_index = (ham_u32_t)(entry - freel_cache_get_entries(cache));

	ham_assert(entry_index >= 0, (0));
	ham_assert(entry_index < freel_cache_get_count(cache), (0));

	ham_assert(hints->page_span_width >= 1, (0));

    /*
     *  We can update this number ONLY WHEN we have an allocation in the
     * edge page;
     *  this is because we have modes where the freelist is checked in
     * random and blindly updating the lower bound here would jeopardize
     * the utilization of the DB.
     */
	if (globalstats->first_page_with_free_space[bucket] == entry_index)
	{
		ham_u16_t b;

		for (b = bucket; b < HAM_FREELIST_SLOT_SPREAD; b++)
		{
			if (globalstats->first_page_with_free_space[b] <= entry_index)
			{
				globalstats->first_page_with_free_space[b] = entry_index + hints->page_span_width;
			}
			/* also update buckets for smaller chunks at the same time */
		}
	}
}



void
db_update_global_stats_find_query(ham_db_t *db, ham_size_t key_size)
{
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);

	ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	globalstats->query_count++;

	opstats->query_count++;
}


void
db_update_global_stats_insert_query(ham_db_t *db, ham_size_t key_size, ham_size_t record_size)
{
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_INSERT);

	ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	globalstats->insert_query_count++;

	opstats->query_count++;
}


void
db_update_global_stats_erase_query(ham_db_t *db, ham_size_t key_size)
{
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_ERASE);

	ham_u16_t bucket = ham_bitcount2bucket_index(key_size / DB_CHUNKSIZE);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

	globalstats->erase_query_count++;

	opstats->query_count++;
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
db_get_global_freelist_hints(freelist_global_hints_t *dst, ham_db_t *db)
{
    freelist_cache_t *cache = db_get_freelist_cache(db);
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);

	ham_u32_t offset;
	ham_u16_t bucket = ham_bitcount2bucket_index(dst->size_bits);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
	ham_assert(dst, (0));
	ham_assert(dst->skip_init_offset == 0, (0));
	ham_assert(dst->skip_step == 1, (0));

	{
		static int c = 0;
		c++;
		if (c % 100000 == 999)
		{
			/* 
			what is our ratio fail vs. search? 

			Since we know search >= fail, we'll calculate the
			reciprocal in integer arithmetic, as that one will be >= 1.0
			*/
			if (globalstats->fail_count)
			{
				ham_u64_t fail_reciprocal_ratio = globalstats->search_count;
				fail_reciprocal_ratio *= 1000;
				fail_reciprocal_ratio /= globalstats->fail_count;

				ham_trace(("GLOBAL FAIL/SEARCH ratio: %f", 1000.0/fail_reciprocal_ratio));
			}
			/*
			and how about our scan cost per scan? and per good scan?
			*/
			if (globalstats->scan_count[bucket])
			{
				ham_u64_t cost_per_scan = globalstats->scan_cost[bucket];
				cost_per_scan *= 1000;
				cost_per_scan /= globalstats->scan_count[bucket];

				ham_trace(("GLOBAL COST/SCAN ratio: %f", cost_per_scan/1000.0));
			}
			if (globalstats->ok_scan_count[bucket])
			{
				ham_u64_t ok_cost_per_scan = globalstats->ok_scan_cost[bucket];
				ok_cost_per_scan *= 1000;
				ok_cost_per_scan /= globalstats->ok_scan_count[bucket];

				ham_trace(("GLOBAL 'OK' COST/SCAN ratio: %f", ok_cost_per_scan/1000.0));
			}
			if (globalstats->erase_query_count
				+ globalstats->insert_query_count)
			{
				ham_u64_t trials_per_query = 0;
				int i;
				
				for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++)
				{
					trials_per_query += globalstats->scan_count[i];
				}
				trials_per_query *= 1000;
				trials_per_query /= globalstats->erase_query_count
									+ globalstats->insert_query_count;

				ham_trace(("GLOBAL TRIALS/QUERY (INSERT + DELETE) ratio: %f", trials_per_query/1000.0));
			}
		}
	}


    /*
     * improve our start position, when we know there's nothing to be
     * found before a given minimum offset
     */
	offset = globalstats->first_page_with_free_space[bucket];
	if (dst->start_entry < offset)
	{
		dst->start_entry = offset;
	}

	/*
	if we are looking for space for a 'huge blob', i.e. a size which spans multiple
	pages, we should let the caller know: round up the number of full pages that we'll
	need for this one.
	*/
	dst->page_span_width = (dst->size_bits + dst->freelist_pagesize_bits - 1) / dst->freelist_pagesize_bits;
	ham_assert(dst->page_span_width >= 1, (0));

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
							| HAM_DAM_RANDOM_WRITE_ACCESS
							| HAM_DAM_FAST_INSERT))
	{
		/* SEQ+RANDOM_ACCESS: impossible mode; nasty trick for testing to help Overflow4 unittest pass: disables global hinting, but does do reverse scan for a bit of speed */
	case HAM_DAM_RANDOM_WRITE_ACCESS | HAM_DAM_SEQUENTIAL_INSERT:
		dst->max_rounds = freel_cache_get_count(cache);
		dst->mgt_mode &= ~HAM_DAM_RANDOM_WRITE_ACCESS;
		if (0)
		{
	default:
			// dst->max_rounds = freel_cache_get_count(cache);
			dst->max_rounds = 32; /* speed up 'classic' for LARGE databases anyhow! */
		}
		if (0)
		{
        /*
         *  here's where we get fancy:
         *
         *  We allow ourselves a bit of magick: for larger freelists, we
         * cut down on the number of pages we'll probe during each
         * operation, thus cutting down on freelist scanning/hinting
         * work out there.
         *
         *  The 'sensible' heuristic here is ...
         *  for 'non-UBER/FAST' modes: a limit of 8 freelist pages,
         *
         *  for 'UBER/FAST' modes: a limit of 3 freelist pages tops.
         */
	case HAM_DAM_SEQUENTIAL_INSERT:
	case HAM_DAM_RANDOM_WRITE_ACCESS:
			dst->max_rounds = 8;
		}
		if (0)
		{
	case HAM_DAM_FAST_INSERT:
	case HAM_DAM_RANDOM_WRITE_ACCESS | HAM_DAM_FAST_INSERT:
	case HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT:
			dst->max_rounds = 3;
		}
		if (dst->max_rounds >= freel_cache_get_count(cache))
		{
			dst->max_rounds = freel_cache_get_count(cache);
        }
        else
        {
            /*
             *  and to facilitate an 'even distribution' of the freelist
             * entries being scanned, we hint the scanner should use a
             * SRNG (semi random number generator) approach by using the
             * principle of a prime-modulo SRNG, where the next value is
             * calculated using a multiplier which is mutual prime with
             * the freelist entry count, followed by a modulo operation.
             *
             *  _WE_ need to tweak that a bit as looking at any freelist
             * entries before the starting index there is useless as we
             * already know those entries don't carry sufficient free
             * space anyhow. Nevertheless we don't need to be very
             * mindful about it; we'll be using a large real number for
             * the semi-random generation of the next freelist entry
             * index, so all we got to do is make sure we've got our
             * 'size' MODULO correct when we use this hinting data.
             *
             *  295075153: we happen to have this large prime which
             * we'll assume will be larger than any sane freelist entry
             * list will ever get in this millenium ;-) so using it for
             * the mutual-prime multiplier in here will be fine.
             *  (Incidentally, we say 'multiplier', but we use it really
             * as an adder, which is perfectly fine as any (A+B) MOD C
             * operation will have a cycle of B when the B is mutual
             * prime to C assuming a constant A; this also means that, as we apply this
             * operation multiple times in sequence, the resulting
             * numbers have a cycle of B and will therefore deliver a
             * rather flat distribution over C when B is suitably large
             * compared to C. (That last bit is not mandatory, but it generally makes
             * for a more semi-random skipping pattern.)
             */
            dst->skip_step=295075153;
            /*
             * The init_offset is just a number to break repetiveness
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
	To accomodate multi-freelist-entry spanning 'huge blob' free space searches,
	we set up the init and step here to match that of a Boyer-Moore search method.

	Yes, this means this code has intimate knowledge of the 'huge blob free space search'
	caller, i.e. the algorithm used when 
	
	  dst->page_span_width > 1

    and I agree it's nasty, but this way the outer call's code is more straight-forward
	in handling both the regular, BM-assisted full scan of the freelist AND the faster
	'skipping' mode(s) possible here (e.g. the UBER-FAST search mode where only part of 
	the freelist will be sampled for each request).
	*/
	if (dst->skip_step < dst->page_span_width)
	{
		/*
		set up for BM: init = 1 step ahead minus 1, as we check the LAST entry instead
		of the FIRST, and skip=span so we jump over the freelist according to the BM plan:
		no hit on the sample means the next possible spot will include sample current+span.
		*/
		dst->skip_init_offset = dst->page_span_width - 1;
		dst->skip_step = dst->page_span_width;
	}
}



/*
 * This call assumes the 'dst' hint values have already been filled
 * with some sane values before; this routine will update those values
 * where it deems necessary.
 */
void
db_get_freelist_entry_hints(freelist_hints_t *dst, ham_db_t *db, freelist_entry_t *entry)
{
	ham_runtime_statistics_globdata_t *globalstats = db_get_global_perf_data(db);
	freelist_page_statistics_t *entrystats = freel_entry_get_statistics(entry);

	ham_u32_t offset;
	ham_u16_t bucket = ham_bitcount2bucket_index(dst->size_bits);
	ham_assert(bucket < HAM_FREELIST_SLOT_SPREAD, (0));
	ham_assert(dst, (0));

    /*
     * we can decide to 'up' the skip/probe_step size in the hints when
     * we find out we're running into a lot of fragmentation, i.e.
     * lots of free slot hints which don't lead to a perfect hit.
     *
     * By bumping up the probestep distance, we can also 'upgrade' our
     * start offset to come from the next bucket: the one meant for the
     * bigger boys out there.
     */

	{
		static int c = 0;
		c++;
	if (c % 100000 == 999)
	{
		/* 
		what is our ratio fail vs. search? 

		Since we know search >= fail, we'll calculate the
		reciprocal in integer arithmetic, as that one will be >= 1.0
		*/
		if (globalstats->fail_count)
		{
			ham_u64_t fail_reciprocal_ratio = globalstats->search_count;
			fail_reciprocal_ratio *= 1000;
			fail_reciprocal_ratio /= globalstats->fail_count;

			ham_trace(("FAIL/SEARCH ratio: %f", 1000.0/fail_reciprocal_ratio));
		}
		/*
		and how about our scan cost per scan? and per good scan?
		*/
		if (globalstats->scan_count[bucket])
		{
			ham_u64_t cost_per_scan = globalstats->scan_cost[bucket];
			cost_per_scan *= 1000;
			cost_per_scan /= globalstats->scan_count[bucket];

			ham_trace(("COST/SCAN ratio: %f", cost_per_scan/1000.0));
		}
		if (globalstats->ok_scan_count[bucket])
		{
			ham_u64_t ok_cost_per_scan = globalstats->ok_scan_cost[bucket];
			ok_cost_per_scan *= 1000;
			ok_cost_per_scan /= globalstats->ok_scan_count[bucket];

			ham_trace(("'OK' COST/SCAN ratio: %f", ok_cost_per_scan/1000.0));
		}
		if (globalstats->erase_query_count
			+ globalstats->insert_query_count)
		{
			ham_u64_t trials_per_query = 0;
			int i;
			
			for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++)
			{
				trials_per_query += globalstats->scan_count[i];
			}
			trials_per_query *= 1000;
			trials_per_query /= globalstats->erase_query_count
								+ globalstats->insert_query_count;

			ham_trace(("TRIALS/QUERY (INSERT + DELETE) ratio: %f", trials_per_query/1000.0));
		}


		/* 
		what is our FREELIST PAGE's ratio fail vs. search? 

		Since we know search >= fail, we'll calculate the
		reciprocal in integer arithmetic, as that one will be >= 1.0
		*/
		if (entrystats->fail_count)
		{
			ham_u64_t fail_reciprocal_ratio = entrystats->search_count;
			fail_reciprocal_ratio *= 1000;
			fail_reciprocal_ratio /= entrystats->fail_count;

			ham_trace(("PAGE FAIL/SEARCH ratio: %f", 1000.0/fail_reciprocal_ratio));
		}
		/*
		and how about our scan cost per scan? and per good scan?
		*/
		if (entrystats->per_size[bucket].scan_count)
		{
			ham_u64_t cost_per_scan = entrystats->per_size[bucket].scan_cost;
			cost_per_scan *= 1000;
			cost_per_scan /= entrystats->per_size[bucket].scan_count;

			ham_trace(("PAGE COST/SCAN ratio: %f", cost_per_scan/1000.0));
		}
		if (entrystats->per_size[bucket].ok_scan_count)
		{
			ham_u64_t ok_cost_per_scan = entrystats->per_size[bucket].ok_scan_cost;
			ok_cost_per_scan *= 1000;
			ok_cost_per_scan /= entrystats->per_size[bucket].ok_scan_count;

			ham_trace(("PAGE 'OK' COST/SCAN ratio: %f", ok_cost_per_scan/1000.0));
		}
	}
	}

	ham_assert(entrystats->last_start >= entrystats->per_size[bucket].first_start, (0));
	ham_assert(entrystats->persisted_bits >= entrystats->last_start, (0));

    /*
     * improve our start position, when we know there's nothing to be
     * found before a given minimum offset
     */
	offset = entrystats->per_size[bucket].first_start;
	if (dst->startpos < offset)
	{
		dst->startpos = offset;
	}

	offset = entrystats->persisted_bits;
	if (offset == 0)
	{
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
	else
	{
		/*
		reduce the search range to span only the really available
		free slots
		*/
		if (dst->endpos > offset)
		{
			dst->endpos = offset;
		}

        /*
         * NOW that we have the range and everything to things we are
         * certain about, we can further improve things by introducing a
         * bit of heuristics a.k.a.
         *  statistical mumbojumbo:
         *
         * when we're in UBER/FAST mode and SEQUENTIAL to boot, we only
         * wish to look at the last chunk of free space and ignore the
         * rest.
         *
         * When we're in UBER/FAST mode, CLASSIC style, we don't feel
         * like wading through an entire freelist every time when we
         * know already that utilization is such that our chances at
         * finding a match are low, which means we'd rather turn this
         * thing into SEQUENTIAL mode, maybe even SEQUENTIAL+UBER/FAST,
         * for as long as the utilization is such that our chance at
         * finding a match is still rather low.
         */
		switch (dst->mgt_mode & (HAM_DAM_SEQUENTIAL_INSERT
								| HAM_DAM_RANDOM_WRITE_ACCESS
								| HAM_DAM_FAST_INSERT))
		{
		default:
		case HAM_DAM_SEQUENTIAL_INSERT:
		case HAM_DAM_RANDOM_WRITE_ACCESS:
			/* 
			we're good as we are; no fancy footwork here. 
			*/
			break;
			
        /*
         * here's where we get fancy:
         *
         * 1) basic FAST_INSERT will be treated like
         * RANDOM_ACCESS+FAST_INSERT:
         *
         * we'll get fed up with scanning the entire freelist when our
         * fail/success ratio, i.e. our utilization indices are through
         * the roof, metaphorically speaking. Right then, we simply say
         * we'd like to act as if we were in SEQUENTIAL, with possibly a
         * FAST thrown in there, mode.
         *
         * 2) SEQUENTAL + FAST gets special treatment in that the start
         * offset will be moved all the way up to the last free zone in
         * this freelist:
         * that's where the trailing free  space is and either it's big
         * enough for us to score a hit, or it's too small and another
         * free page will be added to the database.
         */
		case HAM_DAM_FAST_INSERT:
		case HAM_DAM_RANDOM_WRITE_ACCESS | HAM_DAM_FAST_INSERT:
			{
                /*
                 * calculate ratio; the +1 in the divisor is to prevent
                 * division-by-zero;
                 * it effect is otherwise negligible
                 */
				ham_u64_t cost_ratio; 
				ham_u64_t promille;
				ham_bool_t fast_seq_mode = HAM_FALSE;

				promille = entrystats->per_size[bucket].epic_fail_midrange;
				promille = (promille * 1000) / (1 + promille + entrystats->per_size[bucket].epic_win_midrange);
				
				cost_ratio = entrystats->per_size[bucket].ok_scan_cost;
				cost_ratio = (cost_ratio  * 1000) / (1 + entrystats->per_size[bucket].scan_cost);

                /*
                 * at 50% of searches FAILing, we switch over to
                 * SEQUENTIAL mode:
                 * we may not gain from it directly, but we MAY gain
                 * from this as the search order now reverses so we MAY
                 * hit suitable free slots earlier.
                 *
                 * WHEN we get that gain, we won't get it permanently,
                 * because as fail % goes down, we'll switch back to the
                 * original 'regular'
                 * mode, which may cost us again; so the expectation
                 * here is that, in case of luck, we'll float around
                 * this 50% number here, causing an effective gain of
                 * less than a factor of 2 then.
                 *
                 * Still, it's a gain, and it's adaptive ;-)
                 */
				if (promille > 500)
				{
					dst->mgt_mode &= ~(HAM_DAM_RANDOM_WRITE_ACCESS | HAM_DAM_FAST_INSERT);
					dst->mgt_mode |= HAM_DAM_SEQUENTIAL_INSERT;
				}
                /*
                 * if we don't get any better though, there's to be a
                 * more harsh approach: SEQUENTIAL+FAST when fail rates
                 * went up to 90% !!!
                 *
                 * This implies we'll accept about 10% 'gaps' in our
                 * database file.
                 *
                 *
                 * OR When our FAIL cost ratio is surpassing 90%, we'll
                 * change to SEQ+FAST as well.
                 */
				if (promille > 900 || cost_ratio > 900)
				{
					dst->mgt_mode &= ~(HAM_DAM_RANDOM_WRITE_ACCESS | HAM_DAM_FAST_INSERT);
					dst->mgt_mode |= HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT;

                    /*
                     *  and when we do this, we should act as we do for
                     * SEQ+FAST, so we fall through to the next case:
                     */
					fast_seq_mode = HAM_TRUE;
				}

				if (!fast_seq_mode)
					break;
			}
			/* FALL THROUGH! */

		case HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT:
            /*
             * we're clearly in a hurry to get to the end of the
             * universe. Restaurant appointment there, perchance? ;-)
             *
             * Okay, help is on the way: we'll bump the start offset to
             * point at the very last - and ascertained - chunk of free
             * slots in this freelist
             */
			offset = entrystats->last_start;
			if (dst->startpos < offset)
			{
				dst->startpos = offset;
			}
			break;
		}

		/* take alignment into account as well! */
		if (dst->aligned)
		{
			ham_u32_t alignment = db_get_cooked_pagesize(db) / DB_CHUNKSIZE;
			dst->startpos += alignment - 1;
			dst->startpos -= dst->startpos % alignment;
		}
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
	ham_assert(db_get_allocator(db) != 0, (0));
			ham_mem_free(db, dbdata->lower_bound.data);
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
	ham_assert(db_get_allocator(db) != 0, (0));
			ham_mem_free(db, dbdata->upper_bound.data);
}
		memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
		dbdata->upper_bound_index = 0;
		dbdata->upper_bound_page_address = 0;
		dbdata->upper_bound_set = HAM_FALSE;
	}
}


void stats_update_any_bound(ham_db_t *db, struct ham_page_t *page, ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot)
{
	ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
    btree_node_t *node = ham_page_get_btree_node(page);

	ham_assert(db_get_allocator(db) != 0, (0));
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
             */
			if (dbdata->lower_bound_index != 1
				|| dbdata->upper_bound_index != 0)
			{
				/* only set when not done already */
				if (dbdata->lower_bound.data)
					ham_mem_free(db, dbdata->lower_bound.data);
				if (dbdata->upper_bound.data)
					ham_mem_free(db, dbdata->upper_bound.data);
				memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
				memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
				dbdata->lower_bound_index = 0;
				dbdata->upper_bound_index = 0;
				dbdata->lower_bound_page_address = 0;
				dbdata->upper_bound_page_address = 0;
				dbdata->lower_bound_set = HAM_FALSE;
				dbdata->upper_bound_set = HAM_FALSE;
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
				//if (dbdata->lower_bound.data)
				//	ham_mem_free(db, dbdata->lower_bound.data);
				if (!util_copy_key_int2pub(db, 
						btree_node_get_key(db, node, dbdata->lower_bound_index), 
						&dbdata->lower_bound))
				{
                    ham_assert(!"shouldn't be here!", (""));
					/* panic! is case of failure, just drop the lower bound entirely. */
					if (dbdata->lower_bound.data)
						ham_mem_free(db, dbdata->lower_bound.data);
					memset(&dbdata->lower_bound, 0, sizeof(dbdata->lower_bound));
					dbdata->lower_bound_index = 0;
					dbdata->lower_bound_page_address = 0;
					dbdata->lower_bound_set = HAM_FALSE;
				}
			    page_release_ref(page);
			}
		}
	}
	if (!btree_node_get_right(node))
	{
		/* this is the leaf page which carries the upper bound key */
		ham_assert(btree_node_get_count(node) == 0 ? !btree_node_get_left(node) : 1, (0));
		if (btree_node_get_count(node) != 0)
		{
			/* 
			range is non-empty; the other case has already been handled above
			
			upper bound key is always located at index [size-1]

			update our key info when either our current data is undefined (startup condition)
			or the last key was edited in some way (slot == size-1). This 'copy anyway' approach 
			saves us one costly key comparison.
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
				//if (dbdata->upper_bound.data)
				//	ham_mem_free(db, dbdata->upper_bound.data);
				if (!util_copy_key_int2pub(db, 
						btree_node_get_key(db, node, dbdata->upper_bound_index), 
						&dbdata->upper_bound))
				{
                    ham_assert(!"shouldn't be here!", (""));
					/* panic! is case of failure, just drop the upper bound entirely. */
					if (dbdata->upper_bound.data)
						ham_mem_free(db, dbdata->upper_bound.data);
					memset(&dbdata->upper_bound, 0, sizeof(dbdata->upper_bound));
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
NOTE:

The current statistics collectors recognize scenarios
where insert & delete mix with find, as both insert and delete (pardon, erase)
can split/merge/rebalance the B-tree and thus completely INVALIDATE
btree leaf nodes, the address of which is kept in DB-wide statistics
storage. The current way of doing things is to keep the statistics simple, i.e.
btree leaf node pointers are nuked when an insert operation splits them or an erase
operation merges or erases such pages. I don't think it's really useful
to have complex leaf-node tracking in here to improve hinting in such mixed use
cases.
*/


void 
btree_find_get_hints(find_hints_t *hints, ham_db_t *db, ham_key_t *key)
{
	ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);
	ham_runtime_statistics_opdbdata_t *opstats = db_get_op_perf_data(db, HAM_OPERATION_STATS_FIND);
	ham_u32_t flags = hints->flags;

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

		switch (flags & HAM_HINTS_MASK)
		{
		default:
		case HAM_HINT_RANDOM_ACCESS:
			/* do not provide any hints for the fast track */
			break;

		case 0:
			/* no local preference specified; go with the DB-wide DAM config */
			if (!(db->_data_access_mode & HAM_DAM_SEQUENTIAL_INSERT))
				break;
			/* else: SEQUENTIAL mode --> FALL THROUGH */

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
			int cmp = db_compare_keys(db, key, &dbdata->lower_bound);

			if (cmp < 0)
			{
				hints->key_is_out_of_bounds = HAM_TRUE;
				hints->try_fast_track = HAM_TRUE;
			}
		}

		if (dbdata->upper_bound_set
			&& !db_is_mgt_mode_set(flags, HAM_FIND_LT_MATCH))
		{
			int cmp = db_compare_keys(db, key, &dbdata->upper_bound);

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

	ham_assert(hints->force_append == HAM_FALSE, (0));
	ham_assert(hints->force_prepend == HAM_FALSE, (0));
	ham_assert(hints->try_fast_track == HAM_FALSE, (0));

    if (hints->flags & HAM_HINT_APPEND)
	{
		if (!bt_cursor_is_nil(hints->cursor))
		{
			ham_assert(bt_cursor_is_nil(hints->cursor)==0, ("cursor must not be nil"));
			ham_assert(db == bt_cursor_get_db(hints->cursor), (0));

			/*
			 fetch the page of the cursor. We deem the cost of an uncoupled cursor 
			 too high as that implies calling a full-fledged key search on the
			 given key - which can be rather costly - so we rather wait for the
			 statistical cavallery a little later on in this program then.
			 */
			if (bt_cursor_get_flags(hints->cursor) & BT_CURSOR_FLAG_COUPLED) 
			{
				ham_page_t *page = bt_cursor_get_coupled_page(hints->cursor);
				btree_node_t *node = ham_page_get_btree_node(page);
				ham_assert(btree_node_is_leaf(node), ("cursor points to internal node"));
				ham_assert(!btree_node_get_right(node), ("cursor points to leaf node which is NOT the uppermost/last one"));

				hints->leaf_page_addr = page_get_self(page);
				hints->force_append = HAM_TRUE;
				hints->try_fast_track = HAM_TRUE;
			}
		}
	}
    else if (hints->flags & HAM_HINT_PREPEND)
	{
		if (!bt_cursor_is_nil(hints->cursor))
		{
			ham_assert(bt_cursor_is_nil(hints->cursor)==0, ("cursor must not be nil"));
			ham_assert(db == bt_cursor_get_db(hints->cursor), (0));

			/*
			 fetch the page of the cursor. We deem the cost of an uncoupled cursor 
			 too high as that implies calling a full-fledged key search on the
			 given key - which can be rather costly - so we rather wait for the
			 statistical cavallery a little later on in this program then.
			 */
			if (bt_cursor_get_flags(hints->cursor) & BT_CURSOR_FLAG_COUPLED) 
			{
				ham_page_t *page = bt_cursor_get_coupled_page(hints->cursor);
				btree_node_t *node = ham_page_get_btree_node(page);
				ham_assert(btree_node_is_leaf(node), ("cursor points to internal node"));
				ham_assert(!btree_node_get_left(node), ("cursor points to leaf node which is NOT the lowest/first one"));

				hints->leaf_page_addr = page_get_self(page);
				hints->force_prepend = HAM_TRUE;
				hints->try_fast_track = HAM_TRUE;
			}
		}
	}
    hints->flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);

	/* 
	The statistical cavallery:

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
		if (dbdata->lower_bound_set)
		{
			int cmp = db_compare_keys(db, key, &dbdata->lower_bound);

			if (cmp < 0)
			{
				//hints->flags |= HAM_HINT_PREPEND;
				hints->force_prepend = HAM_TRUE;
				hints->leaf_page_addr = dbdata->lower_bound_page_address;
				hints->try_fast_track = HAM_TRUE;
			}
		}

		if (dbdata->upper_bound_set)
		{
			int cmp = db_compare_keys(db, key, &dbdata->upper_bound);

			if (cmp > 0)
			{
				//hints->flags |= HAM_HINT_APPEND;
				hints->force_append = HAM_TRUE;
				hints->leaf_page_addr = dbdata->upper_bound_page_address;
				hints->try_fast_track = HAM_TRUE;
			}
		}
	}

	/* we don't yet hint about jumping to the last accessed leaf node immediately (yet) */
}


void 
btree_erase_get_hints(erase_hints_t *hints, ham_db_t *db, ham_key_t *key)
{
	ham_runtime_statistics_dbdata_t *dbdata = db_get_db_perf_data(db);

	ham_assert(!(key->_flags & KEY_IS_EXTENDED), (0));
    key->_flags &= ~KEY_IS_EXTENDED;

	/* forget about deleting a key when it's out of bounds */
	if (dbdata->lower_bound_set)
	{
		int cmp = db_compare_keys(db, key, &dbdata->lower_bound);

		if (cmp < 0)
		{
			hints->key_is_out_of_bounds = HAM_TRUE;
			hints->try_fast_track = HAM_TRUE;
		}
	}

	if (dbdata->upper_bound_set)
	{
		int cmp = db_compare_keys(db, key, &dbdata->upper_bound);

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
	/* trash the upper/lower bound keys, when set: */
	if (dbdata->upper_bound.data)
{
	ham_assert(db_get_allocator(db) != 0, (0));
		ham_mem_free(db, dbdata->upper_bound.data);
}
	if (dbdata->lower_bound.data)
{
	ham_assert(db_get_allocator(db) != 0, (0));
		ham_mem_free(db, dbdata->lower_bound.data);
}
	memset(dbdata, 0, sizeof(*dbdata));
}


/**
copy one internal format freelist statistics record to a public format record for the same.

Can't use memcpy() because of alignment issues we don't want the hamsterdb API user
to bother about -- let alone forcing him/her to include the packstart.h and packstop.h
header files too.
*/
static void copy_freelist_page_stat2api_rec(ham_freelist_page_statistics_t *dst, freelist_page_statistics_t *src)
{
	int i;

	for (i = 0; i < HAM_FREELIST_SLOT_SPREAD; i++)
	{
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
The @ref ham_statistics_t cleanup/free callback function: this one is needed as we must
use the same system to free any allocated heap storage as we used for allocating such storage in
the first place, i.e. our freelist stats array.
*/
static void my_cleanup_ham_statistics_t(ham_statistics_t *dst)
{
	mem_allocator_t *a;

	ham_assert(dst, (0));
	a = (mem_allocator_t *)dst->_free_func_internal_arg;
	ham_assert(a, (0));

	/* cleanup is simple: when it was allocated, free the freelist stats array */
	if (dst->freelist_stats)
	{
		allocator_free(a, dst->freelist_stats);
		dst->freelist_stats = NULL;
	}
	dst->freelist_stats_maxalloc = 0;
	//dst->freelist_record_count = 0;

	/* and blow ourselves away from dst, while keeping the other data in dst intact: */
	dst->_free_func = 0;
	dst->_free_func_internal_arg = NULL;
}



ham_status_t
stats_fill_ham_statistics_t(ham_env_t *env, ham_db_t *db, ham_statistics_t *dst)
{
	ham_bool_t collect_globdata;
	ham_bool_t collect_dbdata;
	ham_bool_t collect_freelistdata;

	ham_assert(dst, (0));

	/* copy the user-specified selectors before we zero the whole darn thing */
	collect_globdata = (!dst->dont_collect_global_stats && (env || db));
	collect_dbdata = (!dst->dont_collect_db_stats && db);
	collect_freelistdata = (!dst->dont_collect_freelist_stats && (env || db));

	/* now zero the entire structure to begin with */
	memset(dst, 0, sizeof(*dst));

	/* then see if we can / should collect env/global and db-specific stats in there */
	if (collect_globdata)
	{
		ham_runtime_statistics_globdata_t *globalstats;

		ham_assert(db || env, (0));
		if (db)
		{
			globalstats = db_get_global_perf_data(db);
		}
		else
		{
			globalstats = env_get_global_perf_data(env);
		}
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

	/* now the tougher part: see if we should report the freelist statistics */
	if (collect_freelistdata)
	{
	    freelist_cache_t *cache;
		mem_allocator_t *allocator;

		ham_assert(db || env, (0));
		if (db)
		{
			cache = db_get_freelist_cache(db);
			allocator = db_get_allocator(db);
		}
		else
		{
			cache = env_get_freelist_cache(env);
			allocator = env_get_allocator(env);
		}

		if (!cache || !allocator || !freel_cache_get_entries(cache))
		{
			collect_freelistdata = HAM_FALSE;
		}
		else
		{
			ham_size_t count = freel_cache_get_count(cache);
			
			if (count > 0)
			{
				ham_freelist_page_statistics_t *d;
				ham_size_t i;

				dst->_free_func = my_cleanup_ham_statistics_t;
				/*
				and the next line is both mandatory to support different allocators in here as they
				are in other parts of hamsterdb, and it is the reason for the caveat in the API
				documentation, requiring the user to call this cleanup callback BEFORE either the
				'db' or 'env' are closed/deleted as that would invalidate this 'allocator' reference!
				*/
				dst->_free_func_internal_arg = (void *)allocator;

				d = dst->freelist_stats = allocator_alloc(allocator, count * sizeof(dst->freelist_stats[0]));
				if (!d)
				{
					return db_set_error(db, HAM_OUT_OF_MEMORY);
				}
				memset(d, 0, count * sizeof(dst->freelist_stats[0]));

				/* now fill those API freelist records from the regular (internal) ones: */
				for (i = 0; i < count; i++)
				{
					freelist_entry_t *entry = freel_cache_get_entries(cache) + i;

					copy_freelist_page_stat2api_rec(d + i, freel_entry_get_statistics(entry));
				}
			}

			dst->freelist_stats_maxalloc = count;
			dst->freelist_record_count = count;
		}
	}

	/* and finally mark which sections have actually been fetched */
	dst->dont_collect_global_stats = !collect_globdata;
	dst->dont_collect_db_stats = !collect_dbdata;
	dst->dont_collect_freelist_stats = !collect_freelistdata;
	
	return 0;
}





























#define CACHE_HISTORY_SIZE   64


typedef struct
{
	ham_offset_t addr;
	ham_u64_t count;

	ham_u64_t recall;			
	ham_offset_t recall_dist;
	ham_u64_t refetch;			/* still in history, but already gone from cache and now retrieved again */
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

int history_position = 0;

static
cache_history_t *cache_history_locate_entry(ham_page_t *page, int state)
{
	int i;
	int oob = 1;
	int oldest;
	cache_history_t tmp = {0};
	cache_history_t *ref;

	tmp.addr = page_get_self(page);

	oldest = history_position + CACHE_HISTORY_SIZE - 1;
	oldest %= CACHE_HISTORY_SIZE;

	ref = cache_history + oldest;

	for (i = 0; i < CACHE_HISTORY_SIZE; i++)
	{
		int pos = history_position + i;
		pos %= CACHE_HISTORY_SIZE;

		if (cache_history[pos].addr == page_get_self(page))
		{
			int j;
			int posi;
			int poso;
			int distance;

			// a hit!
			oob = 0;

			distance = pos - history_position + CACHE_HISTORY_SIZE;
			distance %= CACHE_HISTORY_SIZE;

			if (i < CACHE_HISTORY_SIZE - 1)
			{
				tmp = cache_history[oldest];
				cache_history[oldest] = cache_history[pos];

				// move the remainder up:
				for (j = i + 1, poso = pos; j < CACHE_HISTORY_SIZE - 1; j++)
				{
					posi = history_position + j;
					posi %= CACHE_HISTORY_SIZE;
			
					cache_history[poso] = cache_history[posi];

					poso++;
					poso %= CACHE_HISTORY_SIZE;
				}

				cache_history[poso] = tmp;

				posi = history_position + j;
				posi %= CACHE_HISTORY_SIZE;
				ham_assert(posi == oldest, (0));
			}
			else
			{
				ham_assert(pos == oldest, (0));
			}

			if (state == 0)
			{
				cache_history[pos].refetch_dist += distance + 1;
				cache_history[pos].refetch++;
			}
			else
			{
				cache_history[pos].recall_dist += distance + 1;
				cache_history[pos].recall++;
			}
			break;
		}
	}

	if (oob)
	{
		*ref = tmp;
	}

	history_position = oldest;

	ref->cache_cntr = page_get_cache_cntr(page);
	ref->refcount = page_get_refcount(page);

	return ref;
}

static
ham_page_t *cache_get_live_page(ham_cache_t *cache, ham_offset_t addr, char *af, int aflen)
{
    ham_page_t *head;

    head=cache_get_totallist(cache);
    while (head) 
	{
		af++;
		aflen--;
		if (page_get_self(head) == addr)
		{
			if (aflen >= 0)
				af[0] = 0;
			return head;
		}
        head=page_get_next(head, PAGE_LIST_CACHED);
    }
    head=cache_get_garbagelist(cache);
    while (head) {
		af++;
		aflen--;
		if (page_get_self(head) == addr)
		{
			if (aflen >= 0)
				af[0] = 0;
			return head;
		}
        head=page_get_next(head, PAGE_LIST_GARBAGE);
    }

    return NULL;
}

#if HAM_DEBUG
void cache_report_history(ham_db_t *db)
{
	ham_cache_t *cache = db_get_cache(db);
	ham_u32_t i;
	char af[2048];
	int c;
	ham_u64_t minc;

	printf("\n\ncache history:\n");

	memset(af, 0, sizeof(af));
	for (i = 0 ; i < (int)sizeof(af) && i < cache->_cur_elements; i++)
	{
		af[i] = 1;
	}

	for (i = 0; i < CACHE_HISTORY_SIZE; i++)
	{
		ham_page_t *p;
		int pos = history_position + i;
		pos %= CACHE_HISTORY_SIZE;

		p = cache_get_live_page(cache, cache_history[pos].addr, af, sizeof(af));

		if (cache_history[pos].addr && 
			(cache_history[pos].count + cache_history[pos].alloc + cache_history[pos].fetch))
		{
			printf("%p[%2d]: rc#:%lld, rc:%.1f, rf#:%lld, rf:%.1f, age: %d(%d)\n",
				(void *)cache_history[pos].addr, i,
				(long long int)cache_history[pos].recall,
				cache_history[pos].recall_dist * 1.0 / (0.001 + cache_history[pos].recall),
				(long long int)cache_history[pos].refetch,
				cache_history[pos].refetch_dist * 1.0 / (0.001 + cache_history[pos].refetch),
				cache_history[pos].cache_cntr,
				cache_history[pos].refcount);
		}
	}

	printf("\n");
	
	minc = (ham_u64_t)-1;
	c = 0;
	for (i = 0; i < CACHE_HISTORY_SIZE; i++)
	{
		int pos = history_position + i;
		pos %= CACHE_HISTORY_SIZE;

		if (cache_history[pos].addr && 
			(cache_history[pos].count + cache_history[pos].alloc + cache_history[pos].fetch))
		{
			minc += cache_history[pos].count;
			c++;
		}
	}
	if (c != 0)
		minc /= c;
	printf("AVG. COUNT: %lld\n", (long long int)minc);

	for (i = 0; i < CACHE_HISTORY_SIZE; i++)
	{
		ham_page_t *p;
		int pos = history_position + i;
		pos %= CACHE_HISTORY_SIZE;

		p = cache_get_live_page(cache, cache_history[pos].addr, af, sizeof(af));

		if (cache_history[pos].addr && 
			(cache_history[pos].count + cache_history[pos].alloc + cache_history[pos].fetch))
		{
#if 0
			ham_fraction_t fr = {cache_history[pos].fetch, cache_history[pos].alloc};
			if (cache_history[pos].alloc)
				to_fract(&fr, fract2dbl(&fr));
#endif

			printf("[%2d]: f/a:%6lld/%lld, a:%lld, c:%6lld, i:%2lld, r:%2lld, p:%2lld, age: %d(%d)/",
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
	}

	c = 0;
	for (i = 0; i < sizeof(af); i++)
	{
		if (af[i] && i != 0)	   // totallist->head is never in history...
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

	printf("\n\n");
}
#endif

void cache_push_history(ham_page_t *page, int state)
{
	cache_history_t *ref = cache_history_locate_entry(page, 0);

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

void cache_check_history(ham_db_t *db, ham_page_t *page, int state)
{
#if HAM_DEBUG
	cache_history_t *ref = cache_history_locate_entry(page, 1);

	ham_assert(ref->addr == page_get_self(page), (0));

	if (state > 0)
	{
		ref->fetch++;
	}
	else 
	{
		ref->alloc++;
	}

	{
		static int c = 0;

		c++;
		if (c % 500000 == 100000)
		{
			// cache_report_history(db);
		}
	}
#endif
}

