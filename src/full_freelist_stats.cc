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
#include "full_freelist_stats.h"
#include "mem.h"
#include "util.h"
#include "full_freelist.h"

namespace hamsterdb {

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
#define rescale_256(val)              \
  val = ((val + 256 - 1) >> 8)

void
FullFreelistStatistics::rescale_freelist_page_stats(FullFreelist *cache,
                FullFreelistEntry *entry)
{
  PFreelistPageStatistics *entrystats = &entry->perf_data;

  for (ham_u16_t b = 0; b < HAM_FREELIST_SLOT_SPREAD; b++) {
    rescale_256(entrystats->per_size[b].epic_fail_midrange);
    rescale_256(entrystats->per_size[b].epic_win_midrange);
    rescale_256(entrystats->per_size[b].scan_count);
    rescale_256(entrystats->per_size[b].ok_scan_count);
    rescale_256(entrystats->per_size[b].scan_cost);
    rescale_256(entrystats->per_size[b].ok_scan_cost);
  }

  rescale_256(entrystats->insert_count);
  rescale_256(entrystats->delete_count);
  rescale_256(entrystats->extend_count);
  rescale_256(entrystats->fail_count);
  rescale_256(entrystats->search_count);
  rescale_256(entrystats->rescale_monitor);
}

} // namespace hamsterdb
