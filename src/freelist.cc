/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include "ham/hamsterdb_int.h"

#include "db.h"
#include "device.h"
#include "endianswap.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "page_manager.h"
#include "mem.h"
#include "btree_stats.h"
#include "txn.h"

namespace hamsterdb {

/**
 * Search for a sufficiently large free slot in the freelist bit-array.
 *
 * Before v1.0.9, this was a sequential scan, sped up by first scanning
 * QWORDs in an outer loop in order to find spots with at least 1 free
 * bit, then an inner loop which would perform a bit-level scan only a
 * free bit was located by the outer loop.
 *
 * The 'aligned' search acted a little different: it had an outer loop
 * which scanned BYTEs at a time, instead of QWORDs, while the inner
 * bit-level scan loop would only last until the requested number of
 * bits had been scanned, when failing to hit a valid free slot, thus
 * returning to the outer, faster loop (a behaviour which was NOT
 * exhibited by the 'regular' search method: once inside the inner
 * bit-level loop, it would _stay_ there. The 'aligned' scan would also
 * stop scanning when the end-requested_size bit was tested, while the
 * 'regular' loop continued on until the very end of the bitarray.
 *
 * This was very slow, especially in scenarios where tiny free slots
 * are located near the front of the bitarray (which represents the
 * storage layout of the whole database, incidentally).
 *
 * A few improvements can be thought of (and have been implemented):
 *
 * - first off, do as the 'aligned' search already did, but now for
 *   everyone: stop scanning at the END-requested_size bit: any
 *   free space _starting_ beyond that point is too small anyway.
 *
 * - 'aligned' search is searching for space aligned at a DB page
 *   edge (256 bytes or bigger) and since we 'know' the requested size
 *   is also large (and very, very probably a multiple of
 *   64 * kBlobAlignment (== 2K) as the only one requesting page-aligned
 *   storage is requesting an entire page (>=2K!) of storage anyway), we
 *   can get away here by NOT scanning at bit level, but at QWORD level only.
 *
 *   EDIT v1.1.1: This has been augmented by a BYTE-level search as
 *        odd-Kb pagesizes do exist (e.g. 1K pages) and these
 *        are NOT aligned to the QWORD boundary of
 *        <code>64 * kBlobAlignment</code> = 2Kbytes (this is the
 *        amount of storage space covered by a single QWORD
 *        worth of freelist bits). See also the
 *        @ref DB_PAGESIZE_MIN_REQD_ALIGNMENT define.
 *
 * - Boyer-Moore scanning instead of sequential: since a search for free
 *   space is basically a search for a series of SIZE '1' bits, we can
 *   employ characteristics as used by the Boyer-Moore string search
 *   algorithm (and it's later improvements, such as described by
 *   [Hume & Sunday]). While we 'suffer' from the fact that we are looking
 *   for 'string matches' in an array which has a character alphabet of size
 *   2: {0, 1}, as we are considering BITS here, we can still employ
 *   the ideas of Boyer-Moore et al to speed up our search significantly.
 *   Here are several elements to consider:
 *
 * # We can not easily (or not at all) implement the
 *   suggested improvement where there's a sentinel at the
 *   end of the searched range, as we are accessing mapped
 *   memory, which will cause an 'illegal access' exception
 *   to fire when we sample bytes/words outside the alloted
 *   range. Of course, this issue could be resolved by
 *   'tweaking' the freelist pages upon creation by ensuring
 *   there's a 'sentinel range' available at the end of each
 *   freelist page. THAT will be something to consider for
 *   the 'modern' Data Access Mode freelist algorithm(s)...
 *
 * # Since we have an alphabet of size 2, we don't have to
 *   bother with 'least frequent' and 'most frequent'
 *   characters in our pattern: we will _always_ be looking
 *   for a series of 1 bits. However, we can improve the scan,
 *   as was done in the classic search algorithm, by inspecting
 *   QWORDs at a time instead of bits. Still, we can think of
 *   the alphabet as being size = 2, as there's just two
 *   character values of interest: 0 and 'anything else', which
 *   is our '1' in there. Expressing the length of the searched
 *   pattern in QWORDs will also help find probable slots
 *   as we can stick to the QWORD-dimensioned scanning as
 *   long as possible, only resolving to bit-level scans at
 *   the 'edges' of the pattern.
 *
 * # The classic BM (Boyer-Moore) search inspected the character
 *   at the end of the pattern and then backtracked; we can
 *   improve our backtracking by assuming a few things about
 *   both the pattern and the search space: since our pattern
 *   is all-1s and we can can assume that our search space,
 *   delimited by a previous sample which was false, and the
 *   latest sample, distanced pattern_length bits apart, is
 *   mostly 'used bits' (zeroes), we MAY assume that the
 *   free space in there is available more towards the end
 *   of this piece of the range. In other words: the searched
 *   space can be assumed to be SORTED over the current
 *   pattern_length bitrange -- which means we can employ
 *   a binary search mechanism to find the 'lowest' 1-bit
 *   in there. We add an average cost there of the binary
 *   search at O(log(P)) (where P = pattern_size) as we
 *   will have to validate the result returned by such a
 *   binary search by scanning forward sequentially, but
 *   on average, we will save cycles as we do the same
 *   bsearch on the NEXT chunk of size P, where we assume
 *   the data is sorted in REVERSE order and look for the
 *   first '0' instead: these two bsearches will quickly
 *   deliver a sufficiently trustworthy 'probable size of
 *   free area' to do this before we wind down to a (costly)
 *   sequential scan. Note that the two bsearches can
 *   be reduced to the first only, if its verdict is that
 *   the range starts at offset -P+1, i.e. the first bit
 *   past the previous (failed) sample in the skip loop.
 *   The two blocks bsearched are, given the above, assumed
 *   to show a series of '1' bits within an outer zone
 *   of '0' bits on both sides; that's why the second
 *   bsearch should assume REVERSE sorted order, as we wish
 *   to find the first '0' AFTER the last '1' in there,
 *   so that we have an indicator of the end-of-1-range
 *   position in the search space.
 *
 *   # as we look for an all-1 pattern, our skip loop can
 *   skip P-1 bits at a time, as a bit sampled being '0'
 *   means the P'th bit after that one must be '1' to
 *   get us a match. When we get such a hit, we do not
 *   know if it's the start or end of the match yet,
 *   so that's why we scan backwards and forwards using
 *   the bsearches suggested above. (Especially for large
 *   pattern sizes is the bsearch-before-sequential 'prescan'
 *   considered beneficial.)
 *
 *   # As we scan the freelist, we can gather statictics:
 *   how far we had to scan into the entire range before
 *   we hit our _real_ free slot:
 *   by remembering this position, the next search for
 *   a similar sized pattern can be sped up by starting
 *   at the position (adjusted: + old P size, of course)
 *   we found our last match.
 *
 *   When we delete a record, we can adjust this position
 *   to the newly created free space, when the deleted
 *   entry creates a suitably large free area.
 *
 *   This implies that we might want to keep track of
 *   a 'search start position' for a set of sizes instead
 *   of just one: even on a fixed-width DB, there's the
 *   key and the record data. The initial idea here is
 *   to track it for log8(P) ranges, i.e. one tracker
 *   for sizes up to 2^8, one more for sizes up 2^16,
 *   nd so on (maybe later upgrade this to log2(P) ranges).
 *
 * # As we scan the freelist, we can gather statictics:
 *   the number of times we had a 'probable hit' (which
 *   failed to deliver):
 *   As the ratio of the number of 'false hits' versus
 *   actual searches increases, we can speed up our
 *   searches by looking for a larger free slot (maybe
 *   even using the first-pos tracker for the next larger
 *   sizes set as mentioned in the previous point):
 *   by doing so we can, hopefully, start at a higher
 *   position within the range. At the cost of creating
 *   'gaps' in the storage which will remain unused for
 *   a long time (in our current model, these statistics
 *   are gathered per run, so the next open/access/close
 *   run of the DB will reset these statistics).
 *
 * The Boyer-Moore skip loop can help us jump through
 * the freelist pages faster; this skip loop can be
 * employed at both the QWORD and BIT search levels.
 *
 * # The bsearch backtracking 'prescans' should maybe
 *   be disabled for smaller sizes, e.g. for sizes up
 *   to length = 8, as it does not help speed up
 *   matters a whole darn lot in that case anyway.
 *
 * # An alternative to plain Boyer-Moore skip loop, etc.
 *   is to take the bsearch idea a step further: we
 *   know the skip loop step size (P), given the
 *   pattern we are looking for.
 *
 *   We may also assume that most free space is located
 *   at the end of the range: when we express that
 *   free space available anywhere in the freelist
 *   'but at the very end' is less valuable, we can
 *   assume the freelist is SORTED: by not starting
 *   by a sequential skip loop scan, but using a
 *   bsearch to find the lowest available '1'
 *   probable match, we can further improve upon
 *   the concept of 'starting at the last known offset'
 *   as suggested above. This means we can start
 *   the search by a binary search of the range
 *   [last_offset .. end_of_freelist] to find the
 *   first probable sample match, after which we
 *   can go forward using your regular Boyer-Moore
 *   skip loop.
 *
 *   This will [probably] lose free '1' slots which
 *   sit within larger '0' areas, but that's what
 *   this is about. When our DB access behaviour is
 *   generally a lot of insert() and little or
 *   no delete(), we can use this approach to get
 *   us some free space faster.
 *
 * # The above can be enhanced even further by
 *   letting hamsterdb gather our access statistics
 *   (~ count the number of inserts and deletes
 *   during a run) to arrive at an automated choice
 *   for this mechanism over others available;
 *   instead of the user having to specify a
 *   preferred/assumed Data Access Mode, we can
 *   deduce the actual one ourselves.
 *
 *   The drawback of this bsearch-based free slot
 *   searching is that we will not re-use free
 *   slots within the currently oocupied space,
 *   i.e. more freelist fragmentation and a larger
 *   DB file as a result.
 *
 * # Note however that the 'start off with a
 *   range bsearch' is internally different from
 *   the one/two bsearches in the space backtrack
 *   'prescan':
 *
 *   the latter divide up inspected
 *   space to slices of 1 bit each, unless we
 *   limit the bsearch prescan to BYTE-level,
 *   i.e. 8-bit slices only for speed sake.
 *   AH! ANOTHER IMPROVEMENT THERE!
 *
 *   the former (bsearch-at-start) will ALWAYS
 *   limit its divide-and-conquer to slices of
 *   P bits (or more); further reducing the
 *   minimum slice is identical to having a BM
 *   skip loop with a jump distance of P/2 (or
 *   lower), which is considered sub-optimal.
 *   Such a bsearch would be blending the search
 *   pattern into the task area alotted the
 *   dual-bsearch backtrack prescans.
 *
 *   Another notable difference is that the
 *   backtracking/forward-tracking inner bsearch
 *   prescans can act differently on the
 *   discovery of an apparently UNORDERED
 *   search space: those bsearches may hit '0's
 *   within a zone of '1's, i.e. hit the '0' marked
 *   '^' in this search space - which was assumed
 *   to be ORDERED but clearly is NOT:
 *
 *     0000 1111 1111 0^111
 *
 *   And such an occurrence (previous lower
 *   sample=='1', while current sample==='0')
 *   can cause those bsearches to stop scanning
 *   this division and immediate adjust the range
 *   to current_pos+1..end_of_range and continue
 *   to sample the median of that new range.
 *   This would be absolutely valid bahaviour.
 *
 *   (Reverse '0' and '1' and range determination
 *    for the second, forward-tracking bsearch
 *    there, BTW.)
 *
 *   However, the starting, i.e. 'outer' bsearch
 *   may not decide to act that way: after all,
 *   the range may have gaps, one of which
 *   has just been discovered, so here the bsearch
 *   should really assume the newly found in-zone
 *   '1' free marker to be at the END of the
 *   inspected range and look for more '1's down
 *   from here: after all, this bsearch is looking
 *   for the first PROBABLE free slot and as such
 *   is a close relative of the BM skip loop.
 *
 *   # as our pattern is all-1s anyway, there is
 *   a problem in adjusting the BM search so as
 *   to assume we're skiploop-scanning for the
 *   FIRST character in the pattern; after all,
 *   it's identical to the LAST one: '1'.
 *
 *   This implies that we have simpler code while
 *   dealing with aligned searches as well as
 *   regular. And no matter if our skip-search was
 *   meant to look for the last (or first)
 *   character: any hit would mean we've hit a
 *   spot somewhere 'in the middle' of the search
 *   pattern; given the all-1s, we then need to
 *   find out through backtracking (and forward~)
 *   where in the pattern we did land: at the
 *   start, end or really in the middle.
 *
 *   Meanwhile, aligned matches are kept simple
 *   this way, as they now can assume that they
 *   always landed at the START of the pattern.
 *
 * --------
 *
 * FURTHER THOUGHTS:
 *
 * # Given our initial implementation and analysis,
 *   we can assume that the 'header page' is always
 *   reserved in the freelist for any valid database.
 *
 *   This is a major important bit of info, as it
 *   essentially serves as both a sentinel, which
 *   has a pagesize, i.e. is a sentinel as large
 *   as the largest freelist request (as those
 *   come in one page or smaller at a time).
 *
 *   This gives us the chance to implement other
 *   Boyer-Moore optimizations: we don't need
 *   to check the lower bound any longer AND
 *   we can always start each scan at START+PAGE
 *   offset at least, thus skipping those headerpage
 *   '0' bits each time during the regular phase of
 *   each search.
 *
 *   [Edit] Unfortunately, this fact only applies
 *   to the initial freelist page, so we cannot use
 *   it as suggested above :-(
 *
 * # Aligned scans are START-probe based, while
 *   unaligned scans use the classic Boyer-Moore
 *   END-probe; this is faster overall, as the subsequent
 *   REV linear scan will then produce the length
 *   of the leading range, which is (a) often
 *   enough to resolve the request, and (b) is
 *   hugging previous allocations when we're
 *   scanning at the end of the search space,
 *   which is an desirable artifact.
 *
 *   This does not remove the need for some
 *   optional FWD linear scans to determine the
 *   suitability of the local range, but these
 *   will happen less often.
 *
 * @author Ger Hobbelt, ger@hobbelt.com
 */

/** 8 QWORDS or less: 1-stage scan, otherwise, bsearch pre-scan */
#define SIMPLE_SCAN_THRESHOLD      8

/**
 * Adjust the bit index to the lowest MSBit which is part of a
 * consecutive '1' series starting at the top of the QWORD
 */
static __inline ham_u32_t
BITSCAN_MSBit(ham_u64_t v, ham_u32_t pos)
{
  register ham_s64_t value = (ham_s64_t)v;

  /*
   * Test top bit by checking two's complement sign.
   * This is crafted to spend the least number of
   * rounds inside the BM freelist bitarray scans.
   */
  while (value < 0) {
    pos--;
    value <<= 1;
  }
  return pos;
}

static __inline ham_u32_t
BITSCAN_MSBit8(ham_u8_t v, ham_u32_t pos)
{
  register ham_s8_t value = (ham_s8_t)v;

  /*
   * Test top bit by checking two's complement sign.
   * This is crafted to spend the least number of
   * rounds inside the BM freelist bitarray scans.
   */
  while (value < 0) {
    pos--;
    value <<= 1;
  }
  return pos;
}

/**
 * Adjust the bit index to <em> 1 PAST </em> the highest LSBit which is
 * part of a consecutive '1' series starting at the bottom of the QWORD.
 */
static __inline ham_u32_t
BITSCAN_LSBit(ham_u64_t v, ham_u32_t pos)
{
  register ham_u64_t value = v;

  /*
   * Test bottom bit.
   * This is crafted to spend the least number of
   * rounds inside the BM freelist bitarray scans.
   */
  while (value & 0x01) {
    pos++;
    value >>= 1;
  }
  return pos;
}

static __inline ham_u32_t
BITSCAN_LSBit8(ham_u8_t v, ham_u32_t pos)
{
  register ham_u8_t value = v;

  /*
   * Test the bottom bit.
   * This is crafted to spend the least number of
   * rounds inside the BM freelist bitarray scans.
   */
  while (value & 0x01) {
    pos++;
    value >>= 1;
  }
  return pos;
}

ham_status_t
Freelist::free_page(Page *page)
{
  return (free_area(page->get_address(), m_env->get_pagesize()));
}

ham_status_t
Freelist::free_area(ham_u64_t address, ham_size_t size)
{
  ham_status_t st;
  Page *page = 0;

  ham_assert(size % kBlobAlignment == 0);
  ham_assert(address % kBlobAlignment == 0);

  if (m_entries.empty()) {
    st = initialize();
    if (st)
      return (st);
  }

  /* split the chunk if it doesn't fit in one freelist page */
  while (size) {
    PFreelistPayload *fp;
    FreelistStatistics::Hints hints = { 0 };

    /* get the cache entry of this address */
    FreelistEntry *entry = get_entry_for_address(address);

    /* allocate a new page if necessary */
    if (!entry->pageid) {
      if (entry->start_address == m_env->get_pagesize()) {
        fp = m_env->get_freelist_payload();
        ham_assert(fp->get_start_address() != 0);
      }
      else {
        st = alloc_freelist_page(&page, entry);
        if (st)
          return (st);
        fp = PFreelistPayload::from_page(page);
        ham_assert(fp->get_start_address() != 0);
      }
    }
    /* otherwise just fetch the page from the cache or the disk */
    else {
      st = m_env->get_page_manager()->fetch_page(&page, 0, entry->pageid);
      if (st)
        return (st);
      fp = PFreelistPayload::from_page(page);
      ham_assert(fp->get_start_address() != 0);
    }

    ham_assert(address >= fp->get_start_address());

    /* set the bits and update the values in the cache and the fp */
    ham_size_t s = set_bits(entry, fp,
        (ham_size_t)(address - fp->get_start_address())
            / kBlobAlignment,
        size / kBlobAlignment, true, &hints);

    fp->set_free_bits((ham_u32_t)(fp->get_free_bits() + s));
    entry->free_bits = fp->get_free_bits();

    // mark page (or header page) as dirty
    mark_dirty(page);

    size -= s * kBlobAlignment;
    address += s * kBlobAlignment;
  }

  return (0);
}

ham_status_t
Freelist::alloc_page(ham_u64_t *paddr)
{
  return (alloc_area_impl(m_env->get_pagesize(), paddr, true, 0));
}

ham_status_t
Freelist::alloc_area_impl(ham_size_t size, ham_u64_t *paddr, bool aligned,
                ham_u64_t lower_bound_address)
{
  ham_status_t st;
  FreelistEntry *entry = NULL;
  PFreelistPayload *fp = NULL;
  Page *page = 0;
  ham_s32_t s = -1;
  FreelistStatistics::GlobalHints global_hints =
  {
    0,
    1,
    0,
    (ham_u32_t)m_entries.size(),
    0,
    0, /* span_width will be set by the hinter */
    aligned,
    lower_bound_address,
    size / kBlobAlignment,
    get_entry_maxspan()
  };
  FreelistStatistics::Hints hints = {0};

  ham_assert(paddr != 0);
  *paddr = 0;

  if (m_entries.empty()) {
    st = initialize();
    if (st)
      return (st);
  }

  FreelistStatistics::get_global_hints(this, &global_hints);

  ham_assert(size % kBlobAlignment == 0);
  ham_assert(global_hints.page_span_width >= 1);

  /*
   * locate_sufficient_free_space() is used to calculate the
   * next freelist entry page to probe; as a side-effect it also
   * delivers the hints for this entry - no use calculating those a
   * second time for use in search_bits() -- faster to pass
   * them along.
   */
  for (ham_s32_t i = -1;
      0 <= (i = locate_sufficient_free_space(&hints, &global_hints, i)); ) {
    ham_assert(i < (ham_s32_t)m_entries.size());

    entry = &m_entries[i];

    /*
     * when we look for a free slot for a multipage spanning blob
     * ('huge blob'), we could, of course, play nice, and check every
     * bit of freelist, but that takes time.
     *
     * The faster approach employed here is to look for a sufficiently
     * large sequence of /completely free/ freelist pages; the worst
     * case space utilization of this speedup is >50% as the worst case
     * is looking for a chunk of space as large as one freelist page
     * (~ kBlobAlignment db pages) + 1 byte, in which case the second
     * freelist page will not be checked against a subsequent huge size
     * request as it is not 'completely free' any longer, thus effectively
     * occupying 2 freelist page spans where 1 (and a bit) would have
     * sufficed, resulting in a worst case space utilization of a little
     * more than 50%.
     */
    if (global_hints.page_span_width > 1) {
      /*
       * we must employ a different freelist alloc system for requests
       * spanning multiple freelist pages as the regular
       * search_bits() is not able to cope with such
       * requests.
       *
       * hamsterdb versions prior to 1.1.0 would simply call that
       * function and fail every time, resulting in a behaviour where
       * 'huge blobs' could be added or overwritten in the database,
       * but erased huge blobs' space would never be re-used for
       * subsequently inserted 'huge blobs', thus resulting in an ever
       * growing database file when hamsterdb would be subjected to a
       * insert+erase use pattern for huge blobs.
       *
       * Note that the multipage spanning search employs a Boyer-Moore
       * search mechanism, which is (at least partly) built into the
       * locate_sufficient_free_space() function;
       * all that's left for us here is to scan _backwards_ conform
       * BM to see if we have a sufficiently large sequence of
       * completely freed freelist entries.
       */
      ham_size_t pagecount_required = hints.page_span_width;
      ham_size_t start_idx;
      ham_size_t end_idx;
      ham_size_t available = entry->free_bits;

      ham_assert(entry->free_bits <= entry->max_bits);
      if (i < (ham_s32_t)hints.page_span_width) {
        m_count_misses++;
        return (0);
      }
      ham_assert(i >= (ham_s32_t)hints.page_span_width);
      /*
       * entry points at a freelist entry in the possible sequence, scan
       * back and forth to discover our actual sequence length. Scan
       * back first, then forward when we need a tail.
       */
      for (start_idx = 0; ++start_idx < pagecount_required; ) {
        ham_assert(i >= (ham_s32_t)start_idx);
        ham_assert(i - start_idx >= global_hints.start_entry);
        FreelistEntry *e = entry - start_idx;
        if (e->free_bits != e->max_bits)
          break;
        available += e->free_bits;
      }
      start_idx--;

      /*
       * now see if we need (and have) a sufficiently large tail;
       * we can't simply say
       *
       *    pagecount_required -= start_idx;
       *
       * because our very first freelist entry in the sequence may have
       * less maxbits than the others (as it may be the header page!)
       * so we need to properly calculate the number of freelist
       * entries that we need more:
       */
      ham_assert(hints.size_bits + hints.freelist_pagesize_bits - 1
              >= available);
      pagecount_required = hints.size_bits - available;
      /* round up: */
      pagecount_required += hints.freelist_pagesize_bits - 1;
      pagecount_required /= hints.freelist_pagesize_bits;
      FreelistEntry *e = entry + 1;
      for (end_idx = 1;
          end_idx < pagecount_required
                && i + end_idx < m_entries.size()
                && e->free_bits != e->max_bits;
          end_idx++) {
        e = entry + end_idx;
        available += e->free_bits;
      }
      end_idx--;

      /*
       * we can move i forward to the first non-suitable entry and
       * BM-skip from there, HOWEVER, we have two BM modes in here
       * really: one that scans forward (DAM:RANDOM_ACCESS)
       * and one that scans backwards (DAM:SEQUENTIAL) and moving 'i'
       * _up_ would harm the latter.
       *
       * The way out of this is to add end_idx+1 as a skip_offset
       * instead and let locate_sufficient_free_space()
       * handle it from there.
       */
      global_hints.skip_init_offset = end_idx + 1;

      if (available < hints.size_bits) {
        /* register the NO HIT */
        FreelistStatistics::globalhints_no_hit(this, entry, &hints);
      }
      else {
        ham_size_t len;
        ham_u64_t addr = 0;

        /* we have a hit! */
        i -= start_idx;

        start_idx = 0;
        for (len = hints.size_bits; len > 0; i++, start_idx++) {
          ham_size_t fl;

          ham_assert(i < (ham_s32_t)m_entries.size());

          entry = &m_entries[i];
          if (i == 0) {
            page = 0;
            fp = m_env->get_freelist_payload();
          }
          else {
            st = m_env->get_page_manager()->fetch_page(&page, 0, entry->pageid);
            if (st)
              return (st);
            fp = PFreelistPayload::from_page(page);
          }
          ham_assert(entry->free_bits == entry->max_bits);
          ham_assert(fp->get_free_bits() == fp->get_max_bits());

          if (start_idx == 0)
            addr = fp->get_start_address();

          if (len >= entry->free_bits)
            fl = entry->free_bits;
          else
            fl = len;
          set_bits(entry, fp, 0, fl, false, &hints);
          fp->set_free_bits((ham_u32_t)(fp->get_free_bits() - fl));
          entry->free_bits = fp->get_free_bits();
          len -= fl;

          // mark page (or header page) as dirty
          mark_dirty(page);
        }

        ham_assert(addr != 0);
        *paddr = addr;
        m_count_hits++;
        return (HAM_SUCCESS);
      }
    }
    else {
      /*
       * and this is the 'regular' free slot search, where we are
       * looking for sizes which fit into a single freelist entry page
       * in their entirety.
       *
       * Here we take the shortcut of not looking for edge solutions
       * spanning two freelist entries (start in one, last few chunks
       * in the next); this optimization costs little in space
       * utilization losses and gains us a lot in execution speed.
       * This particular optimization was already present in pre-v1.1.0
       * hamsterdb, BTW.
       */
      ham_assert(entry->free_bits >= size/kBlobAlignment);
      ham_assert(hints.startpos + hints.size_bits <= hints.endpos);

      /* yes, load the payload structure */
      if (i == 0) {
        fp = m_env->get_freelist_payload();
        page = 0;
      }
      else {
        st = m_env->get_page_manager()->fetch_page(&page, 0, entry->pageid);
        if (st)
          return (st);
        fp = PFreelistPayload::from_page(page);
      }

      /* now try to allocate from this payload */
      s = search_bits(entry, fp, size / kBlobAlignment, &hints);
      if (s != -1) {
        set_bits(entry, fp, s, size / kBlobAlignment, false, &hints);
        // mark page (or header page) as dirty
        mark_dirty(page);
        break;
      }
      else {
        /* register the NO HIT */
        FreelistStatistics::globalhints_no_hit(this, entry, &hints);
      }
    }
  }

  ham_assert(s != -1 ? fp != NULL : 1);

  if (s != -1) {
    fp->set_free_bits(
        (ham_u32_t)(fp->get_free_bits() - size / kBlobAlignment));
    entry->free_bits = fp->get_free_bits();

    *paddr = (fp->get_start_address() + (s * kBlobAlignment));
    m_count_hits++;
  }
  else
    m_count_misses++;

  return (HAM_SUCCESS);
}

bool
Freelist::is_page_free(ham_u64_t address)
{
  ham_status_t st;

  ham_size_t pagesize = m_env->get_pagesize();
  ham_size_t size_bits = pagesize / kBlobAlignment;
  ham_assert(address % pagesize == 0);

  if (m_entries.empty()) {
    st = initialize();
    if (st)
      return (false);
  }

  /* get the cache entry of this address */
  FreelistEntry *entry = get_entry_for_address(address);

  /* need at least |size_bits| free bits */
  if (entry->free_bits < size_bits)
    return (false);

  Page *page = 0;
  PFreelistPayload *fp = 0;

  /* if page does not exist then the space is not free */
  if (!entry->pageid) {
    if (entry->start_address == m_env->get_pagesize()) {
      fp = m_env->get_freelist_payload();
      ham_assert(fp->get_start_address() != 0);
    }
    else
      return (false);
  }
  /* otherwise just fetch the page from the cache or the disk */
  else {
    st = m_env->get_page_manager()->fetch_page(&page, 0, entry->pageid);
    if (st)
      return (st);
    fp = PFreelistPayload::from_page(page);
    ham_assert(fp->get_start_address() != 0);
  }

  ham_assert(address >= fp->get_start_address());

  /* check the bits */
  if (check_bits(entry, fp,
         (ham_size_t)(address - fp->get_start_address()) / kBlobAlignment,
         size_bits) == (ham_size_t)-1)
    return (false);

  return (true);
}

ham_status_t
Freelist::truncate_page(ham_u64_t address)
{
  ham_status_t st;

  ham_size_t pagesize = m_env->get_pagesize();
  ham_size_t size_bits = pagesize / kBlobAlignment;
  ham_assert(address % pagesize == 0);
  ham_assert(!m_entries.empty());

  /* get the cache entry of this address */
  FreelistEntry *entry = get_entry_for_address(address);

  Page *page = 0;
  PFreelistPayload *fp = 0;

  /* page is the Environment's header page? */
  if (!entry->pageid) {
    if (entry->start_address == m_env->get_pagesize()) {
      fp = m_env->get_freelist_payload();
      ham_assert(fp->get_start_address() != 0);
    }
  }
  /* otherwise just fetch the page from the cache or the disk */
  else {
    st = m_env->get_page_manager()->fetch_page(&page, 0, entry->pageid);
    if (st)
      return (st);
    fp = PFreelistPayload::from_page(page);
    ham_assert(fp->get_start_address() != 0);
  }

  /* adjust the number of free bits. do not set the overflow pointer to NULL;
   * the overflow page is still valid (with 0 free bits). If the overflow
   * page would be freed in here then we might end up with a situation where
   * the freelist adds pages to the freelist while it is shut down - this
   * smells as if it could lead to all kind of problems. */
  ham_assert(entry->free_bits >= size_bits);
  entry->free_bits -= size_bits;
  fp->set_free_bits(entry->free_bits);

  set_bits(entry, fp,
        (ham_size_t)(address - fp->get_start_address()) / kBlobAlignment,
        pagesize / kBlobAlignment, false, 0);

  /* |mark_dirty| stores the page in the changeset if recovery is enabled */
  mark_dirty(page);

  return (0);
}

ham_s32_t
Freelist::search_bits(FreelistEntry *entry, PFreelistPayload *f,
        ham_size_t size_bits, FreelistStatistics::Hints *hints)
{
  ham_assert(hints->cost == 1);
  ham_u64_t *p64 = (ham_u64_t *)f->get_bitmap();
  ham_u32_t start = hints->startpos;
  ham_u32_t end = hints->endpos;
  ham_u32_t min_slice_width = hints->skip_distance;

  /* as freelist pages are created, they should span a multiple of
   * 64(=QWORD bits) DB_CHUNKS! */
  ham_assert(end <= f->get_max_bits());
  ham_assert(f->get_max_bits() % 64 == 0);

  /* sanity checks */
  ham_assert(end > start);
  ham_assert(min_slice_width > 0);
  ham_assert(f->get_max_bits() >= f->get_free_bits());

  /*
   * start-of-scan speedups:
   *
   * 1) freelist pages are created and then filled with zeroes,
   * EXCEPT for those slots which have an actual disc page related to
   * them. Hence, maxbits is a bit of a lie, really: only when a page
   * has 'overflow' can we expect a freelist to be entirely occupied.
   *
   * Hence we can speed up matters a bit by quick-scanning for the
   * end of the occupied zone: from the end of the freelist we descend
   * by pagesize/CHUNK steps probing for free slots. A special case:
   * when none are found, the total range is still assumed to be the
   * entire freelist page, in order to prevent permanent gaps which
   * will never be filled. Of course, this choice is mode-dependent: in
   * higher modes, we care less about those gaps.
   *
   * 2) we can inspect the 'free_bits' count (which decreases as
   * bits are occupied) - this value tells us something about the
   * total number of available free slots. We can discard the chance
   * of any luck finding a suitable slot for any requests which are
   * larger than this number.
   */

  ham_assert(size_bits <= f->get_max_bits());

  /* #2 */
  ham_assert(size_bits <= entry->free_bits);
  ham_assert(size_bits <= f->get_free_bits());

  /* #3: get a hint where to start searching for free space: DONE ALREADY */

  /*
   * make sure the starting point is a valid sample spot. Also, it's
   * no use to go looking when we won't have a chance for a hit
   * anyway.
   */
  if (start + size_bits > end) {
    FreelistStatistics::fail(this, entry, f, hints);
    return (-1);
  }

  /* determine the first aligned starting point: */
  if (hints->aligned) {
    ham_u32_t chunked_pagesize = m_env->get_pagesize() / kBlobAlignment;
    ham_u32_t offset = (ham_u32_t)(f->get_start_address() / kBlobAlignment);
    offset %= chunked_pagesize;
    offset = chunked_pagesize - offset;
    offset %= chunked_pagesize;

    /*
     * now calculate the aligned start position
     *
     * as freelist pages are created, they should span a multiple
     * of 64 DB_CHUNKS!
     */
    if (start < offset)
      start = offset;
    else {
      start -= offset;
      start += chunked_pagesize - 1;
      start -= start % chunked_pagesize;
      start += offset;
    }

    /*
     * align 'end' as well: no use scanning further than that one.
     * (This of course assumes a free page-aligned slot is available
     * ENTIRELY WITHIN the bitspace carried by a single freelist
     * page; alas, there're enough of those, and the ones, if any,
     * crossing over the freelist page boundary, are welcome to to
     * the other free slot searches coming in. ;-)
     *
     * Of course, this also assumes any 'aligned' (or any other!)
     * request for a free zone all are small enough to span only a
     * single freelist page. This is okay; huge blobs are the only
     * possible exception and as far as I gathered those are handled
     * on a page-at-a-time basis anyway, reducing them to multiple
     * 'unrelated' pagesized free zone queries to us here.
     *
     * Note that freelist pages do NOT have to start their bitarray
     * at a pagesize-aligned address, at least not theoretically. We
     * resolve this here by aligning the 'end' by first converting
     * it to a fake address of sorts by subtracting 'offset'. When
     * we have done that, we can align it to a page boundary like
     * everybody else (EXCEPT we need to round DOWN here as we are
     * looking at an END marker instead of a START marker!) and when
     * that's done as well, we shift 'end' back up by offset,
     * putting it back where it should be.
     */
    ham_assert(end >= offset);
    end -= offset;
    end -= end % chunked_pagesize; /* round DOWN to boundary */
    end += offset;

    /*
     * adjust minimum step size also: it's no use scanning the
     * non-aligned spots after all
     */
    min_slice_width += chunked_pagesize - 1;
    min_slice_width -= min_slice_width % chunked_pagesize;

    /*
     * make sure the starting point is a valid sample spot:
     * since we aligned start & end, they may now be identical:
     * no space here then...
     */
    ham_assert(start <= end);
    /*
     * Also, it's no use to go looking when we won't have a chance
     * for a hit anyway.
     */
    if (start + size_bits > end) {
      FreelistStatistics::fail(this, entry, f, hints);
      return (-1);
    }
  }
  ham_assert(start < end);

  /*
   * in order to cut down on the number of overlapping tests, we
   * skip-loop scan for the first probable hit.
   *
   * This way we ensure that, as soon as we've left this mode-switch
   * section and enter the big BM-loop below, our 'start' already
   * points at a probable hit at all times!
   *
   *
   * sequential scan: the usual BM skip loop, with a twist:
   *
   * when the size we're looking for is large enough, we know we need
   * 1 or more all-1s qwords and we search for those then.
   *
   * At least one all-1s QWORD is required when the
   * requested space is >= 2 QWORDS:
   *
   * e.g. layout '0001 1111 1110'
   *
   * and as 'min_slice_width' is a rounded-up value, we'd better
   * check with the original: 'size_bits'
   */
  if (hints->aligned) {
    if (start % 64 == 0 && end % 64 == 0) {
      /*
       * The alignment is a QWORD(64)*CHUNKSIZE(32) multiple (= 2K),
       * so we'll be able to scan the freelist using QWORDs only,
       * which is fastest.
       */

      /* probing START positions; bm_l is the "left" start offset
       * in the bitmap. bm_r is the EXCLUSIVE upper bound */
      ham_u32_t bm_l = start / 64;
      ham_u32_t min_slice_width64 = (min_slice_width + 64 - 1) / 64;
      ham_u32_t bm_r = end / 64 - min_slice_width64 + 1;

      /*
       * we know which start positions are viable; we only
       * inspect those.
       *
       * Besides, we assume ALIGNED searches require 1 all-1s
       * qword at least; this improves our skipscan here.
       */
      while (bm_l < bm_r) {
        hints->cost++;

        if (p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL) {
          /*
           * BM: a hit: see if we have a sufficiently large
           * free zone here
           */
          break;
        }

        bm_l += min_slice_width64;
      }

      /* report our failure to find a free slot */
      if (bm_l >= bm_r) {
        FreelistStatistics::fail(this, entry, f, hints);
        return (-1);
      }

      /* BM search with a startup twist already done */
      for (;;) {
        ham_assert(p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL);

        hints->cost++;

        /*
         * We also assume such aligned scans require all-1s qwords
         * EXCLUSIVELY, i.e. no dangling bit tail for these, my
         * friend. Just all-1s qwords all the way.
         *
         * we already know we're at the STARTING spot of this one:
         * in our case it's just a forward scan, maybe with a
         * little guard check, is all we're gonna need.
         *
         * However, since we happen to know the SIZE we're looking
         * for is rather large, we perform a PRE-SCAN by binary
         * searching the forward
         * range (no need to scan backwards: we've been there in a
         * previous round if there was anything interesting in
         * there).
         *
         * To help the multi-level guard check succeed, we have to
         * assume a few things:
         *
         * we know the START. It is fixed. So all we need to do is
         * to find a '0' bit in the pre-scan of the SIZE range and
         * we can be assured the current zone is toast.
         *
         * We assume in this locality: the '0' bit in there is most
         * probably located near the start of the range, if any.
         *
         * the guard check only remotely looks like a bsearch: it
         * starts at START and then divides the space in 2 on every
         * round, until the END marker is hit. Any '0' bit in the
         * inspected qwords will trigger a FAIL for this zone.
         */
        if (min_slice_width64 > SIMPLE_SCAN_THRESHOLD) {
          ham_u32_t l = bm_l + 1; /* START qword is already checked */
          ham_u32_t r = l + min_slice_width64 - 1; /* EXCLUSIVE upper bound */
          while (l < r) {
            hints->cost++;

            if (p64[l] != 0xFFFFFFFFFFFFFFFFULL)
              break;
            /* make sure we get at l==r at some point: */
            l = (l + r + 1) / 2;
          }
          if (l == r) {
            /*
             * all guard checks have passed.
             *
             * WARNING: note that due to the way the guard
             * check loop was coded, we are now SURE the initial
             * QWORD
             * _and_ last QWORD are all-1s at least, so we
             * don't have to linear-scan those again.
             */

            /* linear forward validation scan */
            r--; /* topmost all-1s qword of the acceptable range + 1 */
            l = bm_l + 1; /* skip first qword */

            for ( ; l < r; l++) {
              hints->cost++;

              if (p64[l] != 0xFFFFFFFFFFFFFFFFULL)
                break;
            }
            if (r == l) {
              /* a perfect hit: report this one as a match! */
              FreelistStatistics::update(this, entry, f, bm_l * 64, hints);
              return (bm_l * 64);
            }
          }
        }
        else {
          /*
           * simple scan only: tiny range
           *
           * Nevertheless, we also have checked our first QWORD,
           * so we can skip that one
           */
          ham_u32_t l = bm_l + 1; /* we have checked the START qword already */
          ham_u32_t r = l + min_slice_width64 - 1; /* EXCLUSIVE upper bound */

          /* linear forward validation scan */
          for ( ; l < r; l++) {
            hints->cost++;

            if (p64[l] != 0xFFFFFFFFFFFFFFFFULL)
              break;
          }
          if (r == l) {
            /* a perfect hit: report this one as a match! */
            FreelistStatistics::update(this, entry, f, bm_l * 64, hints);
            return (bm_l * 64);
          }
        }

        /*
         * when we get here, we've failed the inner sequence
         * validation of an aligned search; all we can do now is try
         * again at the next aligned scan location.
         *
         * This is the simplest post-backtrack skip of the bunch,
         * Sunday/Hume-wise, but nothing improves upon this (unless
         * we were scanning a size span in there which would've been
         * larger than the skip step here, and that NEVER happens
         * thanks to our prep work at the start of this function.
         */
        bm_l += min_slice_width64;

        /*
         * we know which start positions are viable; we only
         * inspect those.
         *
         * Besides, we assume ALIGNED searches require 1 all-1s
         * qword at least; this improves our skipscan here.
         */
        while (bm_l < bm_r) {
          hints->cost++;

          if (p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL) {
            /*
             * BM: a hit: see if we have a sufficiently large
             * free zone here
             */
            break;
          }

          /* BM: a miss: skip to next opportunity sequentially */
          bm_l += min_slice_width64;
        }

        if (bm_l >= bm_r) {
          /* report our failure to find a free slot */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }
      }
    }
    else {
      /*
       * The alignment is NOT a QWORD(64)*CHUNKSIZE(32) multiple (= 2K),
       * so we'll have to contend ourselves with a BYTE-based scan
       * instead, which would mean our minimum allowed alignment would be
       * BYTE(8)*CHUNKSIZE(32) == 256 bytes alignment.
       */
      ham_u8_t *p8=(ham_u8_t *)p64;

      /* probing START positions */
      ham_u32_t bm_l = start / 8;
      ham_u32_t min_slice_width8 = (min_slice_width + 8 - 1) / 8;
      ham_u32_t bm_r = end / 8 - min_slice_width8 + 1; /* EXCLUSIVE upper bound */

      /*
       * we know which start positions are viable; we only
       * inspect those.
       *
       * Besides, we assume ALIGNED searches require 1 all-1s
       * byte at least; this improves our skipscan here.
       */
      while (bm_l < bm_r) {
        hints->cost++;

        if (p8[bm_l] == 0xFFU) {
          /*
          * BM: a hit: see if we have a sufficiently large
          * free zone here
          */
          break;
        }

        /* BM: a miss: skip to next opportunity sequentially */
        bm_l += min_slice_width8;
      }

      if (bm_l >= bm_r) {
        /* report our failure to find a free slot */
        FreelistStatistics::fail(this, entry, f, hints);
        return (-1);
      }

      /* BM search with a startup twist already done */
      for (;;) {
        ham_assert(p8[bm_l] == 0xFFU);

        hints->cost++;

        /*
         * We also assume such aligned scans require all-1s bytes
         * EXCLUSIVELY, i.e. no dangling bit tail for these, my
         * friend. Just all-1s bytes all the way.
         *
         * we already know we're at the STARTING spot of this one:
         * in our case it's just a forward scan, maybe with a
         * little guard check, is all we're gonna need.
         *
         * However, since we happen to know the SIZE we're looking
         * for is rather large, we perform a PRE-SCAN by binary
         * searching the forward
         * range (no need to scan backwards: we've been there in a
         * previous round if there was anything interesting in
         * there).
         *
         * To help the multi-level guard check succeed, we have to
         * assume a few things:
         *
         * we know the START. It is fixed. So all we need to do is
         * to find a '0' bit in the pre-scan of the SIZE range and
         * we can be assured the current zone is toast.
         *
         * We assume in this locality: the '0' bit in there is most
         * probably located near the start of the range, if any.
         *
         * the guard check only remotely looks like a bsearch: it
         * starts at START and then divides the space in 2 on every
         * round, until the END marker is hit. Any '0' bit in the
         * inspected bytes will trigger a FAIL for this zone.
         */
        if (min_slice_width8 > SIMPLE_SCAN_THRESHOLD) {
          ham_u32_t l = bm_l + 1; /* we have checked the START byte already */
          ham_u32_t r = l + min_slice_width8 - 1; /* EXCLUSIVE upper bound */
          while (l < r) {
            hints->cost++;

            if (p8[l] != 0xFFU)
              break;
            /* make sure we get at l==r at some point: */
            l = (l + r + 1) / 2;
          }
          if (l == r) {
            /*
             * all guard checks have passed.
             *
             * WARNING: note that due to the way the guard
             * check loop was coded, we are now SURE the initial
             * BYTE
             * _and_ last BYTE are all-1s at least, so we
             * don't have to linear-scan those again.
             */

            /* linear forward validation scan */
            r--; /* topmost all-1s byte of the acceptable range + 1 */
            l = bm_l + 1; /* skip first byte */

            for ( ; l < r; l++) {
              hints->cost++;

              if (p8[l] != 0xFFU)
                break;
            }
            if (r == l) {
              /* a perfect hit: report this one as a match! */
              FreelistStatistics::update(this, entry, f, bm_l * 8, hints);
              return (bm_l * 8);
            }
          }
        }
        else {
          /*
           * simple scan only: tiny range
           *
           * Nevertheless, we also have checked our first BYTE,
           * so we can skip that one
           */
          ham_u32_t l = bm_l + 1; /* we have checked the START byte already */
          ham_u32_t r = l + min_slice_width8 - 1; /* EXCLUSIVE upper bound */

          /* linear forward validation scan */
          for ( ; l < r; l++) {
            hints->cost++;

            if (p8[l] != 0xFFU)
              break;
          }
          if (r == l) {
            /* a perfect hit: report this one as a match! */
            FreelistStatistics::update(this, entry, f, bm_l * 8, hints);
            return (bm_l * 8);
          }
        }

        /*
         * when we get here, we've failed the inner sequence
         * validation of an aligned search; all we can do now is try
         * again at the next aligned scan location.
         *
         * This is the simplest post-backtrack skip of the bunch,
         * Sunday/Hume-wise, but nothing improves upon this (unless
         * we were scanning a size span in there which would've been
         * larger than the skip step here, and that NEVER happens
         * thanks to our prep work at the start of this function.
         */
        bm_l += min_slice_width8;

        /*
         * we know which start positions are viable; we only
         * inspect those.
         *
         * Besides, we assume ALIGNED searches require 1 all-1s
         * byte at least; this improves our skipscan here.
         */
        while (bm_l < bm_r) {
          hints->cost++;

          if (p8[bm_l] == 0xFFU) {
            /*
             * BM: a hit: see if we have a sufficiently large
             * free zone here
             */
            break;
          }

          /* BM: a miss: skip to next opportunity sequentially */
          bm_l += min_slice_width8;
        }

        if (bm_l >= bm_r) {
          /* report our failure to find a free slot */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }
      }
    }
  } /* hints->aligned */
  else {
    /*
     * UNALIGNED search:
     *
     * now there's two flavors in here, or should I say 3?
     *
     * (1) a search for sizes which span ONE all-1s QWORD at least
     * (i.e. searches for size >= sizeof(2 QWORDS)),
     *
     * (2) a search for sizes which are smaller, but still require
     * spanning an entire BYTE (i.e. searches for size >= sizeof(2
     * BYTES)),
     *
     * (3) a search for sizes even tinier than that
     */
    if (size_bits >= 2 * 64) {
      /* l & r: INCLUSIVE + EXCLUSIVE boundary; probe END markers */
      ham_u32_t min_slice_width64 = min_slice_width / 64; /* roundDOWN */
      ham_u32_t bm_l = start / 64;
      ham_u32_t bm_r = (end + 64 - 1) / 64;
      ham_u32_t lb = bm_l;
      bm_l += min_slice_width64 - 1; /* first END marker to probe */

      /*
       * we know which END positions are viable; we only
       * inspect those.
       *
       * Besides, we know these UNALIGNED searches require 1
       * all-1s qword at least; this improves our skipscan
       * here.
       */
      while (bm_l < bm_r) {
        hints->cost++;

        if (p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL) {
          /*
           * BM: a hit: see if we have a sufficiently
           * large free zone here.
           */
          break;
        }

        /* BM: a miss: skip to next opportunity sequentially */
        bm_l += min_slice_width64;
      }

      if (bm_l >= bm_r) {
        /* report our failure to find a free slot */
        FreelistStatistics::fail(this, entry, f, hints);
        return (-1);
      }

      /* BM search with a startup twist already done */
      for (;;) {
        /* -1 because we have checked the END qword already */
        register ham_u32_t r = bm_l - 1;
        /* +l: INCLUSIVE lower bound */
        register ham_u32_t l = bm_l - min_slice_width64 + 1;

        ham_assert(bm_l > 0);
        ham_assert(bm_l >= min_slice_width64 - 1);
        ham_assert(p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL);

        hints->cost++;

        /*
         * compare comment in aligned search code
         *
         * This time we REV scan down to find the lower bound
         * of the current range. Also note that our REV guard is
         * the inverse of the FWD guard: starting close by and
         * testing at an increasing pace away from the bm_l
         * probe location.
         *
         * Once we've established the lower bound, we FWD scan
         * past the current probe to see if the entire requested
         * range is available at this locality.
         */
        if (min_slice_width64 > SIMPLE_SCAN_THRESHOLD) {
          ham_u32_t d = 1;
          for (;;) {
            hints->cost++;

            if (p64[r] != 0xFFFFFFFFFFFFFFFFULL) {
              l = r + 1; /* lowest PROBABLY okay probe location */
              break;
            }
            if (r < l + d) {
              if (r < l + 1) {
                /* l == lowest PROBABLY okay probe location */
                break;
              }
              else {
                d = 1;
              }
            }
            r -= d;
            /* increase step size by a power of 2;
             * inverted divide and conquer */
            d <<= 1;
          }
          /*
           * the guard check adjusted our expected lower
           * bound in 'l'.
           *
           * WARNING: note that due to the way the guard
           * check loop was coded, we are now SURE the initial
           * QWORD
           * _and_ QWORD[bm_l-1] are all-1s at least, so we
           * don't have to linear-scan those again. However,
           * we 'lost' the QWORD[bm_l-1] info as the guard
           * scan went on, so we have to rescan that one again
           * anyway.
           *
           * REV linear validation scan follows...
           */
        }

        /* REV linear validation scan: */
        ham_assert(bm_l > 0);
        for (r = bm_l - 1; r > l; r--) {
          hints->cost++;

          if (p64[r] != 0xFFFFFFFFFFFFFFFFULL) {
            l = r + 1; /* lowest (last) okay probe location */
            break;
          }
        }
        /* fringe case check: the lowest QWORD... */
        if (r == l && p64[r] != 0xFFFFFFFFFFFFFFFFULL) {
          l = r + 1; /* lowest (last) okay probe location */
        }

        /* do we need more 'good space' FWD? */
        if ((++bm_l - l) * 64 < size_bits) {
          /*
           * FWD linear validation scan:
           *
           * try to scan a range which also spans any
           * possibly extra bits in the non-qword aligned
           * request size. There's no harm in scanning one
           * more qword FWD in here, anyway, as we use it to
           * adjust the next skip on failure anyway.
           */
          r = bm_l + min_slice_width64;
          if (r > bm_r) {
            r = bm_r;
          }
          for ( ; r > bm_l; bm_l++) {
            hints->cost++;

            if (p64[bm_l] != 0xFFFFFFFFFFFFFFFFULL)
              break;
          }
        }

        /*
         * 'bm_l' now points +1 PAST the position for the LAST
         * all-1s qword.
         *
         * But first: see if we can hug the lead space to a '0'
         * bit:
         * 'l' points at the lowest all-1s qword; if it's not
         * sitting on the lower boundary, then inspect the qword
         * below that.
         */
        if (l > lb) {
          /*
           * get fancy: as we perform an unaligned scan, we
           * MAY have some more bits sitting in this spot, as
           * long as they are consecutive with the all-1s
           * qword up next.
           *
           * Right here, it's ENDIANESS that's right dang
           * important, y'all. And there's a cheaper way to
           * check if the top bit has been set ya ken: two's
           * complement sign check, right on!
           */
          ham_u32_t lpos = BITSCAN_MSBit(ham_db2h64(p64[l-1]), l * 64);
          ham_assert(l > 0);

          /* do we have enough free space now? */
          ham_assert(bm_l > 0);
          ham_assert((bm_l - 1) * 64 >= lpos);
          if (size_bits <= (bm_l - 1) * 64 - lpos) {
            /* yeah! */
            FreelistStatistics::update(this, entry, f, lpos, hints);
            return (lpos);
          }

          /*
           * second, we still ain't got enough space, so we
           * MUST count the tail bits at [bm_l] -- at least if
           * we haven't hit the upper bound yet.
           *
           * But only do the (expensive) bitscan when we just
           * need those few extra bits in there to accomplish
           * our goal.
           */
          if (bm_l >= bm_r) {
            /* upper bound hit: we won't be able to report a match. */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
          else { /* if (size_bits <= bm_l * 64 - lpos) */
            ham_u32_t rpos = BITSCAN_LSBit(ham_db2h64(p64[bm_l]), bm_l * 64);
            ham_assert(bm_l > 0);
            ham_assert(rpos >= lpos);
            /*
             * Special assumption! When the 'end' is NOT on
             * a qword boundary, we assume the entire qword
             * is still filled correctly, which means: any
             * bits in there BEYOND 'end'
             * are still correct 0s and 1s. At least we
             * assume they are all _accessible_; as we are
             * conservative, we _do_ limit rpos to 'end' as
             * the stats hinter gave it to us.
             */
            if (rpos > end)
              rpos = end;
            ham_assert(rpos >= lpos);

            /* again: do we have enough free space now? */
            if (size_bits <= rpos - lpos) {
              /* yeah! */
              FreelistStatistics::update(this, entry, f, lpos, hints);
              return (lpos);
            }
          }
        }
        else {
          /* do we have enough free space now? */
          if (size_bits <= (bm_l - l) * 64) {
            /* yeah! */
            FreelistStatistics::update(this, entry, f, l * 64, hints);
            return (l * 64);
          }

          /*
           * second, we still ain't got enough space, so we
           * MUST count the tail bits at [bm_l] -- at least if
           * we haven't hit the upper bound yet.
           *
           * But only do the (expensive) bitscan when we just
           * need those few extra bits in there to accomplish
           * our goal.
           */
          if (bm_l >= bm_r) {
            /* upper bound hit: we won't be able to report a match. */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
          else { /* if (size_bits <= (bm_l - l) * 64) */
            ham_u32_t rpos = BITSCAN_LSBit(ham_db2h64(p64[bm_l]),
                bm_l * 64);
            ham_assert(bm_l > 0);
            ham_assert(rpos >= l * 64);
            /*
             * Special assumption! When the 'end' is NOT on
             * a qword boundary, we assume the entire qword
             * is still filled correctly, which means: any
             * bits in there BEYOND 'end'
             * are still correct 0s and 1s. At least we
             * assume they are all _accessible_; as we are
             * conservative, we _do_ limit rpos to 'end' as
             * the stats hinter gave it to us.
             */
            if (rpos > end)
              rpos = end;
            ham_assert(rpos >= l * 64);

            /* again: do we have enough free space now? */
            ham_assert(rpos >= l * 64);
            if (size_bits <= rpos - l * 64) {
              /* yeah! */
              FreelistStatistics::update(this, entry, f, l*64, hints);
              return (l * 64);
            }
          }
        }

        /*
         * otherwise, we can determine the new skip value: our
         * next probe should be here:
         */
        bm_l += min_slice_width64;

        /* BM skipscan */
        while (bm_l < bm_r) {
          hints->cost++;

          if (p64[bm_l] == 0xFFFFFFFFFFFFFFFFULL) {
            /*
             * BM: a hit: see if we have a sufficiently
             * large free zone here.
             */
            break;
          }

          /* BM: a miss: skip to next opportunity sequentially */
          bm_l += min_slice_width64;
        }

        if (bm_l >= bm_r) {
          /* report our failure to find a free slot */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }
      }
    }
    else if (size_bits >= 2 * 8) {
      ham_u8_t *p8=(ham_u8_t *)p64;

      /* l & r: INCLUSIVE + EXCLUSIVE boundary; probe END markers */
      ham_u32_t min_slice_width8 = min_slice_width / 8; /* roundDOWN */
      ham_u32_t bm_l = start / 8;
      ham_u32_t bm_r = (end + 8 - 1) / 8;
      ham_u32_t lb = bm_l;
      ham_assert(min_slice_width8 > 0);
      bm_l += min_slice_width8 - 1; /* first END marker to probe */

      /*
       * we know which END positions are viable; we only
       * inspect those.
       *
       * Besides, we know these UNALIGNED searches require 1
       * all-1s BYTE at least; this improves our skipscan
       * here.
       */
      while (bm_l < bm_r) {
        hints->cost++;

        if (p8[bm_l] == 0xFFU) {
          /*
           * BM: a hit: see if we have a sufficiently
           * large free zone here.
           */
          break;
        }

        /* BM: a miss: skip to next opportunity sequentially */
        bm_l += min_slice_width8;
      }

      if (bm_l >= bm_r) {
        /* report our failure to find a free slot */
        FreelistStatistics::fail(this, entry, f, hints);
        return (-1);
      }

      /* BM search with a startup twist already done */
      for (;;) {
        /* -1 because we have checked the END byte already */
        register ham_u32_t r = bm_l - 1;
        /* +1 because INCLUSIVE lower bound */
        register ham_u32_t l = bm_l - min_slice_width8 + 1;

        ham_assert(bm_l > 0);
        ham_assert(bm_l >= min_slice_width8 - 1);
        ham_assert(p8[bm_l] == 0xFFU);

        /*
         * compare comment in aligned search code
         *
         * This time we REV scan down to find the lower bound
         * of the current range. Also note that our REV guard is
         * the inverse of the FWD guard: starting close by and
         * testing at an increasing pace away from the bm_l
         * probe location.
         *
         * Once we've established the lower bound, we FWD scan
         * past the current probe to see if the entire requested
         * range is available at this locality.
         */
        if (min_slice_width8 > SIMPLE_SCAN_THRESHOLD) {
          ham_u32_t d = 1;
          for (;;) {
            hints->cost++;

            if (p8[r] != 0xFFU) {
              l = r + 1; /* lowest PROBABLY okay probe location */
              break;
            }
            if (r < l + d) {
              if (r < l + 1) {
                /* l == lowest PROBABLY okay probe location */
                break;
              }
              else {
                d = 1;
              }
            }
            r -= d;
            /* increase step size by a power of 2;
             * inverted divide and conquer */
            d <<= 1;
          }
          /*
           * the guard check adjusted our expected lower
           * bound in 'l'.
           *
           * WARNING: note that due to the way the guard
           * check loop was coded, we are now SURE the initial
           * BYTE
           * _and_ BYTE[bm_l-1] are all-1s at least, so we
           * don't have to linear-scan those again. However,
           * we 'lost' the BYTE[bm_l-1] info as the guard
           * scan went on, so we have to rescan that one again
           * anyway.
           *
           * REV linear validation scan follows...
           */
        }

        /* REV linear validation scan: */
        ham_assert(bm_l > 0);
        for (r = bm_l - 1; r > l; r--) {
          hints->cost++;

          if (p8[r] != 0xFFU) {
            l = r + 1; /* lowest (last) okay probe location */
            break;
          }
        }
        /* fringe case check: the lowest BYTE... */
        if (r == l && p8[r] != 0xFFU) {
          l = r + 1; /* lowest (last) okay probe location */
        }

        /* do we need more 'good space' FWD? */
        if ((++bm_l - l) * 8 < size_bits) {
          /*
           * FWD linear validation scan:
           *
           * try to scan a range which also spans any
           * possibly extra bits in the non-byte aligned
           * request size. There's no harm in scanning one
           * more byte FWD in here, anyway, as we use it to
           * adjust the next skip on failure anyway.
           */
          r = bm_l + min_slice_width8;
          if (r > bm_r)
            r = bm_r;
          for ( ; r > bm_l; bm_l++) {
            hints->cost++;

            if (p8[bm_l] != 0xFFU)
              break;
          }
        }

        /*
         * 'bm_l' now points +1 PAST the position for the LAST
         * all-1s byte.
         *
         * But first: see if we can hug the lead space to a '0'
         * bit:
         * 'l' points at the lowest all-1s byte; if it's not
         * sitting on the lower boundary, then inspect the byte
         * below that.
         */
        if (l > lb) {
          /*
           * get fancy: as we perform an unaligned scan, we
           * MAY have some more bits sitting in this spot, as
           * long as they are consecutive with the all-1s byte
           * up next.
           *
           * Right here, ENDIANESS doesn't matter at all. And
           * there's a cheaper way to check if the top bit has
           * been set ya ken: two's complement sign check,
           * right on!
           */
          ham_u32_t lpos = BITSCAN_MSBit8(p8[l-1], l * 8);
          ham_assert(l > 0);
          ham_assert(bm_l > 0);
          ham_assert((bm_l - 1) * 8 >= lpos);

          /* do we have enough free space now? */
          if (size_bits <= (bm_l - 1) * 8 - lpos) {
            /* yeah! */
            FreelistStatistics::update(this, entry, f, lpos, hints);
            return (lpos);
          }

          /*
           * second, we still ain't got enough space, so we
           * MUST count the tail bits at [bm_l] -- at least if
           * we haven't hit the upper bound yet.
           *
           * But only do the (expensive) bitscan when we just
           * need those few extra bits in there to accomplish
           * our goal.
           */
          if (bm_l >= bm_r) {
            /* upper bound hit: we won't be able to report a match. */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
          else { /* if (size_bits <= bm_l * 8 - lpos) */
            ham_u32_t rpos = BITSCAN_LSBit8(p8[bm_l], bm_l * 8);
            ham_assert(bm_l > 0);
            ham_assert(rpos >= lpos);
            /*
             * Special assumption! When the 'end' is NOT on
             * a qword boundary, we assume the entire qword
             * is still filled correctly, which means: any
             * bits in there BEYOND 'end'
             * are still correct 0s and 1s. At least we
             * assume they are all _accessible_; as we are
             * conservative, we _do_ limit rpos to 'end' as
             * the stats hinter gave it to us.
             */
            if (rpos > end)
              rpos = end;
            ham_assert(rpos >= lpos);

            /* again: do we have enough free space now? */
            if (size_bits <= rpos - lpos) {
              /* yeah! */
              FreelistStatistics::update(this, entry, f, lpos, hints);
              return (lpos);
            }
          }
        }
        else {
          /* do we have enough free space now? */
          if (size_bits <= (bm_l - l) * 8) {
            /* yeah! */
            FreelistStatistics::update(this, entry, f, l * 8, hints);
            return (l * 8);
          }

          /*
           * second, we still ain't got enough space, so we
           * MUST count the tail bits at [bm_l] -- at least if
           * we haven't hit the upper bound yet.
           *
           * But only do the (expensive) bitscan when we just
           * need those few extra bits in there to accomplish
           * our goal.
           */
          if (bm_l >= bm_r) {
            /* upper bound hit: we won't be able to report a match. */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
          else { /* if (size_bits <= (bm_l - l) * 8) */
            ham_u32_t rpos = BITSCAN_LSBit8(p8[bm_l], bm_l * 8);
            ham_assert(bm_l > 0);
            ham_assert(rpos >= l * 8);
            /*
             * Special assumption! When the 'end' is NOT on
             * a qword boundary, we assume the entire qword
             * is still filled correctly, which means: any
             * bits in there BEYOND 'end'
             * are still correct 0s and 1s. At least we
             * assume they are all _accessible_; as we are
             * conservative, we _do_ limit rpos to 'end' as
             * the stats hinter gave it to us.
             */
            if (rpos > end)
              rpos = end;
            ham_assert(rpos >= l * 8);

            /* again: do we have enough free space now? */
            if (size_bits <= rpos - l * 8) {
              /* yeah! */
              FreelistStatistics::update(this, entry, f, l * 8, hints);
              return (l * 8);
            }
          }
        }

        /*
         * otherwise, we can determine the new skip value: our
         * next probe should be here:
         */
        bm_l += min_slice_width8;

        /* BM skipscan */
        while (bm_l < bm_r) {
          hints->cost++;

          if (p8[bm_l] == 0xFFU) {
            /*
             * BM: a hit: see if we have a sufficiently
             * large free zone here.
             */
            break;
          }

          /* BM: a miss: skip to next opportunity sequentially */
          bm_l += min_slice_width8;
        }

        if (bm_l >= bm_r) {
          /* report our failure to find a free slot */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }
      }
    }
    else if (size_bits > 1) {
      ham_u8_t *p=(ham_u8_t *)p64;

      /* l & r: INCLUSIVE + EXCLUSIVE boundary; probe END markers */
      ham_u32_t bm_l = start;
      ham_u32_t bm_r = end;
      ham_assert(min_slice_width > 0);
      bm_l += min_slice_width - 1; /* first END marker to probe */

      /*
       * we know which END positions are viable; we only
       * inspect those.
       */
      for (;;) {
        hints->cost++;

        /*
         * the 'byte level front scanner':
         */
        if (!p[bm_l >> 3]) {
          /*
           * all 0 bits in there. adjust skip
           * accordingly. But first we scan further at
           * byte level, as we assume 0-bytes come in
           * clusters:
           */
          ham_u32_t ub = (bm_r >> 3); /* EXCLUSIVE bound */
          bm_l >>= 3;
          if (min_slice_width <= 8) {
            for (bm_l++; bm_l < ub && !p[bm_l]; bm_l++)
              hints->cost++;
          }
          else {
            /*
             * at a spacing of 9 bits or more, we can
             * skip bytes in the scanner and still be
             * down with it.
             */
            ham_assert(min_slice_width < 16);
            for (bm_l += 2; bm_l < ub && !p[bm_l]; bm_l += 2)
              hints->cost++;
          }

          /*
           * BM: a miss: skip to next opportunity
           * sequentially:
           * first roundUP bm_l to the start of the next
           * byte:
           */
          bm_l <<= 3;

          /*
           * as bm_l now points to the bit just PAST the
           * currently known '0'-series (the byte), it MAY
           * be a '1', so compensate for that by reducing
           * the next part of the skip:
           */
          bm_l += min_slice_width - 1;

          if (bm_l >= bm_r) {
            /* report our failure to find a free slot */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
          continue;
        }

        /* the regular BM scanloop */
        if (p[bm_l >> 3] & (1 << (bm_l & 0x07))) {
          /*
           * BM: a hit: see if we have a sufficiently
           * large free zone here.
           */
          break;
        }
        else {
          /* BM: a miss: skip to next opportunity sequentially */
          bm_l += min_slice_width;
          if (bm_l >= bm_r) {
            /* report our failure to find a free slot */
            FreelistStatistics::fail(this, entry, f, hints);
            return (-1);
          }
        }
      }

      /* BM search with a startup twist already done */
      for (;;) {
        /* -1 because we have checked the END BIT already */
        register ham_u32_t r = bm_l - 1;
        /* +1 because INCLUSIVE lower bound */
        register ham_u32_t l = bm_l - min_slice_width + 1;

        ham_assert(bm_l > 0);
        ham_assert(bm_l >= min_slice_width - 1);
        ham_assert(p[bm_l >> 3] & (1 << (bm_l & 0x07)));

        hints->cost++;

        /*
         * compare comment in aligned search code
         *
         * This time we REV scan down to find the lower bound
         * of the current range. Also note that our REV guard is
         * the inverse of the FWD guard: starting close by and
         * testing at an increasing pace away from the bm_l
         * probe location.
         *
         * Once we've established the lower bound, we FWD scan
         * past the current probe to see if the entire requested
         * range is available at this locality.
         */
        if (min_slice_width > SIMPLE_SCAN_THRESHOLD) {
          ham_u32_t d = 1;
          for (;;) {
            hints->cost++;

            if (!(p[r >> 3] & (1 << (r & 0x07)))) {
              l = r + 1; /* lowest PROBABLY okay probe location */
              break;
            }
            if (r < l + d) {
              if (r < l + 1) {
                /* l == lowest PROBABLY okay probe location */
                break;
              }
              else
                d = 1;
            }
            r -= d;
            /* increase step size by a power of 2;
             * inverted divide and conquer */
            d <<= 1;
          }
          /*
           * the guard check adjusted our expected lower
           * bound in 'l'.
           *
           * WARNING: note that due to the way the guard
           * check loop was coded, we are now SURE the initial
           * BIT
           * _and_ BIT[bm_l-1] are all-1s at least, so we
           * don't have to linear-scan those again. However,
           * we 'lost' the BIT[bm_l-1] info as the guard
           * scan went on, so we have to rescan that one again
           * anyway.
           *
           * REV linear validation scan follows...
           */
        }

        /* REV linear validation scan: */
        ham_assert(bm_l > 0);
        for (r = bm_l - 1; r > l; r--) {
          hints->cost++;

          if (!(p[r >> 3] & (1 << (r & 0x07)))) {
            l = r + 1; /* lowest (last) okay probe location */
            break;
          }
        }
        /* fringe case check: the lowest BIT... */
        if (r == l && !(p[r >> 3] & (1 << (r & 0x07)))) {
          l = r + 1; /* lowest (last) okay probe location */
        }

        /* do we need more 'good space' FWD? */
        if ((++bm_l - l) < size_bits) {
          /* FWD linear validation scan: */
          r = bm_l + min_slice_width - 1;
          if (r > bm_r) {
            r = bm_r;
          }
          for ( ; r > bm_l; bm_l++) {
            hints->cost++;

            if (!(p[bm_l >> 3] & (1 << (bm_l & 0x07)))) {
              break;
            }
          }
        }

        /*
         * 'bm_l' now points +1 PAST the position for the LAST
         * '1' bit.
         *
         * But first: As we are scanning at bit level we are
         * already hugging the lead space to a '0' bit:
         * 'l' points at the lowest '1' bit.
         */

        /* do we have enough free space now? */
        if (size_bits <= (bm_l - l)) {
          /* yeah! */
          FreelistStatistics::update(this, entry, f, l, hints);
          return (l);
        }

        /*
         * otherwise, we can determine the new skip value: our
         * next probe should be here:
         */
        bm_l += min_slice_width;

        /* BM skipscan */
        while (bm_l < bm_r) {
          hints->cost++;

          /*
          the 'byte level front scanner':
          */
          if (!p[bm_l >> 3]) {
            /*
             * all 0 bits in there. adjust skip
             * accordingly. But first we scan further at
             * byte level, as we assume 0-bytes come in
             * clusters:
             */
            ham_u32_t ub = (bm_r >> 3); /* EXCLUSIVE bound */
            bm_l >>= 3;
            for (bm_l++; bm_l < ub && !p[bm_l]; bm_l++)
              hints->cost++;

            /*
             * BM: a miss: skip to next opportunity
             * sequentially:
             * first roundUP bm_l to the start of the next
             * byte:
             */
            bm_l <<= 3;

            /*
             * as bm_l now points to the bit just PAST the
             * currently known '0'-series (the byte), it MAY
             * be a '1', so compensate for that by reducing
             * the next part of the skip:
             */
            bm_l += min_slice_width - 1;
            continue;
          }

          if (p[bm_l >> 3] & (1 << (bm_l & 0x07))) {
            /*
             * BM: a hit: see if we have a sufficiently
             * large free zone here.
             */
            break;
          }
          else {
            /* BM: a miss: skip to next opportunity sequentially */
            bm_l += min_slice_width;
          }
        }

        /*
         * we still ain't got enough space, but we
         * already counted all the tail bits at [bm_l] -- if we
         * haven't hit the upper bound already.
         */
        if (bm_l >= bm_r) {
          /* upper bound hit: we won't be able to report a match. */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }
      }
    }
    else { /* if (size_bits == 1) */
      ham_u8_t *p=(ham_u8_t *)p64;

      /* l & r: INCLUSIVE + EXCLUSIVE boundary; probe END markers */
      ham_u32_t bm_l = start;
      ham_u32_t bm_r = end;
      ham_assert(min_slice_width > 0);

      /*
       * We can do some special things for single-bit slot
       * search;
       * besides that, it would trigger all sorts of subtle
       * nastiness in the section above which handles requests for
       * 2 bits or more, one of the major ones being END==START
       * marker, causing unsigned integer wrap-arounds due to the
       * REVerse scan, etc. done up there.
       *
       * Never mind that; a single-bit search is a GOOD thing to
       * specialize on: tiny keys (any keys which fits in the
       * default 21 bytes hamsterdb
       * reserves for keys) do not need the (slow) REV/FWD
       * bitscans we have to do otherwise. The fun here is that
       * looking for a single
       * '1' bit is the same as looking for ANYTHING that is NOT
       * ZERO.
       *
       * Which means we can go for the jugular here and take
       * either the QWORD scan or 'native integer' size as a
       * scanner basic inspection chunk: when we have thus
       * ascertained a hit, all we need to do is determine _which_
       * bit caused the non-zero-ness of such a multi-byte integer
       * value.
       *
       * Having said that, there's another interesting bit here:
       * since START==END, the prescan is pretty useless... or
       * put in equivalent terms: the prescan IS the ENTIRE scan:
       * since we will hit that sought-after '1'-bit in the
       * prescan for certain, the entire main scan loop can be
       * discarded.
       *
       * And last but not least: we can still apply the prescan
       * optimizations as we do them otherwise; any scheme which
       * is not skipping bytes (and thereby introducing
       * sparseness) is identical to a straight-forward linear
       * scan, due to the pattern width == 1. That means we don't
       * need to perform any fancy footwork here, unless we think
       * we have something that's orders of magnitude better than
       * a linear scan and still promises some reasonable
       * results -- all I can think of here is the binary search
       * 'fast prescan' alternative here, as BM (Boyer-Moore)
       * just lost it, all the way.
       *
       * Anyway, the biggest speed gain we can get is due to the
       * statistics gatherer, which can hint us where to start
       * looking the next time around.
       *
       * The statistics gatherer/hinter does not help with
       * pathological cases such as (create a large filled space, then
       * apply pattern
       * 'write 2 keys, delete 1 key' repetivily, so that each
       * two inserts lands one in the 1-bit gap produced at the
       * start of the file due to the delete/erase operation,
       * while the other insert will have to happen at the end --
       * the only way to cope with this kind of pathology is set
       * 'FAST' mode, which blatantly ignores free space created
       * by 'delete/erase' and have the statistics gatherer know
       * then which free slots are generated 'sufficiently large'
       * to be noted and taken into account for adjusting the
       * where-to-start-looking-next index offset.
       */

      /* bm_l == first END marker to probe (size == 1) */

      /*
       * we know we'll have check each bit, pardon, byte in
       * there. BM is no help, au contraire mon ami, so we sit
       * down and build ourselves a fast byte-level sequential
       * scanner. A bit of Duff's Device inspiration is all
       * that's left to us for speeding this mother up in a
       * portable fashion, i.e. without reverting to ASM
       * coding.
       *
       * given that we HOPE our statistics gatherer/hinter is
       * smart enough to position us NEAR a good spot, it's no
       * use to unroll the scanner into a multi-stage beast
       * where we scan the edges at byte-level, while scanning
       * the core bulk in qword-aligned fashion:
       * we can't simply do qwords all the time as there are
       * CPUs out there that throw a tantrum when addressing
       * integers at odd-address boundaries (several of the
       * CPUs in the MC68K series, for example).
       */
      if (min_slice_width <= 16) {
        /* the usual; just step on it using Duff's Device (loop unrolling) */
        ham_u32_t l = (bm_l >> 3);
        ham_u32_t r = ((bm_r + 8 - 1) >> 3);
        ham_u8_t *e = p + r;
        p += l;

        ham_assert(r > l);
        /* cost is low as this is a cheap loop anyway */
        hints->cost += (r - l + 8 - 1) / 8;

        switch ((r - l)  & 0x07) {
          case 0:
          while (p != e) {
          if (*p++) break;
          case 7:
          if (*p++) break;
          case 6:
          if (*p++) break;
          case 5:
          if (*p++) break;
          case 4:
          if (*p++) break;
          case 3:
          if (*p++) break;
          case 2:
          if (*p++) break;
          case 1:
          if (*p++) break;
          }
          break;
        }
        p--; /* correct p */
        ham_assert(p < e);
        if (!p[0]) {
          /* we struck end of loop without a hit!
          report our failure to find a free slot */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }

        /*
         * now we have the byte with the free bit slot;
         * see which bit it is:
         */
        l = 8 * (ham_u32_t)(p - ((ham_u8_t *)p64)); /* ADD: the number of all-0 bytes we traversed + START offset */

        ham_assert(p[0]);

        for (r = 0;; r++) {
          ham_assert(r < 8);
          if (p[0] & (1 << r)) {
            l += r; /* lowest (last) okay probe location */
            break;
          }
        }

        ham_assert(bm_l <= l);
        ham_assert(size_bits == 1);
        /* found a slot! */
        FreelistStatistics::update(this, entry, f, l, hints);
        return (l);
      }
      else {
        /*
         * big skipsize; the same thing once more, but now
         * without Duff, but that speed gain is compensated
         * for as we will skip multiple bytes during each
         * run, which is another, though less accurate, way
         * to save time in here...
         */
        ham_u32_t l = (bm_l >> 3);
        ham_u32_t r = ((bm_r + 8 - 1) >> 3);
        ham_u8_t *e = p + r;
        ham_u32_t step = (min_slice_width >> 3);

        p += l;

        ham_assert(r > l);
        /* cost is low as this is a cheap loop anyway */
        hints->cost += (r - l + 8 - 1) / step;

        for (; !*p && p < e; p += step)
          ;
        if (p >= e) {
          /*
           * we struck end of loop without a hit!
           *
           * report our failure to find a free slot
           */
          FreelistStatistics::fail(this, entry, f, hints);
          return (-1);
        }

        /*
         * now we have the byte with the free bit slot;
         * see which bit it is:
         */
        l = 8 * (ham_u32_t)(p - ((ham_u8_t *)p64)); /* ADD: the number of all-0 bytes we traversed + START offset */

        ham_assert(p[0]);

        for (r = 0;; r++) {
          ham_assert(r < 8);
          if (p[0] & (1 << r)) {
            l += r; /* lowest (last) okay probe location */
            break;
          }
        }

        ham_assert(bm_l <= l);
        ham_assert(size_bits == 1);
        /* found a slot! */
        FreelistStatistics::update(this, entry, f, l, hints);
        return (l);
      }
      // should never get here
    }
  }
}

ham_s32_t
Freelist::locate_sufficient_free_space(FreelistStatistics::Hints *dst,
        FreelistStatistics::GlobalHints *hints, ham_s32_t start_index)
{
  FreelistEntry *entry;

  if (hints->max_rounds == 0
      || get_count() < hints->start_entry + hints->page_span_width) {
    /* it's the end of the road for this one */
    return (-1);
  }

  ham_assert(hints->max_rounds <= get_count());

  for (; ; hints->max_rounds--) {
    if (hints->max_rounds == 0) {
      /* it's the end of the road for this one */
      return (-1);
    }

    /*
     * 'regular' modes: does this freelist entry have enough
     * free blocks to satisfy the request?
     *
     * Here we start looking for free space in the _oldest_
     * pages, so this classic system has the drawback of
     * increased 'risk' of finding free space near the START of
     * the file; given some pathological use cases, this means
     * we'll be scanning all/many freelist pages in about 50% fo
     * the searches (2 inserts, one delete at start, rinse & repeat -->
     * 1 insert at start + 1 insert at end),
     * resulting in a lot of page cache thrashing as the inserts
     * jump up and down the database; we can't help improve the
     * delete/erase operations in such cases, but we /can/ try
     * to keep the inserts close together.
     *
     * For that, you might be better served with the
     * convervative style of SEQUENTIAL above, as it will scan
     * freelist pages in reverse order.
     */
    if (hints->skip_init_offset) {
      start_index += hints->skip_init_offset;
      ham_assert(start_index >= 0);
      /* only apply the init_offset at the first increment
       * cycle to break repetitiveness */
      hints->skip_init_offset = 0;
    }
    else {
      start_index += hints->skip_step;
      ham_assert(start_index >= 0);
    }

    /*
     * We don't have to be a very good SRNG here, so the 32-bit
     * int wrap around and the case where the result lands below
     * the 'start_index' limit are resolved in an (overly) simple
     * way:
     */
    start_index %= (get_count() - hints->start_entry
        - hints->page_span_width + 1);
    start_index += hints->start_entry;

    ham_assert(start_index < (ham_s32_t)get_count());
    ham_assert(start_index >= (ham_s32_t)hints->start_entry);
    entry = &m_entries[start_index];

    ham_assert(entry->free_bits <= entry->max_bits);

    /*
     * the regular check: no way if there's not enough in there, lump sum
     */
    if (hints->page_span_width > 1) {
      /*
       * handle this a little differently for 'huge blobs' which span
       * multiple freelist entries: there, we'll be looking at _at
       * least_ SPAN-2 'fully free' freelist entries,
       * that is: left edge (freelist entry), right edge entry and
       * zero or more 'full sized freelist entries' in between.
       *
       * Checking for these 'completely free' entries is much easier
       * (and faster) than plodding through their free bits to see
       * whether the requested number of free bits may be available.
       *
       * To keep it simple, we only check the first freelist entry
       * here and leave the rest to the outer search/alloc routine.
       *
       * NOTE: we 'shortcut' the SPAN-2 theoretical layout by aligning
       * such EXTREMELY HUGE BLOBS on a /freelist entry/ size boundary,
       * i.e. we consider such blobs to start at a fully free freelist
       * entry; consequently (thanks to this alignment, introduced
       * as a search optimization) such blobs take up SPAN-1 freelist
       * entries: no left edge, SPAN-1 full entries, right edge (i.e.
       * partial) freelist entry.
       *
       * This shortcut has a side effect: these extremely huge blobs
       * make the database storage space grow faster than absolutely
       * necessary when space efficiency would've been a prime concern:
       * as we 'align' such blobs to a freelist entry, we have a
       * worst-case fill rate of slighty over 50%: 1span+1chunk wide
       * blobs will 'span' 2 entries and is the smallest 'huge blob'
       * which will trigger this shortcut, resulting in it being
       * search-aligned to a fully free freelist entry every time,
       * meaning that we'll have a 'left over' of 1 /almost/ fully
       * free freelist entry per 'huge blob' --> fill
       * ratio = (1+.0000000001)/2 > 50%
       */
      if (entry->free_bits != entry->max_bits)
        continue;
    }
    else {
      /*
       * regular requests do not overflow beyond the freelist entry
       * boundary, i.e. must fit in the current freelist entry page
       * in their entirety.
       */
      if (entry->free_bits < hints->size_bits)
        continue;
    }

    /*
     * check our statistics as well: do we have a sufficiently
     * large chunk free in there?
     *
     * While we CANNOT say that we _know_ about the sizes of the
     * free slot zones available within the range first_start ..
     * last_start, we _do_ know how large the very last free chunk
     * is.
     *
     * Next to that, we also have a bit of a hunch about our level
     * of
     * 'utilization' (or 'fragmentation', depending on how you look
     * at it) of this middle range, so we can apply statistical
     * heuristics to this search: how certain do we want to be in
     * getting a hit in this freelist page?
     *
     * In FAST mode, we want to be dang sure indeed, so we simply
     * state that we want our slot taken out of that last chunk we
     * know all about, while the more conservative modes can improve
     * themselves with a bit of guesswork: when we had a lot of
     * FAILing trials, for instance, we might be best served by
     * accepting a little more sparseness in our storage here by
     * neglecting the midrange where free and filled slots mingle,
     * i.e. we SKIP that range then.
     *
     * More conservative, i.e. space saving folk would not have
     * this and demand we scan the lot, starting at the first free
     * bit in there.
     *
     * To aid this selection process, we call our hinter to give us
     * an (optimistic) estimate. Our current mgt_mode will take it
     * from there...
     */
    dst->startpos = 0;
    if (entry->start_address < hints->lower_bound_address) {
      dst->startpos = (ham_u32_t)((hints->lower_bound_address
                              - entry->start_address) / kBlobAlignment);
    }
    dst->endpos = entry->max_bits;
    dst->skip_distance = hints->size_bits;
    dst->mgt_mode = kDamRandomWrite; // hardcoded for now
    dst->aligned = hints->aligned;
    dst->lower_bound_address = hints->lower_bound_address;
    dst->size_bits = hints->size_bits;
    dst->freelist_pagesize_bits = hints->freelist_pagesize_bits;
    dst->page_span_width = hints->page_span_width;

    dst->cost = 1;

    if (hints->page_span_width > 1) {
      /*
       * with multi-entry spanning searches, there's no requirement
       * for per-page hinting, so we don't do it.
       *
       * However, we like our storage to be db page aligned, thank
       * you very much ;-)
       */
      dst->aligned = true;
    }
    else {
      FreelistStatistics::get_entry_hints(this, entry, dst);
      if (dst->startpos + dst->size_bits > dst->endpos)
        /* forget it: not enough space in there anyway! */
        continue;
    }

    /* we've done all we could to pick a good freelist page; now
     * it's up to the caller */
    break;
  }

  /* always count call as ONE round, at least: that's minus 1 for
   * the successful trial outside */
  hints->max_rounds--;

#if defined(HAM_DEBUG)
  ham_assert(start_index >= 0);
  ham_assert(start_index < (ham_s32_t)get_count());
  ham_assert(start_index >= (ham_s32_t)hints->start_entry);
  entry = &m_entries[start_index];
  ham_assert(hints->page_span_width <= 1
        ? entry->free_bits >= hints->size_bits
        : true);
  ham_assert(hints->page_span_width > 1
        ? entry->free_bits == entry->max_bits
        : true);
#endif

  return (start_index);
}

ham_status_t
Freelist::initialize()
{
  FreelistEntry entry = {0};
  PFreelistPayload *fp = m_env->get_freelist_payload();

  ham_assert(!(m_env->get_flags() & HAM_READ_ONLY));
  ham_assert(m_entries.empty());

  /* add the header page to the freelist */
  entry.start_address = m_env->get_pagesize();
  ham_size_t size = m_env->get_usable_pagesize();
  size -= m_env->sizeof_full_header();
  size -= PFreelistPayload::get_bitmap_offset();
  size -= size % sizeof(ham_u64_t);

  ham_assert((size % sizeof(ham_u64_t)) == 0);
  entry.max_bits = (ham_u32_t)(size * 8);
  entry.free_bits = fp->get_free_bits();

  /* initialize the header page, if we have read/write access */
  if (!(m_env->get_flags() & HAM_READ_ONLY)) {
    fp->set_start_address(m_env->get_pagesize());
    ham_assert((size * 8 % sizeof(ham_u64_t)) == 0);
    fp->set_max_bits(size * 8);
  }

  m_entries.push_back(entry);

  /* now load all other freelist pages */
  while (fp->get_overflow()) {
    resize((ham_size_t)m_entries.size() + 1);

    Page *page;
    ham_status_t st = m_env->get_page_manager()->fetch_page(&page, 0,
            fp->get_overflow());
    if (st)
      return (st);

    fp = PFreelistPayload::from_page(page);
    FreelistEntry *pentry = &m_entries[m_entries.size() - 1];
    ham_assert(pentry->start_address == fp->get_start_address());
    pentry->free_bits = fp->get_free_bits();
    pentry->pageid = page->get_address();
  }

  return (0);
}

FreelistEntry *
Freelist::get_entry_for_address(ham_u64_t address)
{
  ham_size_t i;

  while (true) {
    for (i = 0; i < m_entries.size(); i++) {
      FreelistEntry *entry = &m_entries[i];

      ham_assert(address >= entry->start_address);

      if (address >= entry->start_address
          && address < entry->start_address + entry->max_bits * kBlobAlignment)
        return (entry);
    }

    /*
     * not found? resize the table; we can guestimate where
     * 'address' is going to land within the freelist...
     */
    ham_assert(i == m_entries.size());
    ham_size_t add = (ham_size_t)(address - m_entries[i - 1].start_address
              - m_entries[i - 1].max_bits);
    add += kBlobAlignment - 1;
    add /= kBlobAlignment;

    ham_size_t single_size_bits = get_entry_maxspan();

    add += single_size_bits - 1;
    add /= single_size_bits;
    ham_assert(add >= 1);
    resize(i + add);
    ham_assert(i < m_entries.size());
  }
}

ham_size_t
Freelist::get_entry_maxspan()
{
  ham_size_t size = m_env->get_usable_pagesize()
          - PFreelistPayload::get_bitmap_offset();
  ham_assert((size % sizeof(ham_u64_t)) == 0);
  size -= size % sizeof(ham_u64_t);

  /* the multiplication is very important for pre-v1.1.0 databases as those
   * have an integer overflow issue right here. */
  return (ham_size_t)(size * 8);
}

void
Freelist::resize(ham_size_t new_count)
{
  ham_size_t size_bits = get_entry_maxspan();
  ham_assert(((size_bits / 8) % sizeof(ham_u64_t)) == 0);

  ham_assert(new_count > m_entries.size());

  for (std::size_t i = m_entries.size(); i < new_count; i++) {
    FreelistEntry entry = {0};

    FreelistEntry *prev = &m_entries[m_entries.size() - 1];

    entry.start_address = prev->start_address + prev->max_bits * kBlobAlignment;
    entry.max_bits = (ham_u32_t)size_bits;

    m_entries.push_back(entry);
  }
}

ham_status_t
Freelist::alloc_freelist_page(Page **ppage, FreelistEntry *entry)
{
  FreelistEntry *entries = &m_entries[0];
  Page *page = 0;
  PFreelistPayload *fp;
  ham_size_t size_bits = get_entry_maxspan();

  ham_assert(((size_bits / 8) % sizeof(ham_u64_t)) == 0);

  *ppage = 0;

  if (m_entries.empty()) {
    ham_status_t st = initialize();
    if (st)
      return (st);
  }

  /*
   * it's not enough just to allocate the page - we have to make sure
   * that the freelist pages build a linked list; therefore we
   * might have to allocate more than just one page...
   *
   * we can skip the first element - it's the root page and always exists
   */
  for (ham_size_t i = 1; ; i++) {
    ham_assert(i < m_entries.size());

    if (!entries[i].pageid) {
      ham_status_t st;
      Page *prev_page = 0;

      /*
       * load the previous page and the payload object;
       * make the page dirty.
       */
      if (i == 1) {
        fp = m_env->get_freelist_payload();
        mark_dirty(0);
      }
      else {
        st = m_env->get_page_manager()->fetch_page(&prev_page, 0,
                entries[i - 1].pageid);
        if (st)
          return (st);
        // mark previous page as dirty
        mark_dirty(prev_page);
        fp = PFreelistPayload::from_page(prev_page);
      }

      /* allocate a new page, fix the linked list */
      st = m_env->get_page_manager()->alloc_page(&page, 0, Page::kTypeFreelist,
                    PageManager::kIgnoreFreelist | PageManager::kClearWithZero);
      if (!page) {
        ham_assert(st != 0);
        return (st);
      }
      ham_assert(st == 0);

      // set the link to the next page
      fp->set_overflow(page->get_address());

      // done editing /previous/ freelist page
      fp = PFreelistPayload::from_page(page);
      fp->set_start_address(m_entries[i].start_address);
      fp->set_max_bits(size_bits);

      // mark page as dirty
      mark_dirty(page);
      ham_assert(entries[i].max_bits == fp->get_max_bits());
      m_entries[i].pageid = page->get_address();
    }

    if (&entries[i] == entry) {
      *ppage = page;
      return (0);
    }
  }
}

ham_size_t
Freelist::set_bits(FreelistEntry *entry, PFreelistPayload *fp,
            ham_size_t start_bit, ham_size_t size_bits,
            bool set, FreelistStatistics::Hints *hints)
{
  ham_size_t i;
  ham_u8_t *p = fp->get_bitmap();

  ham_size_t qw_offset = start_bit & (64 - 1);
  ham_size_t qw_start = (start_bit + 64 - 1) >> 6;   /* ROUNDUP(S DIV 64) */
  ham_size_t qw_end;

  ham_assert(start_bit < fp->get_max_bits());

  if (start_bit + size_bits > fp->get_max_bits())
    size_bits = fp->get_max_bits() - start_bit;

  qw_end = (start_bit + size_bits) >> 6;  /* one past the last full QWORD */

  if (hints)
    FreelistStatistics::edit(this, entry, fp, start_bit, size_bits,
            set, hints);

  /* Set the bits to '1' */
  if (set) {
    if (qw_end <= qw_start) {
      for (i = 0; i < size_bits; i++, start_bit++)
        p[start_bit >> 3] |= 1 << (start_bit & (8 - 1));
    }
    else {
      ham_size_t n = size_bits;
      ham_u64_t *p64 = (ham_u64_t *)fp->get_bitmap();
      p64 += qw_start;

      if (qw_offset) {
        p = (ham_u8_t *)&p64[-1];

        for (i = qw_offset; i < 64; i++)
          p[i >> 3] |= 1 << (i & (8 - 1));

        n -= 64 - qw_offset;
      }

      qw_end -= qw_start;
      for (i = 0; i < qw_end; i++)
        p64[i] = 0xFFFFFFFFFFFFFFFFULL;

      p = (ham_u8_t *)&p64[qw_end];

      n -= qw_end << 6;

      for (i = 0; i < n; i++)
        p[i >> 3] |= 1 << (i & (8 - 1));
    }
  }
  /* Or set the bits to '0'... */
  else {
    if (qw_end <= qw_start) {
      for (i = 0; i < size_bits; i++, start_bit++)
        p[start_bit >> 3] &= ~(1 << (start_bit & (8 - 1)));
    }
    else {
      ham_size_t n = size_bits;
      ham_u64_t *p64 = (ham_u64_t *)fp->get_bitmap();
      p64 += qw_start;

      if (qw_offset) {
        p = (ham_u8_t *)&p64[-1];

        for (i = qw_offset; i < 64; i++)
          p[i >> 3] &= ~(1 << (i & (8 - 1)));

        n -= 64 - qw_offset;
      }

      qw_end -= qw_start;
      for (i = 0; i < qw_end; i++)
        p64[i] = 0;

      p = (ham_u8_t *)&p64[qw_end];

      n -= qw_end << 6;

      for (i = 0; i < n; i++)
        p[i >> 3] &= ~(1 << (i & (8 - 1)));
    }
  }

  return (size_bits);
}

ham_size_t
Freelist::check_bits(FreelistEntry *entry, PFreelistPayload *fp,
            ham_size_t start_bit, ham_size_t size_bits)
{
  ham_size_t i;
  ham_u8_t *p = fp->get_bitmap();

  ham_size_t qw_offset = start_bit & (64 - 1);
  ham_size_t qw_start = (start_bit + 64 - 1) >> 6;   /* ROUNDUP(S DIV 64) */
  ham_size_t qw_end;

  ham_assert(start_bit < fp->get_max_bits());

  if (start_bit + size_bits > fp->get_max_bits())
    size_bits = fp->get_max_bits() - start_bit;

  qw_end = (start_bit + size_bits) >> 6;  /* one past the last full QWORD */

  /* check the bits */
  if (qw_end <= qw_start) {
    for (i = 0; i < size_bits; i++, start_bit++) {
      if (!(p[start_bit >> 3] & (1 << (start_bit & (8 - 1)))))
         return (-1);
    }
  }
  else {
    ham_size_t n = size_bits;
    ham_u64_t *p64 = (ham_u64_t *)fp->get_bitmap();
    p64 += qw_start;

    if (qw_offset) {
      p = (ham_u8_t *)&p64[-1];

      for (i = qw_offset; i < 64; i++) {
        if (!(p[i >> 3] & (1 << (i & (8 - 1)))))
          return (-1);
      }
  
      n -= 64 - qw_offset;
    }

    qw_end -= qw_start;
    for (i = 0; i < qw_end; i++) {
      if (p64[i] != 0xFFFFFFFFFFFFFFFFULL)
        return (-1);
    }

    p = (ham_u8_t *)&p64[qw_end];

    n -= qw_end << 6;

    for (i = 0; i < n; i++) {
      if (!(p[i >> 3] & (1 << (i & (8 - 1)))))
        return (-1);
    }
  }

  return (size_bits);
}

void
Freelist::mark_dirty(Page *page)
{
  if (!page)
    page = m_env->get_header_page();

  page->set_dirty(true);
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);
}

void
Freelist::get_metrics(ham_env_metrics_t *metrics) const
{
  metrics->freelist_hits = m_count_hits;
  metrics->freelist_misses = m_count_misses;
}

} // namespace hamsterdb
