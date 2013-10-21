/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

//
// freelist statistics structures, functions and macros
//

#ifndef HAM_FREELIST_STATISTICS_H__
#define HAM_FREELIST_STATISTICS_H__

namespace hamsterdb {

class Freelist;
class PFreelistPayload;

/*
 * As we [can] support record sizes up to 4Gb, at least theoretically,
 * we can express this size range as a spanning aligned size range:
 * 1..N, where N = log2(4Gb) - log2(alignment). As we happen to know
 * alignment == 32, at least for all regular hamsterdb builds, our
 * biggest power-of-2 for the freelist slot count ~ 32-5 = 27, where 0
 * represents slot size = 1 alignment, 1 represents size of 2 * 32,
 * 2 ~ 4 * 32, and so on.
 *
 * EDIT:
 * In order to cut down on statistics management cost due to overhead
 * caused by having to keep up with the latest for VERY large sizes, we
 * cut this number down to support sizes up to a maximum size of 64Kb ~
 * 2^16, meaning any requests for more than 64Kb/CHUNKSIZE bytes is
 * sharing their statistics.
 *
 */
#define HAM_FREELIST_SLOT_SPREAD   (16-5+1) // 1 chunk .. 2^(SPREAD-1) chunks

//
// global freelist algorithm specific run-time info
//
struct GlobalStatistics
{
  GlobalStatistics() {
    memset(this, 0, sizeof(*this));
  }

  ham_u32_t first_page_with_free_space[HAM_FREELIST_SLOT_SPREAD];
};


#include "packstart.h"

//
// freelist statistics as they are persisted on disk.
//
// Stats are kept with each freelist entry record, but we also keep
// some derived data in the nonpermanent space with each freelist:
// it's not required to keep a freelist page in cache just so the
// statistics + our operational mode combined can tell us it's a waste
// of time to go there.
//
typedef HAM_PACK_0 class HAM_PACK_1 PFreelistPageStatistics
{
  public:
    //
    // k-way statistics which stores requested space slot size related data.
    //
    // The data is stored in @ref HAM_FREELIST_SLOT_SPREAD different buckets
    // which partition the statistical info across the entire space request
    // range by using a logarithmic partitioning function.
    //
    // That way, very accurate, independent statistics can be stores for both
    // small, medium and large sized space requests, so that the freelist hinter
    // can deliver a high quality search hint for various requests.
    //
    // Currently we only keep track of the VERY first free slot index.
    //
    // Retrieves the start bit of slot |i|
    ham_u32_t get_first_start(int i) const {
      return (ham_db2h32(m_first_start[i]));
    }

    // Sets the start bit of slot |i|
    void set_first_start(int i, ham_u32_t start) {
      m_first_start[i] = ham_h2db32(start);
    }

    // (bit) offset which tells us which free slot is the EVER LAST
    // created one; after all, freelistpage:maxbits is a scandalously
    // optimistic lie: all it tells us is how large the freelist page
    // _itself_ can grow, NOT how many free slots we actually have
    // _alive_ in there.
    //
    // 0: special case, meaning: not yet initialized...
    //
    // Retrieves the start offset
    ham_u32_t get_last_start() const {
      return (ham_db2h32(m_last_start));
    }

    // Sets the start offset
    void set_last_start(ham_u32_t start) {
      m_last_start = ham_h2db32(start);
    }

    // total number of available bits in the page ~ all the chunks which
    // actually represent a chunk in the DB storage space.
    //
    // (Note that a freelist can be larger (_max_bits) than the actual
    // number of storage pages currently sitting in the database file.)
    //
    // The number of chunks already in use in the database therefore ~
    // persisted_bits - _allocated_bits.
    //
    // Retrieves the number of persisted bits
    ham_u64_t get_persisted_bits() const {
      return (ham_db2h64(m_persisted_bits));
    }

    // Sets the number of persisted bits
    void set_persisted_bits(ham_u64_t bits) {
      m_persisted_bits = ham_h2db64(bits);
    }

  private:
    ham_u64_t m_persisted_bits;
    ham_u32_t m_first_start[HAM_FREELIST_SLOT_SPREAD];
    ham_u32_t m_last_start;

} HAM_PACK_2 PFreelistPageStatistics;

#include "packstop.h"


//
// freelist algorithm specific run-time info per freelist entry (page)
//
typedef PFreelistPageStatistics RuntimePageStatistics;

struct FreelistEntry;

class FreelistStatistics {
  public:
  struct Hints {
    // [in/out] INCLUSIVE bound: where free slots start
    ham_u32_t startpos;

    // [in/out] EXCLUSIVE bound: where free slots end
    ham_u32_t endpos;

    // [in/out] suggested search/skip probe distance
    ham_u32_t skip_distance;

    // [in/out] suggested DAM mgt_mode for the remainder of this request
    ham_u16_t mgt_mode;

    // [input] whether or not we are looking for aligned storage
    bool aligned;

    // [input] the lower bound address of the slot we're looking for. Usually
    // zero(0).
    ham_u64_t lower_bound_address;

    // [input] the size of the slot we're looking for
    ham_size_t size_bits;

    // [input] the size of a freelist page (in chunks)
    ham_size_t freelist_pagesize_bits;

    // [input] the number of (rounded up) pages we need to fulfill the
    // request;
    // 1 for 'regular' (non-huge) requests.
    // Cannot be 0, as that is only correct for a zero-length request.
    ham_size_t page_span_width;

    // [feedback] cost tracking for our statistics
    ham_size_t cost;
  };

  struct GlobalHints {
    // INCLUSIVE bound: at which freelist page entry to start looking
    ham_u32_t start_entry;

    // [in/out] how many entries to skip
    //
    // You'd expect this to be 1 all the time, but in some modes it is
    // expected that a 'semi-random' scan will yield better results;
    // especially when we combine that approach with a limited number of
    // rounds before we switch to SEQUENTIAL+FAST mode.
    //
    // By varying the offset (start) for each operation we then are
    // assured that all freelist pages will be perused once in a while,
    // while we still cut down on freelist entry scanning quite a bit.
    ham_u32_t skip_step;

    // [in/out] and the accompanying start offset for the SRNG
    ham_u32_t skip_init_offset;

    // [in/out] upper bound on number of rounds ~ entries to scan: when
    // to stop looking
    ham_u32_t max_rounds;

    // [in/out] suggested DAM mgt_mode for the remainder of this request
    ham_u16_t mgt_mode;

    // [output] whether or not we are looking for a chunk of storage
    // spanning multiple pages ('huge blobs'): lists the number
    // of (rounded up) pages we need to fulfill the request; 1 for
    // 'regular' (non-huge) requests.
    // Cannot be 0, as that is only correct for a zero-length request.
    ham_size_t page_span_width;

    // [input] whether or not we are looking for aligned storage
    bool aligned;

    // [input] the lower bound address of the slot we're looking for.
    // Usually zero(0).
    ham_u64_t lower_bound_address;

    // [input] the size of the slot we're looking for
    ham_size_t size_bits;

    // [input] the size of a freelist page (in chunks)
    ham_size_t freelist_pagesize_bits;
  };

  static void globalhints_no_hit(Freelist *fl, FreelistEntry *entry,
                FreelistStatistics::Hints *hints);

  static void edit(Freelist *fl, FreelistEntry *entry,
                PFreelistPayload *f, ham_u32_t position,
                ham_size_t size_bits, bool free_these,
                FreelistStatistics::Hints *hints);

  static void fail(Freelist *fl, FreelistEntry *entry,
                PFreelistPayload *f, FreelistStatistics::Hints *hints);

  static void update(Freelist *fl, FreelistEntry *entry,
                PFreelistPayload *f, ham_u32_t position,
                FreelistStatistics::Hints *hints);

  static void get_entry_hints(Freelist *fl, FreelistEntry *entry,
                FreelistStatistics::Hints *dst);

  static void get_global_hints(Freelist *fl,
                FreelistStatistics::GlobalHints *dst);
};

} // namespace hamsterdb

#endif /* HAM_FREELIST_STATISTICS_H__ */
