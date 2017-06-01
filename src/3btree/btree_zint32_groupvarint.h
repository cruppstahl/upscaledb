/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * Compressed 32bit integer keys
 */

#ifndef UPS_BTREE_KEYS_GROUPVARINT_H
#define UPS_BTREE_KEYS_GROUPVARINT_H

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_zint32_block.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

#ifdef __GNUC__
#  define FORCE_INLINE __attribute__((always_inline))
#else
#  define FORCE_INLINE
#endif

static const uint32_t varintgb_mask[4] = { 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF };

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 GroupVarintIndex : IndexBase {
  enum {
    // Initial size of a new block
    kInitialBlockSize = 16, // TODO verify!

    // Maximum keys per block
    kMaxKeysPerBlock = 256 + 1,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint8_t *data, uint32_t block_size) {
    IndexBase::initialize(offset, data, block_size);
    _block_size = block_size;
    _used_size = 0;
    _key_count = 0;
  }

  // returns the used size of the block
  uint32_t used_size() const {
    return _used_size;
  }

  // sets the used size of the block
  void set_used_size(uint32_t size) {
    _used_size = size;
  }

  // returns the total block size
  uint32_t block_size() const {
    return _block_size;
  }

  // sets the total block size
  void set_block_size(uint32_t size) {
    _block_size = size;
  }

  // returns the key count
  uint32_t key_count() const {
    return _key_count;
  }

  // sets the key count
  void set_key_count(uint32_t key_count) {
    _key_count = key_count;
  }

  // copies this block to the |dest| block
  void copy_to(const uint8_t *block_data, GroupVarintIndex *dest,
                  uint8_t *dest_data) {
    dest->set_value(value());
    dest->set_key_count(key_count());
    dest->set_used_size(used_size());
    dest->set_highest(highest());
    ::memcpy(dest_data, block_data, block_size());
  }

  // the total size of this block
  unsigned int _block_size : 11;

  // used size of this block
  unsigned int _used_size : 11;

  // the number of keys in this block
  unsigned int _key_count : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

static uint8_t group_size[] = {
         4,  5,  6,  7,  5,  6,  7,  8,  6,  7,  8,  9,  7,  8,  9, 10,
         5,  6,  7,  8,  6,  7,  8,  9,  7,  8,  9, 10,  8,  9, 10, 11,
         6,  7,  8,  9,  7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12,
         7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13,
         5,  6,  7,  8,  6,  7,  8,  9,  7,  8,  9, 10,  8,  9, 10, 11,
         6,  7,  8,  9,  7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12,
         7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13,
         8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14,
         6,  7,  8,  9,  7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12,
         7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13,
         8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14,
         9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15,
         7,  8,  9, 10,  8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13,
         8,  9, 10, 11,  9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14,
         9, 10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15,
        10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15, 13, 14, 15, 16
        };

struct GroupVarintCodecImpl : BlockCodecBase<GroupVarintIndex> {
  enum {
    kHasCompressApi = 1,
    kHasSelectApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasInsertApi = 1,
    kHasAppendApi = 1,
  };

  static uint32_t compress_block(GroupVarintIndex *index, const uint32_t *in,
                  uint32_t *out) {
    assert(index->key_count() > 0);
    return (uint32_t)encodeArray(index->value(), in,
                            (size_t)index->key_count() - 1, out);
  }

  static uint32_t *uncompress_block(GroupVarintIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    size_t nvalue = index->key_count() - 1;
    assert(nvalue > 0);
    decodeArray(index->value(), block_data, (size_t)index->used_size(),
                      out, nvalue);
    return out;
  }

  static bool append(GroupVarintIndex *index, uint32_t *in,
                  uint32_t key, int *pslot) {
    uint32_t count = index->key_count() - 1;

    key -= index->highest();

    uint8_t *keyp;
    uint8_t *bout = reinterpret_cast<uint8_t *> (in);
    uint8_t *bend = bout + index->used_size();
    int shift;

    // Append a new group of varints, or fast-forward to the last block?
    if (count % 4 != 0) {
      uint32_t size = 0;
      do {
        bout += size;
        size = 1 + group_size[*bout];
      } while (bout + size < bend);
  
      keyp = bout;
      bout = bend;
      shift = (count % 4) * 2;
    }
    else {
      keyp = (uint8_t *)in + index->used_size();
      bout = keyp + 1;
      *keyp = 0;
      shift = 0;
    }

    if (key < (1U << 8)) {
      *bout++ = static_cast<uint8_t> (key);
    }
    else if (key < (1U << 16)) {
      *bout++ = static_cast<uint8_t> (key);
      *bout++ = static_cast<uint8_t> (key >> 8);
      *keyp |= (1 << shift);
    }
    else if (key < (1U << 24)) {
      *bout++ = static_cast<uint8_t> (key);
      *bout++ = static_cast<uint8_t> (key >> 8);
      *bout++ = static_cast<uint8_t> (key >> 16);
      *keyp |= (2 << shift);
    }
    else {
      // the compiler will do the right thing
      *reinterpret_cast<uint32_t *> (bout) = key;
      bout += 4;
      *keyp |= (3 << shift);
    }

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(index->used_size() + (bout - bend));
    *pslot += index->key_count() - 1;
    return true;
  }

  static bool insert(GroupVarintIndex *index, uint32_t *in, uint32_t key,
                  int *pslot) {
    uint32_t initial = index->value();
    int slot = 0;

    uint32_t out[GroupVarintIndex::kMaxKeysPerBlock];

    // if index->value is replaced then the whole block has to be decompressed.
    if (key < initial) {
      if (unlikely(index->key_count() > 1)) {
        uncompress_block(index, in, out);
        std::memmove(out + 1, out, sizeof(uint32_t) * (index->key_count() - 1));
      }
      out[0] = initial;
      index->set_value(key);
      index->set_key_count(index->key_count() + 1);
      index->set_used_size((uint32_t)encodeArray(index->value(), out,
                                (size_t)index->key_count() - 1, in));
      *pslot = 1;
      return true;
    }

    // skip as many groups as possible
    uint8_t *inbyte = reinterpret_cast<uint8_t *> (in);
    const uint8_t *const endbyte = inbyte + index->used_size();
    uint8_t *new_inbyte = inbyte;
    uint32_t new_initial = index->value();
    uint32_t remaining = index->key_count() - 1;

    uint32_t *pout = &out[0];
    bool is_inserted = false;

    while (endbyte > inbyte + 1 + 4 * 4) {
      uint32_t next_initial = initial;
      const uint8_t *next = decodeGroupVarIntDelta(inbyte, &next_initial, pout);

      remaining -= 4;

      // skip this group? then immediately proceed to the next one
      if (key > out[3]) {
        inbyte = (uint8_t *)next;
        initial = next_initial;
        slot += 4;
        continue;
      }

      if (is_inserted == false) {
        new_initial = initial;
        new_inbyte = inbyte;
        initial = next_initial;

        // check for duplicates
        if (key == pout[0]) {
          *pslot = slot + 1;
          return (false);
        }
        if (key == pout[1]) {
          *pslot = slot + 2;
          return (false);
        }
        if (key == pout[2]) {
          *pslot = slot + 3;
          return (false);
        }
        if (key == pout[3]) {
          *pslot = slot + 4;
          return (false);
        }

        // insert the new key
        if (key < pout[0]) {
          std::memmove(&pout[1], &pout[0], 4 * sizeof(uint32_t));
          pout[0] = key;
          *pslot = slot + 1;
        }
        else if (key < pout[1]) {
          std::memmove(&pout[2], &pout[1], 3 * sizeof(uint32_t));
          pout[1] = key;
          *pslot = slot + 2;
        }
        else if (key < pout[2]) {
          std::memmove(&pout[3], &pout[2], 2 * sizeof(uint32_t));
          pout[2] = key;
          *pslot = slot + 3;
        }
        else { // if (key < pout[3])
          pout[4] = pout[3];
          pout[3] = key;
          *pslot = slot + 4;
        }

        is_inserted = true;
        pout += 5; // 4 decoded integers, 1 new key
      }
      else {
        pout += 4;
        slot += 4;
        initial = next_initial;
      }

      inbyte = (uint8_t *)next;
    }

    // from here on all remaining keys will be decoded and re-encoded
    if (is_inserted == false) {
      new_initial = initial;
      new_inbyte = inbyte;
    }

    // continue with the remaining deltas and insert the key if it was not
    // yet inserted
    while (endbyte > inbyte && remaining > 0) {
      uint32_t ints_decoded = remaining;
      inbyte = (uint8_t *)decodeSingleVarintDelta(inbyte, &initial,
                      &pout, &ints_decoded);
      // decodeSingleVarintDelta() increments pout; set it back to the previous
      // position
      pout -= ints_decoded;
      remaining -= ints_decoded;
      assert(inbyte <= endbyte);

      // check if the key already exists; if yes then return false.
      // if not then insert the key, or append it to the list of
      // decoded values 
      if (is_inserted == false) {
        if (key == pout[0])
          return false;
        if (key < pout[0]) {
          std::memmove(&pout[1], &pout[0], ints_decoded * sizeof(uint32_t));
          pout[0] = key;
          *pslot = slot + 1;
          is_inserted = true;
        }
        else if (ints_decoded > 1) {
          if (key == pout[1])
            return false;
          if (key < pout[1]) {
            std::memmove(&pout[2], &pout[1], (ints_decoded - 1) * sizeof(uint32_t));
            pout[1] = key;
            *pslot = slot + 2;
            is_inserted = true;
          }
          else if (ints_decoded > 2) {
            if (key == pout[2])
              return false;
            if (key < pout[2]) {
              std::memmove(&pout[3], &pout[2], (ints_decoded - 2) * sizeof(uint32_t));
              pout[2] = key;
              *pslot = slot + 3;
              is_inserted = true;
            }
            else if (ints_decoded > 3) {
              if (key == pout[3])
                return false;
              if (key < pout[3]) {
                pout[4] = pout[3];
                pout[3] = key;
                *pslot = slot + 4;
                is_inserted = true;
              }
            }
          }
        }
        if (is_inserted)
          pout += ints_decoded + 1;
        else {
          pout += ints_decoded;
          slot += ints_decoded;
        }
      }
      else {
        // is_inserted == true
        pout += ints_decoded;
      }
    }

    // otherwise append the key
    if (is_inserted == false) {
      *pslot = 1 + slot;
      *pout = key;
      pout++;
    }

    // now re-encode the decoded values. The encoded values are written
    // to |new_inbyte|, with |new_initial| as the initial value for the
    // delta calculation.
    size_t ints_to_write = pout - &out[0];
    uint32_t written = encodeArray(new_initial, &out[0], ints_to_write,
                    (uint32_t *)new_inbyte);
    index->set_key_count(index->key_count() + 1);
    index->set_used_size((uint32_t)(new_inbyte - (uint8_t *)in) + written);
    return true;
  }

  static int find_lower_bound(GroupVarintIndex *index, const uint32_t *in,
                  uint32_t key, uint32_t *presult) {
    const uint8_t *inbyte = reinterpret_cast<const uint8_t *> (in);
    const uint8_t *const endbyte = inbyte + index->used_size();
    uint32_t out[4];
    int i = 0;
    uint32_t initial = index->value();
    uint32_t nvalue = index->key_count() - 1;

    while (endbyte > inbyte + 1 + 4 * 4) {
      inbyte = decodeGroupVarIntDelta(inbyte, &initial, out);
      if (key <= out[3]) {
        if (key <= out[0]) {
          *presult = out[0];
          return i + 0;
        }
        if (key <= out[1]) {
          *presult = out[1];
          return i + 1;
        }
        if (key <= out[2]) {
          *presult = out[2];
          return i + 2;
        }
        *presult = out[3];
        return i + 3;
      }
      i += 4;
    }

    while (endbyte > inbyte && nvalue > 0) {
      uint32_t *p = &out[0];
      nvalue = index->key_count() - 1 - i;
      inbyte = decodeSingleVarintDelta(inbyte, &initial, &p, &nvalue);
      assert(inbyte <= endbyte);
      if (key <= out[0]) {
        *presult = out[0];
        return i + 0;
      }
      if (nvalue > 0 && key <= out[1]) {
        *presult = out[1];
        return i + 1;
      }
      if (nvalue > 1 && key <= out[2]) {
        *presult = out[2];
        return i + 2;
      }
      if (nvalue > 2 && key <= out[3]) {
        *presult = out[3];
        return i + 3;
      }
      i += nvalue;
    }
    *presult = key + 1;
    return i;
  }

  // Returns a decompressed value
  static uint32_t select(GroupVarintIndex *index, uint32_t *in, int slot) {
    const uint8_t *inbyte = reinterpret_cast<const uint8_t *> (in);
    uint32_t out[4] = {0};
    uint32_t initial = index->value();
    uint32_t nvalue = index->key_count() - 1;
    int i = 0;

    if (slot + 3 < (int)nvalue) {
      while (true) {
        while (i + 4 <= slot) {
          inbyte = scanGroupVarIntDelta(inbyte, &initial);
          i += 4;
        }
        inbyte = decodeGroupVarIntDelta(inbyte, &initial, out);
        return out[slot - i];
      }
    } // else

    // we finish with the uncommon case
    while (i + 3 < slot) { // a single branch will do for this case (bulk of the computation)
      inbyte = scanGroupVarIntDelta(inbyte, &initial);
      i += 4;
    }
    // lots of branching ahead...
    while (i + 3 < (int)nvalue) {
      inbyte = decodeGroupVarIntDelta(inbyte, &initial, out);
      i += 4;
      if (unlikely(i > slot))
        return out[slot - (i - 4)];
    }
    {
      nvalue = nvalue - i;
      inbyte = decodeCarefully(inbyte, &initial, out, &nvalue);
      if (slot == i)
        return out[0];
      if (nvalue > 1 && slot == i + 1)
        return out[1];
      if (nvalue > 2 && slot == i + 2)
        return out[2];
      if (nvalue > 3 && slot == i + 3)
        return out[3];
    }
    assert(false); // we should never get here
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static uint32_t estimate_required_size(GroupVarintIndex *index,
                        uint8_t *block_data, uint32_t key) {
    // always add one additional byte for the index
    if (key < (1U << 8))
      return index->used_size() + 2;
    if (key < (1U << 16)) 
      return index->used_size() + 3;
    if (key < (1U << 24))
      return index->used_size() + 4;
    return index->used_size() + 5;
  }

  static const uint8_t *scanGroupVarIntDelta(const uint8_t *in, uint32_t *val) {
    const uint32_t sel = *in++;
    if (sel == 0) {
      *val += static_cast<uint32_t>(in[0]);
      *val += static_cast<uint32_t>(in[1]);
      *val += static_cast<uint32_t>(in[2]);
      *val += static_cast<uint32_t>(in[3]);
      return in + 4;
    }
    const uint32_t sel1 = (sel & 3);
    *val += *(reinterpret_cast<const uint32_t*>(in)) & varintgb_mask[sel1];
    in += sel1 + 1;
    const uint32_t sel2 = ((sel >> 2) & 3);
    *val += *(reinterpret_cast<const uint32_t*>(in)) & varintgb_mask[sel2];
    in += sel2 + 1;
    const uint32_t sel3 = ((sel >> 4) & 3);
    *val += *(reinterpret_cast<const uint32_t*>(in)) & varintgb_mask[sel3];
    in += sel3 + 1;
    const uint32_t sel4 = (sel >> 6);
    *val += *(reinterpret_cast<const uint32_t*>(in)) & varintgb_mask[sel4];
    in += sel4 + 1;
    return in;
  }

  static size_t encodeArray(uint32_t initial, const uint32_t *in,
                  size_t length, uint32_t *out) {
    uint8_t *bout = reinterpret_cast<uint8_t *> (out);
    const uint8_t *const initbout = reinterpret_cast<uint8_t *> (out);

    size_t k = 0;
    for (; k + 3 < length;) {
      uint8_t *keyp = bout++;
      *keyp = 0;
      for (int j = 0; j < 8; j += 2, ++k) {
        const uint32_t val = in[k] - initial;
        initial = in[k];
        if (val < (1U << 8)) {
          *bout++ = static_cast<uint8_t> (val);
        }
        else if (val < (1U << 16)) {
          *bout++ = static_cast<uint8_t> (val);
          *bout++ = static_cast<uint8_t> (val >> 8);
          *keyp |= static_cast<uint8_t>(1 << j);
        }
        else if (val < (1U << 24)) {
          *bout++ = static_cast<uint8_t> (val);
          *bout++ = static_cast<uint8_t> (val >> 8);
          *bout++ = static_cast<uint8_t> (val >> 16);
          *keyp |= static_cast<uint8_t>(2 << j);
        }
        else {
          // the compiler will do the right thing
          *reinterpret_cast<uint32_t *> (bout) = val;
          bout += 4;
          *keyp |= static_cast<uint8_t>(3 << j);
        }
      }
    }
    if (k < length) {
      uint8_t * keyp = bout++;
      *keyp = 0;
      for (int j = 0; k < length && j < 8; j += 2, ++k) {
        const uint32_t val = in[k] - initial;
        initial = in[k];
        if (val < (1U << 8)) {
          *bout++ = static_cast<uint8_t> (val);
        }
        else if (val < (1U << 16)) {
          *bout++ = static_cast<uint8_t> (val);
          *bout++ = static_cast<uint8_t> (val >> 8);
          *keyp |= static_cast<uint8_t>(1 << j);
        }
        else if (val < (1U << 24)) {
          *bout++ = static_cast<uint8_t> (val);
          *bout++ = static_cast<uint8_t> (val >> 8);
          *bout++ = static_cast<uint8_t> (val >> 16);
          *keyp |= static_cast<uint8_t>(2 << j);
        }
        else {
          // the compiler will do the right thing
          *reinterpret_cast<uint32_t *> (bout) = val;
          bout += 4;
          *keyp |= static_cast<uint8_t>(3 << j);
        }
      }
    }

    return bout - initbout;
  }

  static const uint8_t *decodeCarefully(const uint8_t *inbyte,
                    uint32_t *initial, uint32_t *out, uint32_t *count) {
    uint32_t val;
    uint32_t k, key = *inbyte++;
    for (k = 0; k < *count && k < 4; k++) {
      const uint32_t howmanybyte = key & 3;
      key = static_cast<uint8_t>(key>>2);
      val = static_cast<uint32_t> (*inbyte++);
      if (howmanybyte >= 1) {
        val |= (static_cast<uint32_t> (*inbyte++) << 8) ;
        if (howmanybyte >= 2) {
          val |= (static_cast<uint32_t> (*inbyte++) << 16) ;
          if (howmanybyte >= 3) {
            val |= (static_cast<uint32_t> (*inbyte++) << 24);
          }
        }
      }
      *initial += val;
      *out = *initial;
      out++;
    }
    *count = k;
    return inbyte;
  }

  template <class T>
  static inline bool needPaddingTo32Bits(T value) {
    return value & 3;
  }

  static void decodeArray(uint32_t initial, const uint32_t *in,
                    size_t size, uint32_t *out, size_t nvalue) 
                FORCE_INLINE {
    const uint8_t * inbyte = reinterpret_cast<const uint8_t *> (in);
    const uint8_t * const endbyte = inbyte + size;
    const uint32_t * const endout(out + nvalue);

    while (endbyte > inbyte + 1 + 4 * 4) {
      inbyte = decodeGroupVarIntDelta(inbyte, &initial, out);
      out += 4;
    }
    while (endbyte > inbyte) {
      uint32_t nvalue = endout - out;
      inbyte = decodeSingleVarintDelta(inbyte, &initial, &out, &nvalue);
      assert(inbyte <= endbyte);
    }
  }

  template <class T>
  static T *padTo32bits(T *inbyte) {
    return reinterpret_cast<T *>((reinterpret_cast<uintptr_t>(inbyte)
                                      + 3) & ~3);
  }

  static const uint8_t *decodeGroupVarIntDelta(const uint8_t* in, uint32_t *val,
                  uint32_t* out) FORCE_INLINE {
    const uint32_t sel = *in++;
    if (sel == 0) {
      out[0] = (* val += static_cast<uint32_t> (in[0]));
      out[1] = (* val += static_cast<uint32_t> (in[1]));
      out[2] = (* val += static_cast<uint32_t> (in[2]));
      out[3] = (* val += static_cast<uint32_t> (in[3]));
      return in + 4;
    }
    const uint32_t sel1 = (sel & 3);
    *val += *((uint32_t*)(in)) & varintgb_mask[sel1];
    *out++ = *val;
    in += sel1 + 1;
    const uint32_t sel2 = ((sel >> 2) & 3);
    *val += *((uint32_t*)(in)) & varintgb_mask[sel2];
    *out++ = *val;
    in += sel2 + 1;
    const uint32_t sel3 = ((sel >> 4) & 3);
    *val += *((uint32_t*)(in)) & varintgb_mask[sel3];
    *out++ = *val;
    in += sel3 + 1;
    const uint32_t sel4 = (sel >> 6);
    *val += *((uint32_t*)(in)) & varintgb_mask[sel4];
    *out++ = *val;
    in += sel4 + 1;
    return in;
  }

  static const uint8_t *decodeSingleVarintDelta(const uint8_t *inbyte,
                  uint32_t *initial, uint32_t **out, uint32_t *count)
                  FORCE_INLINE {
    uint32_t val;
    uint32_t k, key = *inbyte++;
    for (k = 0; k < *count && k < 4; k++) {
      const uint32_t howmanybyte = key & 3;
      key = static_cast<uint8_t>(key>>2);
      val = static_cast<uint32_t> (*inbyte++);
      if (howmanybyte >= 1) {
        val |= (static_cast<uint32_t> (*inbyte++) << 8) ;
        if (howmanybyte >= 2) {
          val |= (static_cast<uint32_t> (*inbyte++) << 16) ;
          if (howmanybyte >= 3) {
            val |= (static_cast<uint32_t> (*inbyte++) << 24);
          }
        }
      }
      *initial += val;
      **out = *initial;
      (*out)++;
    }
    *count = k;
    return inbyte;
  }
};

typedef Zint32Codec<GroupVarintIndex, GroupVarintCodecImpl> GroupVarintCodec;

struct GroupVarintKeyList : BlockKeyList<GroupVarintCodec> {
   // Constructor
   GroupVarintKeyList(LocalDb *db, PBtreeNode *node)
     : BlockKeyList<GroupVarintCodec>(db, node) {
   }
};

} // namespace Zint32

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_GROUPVARINT_H
