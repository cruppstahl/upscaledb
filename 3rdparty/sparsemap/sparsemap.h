//
// SparseMap
//
// This is an implementation for a sparse, compressed bitmap. It is resizable
// and mutable, with good performance for random access modifications
// and lookups.
//
// The implementation is separated into tiers.
//
// Tier 0 (lowest): bits are stored in a BitVector (usually a uint64_t).
//
// Tier 1 (middle): multiple BitVectors are managed in a MiniMap. The MiniMap
//    only stores those BitVectors that have a mixed payload of bits (i.e.
//    some bits are 1, some are 0). As soon as ALL bits in a BitVector are
//    identical, this BitVector is no longer stored. (This is the compression
//    aspect.)
//    The MiniMap therefore stores additional flags (2 bit) for each BitVector
//    in an additional word (same size as the BitVector itself).
//    
//     00 11 22 33
//     ^-- descriptor for BitVector 1
//        ^-- descriptor for BitVector 2
//           ^-- descriptor for BitVector 3
//              ^-- descriptor for BitVector 4
//
//    Those flags (*) can have one of the following values:
//
//     00   The BitVector is all zero -> BitVector is not stored
//     11   The BitVector is all one -> BitVector is not stored
//     10   The BitVector contains a bitmap -> BitVector is stored
//     01   The BitVector is not used (**)
//
//    The serialized size of a MiniMap in memory therefore is at least
//    one BitVector for the flags, and (optionally) additional BitVectors
//    if they are required.
//
//    (*) The code comments often use the Erlang format for binary
//    representation, i.e. 2#10 for (binary) 01.
// 
//    (**) This flag is set to reduce the capacity of a MiniMap. hamsterdb
//    does that if a btree node runs out of space.
//
// Tier 2 (highest): the SparseMap manages multiple MiniMaps. Each MiniMap
//    has its own offset (relative to the offset of the SparseMap). In
//    addition, the SparseMap manages the memory of the MiniMap, and
//    is able to grow or shrink that memory as required.
//

#ifndef SPARSEMAP_H__
#define SPARSEMAP_H__

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdexcept>
#include <limits>

#include "popcount.h"


namespace sparsemap {

//
// This helper structure is returned by MiniMap::set()
//
template<typename BitVector>
struct MultiReturn
{
  // the return code - kOk, kNeedsToGrow, kNeedsToShrink
  int code;

  // the position of the BitVector which is inserted/deleted
  int position;

  // the value of the fill word (for growing) 
  BitVector fill;

  // Constructor
  MultiReturn(int _code, int _position, BitVector _fill)
    : code(_code), position(_position), fill(_fill) {
  }
};


//
// The MiniMap is usually not used directly; it is used by the SparseMap
// and can store up to 2048 bits.
//
template<typename BitVector>
class MiniMap {
  public:
    enum {
      // number of bits that can be stored in a BitVector
      kBitsPerVector = sizeof(BitVector) * 8,

      // number of flags that can be stored in a single index byte
      kFlagsPerIndexByte = 4,

      // number of flags that can be stored in the index
      kFlagsPerIndex = sizeof(BitVector) * kFlagsPerIndexByte,

      // maximum capacity of a MiniMap (in bits)
      kMaxCapacity = kBitsPerVector * kFlagsPerIndex,

      // BitVector payload is all zeroes (2#00)
      kPayloadZeroes = 0,

      // BitVector payload is all ones (2#11)
      kPayloadOnes = 3,

      // BitVector payload is mixed (2#10)
      kPayloadMixed = 2,

      // BitVector is not used (2#01)
      kPayloadNone = 1,

      // a mask for checking flags (2 bits)
      kFlagMask = 3,

      // return code for set(): ok, no further action required
      kOk,

      // return code for set(): needs to grow this MiniMap
      kNeedsToGrow,

      // return code for set(): needs to shrink this MiniMap
      kNeedsToShrink
    };

  public:
    // Constructor
    MiniMap(uint8_t *data)
      : m_data((BitVector *)data) {
    }

    // Sets the capacity
    void set_capacity(size_t capacity) {
      if (capacity >= kMaxCapacity)
        return;

      assert(capacity % kBitsPerVector == 0);

      size_t reduced = 0;
      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = sizeof(BitVector) - 1; i >= 0; i--) {
        for (int j = kFlagsPerIndexByte - 1; j >= 0; j--) {
          p[i] &= ~((BitVector)0x03 << (j * 2));
          p[i] |= ((BitVector)0x01 << (j * 2));
          reduced += kBitsPerVector;
          if (capacity + reduced == kMaxCapacity) {
            assert(get_capacity() == capacity);
            return;
          }
        }
      }

      assert(get_capacity() == capacity);
    }

    // Returns the maximum capacity of this MiniMap
    size_t get_capacity() {
      size_t capacity = kMaxCapacity;

      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < sizeof(BitVector); i++, p++) {
        if (!*p)
          continue;
        for (int j = 0; j < kFlagsPerIndexByte; j++) {
          int flags = ((*p) & ((BitVector)kFlagMask << (j * 2))) >> (j * 2);
          if (flags == kPayloadNone)
            capacity -= kBitsPerVector;
        }
      }
      return (capacity);
    }

    // Returns true if this MiniMap is empty
    bool is_empty() const {
      // The MiniMap is empty if all flags (in m_data[0]) are zero.
      if (m_data[0] == 0)
        return (true);

      // It's also empty if all flags are Zero or None
      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < sizeof(BitVector); i++, p++) {
        if (*p) {
          for (int j = 0; j < kFlagsPerIndexByte; j++) {
            int flags = ((*p) & ((BitVector)kFlagMask << (j * 2))) >> (j * 2);
            if (flags != kPayloadNone && flags != kPayloadZeroes)
              return (false);
          }
        }
      }
      return (true);
    }

    // Returns the size of the data buffer, in bytes
    size_t get_size() const {
      // At least one BitVector is required for the flags (m_data[0])
      size_t size = sizeof(BitVector);
      // Use a lookup table for each byte of the flags
      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < sizeof(BitVector); i++, p++)
        size += sizeof(BitVector) * calc_vector_size(*p);

      return (size);
    }

    // Returns the value of a bit at index |idx|
    bool is_set(size_t idx) const {
      // in which BitVector is |idx| stored?
      int bv = idx / kBitsPerVector;
      assert(bv < kFlagsPerIndex);

      // now retrieve the flags of that BitVector
      int flags = ((*m_data) & ((BitVector)kFlagMask << (bv * 2))) >> (bv * 2);
      switch (flags) {
        case kPayloadZeroes:
        case kPayloadNone:
          return (false);
        case kPayloadOnes:
          return (true);
        default:
          assert(flags == kPayloadMixed);
          // fall through
      }

      // get the BitVector at |bv|
      BitVector w = m_data[1 + get_position(bv)];
      // and finally check the bit in that BitVector
      return ((w & ((BitVector)1 << (idx % kBitsPerVector))) > 0);
    }

    // Sets the value of a bit at index |idx|. This function returns
    // a MultiReturn structure. If MultiReturn::code is |kNeedsToGrow|
    // or |kNeedsToShrink| then the caller has to perform the relevant
    // actions and call set() again, this time with |retried| = true!
    MultiReturn<BitVector> set(size_t idx, bool value, bool retried = false) {
      // in which BitVector is |idx| stored?
      int bv = idx / kBitsPerVector;
      assert(bv < kFlagsPerIndex);

      // now retrieve the flags of that BitVector
      int flags = ((*m_data) & ((BitVector)kFlagMask << (bv * 2))) >> (bv * 2);
      assert(flags != kPayloadNone);
      if (flags == kPayloadZeroes) {
        // easy - set bit to 0 in a BitVector of zeroes
        if (value == false)
          return (MultiReturn<BitVector>(kOk, 0, 0));
        // the SparseMap must grow this MiniMap by one additional BitVector,
        // then try again
        if (!retried)
          return (MultiReturn<BitVector>(kNeedsToGrow,
                                  1 + get_position(bv), 0));
        // new flags are 2#10 (currently, flags are set to 2#00
        // 2#00 | 2#10 = 2#10)
        m_data[0] |= ((BitVector)0x2 << (bv * 2));
        // fall through
      }
      else if (flags == kPayloadOnes) {
        // easy - set bit to 1 in a BitVector of ones
        if (value == true)
          return (MultiReturn<BitVector>(kOk, 0, 0));
        // the SparseMap must grow this MiniMap by one additional BitVector,
        // then try again
        if (!retried)
          return (MultiReturn<BitVector>(kNeedsToGrow,
                                  1 + get_position(bv), (BitVector)-1));
        // new flags are 2#10 (currently, flags are set to 2#11;
        // 2#11 ^ 2#01 = 2#10)
        m_data[0] ^= ((BitVector)0x1 << (bv * 2));
        // fall through
      }

      // now flip the bit
      size_t position = 1 + get_position(bv);
      BitVector w = m_data[position];
      if (value)
        w |= (BitVector)1 << (idx % kBitsPerVector);
      else
        w &= ~((BitVector)1 << (idx % kBitsPerVector));

      // if this BitVector is now all zeroes or ones then we can remove it
      if (w == 0) {
        m_data[0] &= ~((BitVector)kPayloadOnes << (bv * 2));
        return (MultiReturn<BitVector>(kNeedsToShrink, position, 0));
      }
      if (w == (BitVector)-1) {
        m_data[0] |= (BitVector)kPayloadOnes << (bv * 2);
        return (MultiReturn<BitVector>(kNeedsToShrink, position, 0));
      }

      m_data[position] = w;
      return (MultiReturn<BitVector>(kOk, 0, 0));
    }

    // Decompresses the whole bitmap; calls visitor's operator() for all bits
    // Returns the number of (set) bits that were passed to the scanner
    template<typename IndexedType, class Scanner>
    size_t scan(IndexedType start, Scanner &scanner, size_t skip) {
      size_t ret = 0;
      register uint8_t *p = (uint8_t *)m_data;
      IndexedType buffer[kBitsPerVector];
      for (size_t i = 0; i < sizeof(BitVector); i++, p++) {
        if (*p == 0) {
          // skip the zeroes
          continue;
        }

        for (int j = 0; j < kFlagsPerIndexByte; j++) {
          int flags = ((*p) & ((BitVector)kFlagMask << (j * 2))) >> (j * 2);
          if (flags == kPayloadNone || flags == kPayloadZeroes) {
            // ignore the zeroes
          }
          else if (flags == kPayloadOnes) {
            if (skip) {
              if (skip >= kBitsPerVector) {
                skip -= kBitsPerVector;
                ret += kBitsPerVector;
                continue;
              }
              size_t n = 0;
              for (size_t b = skip; b < kBitsPerVector; b++)
                buffer[n++] = start + b;
              scanner(&buffer[0], n);
              ret += n;
              skip = 0;
            }
            else {
              for (size_t b = 0; b < kBitsPerVector; b++)
                buffer[b] = start + b;
              scanner(&buffer[0], kBitsPerVector);
              ret += kBitsPerVector;
            }
          }
          else if (flags == kPayloadMixed) {
            BitVector w = m_data[1 + get_position(i * kFlagsPerIndexByte + j)];
            int n = 0;
            if (skip) {
              for (int b = 0; b < kBitsPerVector; b++) {
                if (w & ((BitVector)1 << b)) {
                  if (skip) {
                    skip--;
                    continue;
                  }
                  buffer[n++] = start + b;
                  ret++;
                }
              }
            }
            else {
              for (int b = 0; b < kBitsPerVector; b++) {
                if (w & ((BitVector)1 << b))
                  buffer[n++] = start + b;
              }
              ret += n;
            }
            assert(n > 0);
            scanner(&buffer[0], n);
          }
        }
      }
      return (ret);
    }

    // Returns the index of the 'nth' set bit; sets |*pnew_n| to 0 if the
    // n'th bit was found in this MiniMap, or to the new, reduced value of |n|
    size_t select(size_t n, ssize_t *pnew_n) {
      size_t ret = 0;

      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < sizeof(BitVector); i++, p++) {
        if (*p == 0) {
          ret += kFlagsPerIndexByte * kBitsPerVector;
          continue;
        }

        for (int j = 0; j < kFlagsPerIndexByte; j++) {
          int flags = ((*p) & ((BitVector)kFlagMask << (j * 2))) >> (j * 2);
          if (flags == kPayloadNone)
            continue;
          if (flags == kPayloadZeroes) {
            ret += kBitsPerVector;
            continue;
          }
          if (flags == kPayloadOnes) {
            if (n > kBitsPerVector) {
              n -= kBitsPerVector;
              ret += kBitsPerVector; 
              continue;
            }

            *pnew_n = -1;
            return (ret + n);
          }
          if (flags == kPayloadMixed) {
            BitVector w = m_data[1 + get_position(i * kFlagsPerIndexByte + j)];
            for (int k = 0; k < kBitsPerVector; k++) {
              if (w & ((BitVector)1 << k)) {
                if (n == 0) {
                  *pnew_n = -1;
                  return (ret);
                }
                n--;
              }
              ret++;
            }
          }
        }
      }

      *pnew_n = n;
      return (ret);
    }

    // Counts the set bits in the range [0, idx]
    size_t calc_popcount(size_t idx) {
      size_t ret = 0;

      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < sizeof(BitVector); i++, p++) {
        for (int j = 0; j < kFlagsPerIndexByte; j++) {
          int flags = ((*p) & ((BitVector)kFlagMask << (j * 2))) >> (j * 2);
          if (flags == kPayloadNone)
            continue;
          if (flags == kPayloadZeroes) {
            if (idx > kBitsPerVector)
              idx -= kBitsPerVector;
            else
              return (ret);
          }
          else if (flags == kPayloadOnes) {
            if (idx > kBitsPerVector) {
              idx -= kBitsPerVector;
              ret += kBitsPerVector;
            }
            else
              return (ret + idx);
          }
          else if (flags == kPayloadMixed) {
            if (idx > kBitsPerVector) {
              idx -= kBitsPerVector;
              ret += popcount((uint64_t)m_data[1
                              + get_position(i * kFlagsPerIndexByte + j)]);
            }
            else {
              BitVector w = m_data[1 + get_position(i * kFlagsPerIndexByte + j)];
              for (size_t k = 0; k < idx; k++) {
                if (w & ((BitVector)1 << k))
                  ret++;
              }
              return (ret);
            }
          }
        }
      }
      return (ret);
    }

  private:
    // Returns the position of a BitVector in m_data
    size_t get_position(int bv) const {
      // handle 4 indices (1 byte) at a time
      size_t num_bytes = bv / (kFlagsPerIndexByte * kBitsPerVector);

      size_t position = 0;
      register uint8_t *p = (uint8_t *)m_data;
      for (size_t i = 0; i < num_bytes; i++, p++)
        position += calc_vector_size(*p);

      bv -= num_bytes * kFlagsPerIndexByte;
      for (int i = 0; i < bv; i++) {
        int flags = ((*m_data) & ((BitVector)kFlagMask << (i * 2))) >> (i * 2);
        if (flags == kPayloadMixed)
          position++;
      }

      return (position);
    }

    // Calculates the number of BitVectors required by a single byte
    // with flags (in m_data[0])
    size_t calc_vector_size(uint8_t b) const {
      static int lookup[] = {
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         2,  2,  3,  2,  2,  2,  3,  2,  3,  3,  4,  3,  2,  2,  3,  2,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0,
         1,  1,  2,  1,  1,  1,  2,  1,  2,  2,  3,  2,  1,  1,  2,  1,
         0,  0,  1,  0,  0,  0,  1,  0,  1,  1,  2,  1,  0,  0,  1,  0
      };
      return ((size_t)lookup[b]);
    }

    // Pointer to the stored data; m_data[0] always contains the index
    BitVector *m_data;
};


//
// The SparseMap is the public interface of this library.
//
// |IndexedType| is the user's numerical data type which is mapped to
// a single bit in the bitmap. Usually this is uint32_t or uint64_t.
// |BitVector| is the storage type for a bit vector used by the MiniMap.
// Usually this is a uint64_t.
//
template<typename IndexedType, typename BitVector>
class SparseMap {
    enum {
      // metadata overhead:
      //    4 bytes for minimap count
      kSizeofOverhead = sizeof(uint32_t)
    };

  public:
    // Constructor
    SparseMap()
      : m_data(0), m_data_size(0), m_data_used(0) {
    }

    // Creates a new SparseMap at the specified buffer
    void create(uint8_t *data, size_t data_size,
            size_t capacity = std::numeric_limits<uint64_t>::max()) {
      m_data = data;
      m_data_size = data_size;
      clear();
    }

    // Opens an existing SparseMap at the specified buffer
    void open(uint8_t *data, size_t data_size) {
      m_data = data;
      m_data_size = data_size;
    }

    // Resizes the data range
    void set_data_size(size_t data_size) {
      m_data_size = data_size;
    }

    // Returns the size of the underlying byte array
    size_t get_range_size() const {
      return (m_data_size);
    }

    // Returns the value of a bit at index |idx|
    bool is_set(size_t idx) {
      assert(get_size() >= kSizeofOverhead);

      // Get the MiniMap which manages this index
      ssize_t offset = get_minimap_offset(idx);

      // No MiniMaps available -> the bit is not set
      if (offset == -1)
        return (false);

      // Otherwise load the MiniMap
      uint8_t *p = get_minimap_data(offset);
      IndexedType start = *(IndexedType *)p;

      MiniMap<BitVector> minimap(p + sizeof(IndexedType));

      // Check if the bit is out of bounds of the MiniMap; if yes then
      // the bit is not set
      if (idx < start || idx - start >= minimap.get_capacity())
        return (false);

      // Otherwise ask the MiniMap whether the bit is set
      return (minimap.is_set(idx - start));
    }

    // Sets the bit at index |idx| to true or false, depending on |value|
    void set(size_t idx, bool value) {
      assert(get_size() >= kSizeofOverhead);

      // Get the MiniMap which manages this index
      ssize_t offset = get_minimap_offset(idx);
      bool dont_grow = false;

      // If there is no MiniMap and the bit is set to zero then return
      // immediately; otherwise create an initial MiniMap
      if (offset == -1) {
        if (value == false)
          return;

        uint8_t buf[sizeof(IndexedType) + sizeof(BitVector) * 2] = {0};
        append_data(&buf[0], sizeof(buf));

        uint8_t *p = get_minimap_data(0);
        *(IndexedType *)p = get_aligned_offset(idx);

        set_minimap_count(1);

        // we already inserted an additional BitVector; later on there
        // is no need to grow the vector even further
        dont_grow = true;
        offset = 0;
      }

      // Load the MiniMap
      uint8_t *p = get_minimap_data(offset);
      IndexedType start = *(IndexedType *)p;

      // The new index is smaller than the first MiniMap: create a new
      // MiniMap and insert it at the front
      if (idx < start) {
        if (value == false) // nothing to do
          return;

        uint8_t buf[sizeof(IndexedType) + sizeof(BitVector) * 2] = {0};
        insert_data(offset, &buf[0], sizeof(buf));

        size_t aligned_idx = get_fully_aligned_offset(idx);
        if (start - aligned_idx < MiniMap<BitVector>::kMaxCapacity) {
          MiniMap<BitVector> minimap(p + sizeof(IndexedType));
          minimap.set_capacity(start - aligned_idx);
        }
        *(IndexedType *)p = start = aligned_idx;

        // we just added another minimap!
        set_minimap_count(get_minimap_count() + 1);

        // we already inserted an additional BitVector; later on there
        // is no need to grow the vector even further
        dont_grow = true;
      }

      // A MiniMap exists, but the new index exceeds its capacities: create
      // a new MiniMap and insert it after the current one
      else {
        MiniMap<BitVector> minimap(p + sizeof(IndexedType));
        if (idx - start >= minimap.get_capacity()) {
          if (value == false) // nothing to do
            return;
        
          size_t size = minimap.get_size();
          offset += sizeof(IndexedType) + size;
          p += sizeof(IndexedType) + size;

          uint8_t buf[sizeof(IndexedType) + sizeof(BitVector) * 2] = {0};
          insert_data(offset, &buf[0], sizeof(buf));

          start += minimap.get_capacity();
          if ((size_t)start + MiniMap<BitVector>::kMaxCapacity < idx)
            start = get_fully_aligned_offset(idx);
          *(IndexedType *)p = start;

          // we just added another minimap!
          set_minimap_count(get_minimap_count() + 1);

          // we already inserted an additional BitVector; later on there
          // is no need to grow the vector even further
          dont_grow = true;
        }
      }

      MiniMap<BitVector> minimap(p + sizeof(IndexedType));

      // Now update the MiniMap
      MultiReturn<BitVector> mret = minimap.set(idx - start, value);
      switch (mret.code) {
        case MiniMap<BitVector>::kOk:
          break;
        case MiniMap<BitVector>::kNeedsToGrow:
          if (!dont_grow) {
            offset += sizeof(IndexedType) + mret.position * sizeof(BitVector);
            insert_data(offset, (uint8_t *)&mret.fill, sizeof(BitVector));
          }
          mret = minimap.set(idx - start, value, true);
          assert(mret.code == MiniMap<BitVector>::kOk);
          break;
        case MiniMap<BitVector>::kNeedsToShrink:
          // if the MiniMap is empty then remove it
          if (minimap.is_empty()) {
            assert(mret.position == 1);
            remove_data(offset, sizeof(IndexedType) + sizeof(BitVector) * 2);
            set_minimap_count(get_minimap_count() - 1);
          }
          else {
            offset += sizeof(IndexedType) + mret.position * sizeof(BitVector);
            remove_data(offset, sizeof(BitVector));
          }
          break;
        default:
          assert(!"shouldn't be here");
          break;
      }
      assert(get_size() >= kSizeofOverhead);
    }

    // Clears the whole buffer
    void clear() {
      m_data_used = kSizeofOverhead;
      set_minimap_count(0);
    }

    // Returns the offset of the very first bit
    IndexedType get_start_offset() {
      if (get_minimap_count() == 0)
        return (0);
      return (*(IndexedType *)get_minimap_data(0));
    }

    // Returns the used size in the data buffer
    size_t get_size() {
      if (m_data_used) {
        assert(m_data_used == get_size_impl());
        return (m_data_used);
      }
      return (m_data_used = get_size_impl());
    }

    // Decompresses the whole bitmap; calls visitor's operator() for all bits
    template<class Scanner>
    void scan(Scanner &scanner, size_t skip) {
      uint8_t *p = get_minimap_data(0);

      size_t count = get_minimap_count();
      for (size_t i = 0; i < count; i++) {
        IndexedType start = *(IndexedType *)p;
        p += sizeof(IndexedType);
        MiniMap<BitVector> minimap(p);
        size_t skipped = minimap.scan(start, scanner, skip);
        if (skip) {
          assert(skip >= skipped);
          skip -= skipped;
        }
        p += minimap.get_size();
      }
    }

    // Appends all MiniMaps from |sstart| to |other|, then reduces the
    // MiniMap-count appropriately
    //
    // |sstart| must be BitVector-aligned!
    void split(size_t sstart, SparseMap<IndexedType, BitVector> *other) {
      assert(sstart % MiniMap<BitVector>::kBitsPerVector == 0);

      // |dst| points to the destination buffer
      uint8_t *dst = other->get_minimap_end();

      // |src| points to the source-MiniMap
      uint8_t *src = get_minimap_data(0);

      // |sstart| is relative to the beginning of this SparseMap; better
      // make it absolute
      sstart += *(IndexedType *)src;

      bool in_middle = false;
      uint8_t *prev = src;
      size_t i, count = get_minimap_count();
      for (i = 0; i < count; i++) {
        IndexedType start = *(IndexedType *)src;
        MiniMap<BitVector> minimap(src + sizeof(IndexedType));
        if (start == sstart)
          break;
        if (start + minimap.get_capacity() > sstart) {
          in_middle = true;
          break;
        }
        if (start > sstart) {
          src = prev;
          i--;
          break;
        }

        prev = src;
        src += sizeof(IndexedType) + minimap.get_size();
      }
      if (i == count) {
        assert(get_size() > kSizeofOverhead);
        assert(other->get_size() > kSizeofOverhead);
        return;
      }

      // Now copy all the remaining MiniMaps
      int moved = 0;

      // If |sstart| is in the middle of a MiniMap then this MiniMap has
      // to be split
      if (in_middle) {
        uint8_t buf[sizeof(IndexedType) + sizeof(BitVector) * 2] = {0};
        memcpy(dst, &buf[0], sizeof(buf));

        *(IndexedType *)dst = sstart;
        dst += sizeof(IndexedType);

        // the |other| SparseMap now has one additional MiniMap
        other->set_minimap_count(other->get_minimap_count() + 1);
        if (other->m_data_used != 0)
          other->m_data_used += sizeof(IndexedType) + sizeof(BitVector);

        src += sizeof(IndexedType);
        MiniMap<BitVector> sminimap(src);
        size_t capacity = sminimap.get_capacity();

        MiniMap<BitVector> dminimap(dst);
        dminimap.set_capacity(capacity - (sstart % capacity));

        // now copy the bits
        size_t d = sstart;
        for (size_t j = sstart % capacity; j < capacity; j++, d++) {
          if (sminimap.is_set(j))
            other->set(d, true);
        }

        src += sminimap.get_size();
        size_t dsize = dminimap.get_size();
        dst += dsize;
        i++;

        // reduce the capacity of the source-MiniMap
        sminimap.set_capacity(sstart % capacity);
      }

      // Now continue with all remaining minimaps
      for (; i < count; i++) {
        IndexedType start = *(IndexedType *)src;
        src += sizeof(IndexedType);
        MiniMap<BitVector> minimap(src);
        size_t s = minimap.get_size();

        *(IndexedType *)dst = start;
        dst += sizeof(IndexedType);
        memcpy(dst, src, s);
        src += s;
        dst += s;

        moved++;
      }

      // force new calculation
      other->m_data_used = 0;
      m_data_used = 0;

      // Update the MiniMap counters
      set_minimap_count(get_minimap_count() - moved);
      other->set_minimap_count(other->get_minimap_count() + moved);

      assert(get_size() >= kSizeofOverhead);
      assert(other->get_size() > kSizeofOverhead);
    }

    // Returns the index of the 'nth' set bit; uses a 0-based index,
    // i.e. n == 0 for the first bit which is set, n == 1 for the second bit etc
    size_t select(size_t n) {
      assert(get_size() >= kSizeofOverhead);
      size_t result = 0;
      size_t count = get_minimap_count();

      uint8_t *p = get_minimap_data(0);

      for (size_t i = 0; i < count; i++) {
        result = *(IndexedType *)p;
        p += sizeof(IndexedType);
        MiniMap<BitVector> minimap(p);
        
        ssize_t new_n = (ssize_t)n;
        size_t index = minimap.select(n, &new_n);
        if (new_n == -1)
          return (result + index);
        n = (size_t)new_n;

        p += minimap.get_size();
      }
      assert(!"shouldn't be here");
      return (0);
    }

    // Counts the set bits in the range [0, idx]
    size_t calc_popcount(size_t idx) {
      assert(get_size() >= kSizeofOverhead);
      size_t result = 0;
      size_t count = get_minimap_count();

      uint8_t *p = get_minimap_data(0);

      for (size_t i = 0; i < count; i++) {
        IndexedType start = *(IndexedType *)p;
        if (start > idx)
          return (result);
        p += sizeof(IndexedType);
        MiniMap<BitVector> minimap(p);
        
        result += minimap.calc_popcount(idx - start);
        p += minimap.get_size();
      }
      return (result);
    }

    // Returns the number of MiniMaps
    size_t get_minimap_count() const {
      return (*(uint32_t *)&m_data[0]);
    }

  private:
    // Returns the used size in the data buffer
    size_t get_size_impl() {
      uint8_t *start = get_minimap_data(0);
      uint8_t *p = start;

      size_t count = get_minimap_count();
      for (size_t i = 0; i < count; i++) {
        p += sizeof(IndexedType);
        MiniMap<BitVector> minimap(p);
        p += minimap.get_size();
      }
      return (kSizeofOverhead + p - start);
    }

    // Returns the byte offset of a MiniMap in m_data
    ssize_t get_minimap_offset(size_t idx) {
      size_t count = get_minimap_count();
      if (count == 0)
        return (-1);

      uint8_t *start = get_minimap_data(0);
      uint8_t *p = start;

      for (size_t i = 0; i < count - 1; i++) {
        IndexedType start = *(IndexedType *)p;
        assert(start == get_aligned_offset(start));
        MiniMap<BitVector> minimap(p + sizeof(IndexedType));
        if (start >= idx || idx < start + minimap.get_capacity())
          break;
        p += sizeof(IndexedType) + minimap.get_size();
      }

      return ((ssize_t)(p - start));
    }

    // Returns the data at the specified |offset|
    uint8_t *get_minimap_data(size_t offset) {
      return (&m_data[kSizeofOverhead + offset]);
    }

    // Returns a pointer after the end of the used data
    // TODO can also use m_data_used?
    uint8_t *get_minimap_end() {
      uint8_t *p = get_minimap_data(0);

      size_t count = get_minimap_count();
      for (size_t i = 0; i < count; i++) {
        p += sizeof(IndexedType);
        MiniMap<BitVector> minimap(p);
        p += minimap.get_size();
      }
      return (p);
    }

    // Returns the aligned offset (aligned to BitVector capacity)
    IndexedType get_aligned_offset(size_t idx) const {
      const size_t capacity = MiniMap<BitVector>::kBitsPerVector;
      return ((idx / capacity) * capacity);
    }

    // Returns the aligned offset (aligned to MiniMap capacity)
    IndexedType get_fully_aligned_offset(size_t idx) const {
      const size_t capacity = MiniMap<BitVector>::kMaxCapacity;
      return ((idx / capacity) * capacity);
    }

    // Sets the number of MiniMaps
    void set_minimap_count(size_t new_count) {
      *(uint32_t *)&m_data[0] = (uint32_t)new_count;
    }

    // Appends more data
    void append_data(uint8_t *buffer, size_t buffer_size) {
      memcpy(&m_data[m_data_used], buffer, buffer_size);
      m_data_used += buffer_size;
    }

    // Inserts data somewhere in the middle of m_data
    void insert_data(size_t offset, uint8_t *buffer, size_t buffer_size) {
      if (m_data_used + buffer_size > m_data_size)
        throw std::overflow_error("buffer overflow");

      uint8_t *p = get_minimap_data(offset);
      memmove(p + buffer_size, p, m_data_used - offset);
      memcpy(p, buffer, buffer_size);
      m_data_used += buffer_size;
    }

    // Removes data from m_data
    void remove_data(size_t offset, size_t gap_size) {
      assert(m_data_used >= offset + gap_size);
      uint8_t *p = get_minimap_data(offset);
      memmove(p, p + gap_size, m_data_used - offset - gap_size);
      m_data_used -= gap_size;
    }

    // The serialized bitmap data
    uint8_t *m_data;

    // The total size of m_data
    size_t m_data_size;

    // The used size of m_data
    size_t m_data_used;
};

} // namespace sparsemap

#endif // SPARSEMAP_H__
