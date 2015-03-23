/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * Compressed 32bit integer keys
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef HAM_BTREE_KEYS_STREAMVBYTE_H
#define HAM_BTREE_KEYS_STREAMVBYTE_H

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_keys_block.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

extern uint8_t *svb_encode_scalar_d1_init(const uint32_t *in,
                uint8_t * /*restrict*/ keyPtr, uint8_t * /*restrict */dataPtr,
                uint32_t count, uint32_t prev);
extern uint8_t *svb_decode_scalar_d1_init(uint32_t *out,
                const uint8_t * keyPtr, uint8_t * dataPtr, uint32_t count,
                uint32_t prev);
extern uint8_t *svb_decode_avx_d1_init(uint32_t *out,
                uint8_t * keyPtr, uint8_t * dataPtr, uint64_t count,
                uint32_t prev);
extern int      svb_find_avx_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, uint32_t key, uint32_t *presult);
extern int      svb_find_scalar_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, uint32_t key, uint32_t *presult);
extern int      svb_select_avx_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, int slot);
extern int      svb_select_scalar_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, int slot);


namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
HAM_PACK_0 class HAM_PACK_1 StreamVbyteIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 20, // 4 + 4 + 4 * 4

      // Grow blocks by this factor
      kGrowFactor = 24,

      // Maximum keys per block
      kMaxKeysPerBlock = 128,

      // Maximum size of an encoded integer (1 byte index + 4 byte uint32)
      kMaxSizePerInt = 5,

      // Maximum block size - not relevant
      kMaxBlockSize = 102400,
    };

    // initialize this block index
    void initialize(uint32_t offset, uint32_t block_size) {
      IndexBase::initialize(offset);
      m_block_size = block_size;
      m_used_size = 0;
      m_key_count = 0;
    }

    // returns the used size of the block
    uint32_t used_size() const {
      return (m_used_size);
    }

    // sets the used size of the block
    void set_used_size(uint32_t size) {
      m_used_size = size;
    }

    // returns the total block size
    uint32_t block_size() const {
      return (m_block_size);
    }

    // sets the total block size
    void set_block_size(uint32_t size) {
      m_block_size = size;
    }

    // returns the key count
    uint32_t key_count() const {
      return (m_key_count);
    }

    // sets the key count
    void set_key_count(uint32_t key_count) {
      m_key_count = key_count;
    }

    // copies this block to the |dest| block
    void copy_to(const uint8_t *block_data, StreamVbyteIndex *dest,
                    uint8_t *dest_data) {
      dest->set_value(value());
      dest->set_key_count(key_count());
      dest->set_used_size(used_size());
      ::memcpy(dest_data, block_data, block_size());
    }

  private:
    // the total size of this block; max 2048-1 bytes
    unsigned int m_block_size : 11;

    // used size of this block; max 2048-1 bytes
    unsigned int m_used_size : 11;

    // the number of keys in this block; max 1024-1 (kMaxKeysPerBlock)
    unsigned int m_key_count : 10;
} HAM_PACK_2;
#include "1base/packstop.h"

struct StreamVbyteCodecImpl : public BlockCodecBase<StreamVbyteIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
  };

  static uint32_t compress_block(StreamVbyteIndex *index, const uint32_t *in,
                  uint32_t *out32) {
    ham_assert(index->key_count() > 0);
    uint8_t *out = (uint8_t *)out32;
    uint32_t count = index->key_count() - 1;
    uint32_t key_len = (count + 3) / 4; // 2-bits rounded to full byte
    uint8_t *data = out + key_len;  // variable byte data after all keys
  
    return (svb_encode_scalar_d1_init(in, out, data, count,
                    index->value()) - out);
  }

  static uint32_t *uncompress_block(StreamVbyteIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    if (index->key_count() > 1) {
      uint32_t count = index->key_count() - 1;
      uint8_t *in = (uint8_t *)block_data;
      uint32_t key_len = ((count + 3) / 4);   // 2-bits per key (rounded up)
      uint8_t *data = in + key_len;    // data starts at end of keys

      if (os_has_avx())
        svb_decode_avx_d1_init(out, in, data, count, index->value());
      else
        svb_decode_scalar_d1_init(out, in, data, count, index->value());
    }
    return (out);
  }

  static int find_lower_bound(StreamVbyteIndex *index,
                  const uint32_t *block_data, uint32_t key, uint32_t *result) {
    uint32_t count = index->key_count() - 1;
    uint8_t *in = (uint8_t *)block_data;
    uint32_t key_len = ((count + 3) / 4);   // 2-bits per key (rounded up)
    uint8_t *data = in + key_len;    // data starts at end of keys

    if (os_has_avx())
      return (svb_find_avx_d1_init(in, data, count, index->value(),
                              key, result));
    else
      return (svb_find_scalar_d1_init(in, data, count, index->value(),
                              key, result));
  }

  // Returns a decompressed value
  static uint32_t select(StreamVbyteIndex *index, uint32_t *block_data,
                        int slot) {
    uint32_t count = index->key_count() - 1;
    uint8_t *in = (uint8_t *)block_data;
    uint32_t key_len = ((count + 3) / 4);   // 2-bits per key (rounded up)
    uint8_t *data = in + key_len;    // data starts at end of keys

    if (os_has_avx())
      return (svb_select_avx_d1_init(in, data, count, index->value(),
                      slot));
    else
      return (svb_select_scalar_d1_init(in, data, count, index->value(),
                      slot));
  }
};

typedef Zint32Codec<StreamVbyteIndex, StreamVbyteCodecImpl> StreamVbyteCodec;

class StreamVbyteKeyList : public BlockKeyList<StreamVbyteCodec>
{
  public:
    // Constructor
    StreamVbyteKeyList(LocalDatabase *db)
      : BlockKeyList<StreamVbyteCodec>(db) {
    }
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_STREAMVBYTE_H */
