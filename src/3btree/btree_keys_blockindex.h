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

#ifndef HAM_BTREE_KEYS_BLOCKINDEX_H
#define HAM_BTREE_KEYS_BLOCKINDEX_H

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_keys_block.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
HAM_PACK_0 class HAM_PACK_1 BlockIndexIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 64 * 4,

      // Grow blocks by this factor
      kGrowFactor = 64,

      // Maximum keys per block
      kMaxKeysPerBlock = 128,

      // Maximum size of an encoded integer
      kMaxSizePerInt = 4,

      // Maximum block size - not relevant for this codec
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
    void copy_to(const uint8_t *block_data, BlockIndexIndex *dest,
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

    // the number of keys in this block; max 255 (kMaxKeysPerBlock)
    unsigned int m_key_count : 8;
} HAM_PACK_2;
#include "1base/packstop.h"

struct BlockIndexCodecImpl : public BlockCodecBase<BlockIndexIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 0,
    kHasDelApi = 1,
    kCompressInPlace = 1,
  };

  static uint32_t compress_block(BlockIndexIndex *index, const uint32_t *in,
                  uint32_t *out) {
    uint32_t used_size = (index->key_count() - 1) * sizeof(uint32_t);
    if (in != out) {
      ::memcpy(out, in, used_size);
    }
    return (used_size);
  }

  static uint32_t *uncompress_block(BlockIndexIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    return (const_cast<uint32_t *>(block_data));
  }

  template<typename GrowHandler>
  static void del(BlockIndexIndex *index, uint32_t *block_data, int slot,
                  GrowHandler *unused) {
    // delete the first value?
    if (slot == 0) {
      index->set_value(block_data[0]);
      slot++;
    }

    if (slot < (int)index->key_count() - 1) {
      ::memmove(&block_data[slot - 1], &block_data[slot],
              sizeof(uint32_t) * (index->key_count() - slot - 1));
    }

    index->set_key_count(index->key_count() - 1);
  }
};

typedef Zint32Codec<BlockIndexIndex, BlockIndexCodecImpl> BlockIndexCodec;

class BlockIndexKeyList : public BlockKeyList<BlockIndexCodec>
{
  public:
    // Constructor
    BlockIndexKeyList(LocalDatabase *db)
      : BlockKeyList<BlockIndexCodec>(db) {
    }
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BLOCKINDEX_H */
