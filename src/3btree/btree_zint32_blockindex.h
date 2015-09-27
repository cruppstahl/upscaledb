/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * Compressed 32bit integer keys
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef UPS_BTREE_KEYS_BLOCKINDEX_H
#define UPS_BTREE_KEYS_BLOCKINDEX_H

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

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 class UPS_PACK_1 BlockIndexIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 64 * 4,

      // Maximum keys per block
      kMaxKeysPerBlock = 129,
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
      dest->set_highest(highest());
      ::memcpy(dest_data, block_data, block_size());
    }

  private:
    // the total size of this block; max 2048-1 bytes
    unsigned int m_block_size : 11;

    // used size of this block; max 2048-1 bytes
    unsigned int m_used_size : 11;

    // the number of keys in this block; max 255 (kMaxKeysPerBlock)
    unsigned int m_key_count : 8;
} UPS_PACK_2;
#include "1base/packstop.h"

struct BlockIndexCodecImpl : public BlockCodecBase<BlockIndexIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 0,
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

  static uint32_t estimate_required_size(BlockIndexIndex *index,
                        uint8_t *block_data, uint32_t key) {
    return (index->key_count() * sizeof(uint32_t));
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

} // namespace upscaledb

#endif /* UPS_BTREE_KEYS_BLOCKINDEX_H */
