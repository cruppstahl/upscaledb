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

#ifndef HAM_BTREE_KEY_H__
#define HAM_BTREE_KEY_H__

namespace hamsterdb {

//
// A helper class wrapping key-related constants into a common namespace.
// This class does not contain any logic.
//
class BtreeKey
{
  public:
    // persisted PBtreeKeyLegacy flags; also used in combination with ham_key_t._flags
    //
    // NOTE: persisted flags must fit within a ham_u8_t (1 byte) --> mask:
    // 0x000000FF
    enum {
      // record size < 8; length is encoded at byte[7] of key->ptr
      kBlobSizeTiny         = 0x01,

      // record size == 8; record is stored in key->ptr
      kBlobSizeSmall        = 0x02,

      // record size == 0; key->ptr == 0
      kBlobSizeEmpty        = 0x04,

      // key is extended with overflow area
      kExtended             = 0x08,

      // key has duplicates
      kDuplicates           = 0x10,

      // memory for a key was allocated in hamsterdb, not by caller
      kAllocated           = 0x20
    };

    // flags used with the ham_key_t::_flags (note the underscore - this
    // field is for INTERNAL USE!)
    //
    // Note: these flags should NOT overlap with the persisted flags for
    // PBtreeKeyLegacy
    //
    // As these flags NEVER will be persisted, they should be located outside
    // the range of a ham_u16_t, i.e. outside the mask 0x0000FFFF.
    enum {
      // Actual key is lower than the requested key
      kLower               = 0x00010000,

      // Actual key is greater than the requested key
      kGreater             = 0x00020000,

      // Actual key is an "approximate match"
      kApproximate         = (kLower | kGreater)
    };
};

} // namespace hamsterdb

#endif /* HAM_BTREE_KEY_H__ */
