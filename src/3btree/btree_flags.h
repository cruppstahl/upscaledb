/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_BTREE_FLAGS_H
#define UPS_BTREE_FLAGS_H

#include "0root/root.h"

#include "ups/types.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// A helper class wrapping key-related constants into a common namespace.
// This class does not contain any logic.
//
struct BtreeKey {
  // persisted btree key flags; also used in combination with ups_key_t._flags
  enum {
    // key is extended with overflow area
    kExtendedKey          = 0x01,

    // key is compressed; the original size is stored in the payload
    kCompressed           = 0x08
  };

  // flags used with the ups_key_t::_flags (note the underscore - this
  // field is for INTERNAL USE!)
  //
  // Note: these flags should NOT overlap with the persisted flags above!
  //
  // As these flags NEVER will be persisted, they should be located outside
  // the range of a uint16_t, i.e. outside the mask 0x0000ffff.
  enum {
    // Actual key is lower than the requested key
    kLower               = 0x00010000,

    // Actual key is greater than the requested key
    kGreater             = 0x00020000,

    // Actual key is an "approximate match"
    kApproximate         = (kLower | kGreater)
  };
};

//
// A helper class wrapping record-related constants into a common namespace.
// This class does not contain any logic.
//
struct BtreeRecord {
  enum {
    // record size < 8; length is encoded at byte[7] of key->ptr
    kBlobSizeTiny         = 0x01,

    // record size == 8; record is stored in key->ptr
    kBlobSizeSmall        = 0x02,

    // record size == 0; key->ptr == 0
    kBlobSizeEmpty        = 0x04,

    // key has duplicates in an overflow area; this is the msb of 1 byte;
    // the lower bits are the counter for the inline duplicate list
    kExtendedDuplicates   = 0x80
  };
};

} // namespace upscaledb

#endif // UPS_BTREE_FLAGS_H
