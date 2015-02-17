/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_FLAGS_H
#define HAM_BTREE_FLAGS_H

#include "0root/root.h"

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// A helper class wrapping key-related constants into a common namespace.
// This class does not contain any logic.
//
struct BtreeKey
{
  // persisted btree key flags; also used in combination with ham_key_t._flags
  enum {
    // key is extended with overflow area
    kExtendedKey          = 0x01,

    // PRO: key is compressed; the original size is stored in the payload
    kCompressed           = 0x08
  };

  // flags used with the ham_key_t::_flags (note the underscore - this
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
struct BtreeRecord
{
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

} // namespace hamsterdb

#endif /* HAM_BTREE_FLAGS_H */
