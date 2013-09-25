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

#ifndef HAM_BTREE_INDEX_FACTORY_H__
#define HAM_BTREE_INDEX_FACTORY_H__

#include "db_local.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

//
// A BtreeIndexFactory creates BtreeIndexProxy objects depending on the
// Database configuration
//
struct BtreeIndexFactory
{
  static BtreeIndex *create(LocalDatabase *db, ham_u32_t descriptor,
                  ham_u32_t flags, ham_u16_t keytype) {
    ham_u32_t dbflags = db->get_rt_flags();

    // Record number database
    if (dbflags & HAM_RECORD_NUMBER)
      return (new BtreeIndexImpl<LegacyNodeLayout, RecordNumberCompare>(db,
                                    descriptor, flags));

    switch (keytype) {
      // Callback function provided by user?
      case HAM_TYPE_CUSTOM:
        return (new BtreeIndexImpl<LegacyNodeLayout, CallbackCompare>(db,
                                    descriptor, flags));
      // BINARY is the default:
      case HAM_TYPE_BINARY:
        // Variable keys with non-constant size (not extended)
        // This is the default!
        //   - HAM_PARAM_KEYSIZE to specify the size limit
        if ((dbflags & HAM_DISABLE_VARIABLE_KEYS) == 0
                && (dbflags & HAM_ENABLE_EXTENDED_KEYS) == 0)
          return (new BtreeIndexImpl<LegacyNodeLayout, VariableSizeCompare>(db,
                                      descriptor, flags));
        // Fixed keys with constant size (not extended)
        //   - HAM_DISABLE_VARIABLE_KEYS
        //   - HAM_PARAM_KEYSIZE to specify the constant size
        if (dbflags & HAM_DISABLE_VARIABLE_KEYS
                && (dbflags & HAM_ENABLE_EXTENDED_KEYS) == 0)
          return (new BtreeIndexImpl<LegacyNodeLayout, FixedSizeCompare>(db,
                                        descriptor, flags));
        // Fixed keys with constant size (extended)
        //   - HAM_DISABLE_VARIABLE_KEYS
        //   - HAM_ENABLE_EXTENDED_KEYS
        if (dbflags & HAM_DISABLE_VARIABLE_KEYS
                && dbflags & HAM_ENABLE_EXTENDED_KEYS)
          return (new BtreeIndexImpl<LegacyNodeLayout, FixedSizeCompare>(db,
                                        descriptor, flags));
        // Variable keys with non-constant size (extended)
        //   - HAM_ENABLE_EXTENDED_KEYS
        if (dbflags & HAM_ENABLE_EXTENDED_KEYS)
          return (new BtreeIndexImpl<LegacyNodeLayout, VariableSizeCompare>(db,
                                        descriptor, flags));
      default:
        ham_assert(!"shouldn't be here");
        return (0);
    }
  }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_FACTORY_H__ */
