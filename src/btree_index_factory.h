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
#include "btree_node_legacy.h"
#include "btree_node_pax.h"
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
    // Record number database
    if (flags & HAM_RECORD_NUMBER)
      return (new BtreeIndexImpl
                      < PaxNodeLayout<PodKeyList<ham_u64_t> >,
                      RecordNumberCompare>(db, descriptor, flags));

    switch (keytype) {
      // 8bit unsigned integer
      case HAM_TYPE_UINT8:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<ham_u8_t> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<ham_u8_t> >,
                        NumericCompare<ham_u8_t> >(db, descriptor, flags));
      // 16bit unsigned integer
      case HAM_TYPE_UINT16:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<ham_u16_t> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<ham_u16_t> >,
                        NumericCompare<ham_u16_t> >(db, descriptor, flags));
      // 32bit unsigned integer
      case HAM_TYPE_UINT32:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<ham_u32_t> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<ham_u32_t> >,
                        NumericCompare<ham_u32_t> >(db, descriptor, flags));
      // 64bit unsigned integer
      case HAM_TYPE_UINT64:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<ham_u64_t> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<ham_u64_t> >,
                        NumericCompare<ham_u64_t> >(db, descriptor, flags));
      // 32bit float
      case HAM_TYPE_REAL32:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<float> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<float> >,
                        NumericCompare<float> >(db, descriptor, flags));
      // 64bit double
      case HAM_TYPE_REAL64:
        if (flags & HAM_ENABLE_DUPLICATES)
          return (new BtreeIndexImpl<LegacyNodeLayout,
                        NumericCompare<double> >(db, descriptor, flags));
        else
          return (new BtreeIndexImpl
                        < PaxNodeLayout<PodKeyList<double> >,
                        NumericCompare<double> >(db, descriptor, flags));
      // Callback function provided by user?
      case HAM_TYPE_CUSTOM:
        // Fixed keys with constant size (not extended)
        //   - HAM_DISABLE_VARIABLE_KEYS
        //   - HAM_PARAM_KEYSIZE to specify the constant size
        if (flags & HAM_DISABLE_VARIABLE_KEYS
                && (flags & HAM_ENABLE_EXTENDED_KEYS) == 0)
          return (new BtreeIndexImpl
                        < PaxNodeLayout<BinaryKeyList >,
                        CallbackCompare>(db, descriptor, flags));
        return (new BtreeIndexImpl<LegacyNodeLayout, CallbackCompare>(db,
                                    descriptor, flags));
      // BINARY is the default:
      case HAM_TYPE_BINARY:
        // Variable keys with non-constant size (not extended)
        // This is the default!
        //   - HAM_PARAM_KEYSIZE to specify the size limit
        if ((flags & HAM_DISABLE_VARIABLE_KEYS) == 0
                && (flags & HAM_ENABLE_EXTENDED_KEYS) == 0)
          return (new BtreeIndexImpl<LegacyNodeLayout, VariableSizeCompare>(db,
                                      descriptor, flags));
        // Fixed keys with constant size (not extended)
        //   - HAM_DISABLE_VARIABLE_KEYS
        //   - HAM_PARAM_KEYSIZE to specify the constant size
        if (flags & HAM_DISABLE_VARIABLE_KEYS
                && (flags & HAM_ENABLE_EXTENDED_KEYS) == 0)
          return (new BtreeIndexImpl
                        < PaxNodeLayout<BinaryKeyList >,
                        FixedSizeCompare>(db, descriptor, flags));
        // Fixed keys with constant size (extended)
        //   - HAM_DISABLE_VARIABLE_KEYS
        //   - HAM_ENABLE_EXTENDED_KEYS
        if (flags & HAM_DISABLE_VARIABLE_KEYS
                && flags & HAM_ENABLE_EXTENDED_KEYS)
          return (new BtreeIndexImpl<LegacyNodeLayout, FixedSizeCompare>(db,
                                        descriptor, flags));
        // Variable keys with non-constant size (extended)
        //   - HAM_ENABLE_EXTENDED_KEYS
        if (flags & HAM_ENABLE_EXTENDED_KEYS)
          return (new BtreeIndexImpl<LegacyNodeLayout, VariableSizeCompare>(db,
                                        descriptor, flags));
        break;
      default:
        break;
    }

    ham_assert(!"shouldn't be here");
    return (0);
  }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_FACTORY_H__ */
