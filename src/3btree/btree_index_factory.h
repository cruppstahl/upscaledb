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

#ifndef UPS_BTREE_INDEX_FACTORY_H
#define UPS_BTREE_INDEX_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_index.h"
#include "3btree/btree_impl_default.h"
#include "3btree/btree_impl_pax.h"
#include "3btree/btree_keys_pod.h"
#include "3btree/btree_keys_binary.h"
#include "3btree/btree_keys_varlen.h"
#include "3btree/btree_zint32_groupvarint.h"
#include "3btree/btree_zint32_maskedvbyte.h"
#include "3btree/btree_zint32_simdcomp.h"
#include "3btree/btree_zint32_for.h"
#include "3btree/btree_zint32_simdfor.h"
#include "3btree/btree_zint32_streamvbyte.h"
#include "3btree/btree_zint32_varbyte.h"
#include "3btree/btree_records_default.h"
#include "3btree/btree_records_inline.h"
#include "3btree/btree_records_internal.h"
#include "3btree/btree_records_duplicate.h"
#include "3btree/btree_records_pod.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// A specialied Traits class using template parameters
//
template<class NodeLayout, class Comparator>
class BtreeIndexTraitsImpl : public BtreeIndexTraits
{
  public:
    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(LocalDatabase *db, ups_key_t *lhs,
            ups_key_t *rhs) const {
      Comparator cmp(db);
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Returns the class name (for testing)
    virtual std::string test_get_classname() const {
      return (get_classname(*this));
    }

    // Implementation of get_node_from_page()
    virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) const {
      return (new BtreeNodeProxyImpl<NodeLayout, Comparator>(page));
    }
};

#define PAX_INTERNAL_NODE(KeyList, Compare) \
          return (new BtreeIndexTraitsImpl                                  \
                    <PaxNodeImpl<KeyList,                                   \
                        PaxLayout::InternalRecordList>,                     \
                    Compare >())

#define DEF_INTERNAL_NODE(KeyList, Compare) \
          return (new BtreeIndexTraitsImpl<                                 \
                  DefaultNodeImpl<KeyList,                                  \
                        PaxLayout::InternalRecordList>,                     \
                  Compare >())

#define PAX_INTERNAL_NUMERIC(type) \
          PAX_INTERNAL_NODE(PaxLayout::PodKeyList<type>, NumericCompare<type> )

#define LEAF_NODE_IMPL(Impl, KeyList, Compare) \
        if (use_duplicates) {                                               \
          if (inline_records) {                                             \
            switch (cfg.record_type) {                                      \
              case UPS_TYPE_UINT8:                                          \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_UINT16:                                         \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_UINT32:                                         \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_UINT64:                                         \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_REAL32:                                         \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_REAL64:                                         \
                ups_assert(!"not yet implemented");                         \
                return (0);                                                 \
              case UPS_TYPE_BINARY:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <DefaultNodeImpl<KeyList,                         \
                                DefLayout::DuplicateInlineRecordList>,      \
                          Compare >());                                     \
              default:                                                      \
                ups_assert(!"shouldn't be here");                           \
                return (0);                                                 \
            }                                                               \
          }                                                                 \
          else                                                              \
            return (new BtreeIndexTraitsImpl                                \
                      <DefaultNodeImpl<KeyList,                             \
                          DefLayout::DuplicateDefaultRecordList>,           \
                      Compare >());                                         \
        }                                                                   \
        else {                                                              \
          if (inline_records)                                               \
            switch (cfg.record_type) {                                      \
              case UPS_TYPE_UINT8:                                          \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<uint8_t> >,\
                          Compare >());                                     \
              case UPS_TYPE_UINT16:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<uint16_t> >,\
                          Compare >());                                     \
              case UPS_TYPE_UINT32:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<uint32_t> >,\
                          Compare >());                                     \
              case UPS_TYPE_UINT64:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<uint64_t> >,\
                          Compare >());                                     \
              case UPS_TYPE_REAL32:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<float> >, \
                          Compare >());                                     \
              case UPS_TYPE_REAL64:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::PodRecordList<double> >,\
                          Compare >());                                     \
              case UPS_TYPE_BINARY:                                         \
                return (new BtreeIndexTraitsImpl                            \
                          <Impl<KeyList, PaxLayout::InlineRecordList>,      \
                          Compare >());                                     \
              default:                                                      \
                ups_assert(!"shouldn't be here");                           \
                return (0);                                                 \
            }                                                               \
          else                                                              \
            return (new BtreeIndexTraitsImpl                                \
                    <Impl<KeyList, PaxLayout::DefaultRecordList>,           \
                      Compare >());                                         \
        }

#define PAX_LEAF_NODE(KeyList, Compare) \
        LEAF_NODE_IMPL(DefaultNodeImpl, KeyList, Compare)

#define PAX_LEAF_NUMERIC(type) \
        LEAF_NODE_IMPL(PaxNodeImpl, PaxLayout::PodKeyList<type>,        \
                    NumericCompare<type> )

//
// A BtreeIndexFactory creates BtreeIndexProxy objects depending on the
// Database configuration
//
struct BtreeIndexFactory
{
  static BtreeIndexTraits *create(LocalDatabase *db, bool is_leaf) {
    const DatabaseConfiguration &cfg = db->config();
    bool inline_records = (is_leaf && (cfg.flags & UPS_FORCE_RECORDS_INLINE));
    bool fixed_keys = (cfg.key_size != UPS_KEY_SIZE_UNLIMITED);
    bool use_duplicates = (cfg.flags & UPS_ENABLE_DUPLICATES) != 0;
    int key_compression = cfg.key_compressor;

    switch (cfg.key_type) {
      // 8bit unsigned integer
      case UPS_TYPE_UINT8:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(uint8_t);
        PAX_LEAF_NUMERIC(uint8_t);
      // 16bit unsigned integer
      case UPS_TYPE_UINT16:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(uint16_t);
        PAX_LEAF_NUMERIC(uint16_t);
      // 32bit unsigned integer
      case UPS_TYPE_UINT32:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(uint32_t);
        switch (key_compression) {
          case UPS_COMPRESSOR_UINT32_VARBYTE:
            PAX_LEAF_NODE(Zint32::VarbyteKeyList, NumericCompare<uint32_t>);
          case UPS_COMPRESSOR_UINT32_SIMDCOMP:
#ifdef HAVE_SSE2
            PAX_LEAF_NODE(Zint32::SimdCompKeyList, NumericCompare<uint32_t>);
#else
            throw Exception(UPS_INV_PARAMETER);
#endif
          case UPS_COMPRESSOR_UINT32_FOR:
            PAX_LEAF_NODE(Zint32::ForKeyList, NumericCompare<uint32_t>);
          case UPS_COMPRESSOR_UINT32_SIMDFOR:
#ifdef HAVE_SSE2
            PAX_LEAF_NODE(Zint32::SimdForKeyList, NumericCompare<uint32_t>);
#else
            throw Exception(UPS_INV_PARAMETER);
#endif
          case UPS_COMPRESSOR_UINT32_GROUPVARINT:
            PAX_LEAF_NODE(Zint32::GroupVarintKeyList, NumericCompare<uint32_t>);
          case UPS_COMPRESSOR_UINT32_STREAMVBYTE:
#ifdef HAVE_SSE2
            PAX_LEAF_NODE(Zint32::StreamVbyteKeyList, NumericCompare<uint32_t>);
#else
            throw Exception(UPS_INV_PARAMETER);
#endif
          case UPS_COMPRESSOR_UINT32_MASKEDVBYTE:
#ifdef HAVE_SSE2
            PAX_LEAF_NODE(Zint32::MaskedVbyteKeyList, NumericCompare<uint32_t>);
#else
            throw Exception(UPS_INV_PARAMETER);
#endif
          default:
            // no key compression
            PAX_LEAF_NUMERIC(uint32_t);
        }
      // 64bit unsigned integer
      case UPS_TYPE_UINT64:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(uint64_t);
        PAX_LEAF_NUMERIC(uint64_t);
      // 32bit float
      case UPS_TYPE_REAL32:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(float);
        PAX_LEAF_NUMERIC(float);
      // 64bit double
      case UPS_TYPE_REAL64:
        if (!is_leaf)
          PAX_INTERNAL_NUMERIC(double);
        PAX_LEAF_NUMERIC(double);
      // Callback function provided by user?
      case UPS_TYPE_CUSTOM:
        // Fixed keys, no duplicates
        if (fixed_keys) {
          if (!is_leaf)
            PAX_INTERNAL_NODE(PaxLayout::BinaryKeyList, CallbackCompare);
          LEAF_NODE_IMPL(PaxNodeImpl, PaxLayout::BinaryKeyList,
                    CallbackCompare);
        } // fixed keys

        // Variable keys with or without duplicates
        if (!is_leaf)
          DEF_INTERNAL_NODE(DefLayout::VariableLengthKeyList,
                    CallbackCompare);
        LEAF_NODE_IMPL(DefaultNodeImpl, DefLayout::VariableLengthKeyList,
                    CallbackCompare);
      // BINARY is the default:
      case UPS_TYPE_BINARY:
        // Fixed keys, no duplicates
        if (fixed_keys) {
          if (!is_leaf)
            PAX_INTERNAL_NODE(PaxLayout::BinaryKeyList, FixedSizeCompare);
          LEAF_NODE_IMPL(PaxNodeImpl, PaxLayout::BinaryKeyList,
                    FixedSizeCompare);
        } // fixed keys

        // variable length keys, with and without duplicates
        if (!is_leaf)
          DEF_INTERNAL_NODE(DefLayout::VariableLengthKeyList,
                    VariableSizeCompare);
        LEAF_NODE_IMPL(DefaultNodeImpl, DefLayout::VariableLengthKeyList,
                    VariableSizeCompare);
      default:
        ups_assert(!"shouldn't be here");
        return (0);
    }
  }
};

} // namespace upscaledb

#endif /* UPS_BTREE_INDEX_FACTORY_H */
