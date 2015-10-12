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
 * @exception_safe: unknown
 * @thread_safe: unknown
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

//
// A BtreeIndexFactory creates BtreeIndexProxy objects depending on the
// Database configuration
//
struct BtreeIndexFactory
{
  static BtreeIndexTraits *create(LocalDatabase *db, uint32_t flags,
                uint16_t key_type, uint16_t key_size, bool is_leaf) {
    bool inline_records = (is_leaf && (flags & UPS_FORCE_RECORDS_INLINE));
    bool fixed_keys = (key_size != UPS_KEY_SIZE_UNLIMITED);
    bool use_duplicates = (flags & UPS_ENABLE_DUPLICATES) != 0;
    int key_compression = db->config().key_compressor;

    switch (key_type) {
      // 8bit unsigned integer
      case UPS_TYPE_UINT8:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<uint8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<uint8_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<uint8_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      PaxNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<uint8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<uint8_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint8_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<uint8_t> >());
        }
      // 16bit unsigned integer
      case UPS_TYPE_UINT16:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<uint16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<uint16_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<uint16_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<uint16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<uint16_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint16_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<uint16_t> >());
        }
      // 32bit unsigned integer
      case UPS_TYPE_UINT32:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<uint32_t> >());
          if (key_compression == UPS_COMPRESSOR_UINT32_VARBYTE) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::VarbyteKeyList,
                              DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::VarbyteKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_SIMDCOMP) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdCompKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                      NumericCompare<uint32_t> >());
          else
            return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdCompKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_FOR) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::ForKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::ForKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                      NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_SIMDFOR) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdForKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdForKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_GROUPVARINT) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::GroupVarintKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::GroupVarintKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_STREAMVBYTE) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::StreamVbyteKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::StreamVbyteKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_MASKEDVBYTE) {
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::MaskedVbyteKeyList,
                            DefLayout::DuplicateInlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::MaskedVbyteKeyList,
                            DefLayout::DuplicateDefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          // no key compression
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<uint32_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<uint32_t> >());
        }
        // duplicates are disabled
        else {
          if (key_compression == UPS_COMPRESSOR_UINT32_VARBYTE) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                          <DefaultNodeImpl<Zint32::VarbyteKeyList,
                                PaxLayout::InlineRecordList>,
                          NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::VarbyteKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_SIMDCOMP) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdCompKeyList,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<uint32_t> >());
          else
            return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdCompKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_FOR) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::ForKeyList,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::ForKeyList,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_SIMDFOR) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdForKeyList,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::SimdForKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_GROUPVARINT) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::GroupVarintKeyList,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::GroupVarintKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_STREAMVBYTE) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::StreamVbyteKeyList,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::StreamVbyteKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else if (key_compression == UPS_COMPRESSOR_UINT32_MASKEDVBYTE) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::MaskedVbyteKeyList,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <DefaultNodeImpl<Zint32::MaskedVbyteKeyList,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
          else { // no key compression
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InternalRecordList>,
                        NumericCompare<uint32_t> >());
            if (inline_records)
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::InlineRecordList>,
                        NumericCompare<uint32_t> >());
            else
              return (new BtreeIndexTraitsImpl
                        <PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                              PaxLayout::DefaultRecordList>,
                        NumericCompare<uint32_t> >());
          }
        }
      // 64bit unsigned integer
      case UPS_TYPE_UINT64:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<uint64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<uint64_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<uint64_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      PaxNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<uint64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<uint64_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<uint64_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<uint64_t> >());
        }
      // 32bit float
      case UPS_TYPE_REAL32:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<float>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<float> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<float>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<float> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<float>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<float> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<float>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<float> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<float>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<float> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<float>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<float> >());
        }
      // 64bit double
      case UPS_TYPE_REAL64:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<double>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<double> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<double>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<double> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<double>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<double> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<double>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<double> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<double>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<double> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<double>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<double> >());
        }
      // Callback function provided by user?
      case UPS_TYPE_CUSTOM:
        // Fixed keys, no duplicates
        if (fixed_keys && !use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::InternalRecordList>,
                      CallbackCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::InlineRecordList>,
                      CallbackCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::DefaultRecordList>,
                      CallbackCompare>());
        }
        // Fixed keys WITH duplicates
        if (fixed_keys && use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::BinaryKeyList,
                          PaxLayout::InternalRecordList>,
                    CallbackCompare >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::BinaryKeyList,
                        DefLayout::DuplicateInlineRecordList>,
                  CallbackCompare >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::BinaryKeyList,
                        DefLayout::DuplicateDefaultRecordList>,
                  CallbackCompare >());
        }
        // Variable keys with or without duplicates
        if (!is_leaf)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::InternalRecordList>,
                  CallbackCompare >());
        if (inline_records && !use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::InlineRecordList>,
                  CallbackCompare >());
        if (inline_records && use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        DefLayout::DuplicateInlineRecordList>,
                  CallbackCompare >());
        if (!inline_records && !use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::DefaultRecordList>,
                  CallbackCompare >());
        if (!inline_records && use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        DefLayout::DuplicateDefaultRecordList>,
                  CallbackCompare >());
        ups_assert(!"shouldn't be here");
      // BINARY is the default:
      case UPS_TYPE_BINARY:
        // Fixed keys, no duplicates
        if (fixed_keys && !use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::InternalRecordList>,
                      FixedSizeCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::InlineRecordList>,
                      FixedSizeCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::BinaryKeyList,
                            PaxLayout::DefaultRecordList>,
                      FixedSizeCompare>());
        }
        // fixed keys with duplicates
        if (fixed_keys && use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::BinaryKeyList,
                          PaxLayout::InternalRecordList>,
                    FixedSizeCompare >());
          if (inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                    DefaultNodeImpl<PaxLayout::BinaryKeyList,
                          DefLayout::DuplicateInlineRecordList>,
                    FixedSizeCompare >());
          if (!inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                    DefaultNodeImpl<PaxLayout::BinaryKeyList,
                          DefLayout::DuplicateDefaultRecordList>,
                    FixedSizeCompare >());
        }
        // variable length keys, with and without duplicates
        if (!is_leaf)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::InternalRecordList>,
                  VariableSizeCompare >());
        if (inline_records && !use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::InlineRecordList>,
                  VariableSizeCompare >());
        if (inline_records && use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        DefLayout::DuplicateInlineRecordList>,
                  VariableSizeCompare >());
        if (!inline_records && !use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        PaxLayout::DefaultRecordList>,
                  VariableSizeCompare >());
        if (!inline_records && use_duplicates)
          return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<DefLayout::VariableLengthKeyList,
                        DefLayout::DuplicateDefaultRecordList>,
                  VariableSizeCompare >());
        ups_assert(!"shouldn't be here");
      default:
        break;
    }

    ups_assert(!"shouldn't be here");
    return (0);
  }
};

} // namespace upscaledb

#endif /* UPS_BTREE_INDEX_FACTORY_H */
