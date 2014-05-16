/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BTREE_INDEX_FACTORY_H__
#define HAM_BTREE_INDEX_FACTORY_H__

#include "db_local.h"
#include "btree_index.h"
#include "btree_impl_default.h"
#include "btree_impl_pax.h"
#include "btree_node_proxy.h"


namespace hamsterdb {

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
    virtual int compare_keys(LocalDatabase *db, ham_key_t *lhs,
            ham_key_t *rhs) const {
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
  static BtreeIndexTraits *create(LocalDatabase *db, ham_u32_t flags,
                ham_u16_t key_type, ham_u16_t key_size, bool is_leaf) {
    bool inline_records = (is_leaf && (flags & HAM_FORCE_RECORDS_INLINE));
    bool fixed_keys = (key_size != HAM_KEY_SIZE_UNLIMITED);
    bool use_duplicates = (flags & HAM_ENABLE_DUPLICATES) != 0;

    // Record number database
    if (flags & HAM_RECORD_NUMBER) {
      if (!is_leaf)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                        PaxLayout::InternalRecordList>,
                    RecordNumberCompare>());
      if (inline_records)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                        PaxLayout::InlineRecordList>,
                    RecordNumberCompare>());
      else
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                        PaxLayout::DefaultRecordList>,
                    RecordNumberCompare>());
    }

    switch (key_type) {
      // 8bit unsigned integer
      case HAM_TYPE_UINT8:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<ham_u8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<ham_u8_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<ham_u8_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      PaxNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<ham_u8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<ham_u8_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u8_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<ham_u8_t> >());
        }
      // 16bit unsigned integer
      case HAM_TYPE_UINT16:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<ham_u16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<ham_u16_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<ham_u16_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<ham_u16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<ham_u16_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u16_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<ham_u16_t> >());
        }
      // 32bit unsigned integer
      case HAM_TYPE_UINT32:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<ham_u32_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<ham_u32_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<ham_u32_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<ham_u32_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<ham_u32_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u32_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<ham_u32_t> >());
        }
      // 64bit unsigned integer
      case HAM_TYPE_UINT64:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<ham_u64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                        DefLayout::DuplicateInlineRecordList>,
                  NumericCompare<ham_u64_t> >());
          else
            return (new BtreeIndexTraitsImpl<
                  DefaultNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                        DefLayout::DuplicateDefaultRecordList>,
                  NumericCompare<ham_u64_t> >());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                            PaxLayout::InternalRecordList>,
                      NumericCompare<ham_u64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                            PaxLayout::InlineRecordList>,
                      NumericCompare<ham_u64_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PaxLayout::PodKeyList<ham_u64_t>,
                            PaxLayout::DefaultRecordList>,
                      NumericCompare<ham_u64_t> >());
        }
      // 32bit float
      case HAM_TYPE_REAL32:
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
      case HAM_TYPE_REAL64:
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
      case HAM_TYPE_CUSTOM:
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
        ham_assert(!"shouldn't be here");
      // BINARY is the default:
      case HAM_TYPE_BINARY:
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
        ham_assert(!"shouldn't be here");
      default:
        break;
    }

    ham_assert(!"shouldn't be here");
    return (0);
  }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_FACTORY_H__ */
