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

#ifndef HAM_BTREE_INDEX_FACTORY_H
#define HAM_BTREE_INDEX_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_index.h"
#include "3btree/btree_impl_default.h"
#include "3btree/btree_impl_pax.h"
#include "3btree/btree_keys_pod.h"
#include "3btree/btree_keys_binary.h"
#include "3btree/btree_keys_varlen.h"
#include "3btree/btree_records_default.h"
#include "3btree/btree_records_inline.h"
#include "3btree/btree_records_internal.h"
#include "3btree/btree_records_duplicate.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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
  static BtreeIndexTraits *create(LocalDatabase *db, uint32_t flags,
                uint16_t key_type, uint16_t key_size, bool is_leaf) {
    bool inline_records = (is_leaf && (flags & HAM_FORCE_RECORDS_INLINE));
    bool fixed_keys = (key_size != HAM_KEY_SIZE_UNLIMITED);
    bool use_duplicates = (flags & HAM_ENABLE_DUPLICATES) != 0;

    switch (key_type) {
      // 8bit unsigned integer
      case HAM_TYPE_UINT8:
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
      case HAM_TYPE_UINT16:
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
      case HAM_TYPE_UINT32:
        if (use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                    PaxNodeImpl<PaxLayout::PodKeyList<uint32_t>,
                          PaxLayout::InternalRecordList>,
                    NumericCompare<uint32_t> >());
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
        else {
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
      // 64bit unsigned integer
      case HAM_TYPE_UINT64:
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

#endif /* HAM_BTREE_INDEX_FACTORY_H */
