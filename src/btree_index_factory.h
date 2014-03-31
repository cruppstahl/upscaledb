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
    // Returns the actual key size (including overhead)
    virtual ham_u16_t get_actual_key_size(ham_u32_t key_size) const {
      return (NodeLayout::get_actual_key_size(key_size));
    }

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
    ham_u32_t page_size = db->get_local_env()->get_page_size();

    typedef FixedLayoutImpl<ham_u16_t, false> FixedLayout16;
    typedef FixedLayoutImpl<ham_u16_t, true> FixedDuplicateLayout16;
    typedef FixedLayoutImpl<ham_u32_t, false> FixedLayout32;
    typedef FixedLayoutImpl<ham_u32_t, true> FixedDuplicateLayout32;
    typedef DefaultLayoutImpl<ham_u16_t, false> DefaultLayout16;
    typedef DefaultLayoutImpl<ham_u16_t, true> DefaultDuplicateLayout16;
    typedef DefaultLayoutImpl<ham_u32_t, false> DefaultLayout32;
    typedef DefaultLayoutImpl<ham_u32_t, true> DefaultDuplicateLayout32;

    typedef DefaultInlineRecordImpl<FixedDuplicateLayout16, true>
                    DefaultInlineRecord16;
    typedef DefaultInlineRecordImpl<FixedDuplicateLayout32, true>
                    DefaultInlineRecord32;
    // internal nodes do not support duplicates
    typedef InternalInlineRecordImpl<FixedLayout16> InternalInlineRecord16; 
    typedef InternalInlineRecordImpl<FixedLayout32> InternalInlineRecord32; 

    // Record number database
    if (flags & HAM_RECORD_NUMBER) {
      if (!is_leaf)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PodKeyList<ham_u64_t>, InternalRecordList>,
                    RecordNumberCompare>());
      if (inline_records)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PodKeyList<ham_u64_t>, InlineRecordList>,
                    RecordNumberCompare>());
      else
        return (new BtreeIndexTraitsImpl
                    <PaxNodeImpl<PodKeyList<ham_u64_t>, DefaultRecordList>,
                    RecordNumberCompare>());
    }

    switch (key_type) {
      // 8bit unsigned integer
      case HAM_TYPE_UINT8:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u8_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<ham_u8_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<ham_u8_t> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u8_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            FixedInlineRecordImpl<FixedDuplicateLayout32> >,
                      NumericCompare<ham_u8_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<ham_u8_t> >());
            }
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u8_t>, InternalRecordList>,
                      NumericCompare<ham_u8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u8_t>, InlineRecordList>,
                      NumericCompare<ham_u8_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u8_t>, DefaultRecordList>,
                      NumericCompare<ham_u8_t> >());
        }
      // 16bit unsigned integer
      case HAM_TYPE_UINT16:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u16_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<ham_u16_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<ham_u16_t> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u16_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            FixedInlineRecordImpl<FixedDuplicateLayout32> >,
                      NumericCompare<ham_u16_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<ham_u16_t> >());
            }
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u16_t>, InternalRecordList>,
                      NumericCompare<ham_u16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u16_t>, InlineRecordList>,
                      NumericCompare<ham_u16_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u16_t>, DefaultRecordList>,
                      NumericCompare<ham_u16_t> >());
        }
      // 32bit unsigned integer
      case HAM_TYPE_UINT32:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u32_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<ham_u32_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<ham_u32_t> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u32_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            FixedInlineRecordImpl<FixedDuplicateLayout32> >,
                      NumericCompare<ham_u32_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<ham_u32_t> >());
            }
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u32_t>, InternalRecordList>,
                      NumericCompare<ham_u32_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u32_t>, InlineRecordList>,
                      NumericCompare<ham_u32_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u32_t>, DefaultRecordList>,
                      NumericCompare<ham_u32_t> >());
        }
      // 64bit unsigned integer
      case HAM_TYPE_UINT64:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u64_t> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<ham_u64_t> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<ham_u64_t> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u64_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<ham_u64_t> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u64_t>, InternalRecordList>,
                      NumericCompare<ham_u64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u64_t>, InlineRecordList>,
                      NumericCompare<ham_u64_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<ham_u64_t>, DefaultRecordList>,
                      NumericCompare<ham_u64_t> >());
        }
      // 32bit float
      case HAM_TYPE_REAL32:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<float> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<float> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<float> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<float> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            FixedInlineRecordImpl<FixedDuplicateLayout32> >,
                      NumericCompare<float> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<float> >());
            }
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<float>, InternalRecordList>,
                      NumericCompare<float> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<float>, InlineRecordList>,
                      NumericCompare<float> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<float>, DefaultRecordList>,
                      NumericCompare<float> >());
        }
      // 64bit double
      case HAM_TYPE_REAL64:
        if (use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<double> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            FixedInlineRecordImpl<FixedDuplicateLayout16> >,
                      NumericCompare<double> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      NumericCompare<double> >());
            }
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<double> >());
            else {
              if (inline_records)
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            FixedInlineRecordImpl<FixedDuplicateLayout32> >,
                      NumericCompare<double> >());
              else
                return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      NumericCompare<double> >());
            }
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<double>, InternalRecordList>,
                      NumericCompare<double> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<double>, InlineRecordList>,
                      NumericCompare<double> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<PodKeyList<double>, DefaultRecordList>,
                      NumericCompare<double> >());
        }
      // Callback function provided by user?
      case HAM_TYPE_CUSTOM:
        // Fixed keys, no duplicates
        if (fixed_keys && !use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, InternalRecordList>,
                      CallbackCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, InlineRecordList>,
                      CallbackCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, DefaultRecordList>,
                      CallbackCompare>());
        }
        // Fixed keys WITH duplicates
        if (fixed_keys && use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      CallbackCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      CallbackCompare >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      CallbackCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      CallbackCompare >());
          }
        }
        // Variable keys with or without duplicates
        if (page_size <= 64 * 1024) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<DefaultLayout16,
                          InternalInlineRecordImpl<DefaultLayout16> >,
                      CallbackCompare>());
          if (inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout16,
                            FixedInlineRecordImpl<DefaultLayout16> >,
                        CallbackCompare>());
          if (inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout16,
                            FixedInlineRecordImpl<DefaultDuplicateLayout16> >,
                        CallbackCompare>());
          if (!inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout16,
                            DefaultInlineRecordImpl<DefaultLayout16, false> >,
                        CallbackCompare>());
          if (!inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout16,
                            DefaultInlineRecordImpl<DefaultDuplicateLayout16, true> >,
                        CallbackCompare>());
          ham_assert(!"shouldn't be here");
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            InternalInlineRecordImpl<DefaultLayout32> >,
                        CallbackCompare>());
          if (inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            FixedInlineRecordImpl<DefaultLayout32> >,
                        CallbackCompare>());
          if (inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout32,
                            FixedInlineRecordImpl<DefaultDuplicateLayout32> >,
                        CallbackCompare>());
          if (!inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            DefaultInlineRecordImpl<DefaultLayout32, false> >,
                        CallbackCompare>());
          if (!inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout32,
                            DefaultInlineRecordImpl<DefaultDuplicateLayout32, true> >,
                        CallbackCompare>());
          ham_assert(!"shouldn't be here");
        }
      // BINARY is the default:
      case HAM_TYPE_BINARY:
        // Fixed keys, no duplicates
        if (fixed_keys && !use_duplicates) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, InternalRecordList>,
                      FixedSizeCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, InlineRecordList>,
                      FixedSizeCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeImpl<BinaryKeyList, DefaultRecordList>,
                      FixedSizeCompare>());
        }
        // fixed keys with duplicates
        if (fixed_keys && use_duplicates) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout16, InternalInlineRecord16>,
                      FixedSizeCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout16,
                            DefaultInlineRecord16>,
                      FixedSizeCompare >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedLayout32, InternalInlineRecord32>,
                      FixedSizeCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeImpl<FixedDuplicateLayout32,
                            DefaultInlineRecord32>,
                      FixedSizeCompare >());
          }
        }
        // variable length keys, with and without duplicates
        if (page_size <= 64 * 1024) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout16,
                            InternalInlineRecordImpl<DefaultLayout16> >,
                        VariableSizeCompare>());
          if (inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout16,
                            FixedInlineRecordImpl<DefaultLayout16> >,
                        VariableSizeCompare>());
          if (inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout16,
                            FixedInlineRecordImpl<DefaultDuplicateLayout16> >,
                        VariableSizeCompare>());
          if (!inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout16,
                            DefaultInlineRecordImpl<DefaultLayout16, false> >,
                        VariableSizeCompare>());
          if (!inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout16,
                            DefaultInlineRecordImpl<DefaultDuplicateLayout16, true> >,
                        VariableSizeCompare>());
          ham_assert(!"shouldn't be here");
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            InternalInlineRecordImpl<DefaultLayout32> >,
                        VariableSizeCompare>());
          if (inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            FixedInlineRecordImpl<DefaultLayout32> >,
                        VariableSizeCompare>());
          if (inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout32,
                            FixedInlineRecordImpl<DefaultDuplicateLayout32> >,
                        VariableSizeCompare>());
          if (!inline_records && !use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultLayout32,
                            DefaultInlineRecordImpl<DefaultLayout32, false> >,
                        VariableSizeCompare>());
          if (!inline_records && use_duplicates)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeImpl<DefaultDuplicateLayout32,
                            DefaultInlineRecordImpl<DefaultDuplicateLayout32, true> >,
                        VariableSizeCompare>());
          ham_assert(!"shouldn't be here");
        }
      default:
        break;
    }

    ham_assert(!"shouldn't be here");
    return (0);
  }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_FACTORY_H__ */
