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
#include "btree_node_default.h"
#include "btree_node_pax.h"
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
    ham_u32_t page_size = db->get_local_env()->get_page_size();

    typedef FixedLayoutImpl<ham_u16_t> FixedLayout16;
    typedef FixedLayoutImpl<ham_u32_t> FixedLayout32;
    typedef DefaultLayoutImpl<ham_u16_t> DefaultLayout16;
    typedef DefaultLayoutImpl<ham_u32_t> DefaultLayout32;
    typedef DefaultInlineRecordImpl<FixedLayout16> DefaultInlineRecord16; 
    typedef DefaultInlineRecordImpl<FixedLayout32> DefaultInlineRecord32; 
    typedef InternalInlineRecordImpl<FixedLayout16> InternalInlineRecord16; 
    typedef InternalInlineRecordImpl<FixedLayout32> InternalInlineRecord32; 

    // Record number database
    if (flags & HAM_RECORD_NUMBER) {
      if (!is_leaf)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeLayout<PodKeyList<ham_u64_t>, InternalRecordList>,
                    RecordNumberCompare>());
      if (inline_records)
        return (new BtreeIndexTraitsImpl
                    <PaxNodeLayout<PodKeyList<ham_u64_t>, InlineRecordList>,
                    RecordNumberCompare>());
      else
        return (new BtreeIndexTraitsImpl
                    <PaxNodeLayout<PodKeyList<ham_u64_t>, DefaultRecordList>,
                    RecordNumberCompare>());
    }

    switch (key_type) {
      // 8bit unsigned integer
      case HAM_TYPE_UINT8:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u8_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<ham_u8_t> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u8_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<ham_u8_t> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u8_t>, InternalRecordList>,
                      NumericCompare<ham_u8_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u8_t>, InlineRecordList>,
                      NumericCompare<ham_u8_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u8_t>, DefaultRecordList>,
                      NumericCompare<ham_u8_t> >());
        }
      // 16bit unsigned integer
      case HAM_TYPE_UINT16:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u16_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<ham_u16_t> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u16_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<ham_u16_t> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u16_t>, InternalRecordList>,
                      NumericCompare<ham_u16_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u16_t>, InlineRecordList>,
                      NumericCompare<ham_u16_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u16_t>, DefaultRecordList>,
                      NumericCompare<ham_u16_t> >());
        }
      // 32bit unsigned integer
      case HAM_TYPE_UINT32:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u32_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<ham_u32_t> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u32_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<ham_u32_t> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u32_t>, InternalRecordList>,
                      NumericCompare<ham_u32_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u32_t>, InlineRecordList>,
                      NumericCompare<ham_u32_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u32_t>, DefaultRecordList>,
                      NumericCompare<ham_u32_t> >());
        }
      // 64bit unsigned integer
      case HAM_TYPE_UINT64:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<ham_u64_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<ham_u64_t> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<ham_u64_t> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<ham_u64_t> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u64_t>, InternalRecordList>,
                      NumericCompare<ham_u64_t> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u64_t>, InlineRecordList>,
                      NumericCompare<ham_u64_t> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<ham_u64_t>, DefaultRecordList>,
                      NumericCompare<ham_u64_t> >());
        }
      // 32bit float
      case HAM_TYPE_REAL32:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<float> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<float> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<float> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<float> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<float>, InternalRecordList>,
                      NumericCompare<float> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<float>, InlineRecordList>,
                      NumericCompare<float> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<float>, DefaultRecordList>,
                      NumericCompare<float> >());
        }
      // 64bit double
      case HAM_TYPE_REAL64:
        if (flags & HAM_ENABLE_DUPLICATES) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      NumericCompare<double> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      NumericCompare<double> >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      NumericCompare<double> >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      NumericCompare<double> >());
          }
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<double>, InternalRecordList>,
                      NumericCompare<double> >());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<double>, InlineRecordList>,
                      NumericCompare<double> >());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<PodKeyList<double>, DefaultRecordList>,
                      NumericCompare<double> >());
        }
      // Callback function provided by user?
      case HAM_TYPE_CUSTOM:
        // Fixed keys, no duplicates
        if (fixed_keys && (flags & HAM_ENABLE_DUPLICATES) == 0) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, InternalRecordList>,
                      CallbackCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, InlineRecordList>,
                      CallbackCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, DefaultRecordList>,
                      CallbackCompare>());
        }
        // Fixed keys WITH duplicates
        if (fixed_keys && (flags & HAM_ENABLE_DUPLICATES) != 0) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      CallbackCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      CallbackCompare >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      CallbackCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      CallbackCompare >());
          }
        }
        // Variable keys with or without duplicates
        if (page_size <= 64 * 1024) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<DefaultLayout16,
                          InternalInlineRecordImpl<DefaultLayout16> >,
                      CallbackCompare>());
          if (inline_records && (flags & HAM_ENABLE_DUPLICATES) == 0)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout16,
                            FixedInlineRecordImpl<DefaultLayout16> >,
                        CallbackCompare>());
          else
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout16,
                            DefaultInlineRecordImpl<DefaultLayout16> >,
                        CallbackCompare>());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            InternalInlineRecordImpl<DefaultLayout32> >,
                        CallbackCompare>());
          if (inline_records && (flags & HAM_ENABLE_DUPLICATES) == 0)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            FixedInlineRecordImpl<DefaultLayout32> >,
                        CallbackCompare>());
          else
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            DefaultInlineRecordImpl<DefaultLayout32> >,
                        CallbackCompare>());
        }
      // BINARY is the default:
      case HAM_TYPE_BINARY:
        // Fixed keys, no duplicates
        if (fixed_keys && (flags & HAM_ENABLE_DUPLICATES) == 0) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, InternalRecordList>,
                      FixedSizeCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, InlineRecordList>,
                      FixedSizeCompare>());
          else
            return (new BtreeIndexTraitsImpl
                      <PaxNodeLayout<BinaryKeyList, DefaultRecordList>,
                      FixedSizeCompare>());
        }
        // fixed keys with duplicates
        if (fixed_keys && (flags & HAM_ENABLE_DUPLICATES) != 0) {
          if (page_size <= 64 * 1024) {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, InternalInlineRecord16>,
                      FixedSizeCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout16, DefaultInlineRecord16>,
                      FixedSizeCompare >());
          }
          else {
            if (!is_leaf)
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, InternalInlineRecord32>,
                      FixedSizeCompare >());
            else
              return (new BtreeIndexTraitsImpl<
                      DefaultNodeLayout<FixedLayout32, DefaultInlineRecord32>,
                      FixedSizeCompare >());
          }
        }
        // without duplicates
        if (page_size <= 64 * 1024) {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout16,
                            InternalInlineRecordImpl<DefaultLayout16> >,
                        VariableSizeCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout16,
                            FixedInlineRecordImpl<DefaultLayout16> >,
                        VariableSizeCompare>());
          else
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout16,
                            DefaultInlineRecordImpl<DefaultLayout16> >,
                        VariableSizeCompare>());
        }
        else {
          if (!is_leaf)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            InternalInlineRecordImpl<DefaultLayout32> >,
                        VariableSizeCompare>());
          if (inline_records)
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            FixedInlineRecordImpl<DefaultLayout32> >,
                        VariableSizeCompare>());
          else
            return (new BtreeIndexTraitsImpl<
                        DefaultNodeLayout<DefaultLayout32,
                            DefaultInlineRecordImpl<DefaultLayout32> >,
                        VariableSizeCompare>());
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
