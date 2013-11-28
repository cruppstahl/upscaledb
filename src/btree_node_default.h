/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * Btree node layout for variable length keys/records and duplicates
 * =================================================================
 *
 * This is the default hamsterdb layout. It is chosen for
 * 1. variable length keys (with or without duplicates)
 * 2. fixed length keys with duplicates
 *
 * Unlike the PAX layout implemented in btree_node_pax.h, the layout implemented
 * here stores key data and records next to each other. However, since keys
 * (and duplicate records) have variable length, each node has a small
 * index area upfront. This index area stores metadata about the key like
 * the key's size, the number of duplicates, flags and the offset of the
 * actual key data.
 *
 * To avoid expensive memcpy-operations, erasing a key only affects this
 * index table: the relevant index is moved to a "freelist". This freelist
 * contains the same meta information as the index table.
 *
 * The flat memory layout looks like this:
 *
 * |Idx1|Idx2|...|Idxn|F1|F2|...|Fn|...(space)...|Key1Rec1|Key2Rec2|...|
 *
 * In addition, the first few bytes in the node store the following
 * information:
 *   0  (4 bytes): total capacity of index keys (used keys + freelist)
 *   4  (4 bytes): number of used freelist entries
 *   8  (4 bytes): offset for the next key at the end of the page
 *
 * In total, |capacity| contains the number of maximum keys (and index
 * entries) that can be stored in the node. The number of used index keys
 * is in |m_node->get_count()|. The number of used freelist entries is
 * |freelist_count|. The freelist indices start directly after the key
 * indices. The key space (with key data and records) starts at
 * N * capacity, where |N| is the size of an index entry (the size depends
 * on the actual btree configuration, i.e. whether key size is fixed,
 * duplicates are used etc).
 *
 * If keys exceed a certain Threshold (g_extended_threshold), they're moved
 * to a blob and the flag |kExtended| is set for this key. These extended
 * keys are cached in a std::map. Otherwise performance is simply way too
 * poor!
 */

#ifndef HAM_BTREE_NODE_DEFAULT_H__
#define HAM_BTREE_NODE_DEFAULT_H__

#include <algorithm>
#include <vector>
#include <map>

#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "duplicates.h"
#include "env_local.h"
#include "btree_index.h"

namespace hamsterdb {

#undef min  // avoid MSVC conflicts with std::min

template<typename LayoutImpl, typename RecordImpl>
class DefaultNodeLayout;

//
// An iterator for the DefaultNodeLayout
//
template<typename LayoutImpl, typename RecordImpl>
class DefaultIterator
{
  public:
    DefaultIterator(DefaultNodeLayout<LayoutImpl, RecordImpl> *node,
                    ham_u32_t slot)
      : m_node(node), m_slot(slot) {
    } 

    DefaultIterator(const DefaultNodeLayout<LayoutImpl, RecordImpl> *node,
                    ham_u32_t slot)
      : m_node((DefaultNodeLayout<LayoutImpl, RecordImpl> *)node),
        m_slot(slot) {
    } 

    void next() {
      m_slot++;
    }

    // Returns the slot
    int get_slot() const {
      return (m_slot);
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_key_flags() const {
      return (m_node->get_key_flags(m_slot));
    }

    // Sets the flags of a key; defined in btree_key.h
    void set_key_flags(ham_u32_t flags) {
      m_node->set_key_flags(m_slot, flags);
    }

    // Returns the logical key size
    ham_u16_t get_key_size() const {
      return (m_node->get_key_size(m_slot));
    }

    // Sets the logical key size
    void set_key_size(ham_u16_t size) {
      return (m_node->set_key_size(m_slot, size));
    }

    // Returns the actually used size of the key
    ham_u16_t get_key_data_size() const {
      return (m_node->get_key_data_size(m_slot));
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key_data() {
      return (m_node->get_key_data(m_slot));
    }

    // Returns a pointer to the key data
    const ham_u8_t *get_key_data() const {
      return (m_node->get_key_data(m_slot));
    }

    // Overwrites the key data
    void set_key_data(const void *ptr, ham_u32_t len) {
      return (m_node->set_key_data(m_slot, ptr, len));
    }
  
    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_blob_id() const {
      ham_u64_t rid = *(ham_u64_t *)get_key_data();
      return (ham_db2h_offset(rid));
    }

    // Sets the record address of an extended key overflow area
    void set_extended_blob_id(ham_u64_t rid) {
      rid = ham_h2db_offset(rid);
      *(ham_u64_t *)get_key_data() = rid;
    }

    // Returns the record id
    ham_u64_t get_record_id() const {
      return (m_node->get_record_id(m_slot));
    }

    // Sets the record id
    void set_record_id(ham_u64_t ptr) {
      return (m_node->set_record_id(m_slot, ptr));
    }

    // Returns true if the record is inline
    bool is_record_inline() const {
      return (m_node->is_record_inline(m_slot));
    }

    // Returns a pointer to the record's inline data
    void *get_inline_record_data() {
      return (m_node->get_inline_record_data(m_slot));
    }

    // Returns a pointer to the record's inline data
    void *get_inline_record_data() const {
      ham_assert(is_record_inline() == true);
      return (m_node->get_inline_record_data(m_slot));
    }

    // Sets the inline record data
    void set_inline_record_data(const void *data, ham_u32_t size) {
      m_node->set_inline_record_data(m_slot, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size() const {
      return (m_node->get_inline_record_size(m_slot));
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (m_node->get_max_inline_record_size());
    }

    // Removes an inline record
    void remove_inline_record() {
      memset(get_inline_record_data(), 0, get_max_inline_record_size());;
    }

    // Allows use of operator-> in the caller
    DefaultIterator *operator->() {
      return (this);
    }

    // Allows use of operator-> in the caller
    const DefaultIterator *operator->() const {
      return (this);
    }

  private:
    DefaultNodeLayout<LayoutImpl, RecordImpl> *m_node;
    int m_slot;
};

//
// A LayoutImplementation for fixed size keys WITH duplicates
// This layout will be removed as soon as duplicates are stored in the node
// instead of an external duplicate table.
//
template<typename Offset>
class FixedLayoutImpl
{
    enum {
      kSpan = 1 + sizeof(Offset) // 1 byte flags + 2 (or 4) byte offset
    };

  public:
    void initialize(ham_u8_t *dataptr, ham_u32_t key_size) {
      m_dataptr = dataptr;
      m_key_size = key_size;
      // this layout only works with fixed sizes!
      ham_assert(m_key_size != HAM_KEY_SIZE_UNLIMITED);
    }

    // Returns a pointer to this key's index
    ham_u8_t *get_key_index_ptr(int slot) {
      return (&m_dataptr[kSpan * slot]);
    }

    // Returns the memory span from one key to the next
    ham_u32_t get_key_index_span() const {
      return (kSpan);
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_key_flags(int slot) const {
      return (m_dataptr[kSpan * slot]);
    }

    // Sets the flags of a key; defined in btree_key.h
    void set_key_flags(int slot, ham_u8_t flags) {
      m_dataptr[kSpan * slot] = flags;
    }

    // Returns the size of the key
    ham_u16_t get_key_size(int slot) const {
      return (m_key_size);
    }

    // Sets the size of the key
    void set_key_size(int slot, ham_u16_t size) {
      ham_assert(size == m_key_size);
    }

    // Sets the start offset of the key data
    void set_key_data_offset(int slot, ham_u32_t offset) {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 1];
      if (sizeof(Offset) == 4)
        *(ham_u32_t *)p = ham_h2db32(offset);
      else
        *(ham_u16_t *)p = ham_h2db16((ham_u16_t)offset);
    }

    // Returns the start offset of the key data
    ham_u32_t get_key_data_offset(int slot) const {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 1];
      if (sizeof(Offset) == 4)
        return (ham_db2h32(*(ham_u32_t *)p));
      else
        return (ham_db2h16(*(ham_u16_t *)p));
    }

  private:
    // the serialized data
    ham_u8_t *m_dataptr;

    // the constant key size
    ham_u16_t m_key_size;
};

//
// A LayoutImplementation for variable size keys without duplicates
//
template<typename Offset>
class DefaultLayoutImpl
{
    enum {
      // 1 byte flags + 2 byte key size + 2 (or 4) byte offset
      kSpan = 3 + sizeof(Offset)
    };

  public:
    void initialize(ham_u8_t *dataptr, ham_u32_t key_size) {
      m_dataptr = dataptr;
      // this layout only works with unlimited/variable sizes!
      ham_assert(key_size == HAM_KEY_SIZE_UNLIMITED);
    }

    // Returns a pointer to this key's index
    ham_u8_t *get_key_index_ptr(int slot) {
      return (&m_dataptr[kSpan * slot]);
    }

    // Returns the memory span from one key to the next
    ham_u32_t get_key_index_span() const {
      return (kSpan);
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_key_flags(int slot) const {
      return (m_dataptr[kSpan * slot]);
    }

    // Sets the flags of a key; defined in btree_key.h
    void set_key_flags(int slot, ham_u8_t flags) {
      m_dataptr[kSpan * slot] = flags;
    }

    // Returns the size of a key
    ham_u16_t get_key_size(int slot) const {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 1];
      return (ham_db2h16(*(ham_u16_t *)p));
    }

    // Sets the size of a key
    void set_key_size(int slot, ham_u16_t size) {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 1];
      *(ham_u16_t *)p = ham_h2db16(size);
    }

    // Sets the start offset of the key data
    void set_key_data_offset(int slot, ham_u32_t offset) {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 3];
      if (sizeof(Offset) == 4)
        *(ham_u32_t *)p = ham_h2db32(offset);
      else
        *(ham_u16_t *)p = ham_h2db16((ham_u16_t)offset);
    }

    // Returns the start offset of the key data
    ham_u32_t get_key_data_offset(int slot) const {
      ham_u8_t *p = &m_dataptr[kSpan * slot + 3];
      if (sizeof(Offset) == 4)
        return (ham_db2h32(*(ham_u32_t *)p));
      else
        return (ham_db2h16(*(ham_u16_t *)p));
    }

  private:
    // the serialized data
    ham_u8_t *m_dataptr;
};

struct SortHelper {
  ham_u32_t offset;
  ham_u32_t slot;

  bool operator<(const SortHelper &rhs) const {
    return (offset < rhs.offset);
  }
};

static bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs) {
  return (lhs.offset < rhs.offset);
}

// move every key > threshold to a blob
extern ham_u32_t g_extended_threshold;

//
// A RecordProxy for the default inline records
//
template<typename LayoutImpl>
class DefaultInlineRecordImpl
{
    typedef DefaultNodeLayout<LayoutImpl, DefaultInlineRecordImpl> NodeType;

  public:
    DefaultInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_layout(layout) {
    }

    DefaultInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_layout((NodeType *)layout) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot) const {
      ham_u32_t flags = m_layout->get_key_flags(slot);
      return ((flags & BtreeKey::kBlobSizeTiny)
              || (flags & BtreeKey::kBlobSizeSmall)
              || (flags & BtreeKey::kBlobSizeEmpty) != 0);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size) {
      ham_u32_t flags = m_layout->get_key_flags(slot);
      // make sure that the flags are zeroed out
      flags &= ~(BtreeKey::kBlobSizeSmall
                      | BtreeKey::kBlobSizeTiny
                      | BtreeKey::kBlobSizeEmpty);
      if (size == 0) {
        flags |= BtreeKey::kBlobSizeEmpty;
      }
      else if (size < 8) {
        flags |= BtreeKey::kBlobSizeTiny;
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)m_layout->get_inline_record_data(slot);
        p[sizeof(ham_u64_t) - 1] = size;
        memcpy(p, data, size);
      }
      else if (size == 8) {
        flags |= BtreeKey::kBlobSizeSmall;
        char *p = (char *)m_layout->get_inline_record_data(slot);
        memcpy(p, data, size);
      }
      else
        ham_assert(!"shouldn't be here");
      m_layout->set_key_flags(slot, flags);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      ham_assert(m_layout->is_record_inline(slot) == true);
      ham_u32_t flags = m_layout->get_key_flags(slot);
      if (flags & BtreeKey::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)m_layout->get_inline_record_data(slot);
        return (p[sizeof(ham_u64_t) - 1]);
      }
      else if (flags & BtreeKey::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      else if (flags & BtreeKey::kBlobSizeEmpty)
        return (0);
      else
        ham_assert(!"shouldn't be here");
      return (0);
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

  private:
    NodeType *m_layout;
};

//
// A RecordProxy for fixed length inline records
//
template<typename LayoutImpl>
class FixedInlineRecordImpl
{
    typedef DefaultNodeLayout<LayoutImpl, FixedInlineRecordImpl> NodeType;

  public:
    FixedInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_layout(layout), m_record_size(record_size) {
    }

    FixedInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_layout((NodeType *)layout), m_record_size(record_size) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot) const {
      return (true);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size) {
      ham_assert(size == m_record_size);
      char *p = (char *)m_layout->get_inline_record_data(slot);
      memcpy(p, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      return (m_record_size);
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (m_record_size);
    }

  private:
    NodeType *m_layout;
    ham_u32_t m_record_size;
};

//
// A RecordProxy for fixed length inline records of size 8 (for internal
// nodes)
//
template<typename LayoutImpl>
class InternalInlineRecordImpl
{
    typedef DefaultNodeLayout<LayoutImpl, InternalInlineRecordImpl> NodeType;

  public:
    InternalInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_layout(layout) {
    }

    InternalInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_layout((NodeType *)layout) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot) const {
      return (true);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size) {
      ham_assert(size == sizeof(ham_u64_t));
      char *p = (char *)m_layout->get_inline_record_data(slot);
      memcpy(p, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      return (sizeof(ham_u64_t));
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

  private:
    NodeType *m_layout;
};

//
// A BtreeNodeProxy layout which stores key flags, key size, key data
// and the record pointer next to each other.
// This is the format used since the initial hamsterdb version.
//
template<typename LayoutImpl, typename RecordImpl>
class DefaultNodeLayout
{
    typedef std::map<ham_u64_t, ByteArray> ExtKeyCache;

    enum {
      kPayloadOffset = 12,      // for capacity, freelist_count, next_offset
      kRearrangeThreshold = 5   // only rearrange if freelist_count > 32
    };

  public:
    typedef DefaultIterator<LayoutImpl, RecordImpl> Iterator;
    typedef const DefaultIterator<LayoutImpl, RecordImpl> ConstIterator;

    DefaultNodeLayout(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(m_page)),
        m_record_proxy(this, m_page->get_db()->get_record_size()),
        m_extkey_cache(0) {
      initialize();
    }

    ~DefaultNodeLayout() {
      clear_extkey_cache();
    }

    // Returns the actual key size (including overhead, without record)
    static ham_u16_t get_actual_key_size(ham_u32_t key_size) {
      // unlimited/variable keys require 5 bytes for flags + key size + offset;
      // assume an average key size of 32 bytes (this is a random guess, but
      // will be good enough)
      if (key_size == HAM_KEY_SIZE_UNLIMITED)
        return ((ham_u16_t)32 - 8);// + 5 - 8

      // otherwise 1 byte for flags
      return ((ham_u16_t)(key_size + 1));
    }

    Iterator begin() {
      return (at(0));
    }

    Iterator begin() const {
      return (at(0));
    }

    // note that this function does not check the boundaries (i.e. whether
    // i <= get_count(), because some functions deliberately write to
    // elements "after" get_count()
    Iterator at(int slot) {
      return (Iterator(this, slot));
    }

    ConstIterator at(int slot) const {
      return (ConstIterator(this, slot));
    }

    void check_integrity() const {
      if (m_node->get_count() == 0)
        return;

      ByteArray arena;
      ham_u32_t count = m_node->get_count();
      Iterator it = begin();
      for (ham_u32_t i = 0; i < count; i++, it->next()) {
        // internal nodes: only allowed flag is kExtended
        if ((it->get_key_flags() != 0
            && it->get_key_flags() != BtreeKey::kExtended)
            && !m_node->is_leaf()) {
          ham_log(("integrity check failed in page 0x%llx: item #0 "
                  "has flags but it's not a leaf page",
                  m_page->get_address()));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (it->get_key_size() > g_extended_threshold
            && !(it->get_key_flags() & BtreeKey::kExtended)) {
          ham_log(("key size %d, but is not extended", it->get_key_size()));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (it->get_key_flags() & BtreeKey::kExtended) {
          ham_u64_t blobid = it->get_extended_blob_id();
          if (!blobid) {
            ham_log(("integrity check failed in page 0x%llx: item "
                    "is extended, but has no blob", m_page->get_address()));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }

          // make sure that the extended blob can be loaded
          ham_record_t record = {0};
          m_page->get_db()->get_local_env()->get_blob_manager()->read(
                          m_page->get_db(), blobid, &record, 0, &arena);

          // compare it to the cached key (if there is one)
          if (m_extkey_cache) {
            ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
            if (it != m_extkey_cache->end()) {
              if (record.size != it->second.get_size()) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
              if (memcmp(record.data, it->second.get_ptr(), record.size)) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
            }
          }
        }
      }

      check_index_integrity(m_node->get_count());
    }

    template<typename Cmp>
    int compare(const ham_key_t *lhs, Iterator it, Cmp &cmp) {
      if (it->get_key_flags() & BtreeKey::kExtended) {
        ham_key_t tmp = {0};
        get_extended_key(it->get_extended_blob_id(), &tmp);
        return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
      }
      return (cmp(lhs->data, lhs->size, it->get_key_data(),
                              it->get_key_size()));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find(ham_key_t *key, Cmp &comparator, int *pcmp = 0) {
      ham_u32_t count = m_node->get_count();
      int i, l = 1, r = count - 1;
      int ret = 0, last = count + 1;
      int cmp = -1;

#ifdef HAM_DEBUG
      check_index_integrity(count);
#endif

      ham_assert(count > 0);

      /* only one element in this node? */
      if (r == 0) {
        cmp = compare(key, at(0), comparator);
        if (pcmp)
          *pcmp = cmp;
        return (cmp < 0 ? -1 : 0);
      }

      for (;;) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)count);
          cmp = 1;
          ret = i;
          break;
        }

        /* compare it against the key */
        cmp = compare(key, at(i), comparator);

        /* found it? */
        if (cmp == 0) {
          ret = i;
          break;
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            ret = -1;
            break;
          }
          r = i - 1;
        }
        else {
          last = i;
          l = i + 1;
        }
      }

      if (pcmp)
        *pcmp = cmp;
      return (ret);
    }

    // Returns a deep copy of the key
    void get_key(ConstIterator it, ByteArray *arena, ham_key_t *dest) {
      LocalDatabase *db = m_page->get_db();

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(it->get_key_size());
        dest->data = arena->get_ptr();
        dest->size = it->get_key_size();
      }

      if (it->get_key_flags() & BtreeKey::kExtended) {
        ham_key_t tmp = {0};
        get_extended_key(it->get_extended_blob_id(), &tmp);
        memcpy(dest->data, tmp.data, tmp.size);
      }
      else
        memcpy(dest->data, it->get_key_data(), it->get_key_size());

      /* recno databases: recno is stored in db-endian! */
      if (db->get_rt_flags() & HAM_RECORD_NUMBER) {
        ham_assert(dest->data != 0);
        ham_assert(dest->size == sizeof(ham_u64_t));
        ham_u64_t recno = *(ham_u64_t *)dest->data;
        recno = ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
      }
    }

    ham_u32_t get_duplicate_count(Iterator it) const {
      if (!(it->get_key_flags() & BtreeKey::kDuplicates))
        return (1);

      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      return (env->get_duplicate_manager()->get_count(it->get_record_id(), 0));
    }

    // Returns the full record and stores it in |dest|
    void get_record(Iterator it, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u32_t duplicate_index,
                    PDupeEntry *duplicate_entry) const {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // handle duplicates
      if (it->get_key_flags() & BtreeKey::kDuplicates) {
        PDupeEntry tmp;
        if (!duplicate_entry)
          duplicate_entry = &tmp;
        env->get_duplicate_manager()->get(it->get_record_id(),
                        duplicate_index, duplicate_entry);
        record->_intflags = dupe_entry_get_flags(duplicate_entry);
        record->_rid = dupe_entry_get_rid(duplicate_entry);

        ham_u32_t size = 0xffffffff;
        if (record->_intflags & BtreeKey::kBlobSizeTiny) {
          /* the highest byte of the record id is the size of the blob */
          char *p = (char *)&record->_rid;
          size = p[sizeof(ham_u64_t) - 1];
        }
        else if (record->_intflags & BtreeKey::kBlobSizeSmall)
          size = sizeof(ham_u64_t);
        else if (record->_intflags & BtreeKey::kBlobSizeEmpty)
          size = 0;

        if (size == 0) {
          record->data = 0;
          record->size = 0;
          return;
        }
        if (size <= 8) {
          if (flags & HAM_PARTIAL) {
            ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
            throw Exception(HAM_INV_PARAMETER);
          }
          if (!(record->flags & HAM_RECORD_USER_ALLOC)
              && (flags & HAM_DIRECT_ACCESS)) {
            record->data = &record->_rid;
            record->size = size;
            return;
          }
          if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(size);
            record->data = arena->get_ptr();
          }
          record->size = size;
          memcpy(record->data, &record->_rid, size);
          return;
        }
        env->get_blob_manager()->read(db, record->_rid, record, flags, arena);
        return;
      }

      // regular inline record, no duplicates
      if (it->is_record_inline()) {
        ham_u32_t size = it->get_inline_record_size();
        if (size == 0) {
          record->data = 0;
          record->size = 0;
          return;
        }
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
          throw Exception(HAM_INV_PARAMETER);
        }
        if (!(record->flags & HAM_RECORD_USER_ALLOC)
            && (flags & HAM_DIRECT_ACCESS)) {
          record->data = it->get_inline_record_data();
        }
        else {
          if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, it->get_inline_record_data(), size);
        }
        record->size = size;
        return;
      }

      // non-inline record, no duplicates
      env->get_blob_manager()->read(db, it->get_record_id(), record,
                                flags, arena);
    }

    void set_record(Iterator it, Transaction *txn,
                    ham_record_t *record, ham_u32_t duplicate_position,
                    ham_u32_t flags, ham_u32_t *new_duplicate_position) {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      ham_u64_t ptr = it->get_record_id();
      ham_u8_t oldflags = it->get_key_flags();

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // key does not yet exist
      if (!ptr && !it->is_record_inline()) {
        // a new inline key is inserted
        if (record->size <= it->get_max_inline_record_size()) {
          it->set_inline_record_data(record->data, record->size);
        }
        // a new (non-inline) key is inserted
        else {
          ptr = env->get_blob_manager()->allocate(db, record, flags);
          it->set_record_id(ptr);
        }
        return;
      }

      bool insert_duplicate =
              (flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST)) != 0;
      if (oldflags & BtreeKey::kDuplicates)
        insert_duplicate = true;

      // an inline key exists
      if (!insert_duplicate && it->is_record_inline()) {
        // ... and is overwritten with another inline key
        if (record->size <= it->get_max_inline_record_size()) {
          it->set_inline_record_data(record->data, record->size);
        }
        // ... or with a (non-inline) key
        else {
          ptr = env->get_blob_manager()->allocate(db, record, flags);
          it->set_key_flags(oldflags & ~(BtreeKey::kBlobSizeSmall
                                | BtreeKey::kBlobSizeEmpty
                                | BtreeKey::kBlobSizeTiny));
          it->set_record_id(ptr);
        }
        return;
      }

      // a (non-inline) key exists
      if (ptr && !insert_duplicate) {
        // ... and is overwritten by a inline key
        if (record->size <= it->get_max_inline_record_size()) {
          env->get_blob_manager()->free(db, ptr);
          it->set_inline_record_data(record->data, record->size);
        }
        // ... and is overwritten by a (non-inline) key
        else {
          ptr = env->get_blob_manager()->overwrite(db, ptr, record, flags);
          it->set_record_id(ptr);
        }
        return;
      }

      /* the key is added as a duplicate
       *
       * a duplicate of an existing key - always insert it at the end of
       * the duplicate list (unless the DUPLICATE flags say otherwise OR
       * when we have a duplicate-record comparison function for
       * ordered insertion of duplicate records)
       *
       * create a duplicate list, if it does not yet exist
       */
      else {
        PDupeEntry entries[2];
        ham_u64_t rid = 0;
        int i = 0;
        ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST | HAM_OVERWRITE)));
        memset(entries, 0, sizeof(entries));
        if (!(oldflags & BtreeKey::kDuplicates)) {
          ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                      | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                      | HAM_DUPLICATE_INSERT_LAST)));
          dupe_entry_set_flags(&entries[i],
                    oldflags & (BtreeKey::kBlobSizeSmall
                        | BtreeKey::kBlobSizeTiny
                        | BtreeKey::kBlobSizeEmpty));
          dupe_entry_set_rid(&entries[i], ptr);
          i++;
        }
        if (record->size <= sizeof(ham_u64_t)) {
          if (record->data)
            memcpy(&rid, record->data, record->size);
          if (record->size == 0)
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeEmpty);
          else if (record->size < sizeof(ham_u64_t)) {
            char *p = (char *)&rid;
            p[sizeof(ham_u64_t) - 1] = (char)record->size;
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeTiny);
          }
          else
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeSmall);
          dupe_entry_set_rid(&entries[i], rid);
        }
        else {
          rid = env->get_blob_manager()->allocate(db, record, flags);
          dupe_entry_set_flags(&entries[i], 0);
          dupe_entry_set_rid(&entries[i], rid);
        }
        i++;

        rid = 0;
        env->get_duplicate_manager()->insert(db, 0,
                (i == 2 ? 0 : ptr), record, duplicate_position,
                flags, &entries[0], i, &rid, new_duplicate_position);

        // disable small/tiny/empty flags, enable duplicates
        it->set_key_flags((oldflags & ~(BtreeKey::kBlobSizeSmall
                            | BtreeKey::kBlobSizeTiny
                            | BtreeKey::kBlobSizeEmpty))
                        | BtreeKey::kDuplicates);
        if (rid)
          it->set_record_id(rid);
      }
    }

    // Returns the record size of a key or one of its duplicates
    ham_u64_t get_record_size(Iterator it, int duplicate_index) const {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      ham_u32_t keyflags = 0;
      ham_u64_t *ridptr = 0;
      ham_u64_t rid = 0;

      if (it->get_key_flags() & BtreeKey::kDuplicates) {
        PDupeEntry dupeentry;
        env->get_duplicate_manager()->get(it->get_record_id(),
                        duplicate_index, &dupeentry);
        keyflags = dupe_entry_get_flags(&dupeentry);
        ridptr = &dupeentry._rid;
        rid = dupeentry._rid;

        if (keyflags & BtreeKey::kBlobSizeTiny) {
          // the highest byte of the record id is the size of the blob
          char *p = (char *)ridptr;
          return (p[sizeof(ham_u64_t) - 1]);
        }
        else if (keyflags & BtreeKey::kBlobSizeSmall) {
          // record size is sizeof(ham_u64_t)
          return (sizeof(ham_u64_t));
        }
        else if (keyflags & BtreeKey::kBlobSizeEmpty) {
          // record size is 0
          return (0);
        }
        return (env->get_blob_manager()->get_datasize(db, rid));
      }

      if (it->is_record_inline())
        return (it->get_inline_record_size());
      return (env->get_blob_manager()->get_datasize(db, it->get_record_id()));
    }

    void erase_key(Iterator it) {
      /* delete the extended key */
      if (it->get_key_flags() & BtreeKey::kExtended)
        erase_extended_key(it->get_extended_blob_id());
    }

    void erase_record(Iterator it, int duplicate_id, bool all_duplicates) {
      ham_u64_t rid;
      LocalDatabase *db = m_page->get_db();

      /* delete one (or all) duplicates */
      if (it->get_key_flags() & BtreeKey::kDuplicates) {
        db->get_local_env()->get_duplicate_manager()->erase(db,
                          it->get_record_id(), duplicate_id,
                          all_duplicates, &rid);
        if (all_duplicates) {
          it->set_key_flags(it->get_key_flags() & ~BtreeKey::kDuplicates);
          it->set_record_id(0);
        }
        else {
          it->set_record_id(rid);
          if (!rid) /* rid == 0: the last duplicate was deleted */
            it->set_key_flags(0);
        }
      }
      else {
        if (it->is_record_inline()) {
          it->set_key_flags(it->get_key_flags() & ~(BtreeKey::kBlobSizeTiny
                    | BtreeKey::kBlobSizeSmall
                    | BtreeKey::kBlobSizeEmpty));
          it->remove_inline_record();
        }
        else {
          /* delete the blob */
          db->get_local_env()->get_blob_manager()->free(db,
                          it->get_record_id(), 0);
          it->set_record_id(0);
        }
      }
    }

    void erase(ham_u32_t slot) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // if this is the last key in this page: just re-initialize
      if (m_node->get_count() == 1) {
        set_freelist_count(0);
        set_next_offset(0);
        return;
      }

      Iterator it = at(slot);

      // get rid of the extended key (if there is one)
      if (get_key_flags(slot) & BtreeKey::kExtended)
        erase_key(it);

      // now add this key to the freelist
      freelist_add(slot);

      // then remove index key by shifting all remaining indices/freelist
      // items "to the left"
      memmove(m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_ptr(slot + 1),
                      m_layout.get_key_index_span()
                            * (get_freelist_count() + m_node->get_count()
                                    - slot - 1));

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count() - 1);
#endif
    }

    void insert(ham_u32_t slot, const ham_key_t *key) {
      ham_u32_t count = m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      bool extended_key = key->size > g_extended_threshold;

      // search the freelist for free key space
      int idx = freelist_find(count,
                      extended_key ? sizeof(ham_u64_t) : key->size);

      ham_u32_t offset = (ham_u32_t)-1;

      // found: remove this freelist entry
      if (idx != -1) {
        offset = m_layout.get_key_data_offset(idx);
        // if there's not at least a 16 byte gap: don't bother adding the
        // gap to the freelist
        ham_u32_t size = get_key_data_size(idx);
        if (size > key->size + sizeof(ham_u64_t) * 2) {
          m_layout.set_key_size(idx,
                          size - (key->size + sizeof(ham_u64_t)));
          m_layout.set_key_data_offset(idx,
                          offset + key->size + sizeof(ham_u64_t));
        }
        else {
          freelist_remove(idx);
          // adjust the next key offset, if required
          if (get_next_offset() == offset + size + sizeof(ham_u64_t))
            set_next_offset(offset
                        + (extended_key ? sizeof(ham_u64_t) : key->size)
                        + sizeof(ham_u64_t));
        }
      }
      // not found: append at the end
      else {
        if (count == 0) {
          idx = 0;
          offset = 0;
        }
        else {
          offset = get_next_offset();
        }
        // make sure that the key really fits! if not then use an extended key.
        // this can happen if a page is split, but the new key still doesn't
        // fit into the splitted page.
        if (!extended_key) {
          if (offset + kPayloadOffset
              + m_layout.get_key_index_span() * get_capacity()
              + key->size + sizeof(ham_u64_t)
                  >= m_page->get_db()->get_local_env()->get_page_size()
                      - PBtreeNode::get_entry_offset()
                      - Page::sizeof_persistent_header) {
            extended_key = true;
          }
        }

        set_next_offset(offset
                        + (extended_key ? sizeof(ham_u64_t) : key->size)
                        + sizeof(ham_u64_t));
      }

      // once more assert that the new key fits
      ham_assert(offset + kPayloadOffset
              + m_layout.get_key_index_span() * get_capacity()
              + (extended_key ? sizeof(ham_u64_t) : key->size)
              + sizeof(ham_u64_t)
                  <= m_page->get_db()->get_local_env()->get_page_size()
                      - PBtreeNode::get_entry_offset()
                      - Page::sizeof_persistent_header);

      // make space for the new index
      if (slot < count || get_freelist_count() > 0) {
        memmove(m_layout.get_key_index_ptr(slot + 1),
                      m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_span()
                            * (count + get_freelist_count() - slot));
      }

      // store the key index
      m_layout.set_key_data_offset(slot, offset);

      // now finally copy the key data
      if (extended_key) {
        Iterator it = at(slot);
        ham_u64_t blobid = add_extended_key(key);

        it->set_extended_blob_id(blobid);
        // remove all flags, set Extended flag
        it->set_key_flags(BtreeKey::kExtended);
      }
      else {
        set_key_flags(slot, 0);
        set_key_data(slot, key->data, key->size);
      }

      set_key_size(slot, key->size);
      set_record_id(slot, 0);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count() + 1);
#endif
    }

    // Same as above, but copies the key from |src_node[src_slot]|
    void insert(ham_u32_t slot, DefaultNodeLayout *src_node,
                    ham_u32_t src_slot) {
      ham_key_t key = {0};
      ConstIterator it = src_node->at(src_slot);
      if (it->get_key_flags() & BtreeKey::kExtended) {
        get_extended_key(it->get_extended_blob_id(), &key);
      }
      else {
        key.data = (void *)it->get_key_data();
        key.size = it->get_key_size();
      }
      insert(slot, &key);
    }

    // Replace |dest| with |src|
    void replace_key(ConstIterator src, Iterator dest,
                    bool dest_is_internal) const {
      ham_key_t key;
      key.flags = 0;
      key.data = src->get_key_data();
      key.size = src->get_key_size();
      key._flags = src->get_key_flags();

      replace_key(&key, dest, dest_is_internal);
    }

    // Replace |dest| with |src|
    void replace_key(ham_key_t *src, Iterator dest, bool dest_is_internal) {
      dest->set_key_flags(src->_flags);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // internal nodes are not allowed to have blob-related flags, because
      // only leaf-nodes can manage blobs. Therefore disable those flags if
      // an internal key is replaced.
      if (dest_is_internal)
        dest->set_key_flags(dest->get_key_flags() &
                ~(BtreeKey::kBlobSizeTiny
                    | BtreeKey::kBlobSizeSmall
                    | BtreeKey::kBlobSizeEmpty
                    | BtreeKey::kDuplicates));

      ham_u64_t rid = 0;

      // copy the extended key, if there is one
      if (src->_flags & BtreeKey::kExtended) {
        ham_u64_t newblobid, oldblobid = *(ham_u64_t *)src->data;
        oldblobid = ham_db2h_offset(oldblobid);
        newblobid = copy_extended_key(oldblobid);
        dest->set_extended_blob_id(newblobid);
        dest->set_key_flags(BtreeKey::kExtended);
      }
      else {
        // check if the current key space is large enough; if not then move the
        // space to the freelist and allocate new space.
        // however, there are two caveats:
        //  1. it's possible that the number of used slots already reached the 
        //      capacity limit
        //  2. it's possible that the new key does not fit into the page.
        //      in this case we simply allocate an extended key, which
        //      only requires 8 bytes
        if (dest->get_key_data_size() < src->size) {
          // copy the record ID, will be required later
          rid = dest->get_record_id();
          // add this slot to the freelist if there's enough capacity
          if (get_freelist_count() + m_node->get_count() < get_capacity())
            freelist_add(dest->get_slot());
          // and append the new key, if there's enough space available
          if (!requires_split(src)) {
            m_layout.set_key_data_offset(dest->get_slot(),
                    append_key(dest->get_slot(), m_node->get_count(),
                          src->data, src->size, true));
          }
          // otherwise allocate and store an extended key
          else {
            ham_u64_t blobid = add_extended_key(src);
            dest->set_extended_blob_id(blobid);
            dest->set_key_flags(BtreeKey::kExtended);
          }
        }
        // if the existing space is too large then we COULD move the remainder
        // to the freelist, but i'm not sure it's worth the effort
        else {
          // copy the record ID
          rid = dest->get_record_id();
          // adjust next offset?
          if (src->size < dest->get_key_data_size()) {
            ham_u32_t next = m_layout.get_key_data_offset(dest->get_slot())
                            + dest->get_key_data_size()
                            + sizeof(ham_u64_t);
            if (next == get_next_offset())
              set_next_offset(next - (dest->get_key_data_size() - src->size));
          }
          // now copy the key data
          dest->set_key_data(src->data, src->size);
        }
      }

      dest->set_key_size(src->size);
      if (rid)
        dest->set_record_id(rid);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Returns true if |key| cannot be inserted because a split is required
    // Rearranges the node if required
    bool requires_split(const ham_key_t *key) {
      if (!requires_split_impl(key))
        return (false);

      // if freelist is nearly empty: do not attempt to rearrange
      if (get_freelist_count() <= kRearrangeThreshold)
        return (true);

      rearrange(m_node->get_count());
      if (requires_split_impl(key))
        return (true);
      return (resize(m_node->get_count() + 1, key));
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return (m_node->get_count() <= 3);
    }

    void split(DefaultNodeLayout *other, int pivot) {
      int start_slot = pivot;
      int count = m_node->get_count() - pivot;

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
      ham_assert(0 == other->m_node->get_count());
      ham_assert(0 == other->get_freelist_count());

      // if we split a leaf then the pivot element is inserted in the leaf
      // page. in internal nodes it is propagated to the parent instead.
      // (this propagation is handled by the caller.)
      if (!m_node->is_leaf()) {
        start_slot++;
        count--;
      }

      clear_extkey_cache();

      // move half of the keys to the other node
      memcpy(other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_ptr(start_slot),
                      m_layout.get_key_index_span() * count);
      for (int i = 0; i < count; i++) {
        ham_u32_t size = get_key_data_size(start_slot + i);
        ham_u8_t *data = get_key_data(start_slot + i);
        ham_u32_t offset = other->append_key(i, i, data, size, false);
        other->m_layout.set_key_data_offset(i, offset);
      }

      // now move all shifted keys to the freelist. those shifted keys are
      // always at the "right end" of the node, therefore we just decrease
      // m_node->get_count() and increase freelist_count simultaneously
      // (m_node->get_count() is decreased by the caller).
      set_freelist_count(get_freelist_count() + count);
      set_next_offset(calc_next_offset(pivot));
      if (get_freelist_count() > kRearrangeThreshold)
        rearrange(pivot);

#ifdef HAM_DEBUG
      check_index_integrity(pivot);
      other->check_index_integrity(count);
#endif
    }

    void merge_from(DefaultNodeLayout *other) {
      ham_u32_t count = m_node->get_count();
      ham_u32_t other_count = other->m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(count);
      other->check_index_integrity(other_count);
#endif

      other->clear_extkey_cache();

      // re-arrange the node: moves all keys sequentially to the beginning
      // of the key space, removes the whole freelist
      rearrange(m_node->get_count());

      // now append all indices from the sibling
      memcpy(m_layout.get_key_index_ptr(count),
                      other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_span() * other_count);

      // for each new key: copy the key data
      for (ham_u32_t i = 0; i < other_count; i++) {
        ham_u32_t size = other->get_key_data_size(i);
        ham_u8_t *data = other->get_key_data(i);
        ham_u32_t offset = append_key(count + i, count + i, data, size, false);
        m_layout.set_key_data_offset(count + i, offset);
        m_layout.set_key_size(count + i, other->get_key_size(i));
      }

      other->set_next_offset(0);
      other->set_freelist_count(0);
#ifdef HAM_DEBUG
      check_index_integrity(count + other_count);
#endif
    }

    void shift_from_right(DefaultNodeLayout *other, int count) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
      other->check_index_integrity(other->m_node->get_count());
#endif

      // re-arrange the node: moves all keys sequentially to the beginning
      // of the key space, removes the whole freelist
      rearrange(m_node->get_count());

      other->clear_extkey_cache();

      ham_u32_t pos = m_node->get_count();

      // shift |count| indices from |other| to this page
      memcpy(m_layout.get_key_index_ptr(pos),
                      other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_span() * count);

      // now shift the keys
      for (int i = 0; i < count; i++) {
        ham_u32_t size = other->get_key_data_size(i);
        ham_u8_t *data = other->get_key_data(i);
        ham_u32_t offset = append_key(pos + i, pos + i, data, size, false);
        m_layout.set_key_data_offset(pos + i, offset);
        m_layout.set_key_size(pos + i, other->get_key_size(i));
      }

      // now close the "gap" in the |other| page by moving the shifted
      // keys to the freelist
      other->freelist_add_many(0, count);

#ifdef HAM_DEBUG
      check_index_integrity(pos + count);
      other->check_index_integrity(other->m_node->get_count() - count);
#endif
    }

    void shift_to_right(DefaultNodeLayout *other, int pos, int count) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
      other->check_index_integrity(other->m_node->get_count());
#endif

      // re-arrange the node: moves all keys sequentially to the beginning
      // of the key space, removes the whole freelist
      other->rearrange(other->m_node->get_count());
      clear_extkey_cache();

      // make room in the sibling's index area
      memmove(other->m_layout.get_key_index_ptr(count),
                      other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_span()
                            * other->m_node->get_count());

      // now copy the indices
      memcpy(other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_ptr(pos),
                      m_layout.get_key_index_span() * count);

      // and the key data
      for (int i = 0; i < count; i++) {
        ham_u32_t size = get_key_data_size(pos + i);
        ham_u8_t *data = get_key_data(pos + i);
        ham_u32_t offset = other->append_key(i, other->m_node->get_count() + i,
                        data, size, false);
        other->m_layout.set_key_data_offset(i, offset);
        other->m_layout.set_key_size(i, get_key_size(pos + i));
      }

      // and rearrange the page because it's nearly empty
      rearrange(pos);

#ifdef HAM_DEBUG
      check_index_integrity(pos);
      other->check_index_integrity(other->m_node->get_count() + count);
#endif
    }

    // Clears the page with zeroes and reinitializes it
    void test_clear_page() {
      ham_u32_t page_size = m_page->get_db()->get_local_env()->get_page_size();
      memset(m_page->get_raw_payload(), 0, page_size);
      initialize();
    }

  private:
    friend class DefaultIterator<LayoutImpl, RecordImpl>;
    friend class DefaultInlineRecordImpl<LayoutImpl>;
    friend class FixedInlineRecordImpl<LayoutImpl>;
    friend class InternalInlineRecordImpl<LayoutImpl>;

    void initialize() {
      LocalDatabase *db = m_page->get_db();
      ham_u32_t key_size = db->get_btree_index()->get_key_size();

      m_layout.initialize(m_node->get_data() + kPayloadOffset, key_size);

      if (m_node->get_count() == 0) {
        ham_u32_t rec_size = db->get_btree_index()->get_record_size();
        ham_u32_t page_size = db->get_local_env()->get_page_size()
                - kPayloadOffset;
        /* adjust page size and key size by adding the overhead */
        page_size -= PBtreeNode::get_entry_offset();
        page_size -= Page::sizeof_persistent_header;

        /* this calculation is identical to BtreeIndex::get_maxkeys() */
        ham_u32_t capacity;
        if (rec_size == HAM_RECORD_SIZE_UNLIMITED)
          capacity = page_size / (get_actual_key_size(key_size) + 8);
        else
          capacity = page_size / (get_actual_key_size(key_size) + rec_size);
        capacity = (capacity & 1 ? capacity - 1 : capacity);

        set_capacity(capacity);
        set_freelist_count(0);
        set_next_offset(0);
      }
    }

    void clear_extkey_cache() {
      if (m_extkey_cache) {
        delete m_extkey_cache;
        m_extkey_cache = 0;
      }
    }

    // Retrieves the extended key at |blobid| and stores it in |key|; will
    // use the cache.
    void get_extended_key(ham_u64_t blobid, ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();
      else {
        ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
        if (it != m_extkey_cache->end()) {
          key->size = it->second.get_size();
          key->data = it->second.get_ptr();
          return;
        }
      }

      ByteArray arena;
      ham_record_t record = {0};
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->read(db, blobid, &record,
                      0, &arena);
      (*m_extkey_cache)[blobid] = arena;
      arena.disown();
      key->data = record.data;
      key->size = record.size;
    }

    // Erases an extended key from disk and from the cache
    void erase_extended_key(ham_u64_t blobid) {
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->free(db, blobid);
      if (m_extkey_cache)
        m_extkey_cache->erase(m_extkey_cache->find(blobid));
    }

    ham_u64_t copy_extended_key(ham_u64_t oldblobid) {
      ham_key_t oldkey = {0};

      // do NOT use the cache when retrieving the existing blob - this
      // blob belongs to a different page and we do not have access to
      // its layout
      ham_record_t record = {0};
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->read(db, oldblobid, &record,
                      0, &m_arena);
      oldkey.data = record.data;
      oldkey.size = record.size;

      return (add_extended_key(&oldkey));
    }

    ham_u64_t add_extended_key(const ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();

      ham_record_t rec = {0};
      rec.data = key->data;
      rec.size = key->size;

      LocalDatabase *db = m_page->get_db();
      ham_u64_t blobid = db->get_local_env()->get_blob_manager()->allocate(db,
                            &rec, 0);
      ham_assert(blobid != 0);
      ham_assert(m_extkey_cache->find(blobid) == m_extkey_cache->end());

      ByteArray arena;
      arena.resize(key->size);
      memcpy(arena.get_ptr(), key->data, key->size);
      (*m_extkey_cache)[blobid] = arena;
      arena.disown();
      return (blobid);
    }

    ham_u32_t get_key_flags(int slot) const {
      return (m_layout.get_key_flags(slot));
    }

    void set_key_flags(int slot, ham_u32_t flags) {
      m_layout.set_key_flags(slot, flags);
    }

    // Returns the key size as specified by the user
    ham_u32_t get_key_size(int slot) const {
      return (m_layout.get_key_size(slot));
    }

    // Returns the size of the memory occupied by the key
    ham_u32_t get_key_data_size(int slot) const {
      if (m_layout.get_key_flags(slot) & BtreeKey::kExtended)
        return (sizeof(ham_u64_t));
      return (m_layout.get_key_size(slot));
    }

    void set_key_size(int slot, ham_u32_t size) {
      m_layout.set_key_size(slot, size);
    }

    ham_u8_t *get_key_data(int slot) {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      return (m_node->get_data() + kPayloadOffset + offset);
    }

    ham_u8_t *get_key_data(int slot) const {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      return (m_node->get_data() + kPayloadOffset + offset);
    }

    void set_key_data(int slot, const void *ptr, ham_u32_t len) {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      memcpy(m_node->get_data() + kPayloadOffset + offset, ptr, len);
    }

    ham_u64_t get_record_id(int slot) const {
      ham_u64_t ptr = *(ham_u64_t *)get_inline_record_data(slot);
      return (ham_db2h_offset(ptr));
    }

    void set_record_id(int slot, ham_u64_t ptr) {
      ham_u8_t *p = (ham_u8_t *)get_inline_record_data(slot);
      *(ham_u64_t *)p = ham_h2db_offset(ptr);
    }

    void *get_inline_record_data(int slot) {
      return (get_key_data(slot) + get_key_data_size(slot));
    }

    void *get_inline_record_data(int slot) const {
      return (get_key_data(slot) + get_key_data_size(slot));
    }

    // Searches for a freelist index with at least |size| bytes; returns
    // its index
    int freelist_find(ham_u32_t count, ham_u32_t key_size) const {
      ham_u32_t freelist_count = get_freelist_count();
      for (ham_u32_t i = 0; i < freelist_count; i++) {
        if (get_key_data_size(count + i) >= key_size)
          return (count + i);
      }
      return (-1);
    }

    // Removes a freelist entry at |slot|
    void freelist_remove(int slot) {
      ham_assert(get_freelist_count() > 0);

      if ((ham_u32_t)slot < m_node->get_count() + get_freelist_count() - 1) {
        memmove(m_layout.get_key_index_ptr(slot),
                        m_layout.get_key_index_ptr(slot + 1),
                        m_layout.get_key_index_span()
                              * (m_node->get_count() + get_freelist_count()
                                      - slot - 1));
      }

      set_freelist_count(get_freelist_count() - 1);
    }

    void freelist_add_many(int start, int count) {
      // copy the indices to the freelist area
      memmove(m_layout.get_key_index_ptr(m_node->get_count()
                              + get_freelist_count()),
                      m_layout.get_key_index_ptr(start),
                      m_layout.get_key_index_span() * count);

      set_freelist_count(get_freelist_count() + count);

      // then remove the deleted index keys by shifting all remaining
      // indices/freelist items "to the left"
      memmove(m_layout.get_key_index_ptr(start),
                      m_layout.get_key_index_ptr(start + count),
                      m_layout.get_key_index_span()
                            * (get_freelist_count() + m_node->get_count()
                                    - start - count));
      ham_assert(get_freelist_count() + m_node->get_count() - count
                      <= get_capacity());
    }

    // Adds the index at |slot| to the freelist
    void freelist_add(int slot) {
      memcpy(m_layout.get_key_index_ptr(m_node->get_count()
                              + get_freelist_count()),
                      m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_span());

      set_freelist_count(get_freelist_count() + 1);

      ham_assert(get_freelist_count() + m_node->get_count() <= get_capacity());
    }

    // Appends a key to the key space; if |use_freelist| is true, it will
    // first search for a sufficiently large freelist entry. Returns the
    // offset of the new key.
    int append_key(ham_u32_t slot, ham_u32_t count,
                    const void *key_data, ham_u32_t key_size,
                    bool use_freelist) {
      // search the freelist for free key space
      int idx = -1;
      ham_u32_t offset;

      bool extended_key = key_size > g_extended_threshold;
     
      if (use_freelist) {
        idx = freelist_find(count, key_size);
        // found: remove this freelist entry
        if (idx != -1) {
          offset = m_layout.get_key_data_offset(idx);
          // if there's not at least a 16 byte gap: don't bother adding the
          // gap to the freelist
          ham_u32_t size = get_key_data_size(idx);
          if (size > key_size + sizeof(ham_u64_t) * 2) {
            m_layout.set_key_size(idx,
                            size - (key_size + sizeof(ham_u64_t)));
            m_layout.set_key_data_offset(idx,
                            offset + key_size + sizeof(ham_u64_t));
          }
          else {
            freelist_remove(idx);
            // adjust the next key offset, if required
            if (get_next_offset() == offset + size + sizeof(ham_u64_t))
              set_next_offset(offset + key_size + sizeof(ham_u64_t));
          }
        }
      }

      if (idx == -1) {
        if (count == 0) {
          offset = 0;
          set_next_offset((extended_key ? sizeof(ham_u64_t) : key_size)
                                + sizeof(ham_u64_t));
        }
        else {
          offset = get_next_offset();
          set_next_offset(offset
                          + (extended_key ? sizeof(ham_u64_t) : key_size)
                          + sizeof(ham_u64_t));
        }
      }

      // copy the key data AND the record data
      ham_u8_t *p = m_node->get_data() + kPayloadOffset + offset
              + m_layout.get_key_index_span() * get_capacity();
      memcpy(p, key_data, key_size + sizeof(ham_u64_t));

      // return the freelist index
      return (offset);
    }

    ham_u32_t calc_next_offset(ham_u32_t count) {
      ham_u32_t next_offset = 0;
      ham_u32_t total = count + get_freelist_count();
      for (ham_u32_t i = 0; i < total; i++) {
        ham_u32_t next = m_layout.get_key_data_offset(i)
                    + get_key_data_size(i)
                    + sizeof(ham_u64_t);
        if (next >= next_offset)
          next_offset = next;
      }
      return (next_offset);
    }

    // create a map with all occupied ranges in freelist and indices;
    // then make sure that there are no overlaps
    void check_index_integrity(ham_u32_t count) const {
      if (count + get_freelist_count() <= 1)
        return;

      typedef std::pair<ham_u32_t, ham_u32_t> Range;
      typedef std::vector<Range> RangeVec;
      ham_u32_t total = count + get_freelist_count();
      RangeVec ranges;
      ranges.reserve(total);
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total; i++) {
        ham_u32_t next = m_layout.get_key_data_offset(i)
                    + get_key_data_size(i)
                    + sizeof(ham_u64_t);
        if (next >= next_offset)
          next_offset = next;
        ranges.push_back(std::make_pair(m_layout.get_key_data_offset(i),
                             get_key_data_size(i)));
      }
      std::sort(ranges.begin(), ranges.end());
      for (ham_u32_t i = 0; i < ranges.size() - 1; i++) {
        if (ranges[i].first + ranges[i].second + sizeof(ham_u64_t)
                        > ranges[i + 1].first) {
          ham_trace(("integrity violated: slot %u/%u + 8 overlaps with %lu",
                      ranges[i].first, ranges[i].second, ranges[i + 1].first));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }
      }
      if (next_offset != get_next_offset()) {
        ham_trace(("integrity violated: next offset %d, cached offset %d",
                    next_offset, get_next_offset()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
    }

    // re-arrange the node: moves all keys sequentially to the beginning
    // of the key space, removes the whole freelist
    void rearrange(ham_u32_t count) {
      // already properly arranged? then return
      if (get_freelist_count() == 0) {
        set_next_offset(calc_next_offset(count));
        return;
      }

      // get rid of the freelist - this node is now completely rewritten,
      // and the freelist would just complicate things
      set_freelist_count(0);

      // make a copy of all indices (excluding the freelist)
      SortHelper *s = (SortHelper *)m_arena.resize(count * sizeof(SortHelper));
      for (ham_u32_t i = 0; i < count; i++) {
        s[i].slot = i;
        s[i].offset = m_layout.get_key_data_offset(i);
      }

      // sort them by offset
      std::sort(&s[0], &s[count], sort_by_offset);

      // shift all keys to the left, get rid of all gaps at the front of the
      // key data or between the keys
      ham_u32_t next_offset = 0;
      ham_u32_t start = kPayloadOffset
                       + m_layout.get_key_index_span() * get_capacity();
      for (ham_u32_t i = 0; i < count; i++) {
        ham_u32_t offset = s[i].offset;
        ham_u32_t slot = s[i].slot;
        if (offset != next_offset) {
          // shift key to the left
          memmove(m_node->get_data() + start + next_offset,
                          get_key_data(slot),
                          get_key_data_size(slot) + sizeof(ham_u64_t));
          // store the new offset
          m_layout.set_key_data_offset(slot, next_offset);
        }
        next_offset += get_key_data_size(slot) + sizeof(ham_u64_t);
      }

      set_next_offset(next_offset);

#ifdef HAM_DEBUG
      check_index_integrity(count);
#endif
    }

    // Tries to resize the node's capacity to fit |new_count| keys and at
    // least |key->size| additional bytes
    bool resize(ham_u32_t new_count, const ham_key_t *key) {
      ham_u32_t count = m_node->get_count();

      // the usable page_size
      ham_u32_t page_size = m_page->get_db()->get_local_env()->get_page_size()
                  - PBtreeNode::get_entry_offset()
                  - Page::sizeof_persistent_header
                  - kPayloadOffset;

      // increase capacity of the indices by shifting keys "to the right"
      if (count + get_freelist_count() >= new_count - 1) {
        // the absolute offset of the new key (including length and record)
        ham_u32_t offset = get_next_offset();
        offset += (key->size > g_extended_threshold
                        ? sizeof(ham_u64_t)
                        : key->size)
                + sizeof(ham_u64_t);
        offset += m_layout.get_key_index_span() * get_capacity();

        if (offset >= page_size)
          return (true);

        ham_u32_t capacity = get_capacity();
        ham_u8_t *src = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        capacity++;
        ham_u8_t *dst = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        memmove(dst, src, get_next_offset());

        // store the new capacity
        set_capacity(capacity);

        // check if the new space is sufficient
        return (requires_split_impl(key));
      }

      // increase key data capacity by reducing capacity and shifting
      // keys "to the left"
      else {
        // number of slots that we would have to shift left to get enough
        // room for the new key
        ham_u32_t gap = (key->size + sizeof(ham_u64_t))
                            / m_layout.get_key_index_span();
        gap++;

        // if the space is not available then return, and the caller can
        // perform a split
        if (gap + new_count + get_freelist_count() >= get_capacity())
          return (true);

        ham_u32_t capacity = get_capacity();

        // if possible then shift a bit more, hopefully this can avoid another
        // shift when the next key is inserted
        gap = std::min(gap, (capacity - new_count - get_freelist_count()) / 2);

        // now shift the keys and adjust the capacity
        ham_u8_t *src = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        capacity -= gap;
        ham_u8_t *dst = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        memmove(dst, src, get_next_offset());

        // store the new capacity
        set_capacity(capacity);

        return (false);
      }
    }

    // Returns true if |key| cannot be inserted because a split is required
    bool requires_split_impl(const ham_key_t *key) {
      ham_u32_t count = m_node->get_count();

      if (count == 0) {
        set_freelist_count(0);
        return (false);
      }

      if (count + get_freelist_count() >= get_capacity() - 1)
        return (true);

      // if there's a freelist entry which can store the new key then
      // a split won't be required
      if (-1 != freelist_find(count, key->size))
        return (false);

      ham_u32_t offset = get_next_offset();
      offset += (key->size > g_extended_threshold
                      ? sizeof(ham_u64_t)
                      : key->size)
              + sizeof(ham_u64_t);
      offset += kPayloadOffset + m_layout.get_key_index_span() * get_capacity();
      if (offset >= m_page->get_db()->get_local_env()->get_page_size()
                    - PBtreeNode::get_entry_offset()
                    - Page::sizeof_persistent_header)
        return (true);
      return (false);
    }

    // Returns the index capacity
    ham_u32_t get_capacity() const {
      return (ham_db2h32(*(ham_u32_t *)m_node->get_data()));
    }

    // Sets the index capacity
    void set_capacity(ham_u32_t capacity) {
      *(ham_u32_t *)m_node->get_data() = ham_h2db32(capacity);
    }

    // Returns the number of freelist entries
    ham_u32_t get_freelist_count() const {
      return (ham_db2h32(*(ham_u32_t *)(m_node->get_data() + 4)));
    }

    // Sets the number of freelist entries
    void set_freelist_count(ham_u32_t freelist_count) {
      *(ham_u32_t *)(m_node->get_data() + 4) = ham_h2db32(freelist_count);
    }

    // Returns the offset of the unused space at the end of the page
    ham_u32_t get_next_offset() const {
      return (ham_db2h32(*(ham_u32_t *)(m_node->get_data() + 8)));
    }

    // Sets the offset of the unused space at the end of the page
    void set_next_offset(ham_u32_t next_offset) {
      *(ham_u32_t *)(m_node->get_data() + 8) = ham_h2db32(next_offset);
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot) const {
      return (m_record_proxy.is_record_inline(slot));
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size) {
      m_record_proxy.set_inline_record_data(slot, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      return (m_record_proxy.get_inline_record_size(slot));
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (m_record_proxy.get_max_inline_record_size());
    }

    Page *m_page;
    PBtreeNode *m_node;
    LayoutImpl m_layout;
    RecordImpl m_record_proxy;
    ByteArray m_arena;
    ExtKeyCache *m_extkey_cache;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_DEFAULT_H__ */
