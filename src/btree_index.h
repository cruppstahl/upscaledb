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

#ifndef HAM_BTREE_INDEX_H__
#define HAM_BTREE_INDEX_H__

#include <algorithm>

#include "endianswap.h"

#include "db.h"
#include "abi.h"
#include "util.h"
#include "btree_cursor.h"
#include "btree_stats.h"
#include "btree_node.h"

namespace hamsterdb {

#include "packstart.h"

#undef max // avoid MSVC conflicts with std::max

// for counting extended keys
extern ham_u64_t g_extended_keys;

// for counting extended duplicate tables
extern ham_u64_t g_extended_duptables;

// Tracking key bytes before compression
extern ham_u64_t g_bytes_before_compression;

// Tracking key bytes after compression
extern ham_u64_t g_bytes_after_compression;

//
// The persistent btree index descriptor. This structure manages the
// persistent btree metadata.
//
HAM_PACK_0 class HAM_PACK_1 PBtreeHeader
{
  public:
    PBtreeHeader() {
      memset(this, 0, sizeof(*this));
    }

    // Returns the database name
    ham_u16_t get_dbname() const {
      return (ham_db2h16(m_dbname));
    }

    // Sets the database name
    void set_dbname(ham_u16_t n) {
      m_dbname = ham_h2db16(n);
    }

    // Returns the btree's max. key_size
    ham_u16_t get_key_size() const {
      return (ham_db2h16(m_key_size));
    }

    // Sets the btree's max. key_size
    void set_key_size(ham_u16_t n) {
      m_key_size = ham_h2db16(n);
    }

    // Returns the record size (or 0 if none was specified)
    ham_u32_t get_record_size() const {
      return (ham_db2h32(m_rec_size));
    }

    // Sets the record size
    void set_rec_size(ham_u32_t n) {
      m_rec_size = ham_h2db32(n);
    }

    // Returns the btree's key type
    ham_u16_t get_key_type() const {
      return (ham_db2h16(m_key_type));
    }

    // Sets the btree's key type
    void set_key_type(ham_u16_t type) {
      m_key_type = ham_h2db16(type);
    }

    // Returns the address of the btree's root page.
    ham_u64_t get_root_address() const {
      return (ham_db2h_offset(m_root_address));
    }

    // Sets the address of the btree's root page.
    void set_root_address(ham_u64_t n) {
      m_root_address = ham_h2db_offset(n);
    }

    // Returns the btree's flags
    ham_u32_t get_flags() const {
      return (ham_db2h32(m_flags));
    }

    // Sets the btree's flags
    void set_flags(ham_u32_t n) {
      m_flags = ham_h2db32(n);
    }

    // PRO: Returns the record compression
    ham_u8_t get_record_compression() const {
      return (m_compression >> 4);
    }

    // PRO: Sets the record compression
    void set_record_compression(int algorithm) {
      m_compression = algorithm << 4;
    }

    // PRO: Returns the key compression
    ham_u8_t get_key_compression() const {
      return (m_compression & 0xf);
    }

    // PRO: Sets the key compression
    void set_key_compression(int algorithm) {
      m_compression |= algorithm & 0xf;
    }

  private:
    // address of the root-page
    ham_u64_t m_root_address;

    // flags for this database
    ham_u32_t m_flags;

    // The name of the database
    ham_u16_t m_dbname;

    // key size used in the pages
    ham_u16_t m_key_size;

    // key type
    ham_u16_t m_key_type;

    // PRO: for storing key and record compression algorithm */
    ham_u8_t m_compression;

    // reserved for padding
    ham_u8_t m_padding1;

    // the record size
    ham_u32_t m_rec_size;

} HAM_PACK_2;

#include "packstop.h"

class LocalDatabase;
class BtreeNodeProxy;
struct PDupeEntry;

struct BtreeVisitor {
  virtual bool operator()(BtreeNodeProxy *node, const void *key_data,
                  ham_u8_t key_flags, ham_u32_t key_size, 
                  ham_u64_t record_id) = 0;
};

//
// Abstract base class, overwritten by a templated version
//
class BtreeIndexTraits
{
  public:
    // virtual destructor
    virtual ~BtreeIndexTraits() { }

    // Returns the actual key size (including overhead)
    virtual ham_u16_t get_actual_key_size(ham_u32_t key_size) const = 0;

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(LocalDatabase *db, ham_key_t *lhs,
            ham_key_t *rhs) const = 0;

    // Returns the class name (for testing)
    virtual std::string test_get_classname() const = 0;

    // Implementation of get_node_from_page()
    virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) const = 0;
};

//
// The Btree. Derived by BtreeIndexImpl, which uses template policies to
// define the btree node layout.
//
class BtreeIndex
{
  public:
    enum {
      // for get_node_from_page(): Page is a leaf
      kLeafPage = 1,

      // for get_node_from_page(): Page is an internal node
      kInternalPage = 2
    };

    // Constructor; creates and initializes a new btree
    BtreeIndex(LocalDatabase *db, ham_u32_t descriptor, ham_u32_t flags,
            ham_u32_t key_type, ham_u32_t key_size);

    ~BtreeIndex() {
      delete m_leaf_traits;
      m_leaf_traits = 0;
      delete m_internal_traits;
      m_internal_traits = 0;
    }

    // Returns the database pointer
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the database pointer
    LocalDatabase *get_db() const {
      return (m_db);
    }

    // Returns the internal key size
    ham_u16_t get_key_size() const {
      return (m_key_size);
    }

    // Returns the record size
    ham_u32_t get_record_size() const {
      return (m_rec_size);
    }

    // Returns the internal key type
    ham_u16_t get_key_type() const {
      return (m_key_type);
    }

    // Returns the address of the root page
    ham_u64_t get_root_address() const {
      return (m_root_address);
    }

    // Returns the btree flags
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    // Calculates the answer for "HAM_PARAM_MAX_KEYS_PER_PAGE"
    ham_u32_t get_max_keys_per_page() const;

    // Creates and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    void create(ham_u16_t key_type, ham_u32_t key_size, ham_u32_t rec_size);

    // Opens and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    void open();

    // Lookup a key in the index (ham_db_find)
    ham_status_t find(Transaction *txn, Cursor *cursor,
            ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Inserts (or updates) a key/record in the index (ham_db_insert)
    ham_status_t insert(Transaction *txn, Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

    // Erases a key/record from the index (ham_db_erase).
    // If |duplicate| is 0 then all duplicates are erased, otherwise only
    // the specified duplicate is erased.
    ham_status_t erase(Transaction *txn, Cursor *cursor, ham_key_t *key,
            ham_u32_t duplicate, ham_u32_t flags);

    // Iterates over the whole index and enumerate every item
    void enumerate(BtreeVisitor &visitor,
                    bool visit_internal_nodes = false);

    // Checks the integrity of the btree (ham_db_check_integrity)
    void check_integrity(ham_u32_t flags);

    // Counts the keys in the btree (ham_db_get_key_count)
    ham_u64_t get_key_count(ham_u32_t flags);

    // Erases all records, overflow areas, extended keys etc from the index;
    // used to avoid memory leaks when closing in-memory Databases and to
    // clean up when deleting on-disk Databases.
    void release();

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    int compare_keys(ham_key_t *lhs, ham_key_t *rhs) const {
      return (m_leaf_traits->compare_keys(m_db, lhs, rhs));
    }

    // Returns a BtreeNodeProxy for a Page
    BtreeNodeProxy *get_node_from_page(Page *page) {
      if (page->get_node_proxy())
        return (page->get_node_proxy());

      BtreeNodeProxy *proxy;
      PBtreeNode *node = PBtreeNode::from_page(page);
      if (node->is_leaf())
        proxy = get_leaf_node_from_page_impl(page);
      else
        proxy = get_internal_node_from_page_impl(page);

      page->set_node_proxy(proxy);
      return (proxy);
    }

    // Returns the usage metrics
    static void get_metrics(ham_env_metrics_t *metrics) {
      metrics->btree_smo_split = ms_btree_smo_split;
      metrics->btree_smo_merge = ms_btree_smo_merge;
      metrics->extended_keys = g_extended_keys;
      metrics->extended_duptables = g_extended_duptables;
    }

    // Returns the btree usage statistics
    BtreeStatistics *get_statistics() {
      return (&m_statistics);
    }

    // Returns the class name (for testing)
    std::string test_get_classname() const {
      return (m_leaf_traits->test_get_classname());
    }

  private:
    friend class BtreeCheckAction;
    friend class BtreeEnumAction;
    friend class BtreeEraseAction;
    friend class BtreeFindAction;
    friend class BtreeInsertAction;
    friend class BtreeCursor;
    friend struct MiscFixture;
    friend struct BtreeKeyFixture;
    friend struct BtreeCursorFixture;
    friend struct DbFixture;
    friend struct DuplicateFixture;
    friend struct RecordNumberFixture;

    // Implementation of get_node_from_page() (for leaf nodes)
    BtreeNodeProxy *get_leaf_node_from_page_impl(Page *page) const {
      return (m_leaf_traits->get_node_from_page_impl(page));
    }

    // Implementation of get_node_from_page() (for internal nodes)
    BtreeNodeProxy *get_internal_node_from_page_impl(Page *page) const {
      return (m_internal_traits->get_node_from_page_impl(page));
    }

    // Sets the address of the root page
    void set_root_address(ham_u64_t address) {
      m_root_address = address;
      flush_descriptor();
    }

    // Flushes the PBtreeHeader to the Environment's header page
    void flush_descriptor();

    // Searches |parent| page for key |key| and returns the child
    // page in |child|.
    //
    // if |idxptr| is a valid pointer then it will return the anchor index
    // of the loaded page.
    Page *find_internal(Page *parent, ham_key_t *key, ham_s32_t *idxptr = 0);

    // Searches a leaf node for a key.
    //
    // !!!
    // only works with leaf nodes!!
    //
    // Returns the index of the key, or -1 if the key was not found, or
    // another negative status code value when an unexpected error occurred.
    ham_s32_t find_leaf(Page *page, ham_key_t *key, ham_u32_t flags);

    // pointer to the database object
    LocalDatabase *m_db;

    // the Traits class wrapping the template parameters (factory for
    // leaf nodes)
    BtreeIndexTraits *m_leaf_traits;

    // the Traits class wrapping the template parameters (factory for
    // internal nodes)
    BtreeIndexTraits *m_internal_traits;

    // the key_size of this btree index
    ham_u16_t m_key_size;

    // the key_type of this btree index
    ham_u16_t m_key_type;

    // the record size (or 0 if none was specified)
    ham_u32_t m_rec_size;

    // the index of the PBtreeHeader in the Environment's header page
    ham_u32_t m_descriptor_index;

    // the persistent flags of this btree index
    ham_u32_t m_flags;

    // address of the root-page
    ham_u64_t m_root_address;

    // the btree statistics
    BtreeStatistics m_statistics;

    // usage metrics - number of page splits
    static ham_u64_t ms_btree_smo_split;

    // usage metrics - number of page merges
    static ham_u64_t ms_btree_smo_merge;

    // usage metrics - number of page shifts
    static ham_u64_t ms_btree_smo_shift;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_H__ */
