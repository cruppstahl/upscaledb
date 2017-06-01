/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifndef UPS_BTREE_INDEX_H
#define UPS_BTREE_INDEX_H

#include "0root/root.h"

#include <algorithm>

// Always verify that a file of level N does not include headers > N!
#include "1base/abi.h"
#include "1base/dynamic_array.h"
#include "1base/scoped_ptr.h"
#include "1globals/globals.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_node.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
struct DbConfig;
struct PageManager;
struct LocalCursor;

typedef std::pair<const void *, size_t> ScanResult;

#include "1base/packstart.h"

//
// The persistent btree index descriptor. This structure manages the
// persistent btree metadata.
//
UPS_PACK_0 struct UPS_PACK_1 PBtreeHeader {
  PBtreeHeader() {
    ::memset(this, 0, sizeof(*this));
  }

  // Returns the record compression
  uint8_t record_compression() const {
    return (compression >> 4);
  }

  // Sets the record compression
  void set_record_compression(int algorithm) {
    compression |= algorithm << 4;
  }

  // Returns the key compression
  uint8_t key_compression() const {
    return (compression & 0xf);
  }

  // Sets the key compression
  void set_key_compression(int algorithm) {
    compression |= algorithm & 0xf;
  }

  // address of the root-page
  uint64_t root_address;

  // flags for this database
  uint32_t flags;

  // The name of the database
  uint16_t dbname;

  // key size used in the pages
  uint16_t key_size;

  // key type
  uint16_t key_type;

  // for storing key and record compression algorithm */
  uint8_t compression;

  // reserved
  uint8_t _reserved1;

  // the record size
  uint32_t record_size;

  // hash of the custom compare function
  uint32_t compare_hash;

  // the record type
  uint16_t record_type;
} UPS_PACK_2;

#include "1base/packstop.h"

struct Context;
struct LocalDb;
struct BtreeNodeProxy;
struct PDupeEntry;
struct BtreeVisitor;

//
// Abstract base class, overwritten by a templated version
//
struct BtreeIndexTraits {
  // virtual destructor
  virtual ~BtreeIndexTraits() { }

  // Compares two keys
  // Returns -1, 0, +1 or higher positive values are the result of a
  // successful key comparison (0 if both keys match, -1 when
  // LHS < RHS key, +1 when LHS > RHS key).
  virtual int compare_keys(LocalDb *db, ups_key_t *lhs,
                  ups_key_t *rhs) const = 0;

  // Returns the class name (for testing)
  virtual std::string test_get_classname() const = 0;

  // Implementation of get_node_from_page()
  virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) const = 0;
};


struct BtreeIndexState {
  // The Environment's page manager
  PageManager *page_manager;

  // pointer to the database object
  LocalDb *db;

  // the Traits class wrapping the template parameters (factory for
  // leaf nodes)
  ScopedPtr<BtreeIndexTraits> leaf_traits;

  // the Traits class wrapping the template parameters (factory for
  // internal nodes)
  ScopedPtr<BtreeIndexTraits> internal_traits;

  // the index of the PBtreeHeader in the Environment's header page
  PBtreeHeader *btree_header;

  // the root page of the Btree
  Page *root_page;

  // the btree statistics
  BtreeStatistics statistics;
};

//
// The Btree. Derived by BtreeIndexImpl, which uses template policies to
// define the btree node layout.
//
struct BtreeIndex {
  enum {
    // for get_node_from_page(): Page is a leaf
    kLeafPage = 1,

    // for get_node_from_page(): Page is an internal node
    kInternalPage = 2
  };

  // Constructor; creates and initializes a new btree
  BtreeIndex(LocalDb *db) {
    state.db = db;
    state.btree_header = 0;
    state.root_page = 0;
  }

  // Returns the database pointer
  LocalDb *db() {
    return state.db;
  }

  // Returns the database pointer
  LocalDb *db() const {
    return state.db;
  }

  // Returns the root page
  Page *root_page(Context *context);

  // Sets the new root page
  void set_root_page(Page *root_page) {
    root_page->set_type(Page::kTypeBroot);
    state.btree_header->root_address = root_page->address();
    state.root_page = root_page;
  }

  // Returns the hash of the compare function
  uint32_t compare_hash() const {
    return state.btree_header->compare_hash;
  }

  // Creates and initializes the btree
  //
  // This function is called after the ups_db_t structure was allocated
  // and the file was opened
  void create(Context *context, PBtreeHeader *btree_header, DbConfig *dbconfig);

  // Opens and initializes the btree
  //
  // This function is called after the ups_db_t structure was allocated
  // and the file was opened
  void open(PBtreeHeader *btree_header, DbConfig *dbconfig);

  // Lookup a key in the index (ups_db_find)
  ups_status_t find(Context *context, LocalCursor *cursor, ups_key_t *key,
                  ByteArray *key_arena, ups_record_t *record,
                  ByteArray *record_arena, uint32_t flags);

  // Inserts (or updates) a key/record in the index (ups_db_insert)
  ups_status_t insert(Context *context, LocalCursor *cursor, ups_key_t *key,
                  ups_record_t *record, uint32_t flags);

  // Erases a key/record from the index (ups_db_erase).
  // If |duplicate_index| is 0 then all duplicates are erased, otherwise only
  // the specified duplicate is erased.
  ups_status_t erase(Context *context, LocalCursor *cursor, ups_key_t *key,
                  int duplicate_index, uint32_t flags);

  // Iterates over the whole index and calls |visitor| on every node
  void visit_nodes(Context *context, BtreeVisitor &visitor,
                  bool visit_internal_nodes);

  // Checks the integrity of the btree (ups_db_check_integrity)
  void check_integrity(Context *context, uint32_t flags);

  // Counts the keys in the btree
  uint64_t count(Context *context, bool distinct);

  // Drops this index. Deletes all records, overflow areas, extended
  // keys etc from the index; also used to avoid memory leaks when closing
  // in-memory Databases and to clean up when deleting on-disk Databases.
  void drop(Context *context);

  // Searches |parent| page for key |key| and returns the child
  // page in |child|.
  //
  // |page_manager_flags| are forwarded to PageManager::fetch.
  //
  // if |idxptr| is a valid pointer then it will return the anchor index
  // of the loaded page.
  Page *find_lower_bound(Context *context, Page *parent, const ups_key_t *key,
                  uint32_t page_manager_flags, int *idxptr);

  // Compares two keys
  // Returns -1, 0, +1 or higher positive values are the result of a
  // successful key comparison (0 if both keys match, -1 when
  // LHS < RHS key, +1 when LHS > RHS key).
  int compare_keys(ups_key_t *lhs, ups_key_t *rhs) const {
    return state.leaf_traits->compare_keys(state.db, lhs, rhs);
  }

  // Returns a BtreeNodeProxy for a Page
  BtreeNodeProxy *get_node_from_page(Page *page) {
    if (likely(page->node_proxy() != 0))
      return page->node_proxy();

    BtreeNodeProxy *proxy;
    PBtreeNode *node = PBtreeNode::from_page(page);
    if (node->is_leaf())
      proxy = leaf_node_from_page_impl(page);
    else
      proxy = internal_node_from_page_impl(page);

    page->set_node_proxy(proxy);
    return proxy;
  }

  // Returns the usage metrics
  static void fill_metrics(ups_env_metrics_t *metrics) {
    metrics->btree_smo_split = Globals::ms_btree_smo_split;
    metrics->btree_smo_merge = Globals::ms_btree_smo_merge;
    metrics->extended_keys = Globals::ms_extended_keys;
    metrics->extended_duptables = Globals::ms_extended_duptables;
    metrics->key_bytes_before_compression
            = Globals::ms_bytes_before_compression;
    metrics->key_bytes_after_compression
          = Globals::ms_bytes_after_compression;
  }

  // Returns the btree usage statistics
  BtreeStatistics *statistics() {
    return &state.statistics;
  }

  // Returns the class name (for testing)
  std::string test_get_classname() const {
    return state.leaf_traits->test_get_classname();
  }

  // Implementation of get_node_from_page() (for leaf nodes)
  BtreeNodeProxy *leaf_node_from_page_impl(Page *page) const {
    return state.leaf_traits->get_node_from_page_impl(page);
  }

  // Implementation of get_node_from_page() (for internal nodes)
  BtreeNodeProxy *internal_node_from_page_impl(Page *page) const {
    return state.internal_traits->get_node_from_page_impl(page);
  }

  // Flushes the PBtreeHeader to the Environment's header page
  void persist_configuration(Context *context, const DbConfig *dbconfig);

  BtreeIndexState state;
};

} // namespace upscaledb

#endif // UPS_BTREE_INDEX_H
