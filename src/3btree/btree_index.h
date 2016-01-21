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

/*
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_BTREE_INDEX_H
#define UPS_BTREE_INDEX_H

#include "0root/root.h"

#include <algorithm>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/abi.h"
#include "1base/dynamic_array.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_node.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
class LocalCursor;

#include "1base/packstart.h"

//
// The persistent btree index descriptor. This structure manages the
// persistent btree metadata.
//
UPS_PACK_0 class UPS_PACK_1 PBtreeHeader
{
  public:
    PBtreeHeader() {
      ::memset(this, 0, sizeof(*this));
    }

    // Returns the database name
    uint16_t database_name() const {
      return (m_dbname);
    }

    // Sets the database name
    void set_database_name(uint16_t name) {
      m_dbname = name;
    }

    // Returns the btree's max. key_size
    size_t key_size() const {
      return (m_key_size);
    }

    // Sets the btree's max. key_size
    void set_key_size(uint16_t key_size) {
      m_key_size = key_size;
    }

    // Returns the record size (or 0 if none was specified)
    uint32_t record_size() const {
      return (m_rec_size);
    }

    // Sets the record size
    void set_rec_size(uint32_t rec_size) {
      m_rec_size = rec_size;
    }

    // Returns the btree's key type
    uint16_t key_type() const {
      return (m_key_type);
    }

    // Sets the btree's key type
    void set_key_type(uint16_t key_type) {
      m_key_type = key_type;
    }

    // Returns the address of the btree's root page.
    uint64_t root_address() const {
      return (m_root_address);
    }

    // Sets the address of the btree's root page.
    void set_root_address(uint64_t root_address) {
      m_root_address = root_address;
    }

    // Returns the btree's flags
    uint32_t flags() const {
      return (m_flags);
    }

    // Sets the btree's flags
    void set_flags(uint32_t flags) {
      m_flags = flags;
    }

    // Returns the record compression
    uint8_t record_compression() const {
      return (m_compression >> 4);
    }

    // Sets the record compression
    void set_record_compression(int algorithm) {
      m_compression |= algorithm << 4;
    }

    // Returns the key compression
    uint8_t key_compression() const {
      return (m_compression & 0xf);
    }

    // Sets the key compression
    void set_key_compression(int algorithm) {
      m_compression |= algorithm & 0xf;
    }

    // Returns the hash of the compare function
    uint32_t compare_hash() const {
      return (m_compare_hash);
    }

    // Sets the hash of the compare function
    void set_compare_hash(uint32_t compare_hash) {
      m_compare_hash = compare_hash;
    }

  private:
    // address of the root-page
    uint64_t m_root_address;

    // flags for this database
    uint32_t m_flags;

    // The name of the database
    uint16_t m_dbname;

    // key size used in the pages
    uint16_t m_key_size;

    // key type
    uint16_t m_key_type;

    // PRO: for storing key and record compression algorithm */
    uint8_t m_compression;

    // reserved
    uint8_t m_reserved1;

    // the record size
    uint32_t m_rec_size;

    // hash of the custom compare function
    uint32_t m_compare_hash;
} UPS_PACK_2;

#include "1base/packstop.h"

struct Context;
class LocalDatabase;
class BtreeNodeProxy;
struct PDupeEntry;
struct BtreeVisitor;

//
// Abstract base class, overwritten by a templated version
//
class BtreeIndexTraits
{
  public:
    // virtual destructor
    virtual ~BtreeIndexTraits() { }

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(LocalDatabase *db, ups_key_t *lhs,
                    ups_key_t *rhs) const = 0;

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
    BtreeIndex(LocalDatabase *db, PBtreeHeader *btree_header,
                    uint32_t flags, uint32_t key_type, uint32_t key_size);

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
    size_t key_size() const {
      return (m_key_size);
    }

    // Returns the record size
    size_t record_size() const {
      return (m_rec_size);
    }

    // Returns the internal key type
    uint16_t key_type() const {
      return (m_key_type);
    }

    // Returns the address of the root page
    uint64_t root_address() const {
      return (m_root_address);
    }

    // Returns the btree flags
    uint32_t flags() const {
      return (m_flags);
    }

    // Returns the hash of the compare function
    uint32_t compare_hash() const {
      return (m_compare_hash);
    }

    // Creates and initializes the btree
    //
    // This function is called after the ups_db_t structure was allocated
    // and the file was opened
    void create(Context *context, uint16_t key_type, uint32_t key_size,
                    uint32_t rec_size, const std::string &compare_name);

    // Opens and initializes the btree
    //
    // This function is called after the ups_db_t structure was allocated
    // and the file was opened
    void open();

    // Sets the record compression algorithm
    void set_record_compression(Context *context, int algo);

    // Returns the record compression algorithm
    int record_compression();

    // Sets the key compression algorithm
    void set_key_compression(Context *context, int algo);

    // Returns the key compression algorithm
    int key_compression();

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

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    int compare_keys(ups_key_t *lhs, ups_key_t *rhs) const {
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
    static void fill_metrics(ups_env_metrics_t *metrics) {
      metrics->btree_smo_split = ms_btree_smo_split;
      metrics->btree_smo_merge = ms_btree_smo_merge;
      metrics->extended_keys = Globals::ms_extended_keys;
      metrics->extended_duptables = Globals::ms_extended_duptables;
      metrics->key_bytes_before_compression
              = Globals::ms_bytes_before_compression;
      metrics->key_bytes_after_compression
              = Globals::ms_bytes_after_compression;
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
    friend class BtreeUpdateAction;
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

    // Implementation of get_node_from_page() (for leaf nodes)
    BtreeNodeProxy *get_leaf_node_from_page_impl(Page *page) const {
      return (m_leaf_traits->get_node_from_page_impl(page));
    }

    // Implementation of get_node_from_page() (for internal nodes)
    BtreeNodeProxy *get_internal_node_from_page_impl(Page *page) const {
      return (m_internal_traits->get_node_from_page_impl(page));
    }

    // Sets the address of the root page
    void set_root_address(Context *context, uint64_t address) {
      m_root_address = address;
      flush_descriptor(context);
    }

    // Flushes the PBtreeHeader to the Environment's header page
    void flush_descriptor(Context *context);

    // Searches |parent| page for key |key| and returns the child
    // page in |child|.
    //
    // |page_manager_flags| are forwarded to PageManager::fetch.
    //
    // if |idxptr| is a valid pointer then it will return the anchor index
    // of the loaded page.
    Page *find_lower_bound(Context *context, Page *parent, const ups_key_t *key,
                    uint32_t page_manager_flags, int *idxptr);

    // pointer to the database object
    LocalDatabase *m_db;

    // the Traits class wrapping the template parameters (factory for
    // leaf nodes)
    BtreeIndexTraits *m_leaf_traits;

    // the Traits class wrapping the template parameters (factory for
    // internal nodes)
    BtreeIndexTraits *m_internal_traits;

    // the key_size of this btree index
    uint16_t m_key_size;

    // the key_type of this btree index
    uint16_t m_key_type;

    // the record size (or 0 if none was specified)
    uint32_t m_rec_size;

    // the index of the PBtreeHeader in the Environment's header page
    PBtreeHeader *m_btree_header;

    // the persistent flags of this btree index
    uint32_t m_flags;

    // address of the root-page
    uint64_t m_root_address;

    // hash of the custom compare function
    uint32_t m_compare_hash;

    // the btree statistics
    BtreeStatistics m_statistics;

    // usage metrics - number of page splits
    static uint64_t ms_btree_smo_split;

    // usage metrics - number of page merges
    static uint64_t ms_btree_smo_merge;

    // usage metrics - number of page shifts
    static uint64_t ms_btree_smo_shift;
};

} // namespace upscaledb

#endif /* UPS_BTREE_INDEX_H */
