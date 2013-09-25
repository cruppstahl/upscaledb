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

#ifndef HAM_BTREE_INDEX_H__
#define HAM_BTREE_INDEX_H__

#include "endianswap.h"

#include "db.h"
#include "util.h"
#include "btree_cursor.h"
#include "btree_key.h"
#include "btree_stats.h"

namespace hamsterdb {

#include "packstart.h"

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

    // Returns the max. number of keys in a page
    ham_u16_t get_maxkeys() const {
      return (ham_db2h16(m_maxkeys));
    }

    // Returns the max. number of keys in a page
    void set_maxkeys(ham_u16_t maxkeys) {
      m_maxkeys = ham_h2db16(maxkeys);
    }

    // Returns the btree's max. keysize
    ham_u16_t get_keysize() const {
      return (ham_db2h16(m_keysize));
    }

    // Sets the btree's max. keysize
    void set_keysize(ham_u16_t n) {
      m_keysize = ham_h2db16(n);
    }

    // Returns the btree's key type
    ham_u16_t get_keytype() const {
      return (ham_db2h16(m_keytype));
    }

    // Sets the btree's key type
    void set_keytype(ham_u16_t type) {
      m_keytype = ham_h2db16(type);
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

  private:
    // address of the root-page
    ham_u64_t m_root_address;

    // flags for this database
    ham_u32_t m_flags;

    // The name of the database
    ham_u16_t m_dbname;

    // maximum keys in an internal page
    ham_u16_t m_maxkeys;

    // key size used in the pages
    ham_u16_t m_keysize;

    // key type
    ham_u16_t m_keytype;

    // reserved for record size
    ham_u32_t m_recsize;

    // start of reserved space for this index
    ham_u64_t m_reserved_start;

    // number of reserved pages for this index
    ham_u16_t m_reserved_pages;

    // reserved for padding
    ham_u16_t m_padding1;
    ham_u32_t m_padding2;

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
// The Btree. Derived by BtreeIndexImpl, which uses template policies to
// define the btree node layout.
//
class BtreeIndex
{
  public:
    // Constructor; creates and initializes a new btree
    BtreeIndex(LocalDatabase *db, ham_u32_t descriptor, ham_u32_t flags = 0);

    // Virtual destructor
    virtual ~BtreeIndex() { }

    // Returns the database pointer
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the database pointer
    LocalDatabase *get_db() const {
      return (m_db);
    }

    // Returns the internal key size
    ham_u16_t get_keysize() const {
      return (m_keysize);
    }

    // Returns the internal key type
    ham_u16_t get_keytype() const {
      return (m_keytype);
    }

    // Returns the address of the root page
    ham_u64_t get_root_address() const {
      return (m_root_address);
    }

    // Returns the btree flags
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    // Returns maximum number of keys per (internal) node
    ham_u16_t get_maxkeys() const {
      return (m_maxkeys);
    }

    // Returns the minimum number of keys per node - less keys require a
    // SMO (merge or shift)
    ham_u16_t get_minkeys() const {
      return (m_maxkeys / 5);
    }

    // Returns the default key size
    virtual ham_u16_t get_default_keysize() const = 0;

    // Creates and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    ham_status_t create(ham_u16_t keysize, ham_u16_t keytype);

    // Opens and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    ham_status_t open();

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
    ham_status_t enumerate(BtreeVisitor &visitor,
                    bool visit_internal_nodes = false);

    // Checks the integrity of the btree (ham_db_check_integrity)
    ham_status_t check_integrity();

    // Counts the keys in the btree (ham_db_get_key_count)
    ham_status_t get_key_count(ham_u32_t flags, ham_u64_t *pkeycount);

    // Erases all records, overflow areas, extended keys etc from the index;
    // used to avoid memory leaks when closing in-memory Databases and to
    // clean up when deleting on-disk Databases.
    ham_status_t release();

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(ham_key_t *lhs, ham_key_t *rhs) const = 0;

    // Returns a BtreeNodeProxy for a Page
    BtreeNodeProxy *get_node_from_page(Page *page);

    // Returns the usage metrics
    static void get_metrics(ham_env_metrics_t *metrics) {
      metrics->btree_smo_split = ms_btree_smo_split;
      metrics->btree_smo_merge = ms_btree_smo_merge;
      metrics->btree_smo_shift = ms_btree_smo_shift;
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

    // Implementation of get_node_from_page()
    virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) = 0;

    // Sets the address of the root page
    void set_root_address(ham_u64_t address) {
      m_root_address = address;
      flush_descriptor();
    }

    // Calculates the "maxkeys" values - the limit of keys per page
    ham_size_t calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize);

    // Flushes the PBtreeHeader to the Environment's header page
    void flush_descriptor();

    // Returns the btree usage statistics
    BtreeStatistics *get_statistics() {
      return (&m_statistics);
    }

    // Searches |parent| page for key |key| and returns the child
    // page in |child|.
    //
    // if |idxptr| is a valid pointer then it will return the anchor index
    // of the loaded page.
    ham_status_t find_internal(Page *parent, ham_key_t *key, Page **pchild,
                    ham_s32_t *idxptr = 0);

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

    // the keysize of this btree index
    ham_u16_t m_keysize;

    // the keytype of this btree index
    ham_u16_t m_keytype;

    // the index of the PBtreeHeader in the Environment's header page
    ham_u32_t m_descriptor_index;

    // the persistent flags of this btree index
    ham_u32_t m_flags;

    // address of the root-page
    ham_u64_t m_root_address;

    // maximum keys in an internal page
    ham_u16_t m_maxkeys;

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

#include "btree_node_proxy.h"
#include "btree_node_legacy.h"

namespace hamsterdb {

//
// A Btree which uses template parameters to define the btree node layout.
//
template<class NodeLayout, class Comparator>
class BtreeIndexImpl : public BtreeIndex
{
  public:
    // Constructor; creates and initializes a new btree
    BtreeIndexImpl(LocalDatabase *db, ham_u32_t descriptor,
                    ham_u32_t flags = 0)
      : BtreeIndex(db, descriptor, flags) {
    }

    // Returns the default key size
    virtual ham_u16_t get_default_keysize() const {
      return (NodeLayout::get_default_keysize());
    }

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(ham_key_t *lhs, ham_key_t *rhs) const {
      Comparator cmp(get_db());
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

  private:
    // Implementation of get_node_from_page()
    virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) {
      return new BtreeNodeProxyImpl<NodeLayout, Comparator>(page);
    }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_H__ */
