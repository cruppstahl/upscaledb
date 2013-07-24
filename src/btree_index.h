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

#include "btree_cursor.h"
#include "btree_key.h"
#include "db.h"
#include "util.h"
#include "btree_enum.h"
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

    // Returns the address of the btree's root page.
    ham_u64_t get_root_address() const {
      return (ham_db2h_offset(m_self));
    }

    // Sets the address of the btree's root page.
    void set_root_address(ham_u64_t n) {
      m_self = ham_h2db_offset(n);
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
    // The name of the DB: 1..HAM_DEFAULT_DATABASE_NAME-1
    ham_u16_t m_dbname;

    // maximum keys in an internal page
    ham_u16_t m_maxkeys;

    // key size used in the pages
    ham_u16_t m_keysize;

    // reserved
    ham_u16_t m_reserved1;

    // address of the root-page
    ham_u64_t m_self;

    // flags for this database
    ham_u32_t m_flags;

    // reserved
    ham_u64_t m_reserved2;

    // reserved
    ham_u32_t m_reserved3;
} HAM_PACK_2;

#include "packstop.h"

class LocalDatabase;

//
// The Btree.
//
class BtreeIndex
{
  public:
    // Constructor; creates and initializes a new btree
    BtreeIndex(LocalDatabase *db, ham_u32_t descriptor, ham_u32_t flags = 0);

    // Creates and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    ham_status_t create(ham_u16_t keysize);

    // Opens and initializes the btree
    //
    // This function is called after the ham_db_t structure was allocated
    // and the file was opened
    ham_status_t open();

    // Returns the database pointer
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the internal key size
    ham_u16_t get_keysize() const {
      return (m_keysize);
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
      return (m_maxkeys / 2);
    }

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
    ham_status_t enumerate(BtreeVisitor *visitor);

    // Checks the integrity of the btree (ham_db_check_integrity)
    ham_status_t check_integrity();

    // Counts the keys in the btree (ham_db_get_key_count)
    ham_status_t get_key_count(ham_u32_t flags, ham_u64_t *pkeycount);

    // Erases all blobs from the index; used for cleaning up in-memory
    // blobs to avoid memory leaks
    ham_status_t free_all_blobs();

    // Erases all extended keys from a page
    static ham_status_t free_page_extkeys(Page *page, ham_u32_t flags);

    // Returns the ByteArray for keydata1
    ByteArray *get_keyarena1() {
      return (&m_keydata1);
    }

    // Returns the ByteArray for keydata2
    ByteArray *get_keyarena2() {
      return (&m_keydata2);
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

    // Performs a binary search for the smallest element which is >= the
    // key. also returns the comparison value in cmp; if *cmp == 0 then
    // the keys are equal
    ham_status_t get_slot(Page *page, ham_key_t *key, ham_s32_t *slot,
                    int *cmp = 0);

    // Copares a public key (ham_key_t, LHS) to an internal key indexed in a
    // page
    //
    // Returns -1, 0, +1: lhs < rhs, lhs = rhs, lhs > rhs
    // A return values less than -1 is a ham_status_t error
    int compare_keys(Page *page, ham_key_t *lhs, ham_u16_t rhs);

    // Creates a preliminary copy of an PBtreeKey |src| to a ham_key_t |dest|
    // in such a way that LocalDatabase::compare_keys can use the data and
    // optionally call LocalDatabase::get_extended_key on this key to obtain
    // all key data, when this is an extended key.
    //
    // |which| specifies whether keydata1 (which = 0) or keydata2 is used
    // to store the pointer in the btree. The pointers are kept to avoid
    // permanent re-allocations (improves performance)
    ham_status_t prepare_key_for_compare(int which, PBtreeKey *src,
                    ham_key_t *dest);

    // Copies an internal key;
    // Allocates memory unless HAM_KEY_USER_ALLOC is set
    ham_status_t copy_key(const PBtreeKey *source, ham_key_t *dest);

    // Reads a key and stores a copy in |dest|
    //
    // The memory backing dest->data can either be provided by the user
    // (HAM_KEY_USER_ALLOC) or is allocated using the ByteArray in the
    // Transaction or in the Database.
    // TODO use arena; get rid of txn parameter
    ham_status_t read_key(Transaction *txn, PBtreeKey *source,
                    ham_key_t *dest);

    // Reads a record and stores it in |dest|. The record is identified
    // by |ridptr|. TINY and SMALL records are also respected.
    // If HAM_DIRECT_ACCESS is set then the rid-pointer is casted to the
    // original record data.
    // TODO use arena; get rid of txn parameter
    ham_status_t read_record(Transaction *txn, ham_u64_t *ridptr,
                    ham_record_t *record, ham_u32_t flags);

    // pointer to the database object
    LocalDatabase *m_db;

    // the keysize of this btree index
    ham_u16_t m_keysize;

    // the index of the PBtreeHeader in the Environment's header page
    ham_u32_t m_descriptor_index;

    // the persistent flags of this btree index
    ham_u32_t m_flags;

    // address of the root-page
    ham_u64_t m_root_address;

    // maximum keys in an internal page
    ham_u16_t m_maxkeys;

    // two pointers for managing key data; these pointers are used to
    // avoid frequent mallocs in key_compare_pub_to_int() etc
    ByteArray m_keydata1;
    ByteArray m_keydata2;

    // the btree statistics
    BtreeStatistics m_statistics;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_H__ */
