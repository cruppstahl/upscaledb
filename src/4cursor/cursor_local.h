/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * Cursor implementation for local databases
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_CURSOR_LOCAL_H
#define UPS_CURSOR_LOCAL_H

#include "0root/root.h"

#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "4txn/txn_cursor.h"
#include "3btree/btree_cursor.h"
#include "4db/db_local.h"
#include "4env/env.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;

// A single line in the dupecache structure - can reference a btree
// record or a txn-op
class DupeCacheLine
{
  public:
    DupeCacheLine(bool use_btree = true, uint64_t btree_dupeidx = 0)
      : m_btree_dupeidx(btree_dupeidx), m_op(0), m_use_btree(use_btree) {
      ups_assert(use_btree == true);
    }

    DupeCacheLine(bool use_btree, TransactionOperation *op)
      : m_btree_dupeidx(0), m_op(op), m_use_btree(use_btree) {
      ups_assert(use_btree == false);
    }

    // Returns true if this cache entry is a duplicate in the btree index
    // (otherwise it's a duplicate in the transaction index)
    bool use_btree() const {
      return (m_use_btree);
    }

    // Returns the btree duplicate index
    uint64_t get_btree_dupe_idx() {
      ups_assert(m_use_btree == true);
      return (m_btree_dupeidx);
    }

    // Sets the btree duplicate index
    void set_btree_dupe_idx(uint64_t idx) {
      m_use_btree = true;
      m_btree_dupeidx = idx;
      m_op = 0;
    }

    // Returns the txn-op duplicate
    TransactionOperation *get_txn_op() {
      ups_assert(m_use_btree == false);
      return (m_op);
    }

    // Sets the txn-op duplicate
    void set_txn_op(TransactionOperation *op) {
      m_use_btree = false;
      m_op = op;
      m_btree_dupeidx = 0;
    }

  private:
    // The btree duplicate index (of the original btree dupe table)
    uint64_t m_btree_dupeidx;

    // The txn op structure that we refer to
    TransactionOperation *m_op;

    // using btree or txn duplicates?
    bool m_use_btree;
};

//
// The dupecache is a cache for duplicate keys
//
class DupeCache
{
  public:
    // default constructor - creates an empty dupecache with room for 8
    // duplicates
    DupeCache() {
      m_elements.reserve(8);
    }

    // Returns the number of elements in the cache
    uint32_t get_count() const {
      return ((uint32_t)m_elements.size());
    }

    // Returns an element from the cache
    DupeCacheLine *get_element(unsigned idx) {
      return (&m_elements[idx]);
    }

    // Clones this dupe-cache into 'other'
    void clone(DupeCache *other) {
      other->m_elements = m_elements;
    }

    // Inserts a new item somewhere in the cache; resizes the
    // cache if necessary
    void insert(unsigned position, const DupeCacheLine &dcl) {
      m_elements.insert(m_elements.begin() + position, dcl);
    }

    // Append an element to the dupecache
    void append(const DupeCacheLine &dcl) {
      m_elements.push_back(dcl);
    }

    // Erases an item
    void erase(uint32_t position) {
      m_elements.erase(m_elements.begin() + position);
    }

    // Clears the cache; frees all resources
    void clear() {
      m_elements.resize(0);
    }

  private:
    // The cached elements
    std::vector<DupeCacheLine> m_elements;
};


//
// the Database Cursor
//
class LocalCursor : public Cursor
{
  public:
    // The flags have ranges:
    //  0 - 0x1000000-1:      btree_cursor
    //    > 0x1000000:        cursor
    enum {
      // Flags for set_to_nil, is_nil
      kBoth  = 0,
      kBtree = 1,
      kTxn   = 2,

      // Flag for sync(): do not use approx matching if the key
      // is not available
      kSyncOnlyEqualKeys = 0x200000,

      // Flag for sync(): do not load the key if there's an approx.
      // match. Only positions the cursor.
      kSyncDontLoadKey   = 0x100000,

      // Cursor flag: cursor is coupled to the txn-cursor
      kCoupledToTxn      = 0x1000000,

      // Flag for set_last_operation()
      kLookupOrInsert    = 0x10000
    };

  public:
    // Constructor; retrieves pointer to db and txn, initializes all members
    LocalCursor(LocalDatabase *db, Transaction *txn = 0);

    // Copy constructor; used for cloning a Cursor
    LocalCursor(LocalCursor &other);

    // Destructor; sets cursor to nil
    ~LocalCursor() {
      set_to_nil();
    }

    // Returns the Database that this cursor is operating on
    LocalDatabase *ldb() {
      return ((LocalDatabase *)m_db);
    }

    // Returns the Transaction cursor
    // TODO required?
    TransactionCursor *get_txn_cursor() {
      return (&m_txn_cursor);
    }

    // Returns the Btree cursor
    // TODO required?
    BtreeCursor *get_btree_cursor() {
      return (&m_btree_cursor);
    }

    // Sets the cursor to nil
    void set_to_nil(int what = kBoth);

    // Returns true if a cursor is nil (Not In List - does not point to any
    // key)
    // |what| is one of the flags kBoth, kTxn, kBtree
    bool is_nil(int what = kBoth);

    // Couples the cursor to the btree key
    void couple_to_btree() {
      m_flags &= ~kCoupledToTxn;
    }

    // Returns true if a cursor is coupled to the btree
    bool is_coupled_to_btree() const {
      return (!(m_flags & kCoupledToTxn));
    }

    // Couples the cursor to the txn-op
    void couple_to_txnop() {
      m_flags |= kCoupledToTxn;
    }

    // Returns true if a cursor is coupled to a txn-op
    bool is_coupled_to_txnop() const {
#if UPS_DEBUG
      if (m_flags & kCoupledToTxn)
        ups_assert(m_txn_cursor.get_coupled_op() != 0);
#endif
      return ((m_flags & kCoupledToTxn) ? true : false);
    }

    // Moves a Cursor (ups_cursor_move)
    ups_status_t move(Context *context, ups_key_t *key, ups_record_t *record,
                    uint32_t flags);

    // Closes the cursor (ups_cursor_close)
    virtual void close();

    // Couples the cursor to a duplicate in the dupe table
    // dupe_id is a 1 based index!!
    void couple_to_dupe(uint32_t dupe_id);

    // Synchronizes txn- and btree-cursor
    //
    // If txn-cursor is nil then try to move the txn-cursor to the same key
    // as the btree cursor.
    // If btree-cursor is nil then try to move the btree-cursor to the same key
    // as the txn cursor.
    // If both are nil, or both are valid, then nothing happens
    //
    // |equal_key| is set to true if the keys in both cursors are equal.
    void sync(Context *context, uint32_t flags, bool *equal_keys);

    // Returns the number of duplicates in the duplicate cache
    // The duplicate cache is updated if necessary
    uint32_t get_dupecache_count(Context *context, bool clear_cache = false) {
      if (!(m_db->get_flags() & UPS_ENABLE_DUPLICATE_KEYS))
        return (0);

      if (clear_cache)
        clear_dupecache();

      if (is_coupled_to_txnop())
        update_dupecache(context, kBtree | kTxn);
      else
        update_dupecache(context, kBtree);
      return (m_dupecache.get_count());
    }

    // Returns a pointer to the duplicate cache
    // TODO really required?
    DupeCache *get_dupecache() {
      return (&m_dupecache);
    }

    // Returns a pointer to the duplicate cache
    // TODO really required?
    const DupeCache *get_dupecache() const {
      return (&m_dupecache);
    }

    // Returns the current index in the dupe cache
    uint32_t get_dupecache_index() const {
      return (m_dupecache_index);
    }

    // Sets the current index in the dupe cache
    // TODO rename to set_duplicate_position()
    void set_dupecache_index(uint32_t index) {
      m_dupecache_index = index;
    }

    // Returns true if this cursor was never used before
    bool is_first_use() const {
      return (m_is_first_use);
    }

    // Stores the current operation; needed for ups_cursor_move
    // TODO should be private
    void set_last_operation(uint32_t last_operation) {
      m_last_operation = last_operation;
      m_is_first_use = false;
    }

    // Returns number of duplicates (ups_cursor_get_duplicate_count)
    uint32_t get_duplicate_count(Context *context);

  private:
    friend struct TxnCursorFixture;

    // Returns the LocalEnvironment instance
    LocalEnvironment *lenv() {
      return ((LocalEnvironment *)ldb()->get_env());
    }

    // Implementation of overwrite()
    virtual ups_status_t do_overwrite(ups_record_t *record, uint32_t flags);

    // Returns number of duplicates (ups_cursor_get_duplicate_count)
    virtual ups_status_t do_get_duplicate_count(uint32_t flags,
                                uint32_t *pcount);

    // Get current record size (ups_cursor_get_record_size)
    virtual ups_status_t do_get_record_size(uint64_t *psize);

    // Implementation of get_duplicate_position()
    virtual ups_status_t do_get_duplicate_position(uint32_t *pposition);

    // Clears the dupecache and disconnect the Cursor from any duplicate key
    void clear_dupecache() {
      m_dupecache.clear();
      set_dupecache_index(0);
    }

    // Updates (or builds) the dupecache for a cursor
    //
    // The |what| parameter specifies if the dupecache is initialized from
    // btree (kBtree), from txn (kTxn) or both.
    void update_dupecache(Context *context, uint32_t what);

    // Appends the duplicates of the BtreeCursor to the duplicate cache.
    void append_btree_duplicates(Context *context, BtreeCursor *btc,
                    DupeCache *dc);

    // Checks if a btree cursor points to a key that was overwritten or erased
    // in the txn-cursor
    //
    // This is needed when moving the cursor backwards/forwards
    // and consolidating the btree and the txn-tree
    ups_status_t check_if_btree_key_is_erased_or_overwritten(Context *context);

    // Compares btree and txn-cursor; stores result in lastcmp
    int compare(Context *context);

    // Returns true if this key has duplicates
    bool has_duplicates() const {
      return (m_dupecache.get_count() > 0);
    }

    // Moves cursor to the first duplicate
    ups_status_t move_first_dupe(Context *context);

    // Moves cursor to the last duplicate
    ups_status_t move_last_dupe(Context *context);

    // Moves cursor to the next duplicate
    ups_status_t move_next_dupe(Context *context);

    // Moves cursor to the previous duplicate
    ups_status_t move_previous_dupe(Context *context);

    // Moves cursor to the first key
    ups_status_t move_first_key(Context *context, uint32_t flags);

    // Moves cursor to the last key
    ups_status_t move_last_key(Context *context, uint32_t flags);

    // Moves cursor to the next key
    ups_status_t move_next_key(Context *context, uint32_t flags);

    // Moves cursor to the previous key
    ups_status_t move_previous_key(Context *context, uint32_t flags);

    // Moves cursor to the first key - helper function
    ups_status_t move_first_key_singlestep(Context *context);

    // Moves cursor to the last key - helper function
    ups_status_t move_last_key_singlestep(Context *context);

    // Moves cursor to the next key - helper function
    ups_status_t move_next_key_singlestep(Context *context);

    // Moves cursor to the previous key - helper function
    ups_status_t move_previous_key_singlestep(Context *context);

    // A Cursor which can walk over Transaction trees
    TransactionCursor m_txn_cursor;

    // A Cursor which can walk over B+trees
    BtreeCursor m_btree_cursor;

    // A cache for all duplicates of the current key. needed for
    // ups_cursor_move, ups_find and other functions. The cache is
    // used to consolidate all duplicates of btree and txn.
    DupeCache m_dupecache;

    /** The current position of the cursor in the cache. This is a
     * 1-based index. 0 means that the cache is not in use. */
    uint32_t m_dupecache_index;

    // The last operation (insert/find or move); needed for
    // ups_cursor_move. Values can be UPS_CURSOR_NEXT,
    // UPS_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT
    uint32_t m_last_operation;

    // flags & state of the cursor
    uint32_t m_flags;

    // The result of the last compare operation
    int m_last_cmp;

    // true if this cursor was never used
    bool m_is_first_use;
};

} // namespace upscaledb

#endif /* UPS_CURSOR_LOCAL_H */
