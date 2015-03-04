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
 * A Cursor is an object which is used to traverse a Database.
 *
 * A Cursor structure is separated into 3 components:
 * 1. The btree cursor
 *      This cursor can traverse btrees. It is described and implemented
 *      in btree_cursor.h.
 * 2. The txn cursor
 *      This cursor can traverse txn-trees. It is described and implemented
 *      in txn_cursor.h.
 * 3. The upper layer
 *      This layer acts as a kind of dispatcher for both cursors. If
 *      Transactions are used, then it also uses a duplicate cache for
 *      consolidating the duplicate keys from both cursors. This layer is
 *      described and implemented in cursor.h (this file).
 *
 * A Cursor can have several states. It can be
 * 1. NIL (not in list) - this is the default state, meaning that the Cursor
 *      does not point to any key. If the Cursor was initialized, then it's
 *      "NIL". If the Cursor was erased (i.e. with ham_cursor_erase) then it's
 *      also "NIL".
 *
 *      relevant functions:
 *          Cursor::is_nil
 *          Cursor::set_to_nil
 *
 * 2. Coupled to the txn-cursor - meaning that the Cursor points to a key
 *      that is modified in a Transaction. Technically, the txn-cursor points
 *      to a TransactionOperation structure.
 *
 *      relevant functions:
 *          Cursor::is_coupled_to_txnop
 *          Cursor::couple_to_txnop
 *
 * 3. Coupled to the btree-cursor - meaning that the Cursor points to a key
 *      that is stored in a Btree. A Btree cursor itself can then be coupled
 *      (it directly points to a page in the cache) or uncoupled, meaning that
 *      the page was purged from the cache and has to be fetched from disk when
 *      the Cursor is used again. This is described in btree_cursor.h.
 *
 *      relevant functions:
 *          Cursor::is_coupled_to_btree
 *          Cursor::couple_to_btree
 *
 * The dupecache is used when information from the btree and the txn-tree
 * is merged. The btree cursor has its private dupecache. The dupecache
 * increases performance (and complexity).
 *
 * The cursor interface is used in db_local.cc. Many of the functions use
 * a high-level cursor interface (i.e. @ref cursor_create, @ref cursor_clone)
 * while some directly use the low-level interfaces of btree_cursor.h and
 * txn_cursor.h. Over time i will clean this up, trying to maintain a clear
 * separation of the 3 layers, and only accessing the top-level layer in
 * cursor.h. This is work in progress.
 *
 * In order to speed up Cursor::move() we keep track of the last compare
 * between the two cursors. i.e. if the btree cursor is currently pointing to
 * a larger key than the txn-cursor, the 'lastcmp' field is <0 etc.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_CURSORS_H
#define HAM_CURSORS_H

#include "0root/root.h"

#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "4txn/txn_cursor.h"
#include "3btree/btree_cursor.h"
#include "3blob_manager/blob_manager.h"
#include "4db/db_local.h"
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ham_cursor_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_cursor_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ham_cursor_t
{
  bool _dummy;
};

namespace hamsterdb {

struct Context;

// A single line in the dupecache structure - can reference a btree
// record or a txn-op
class DupeCacheLine
{
  public:
    DupeCacheLine(bool use_btree = true, uint64_t btree_dupeidx = 0)
      : m_btree_dupeidx(btree_dupeidx), m_op(0), m_use_btree(use_btree) {
      ham_assert(use_btree == true);
    }

    DupeCacheLine(bool use_btree, TransactionOperation *op)
      : m_btree_dupeidx(0), m_op(op), m_use_btree(use_btree) {
      ham_assert(use_btree == false);
    }

    // Returns true if this cache entry is a duplicate in the btree index
    // (otherwise it's a duplicate in the transaction index)
    bool use_btree() const {
      return (m_use_btree);
    }

    // Returns the btree duplicate index
    uint64_t get_btree_dupe_idx() {
      ham_assert(m_use_btree == true);
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
      ham_assert(m_use_btree == false);
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
class DupeCache {
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

    // Returns a pointer to the first element from the cache
    DupeCacheLine *get_first_element() {
      return (&m_elements[0]);
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
class Cursor
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

      // Flag for set_lastop()
      kLookupOrInsert    = 0x10000
    };

  public:
    // Constructor; retrieves pointer to db and txn, initializes all members
    Cursor(LocalDatabase *db, Transaction *txn = 0, uint32_t flags = 0);

    // Copy constructor; used for cloning a Cursor
    Cursor(Cursor &other);

    // Destructor; sets cursor to nil
    ~Cursor() {
      set_to_nil();
    }

    // Returns the Database
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the Transaction handle
    Transaction *get_txn() {
      return (m_txn);
    }

    // Sets the Transaction handle; often used to assign a temporary
    // Transaction to this cursor
    void set_txn(Transaction *txn) {
      m_txn = txn;
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
      return ((m_flags & kCoupledToTxn) ? true : false);
    }

    // Retrieves the number of duplicates of the current key
    uint32_t get_record_count(Context *context, uint32_t flags);

    // Retrieves the duplicate position of a cursor
    uint32_t get_duplicate_position();

    // Retrieves the size of the current record
    uint64_t get_record_size(Context *context);

    // Overwrites the record of the current key
    //
    // The Transaction is passed as a separate pointer since it might be a
    // local/temporary Transaction that was created only for this single
    // operation.
    ham_status_t overwrite(Context *context, Transaction *txn,
                    ham_record_t *record, uint32_t flags);

    // Moves a Cursor (ham_cursor_move)
    ham_status_t move(Context *context, ham_key_t *key, ham_record_t *record,
                    uint32_t flags);

    // Closes an existing cursor (ham_cursor_close)
    void close();

    // Updates (or builds) the dupecache for a cursor
    //
    // The |what| parameter specifies if the dupecache is initialized from
    // btree (kBtree), from txn (kTxn) or both.
    void update_dupecache(Context *context, uint32_t what);

    // Appends the duplicates of the BtreeCursor to the duplicate cache.
    void append_btree_duplicates(Context *context, BtreeCursor *btc,
                    DupeCache *dc);

    // Clears the dupecache and disconnect the Cursor from any duplicate key
    void clear_dupecache() {
      m_dupecache.clear();
      set_dupecache_index(0);
    }

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
    uint32_t get_dupecache_count(Context *context) {
      if (!(m_db->get_flags() & HAM_ENABLE_DUPLICATE_KEYS))
        return (0);

      TransactionCursor *txnc = get_txn_cursor();
      if (txnc->get_coupled_op())
        update_dupecache(context, kBtree | kTxn);
      else
        update_dupecache(context, kBtree);
      return (m_dupecache.get_count());
    }

    // Get the 'next' Cursor in this Database
    Cursor *get_next() {
      return (m_next);
    }

    // Set the 'next' Cursor in this Database
    void set_next(Cursor *next) {
      m_next = next;
    }

    // Get the 'previous' Cursor in this Database
    Cursor *get_previous() {
      return (m_previous);
    }

    // Set the 'previous' Cursor in this Database
    void set_previous(Cursor *previous) {
      m_previous = previous;
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

    // Returns the remote Cursor handle
    uint64_t get_remote_handle() {
      return (m_remote_handle);
    }

    // Returns the remote Cursor handle
    void set_remote_handle(uint64_t handle) {
      m_remote_handle = handle;
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
    void set_dupecache_index(uint32_t index) {
      m_dupecache_index = index;
    }

    // Returns true if this cursor was never used before
    // TODO this is identical to is_nil()??
    bool is_first_use() const {
      return (m_is_first_use);
    }

    // Stores the current operation; needed for ham_cursor_move
    // TODO should be private
    void set_lastop(uint32_t lastop) {
      m_lastop = lastop;
      m_is_first_use = false;
    }

  private:
    // Checks if a btree cursor points to a key that was overwritten or erased
    // in the txn-cursor
    //
    // This is needed when moving the cursor backwards/forwards
    // and consolidating the btree and the txn-tree
    ham_status_t check_if_btree_key_is_erased_or_overwritten(Context *context);

    // Compares btree and txn-cursor; stores result in lastcmp
    int compare(Context *context);

    // Returns true if this key has duplicates
    bool has_duplicates() const {
      return (m_dupecache.get_count() > 0);
    }

    // Moves cursor to the first duplicate
    ham_status_t move_first_dupe(Context *context);

    // Moves cursor to the last duplicate
    ham_status_t move_last_dupe(Context *context);

    // Moves cursor to the next duplicate
    ham_status_t move_next_dupe(Context *context);

    // Moves cursor to the previous duplicate
    ham_status_t move_previous_dupe(Context *context);

    // Moves cursor to the first key
    ham_status_t move_first_key(Context *context, uint32_t flags);

    // Moves cursor to the last key
    ham_status_t move_last_key(Context *context, uint32_t flags);

    // Moves cursor to the next key
    ham_status_t move_next_key(Context *context, uint32_t flags);

    // Moves cursor to the previous key
    ham_status_t move_previous_key(Context *context, uint32_t flags);

    // Moves cursor to the first key - helper function
    ham_status_t move_first_key_singlestep(Context *context);

    // Moves cursor to the last key - helper function
    ham_status_t move_last_key_singlestep(Context *context);

    // Moves cursor to the next key - helper function
    ham_status_t move_next_key_singlestep(Context *context);

    // Moves cursor to the previous key - helper function
    ham_status_t move_previous_key_singlestep(Context *context);

    // Pointer to the Database object
    LocalDatabase *m_db;

    // Pointer to the Transaction
    Transaction *m_txn;

    // A Cursor which can walk over Transaction trees
    TransactionCursor m_txn_cursor;

    // A Cursor which can walk over B+trees
    BtreeCursor m_btree_cursor;

    // The remote database handle
    uint64_t m_remote_handle;

    // Linked list of all Cursors in this Database
    Cursor *m_next, *m_previous;

    // A cache for all duplicates of the current key. needed for
    // ham_cursor_move, ham_find and other functions. The cache is
    // used to consolidate all duplicates of btree and txn.
    DupeCache m_dupecache;

    /** The current position of the cursor in the cache. This is a
     * 1-based index. 0 means that the cache is not in use. */
    uint32_t m_dupecache_index;

    // The last operation (insert/find or move); needed for
    // ham_cursor_move. Values can be HAM_CURSOR_NEXT,
    // HAM_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT
    uint32_t m_lastop;

    // The result of the last compare operation
    int m_last_cmp;

    // Cursor flags
    uint32_t m_flags;

    // true if this cursor was never used
    bool m_is_first_use;
};

} // namespace hamsterdb

#endif /* HAM_CURSORS_H */
