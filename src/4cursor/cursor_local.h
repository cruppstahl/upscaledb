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
 * Cursor implementation for local databases
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_CURSOR_LOCAL_H
#define HAM_CURSOR_LOCAL_H

#include "0root/root.h"

#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "4txn/txn_cursor.h"
#include "3btree/btree_cursor.h"
#include "4db/db_local.h"
#include "4env/env.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;

// A single line in the dupecache structure - can reference a btree
// record or a txn-op
class Duplicate
{
  public:
    Duplicate(int duplicate_index)
      : m_duplicate_index(duplicate_index), m_action(0) {
    }

    Duplicate(DeltaAction *action)
      : m_duplicate_index(-1), m_action(action) {
    }

    // Returns the btree duplicate index
    int duplicate_index() const {
      return (m_duplicate_index);
    }

    // Sets the btree duplicate index
    void set_duplicate_index(int index) {
      m_duplicate_index = index;
      m_action = 0;
    }

    // Returns the DeltaAction
    DeltaAction *action() {
      return (m_action);
    }

    // Sets the DeltaAction pointer
    void set_action(DeltaAction *action) {
      m_action = action;
      m_duplicate_index = -1;
    }

  private:
    // The btree duplicate index (of the original btree dupe table)
    int m_duplicate_index;

    // The DeltaAction structure
    DeltaAction *m_action;
};

typedef std::vector<Duplicate> DuplicateCache;

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
      kBoth        = 0,
      kBtree       = 1,
      kTxn         = 2,
      kDeltaUpdate = 3,

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
      return ((m_flags & kCoupledToTxn) ? true : false);
    }

    // Moves a Cursor (ham_cursor_move)
    ham_status_t move(Context *context, ham_key_t *key, ham_record_t *record,
                    uint32_t flags);

    // Closes the cursor (ham_cursor_close)
    virtual void close();

    // Synchronizes BtreeCursor with the DeltaUpdates; sets m_last_cmp.
    // Will not do anything if m_last_cmp is already 0.
    // Clears the DuplicateCache.
    void sync(Context *context);

    // Returns the current index in the DuplicateCache
    int get_dupecache_index() const {
      return (m_dupecache_index);
    }

    // Sets the current index in the DuplicateCache
    // TODO rename to set_duplicate_position()
    void set_dupecache_index(int index) {
      m_dupecache_index = index;
    }

    // Returns true if this cursor was never used before
    bool is_first_use() const {
      return (m_is_first_use);
    }

    // Stores the current operation; needed for ham_cursor_move
    // TODO should be private
    void set_last_operation(uint32_t last_operation) {
      m_last_operation = last_operation;
      m_is_first_use = false;
    }

    // Returns number of duplicates (ham_cursor_get_duplicate_count)
    uint32_t duplicate_count(Context *context);

    // Couples the cursor to the duplicate at m_duplicate_cache[index],
    // sets m_duplicate_index
    void couple_to_duplicate(int index);

    // Updates (or builds) the DuplicateCache for a cursor
    void update_duplicate_cache(Context *context, bool force_sync = false);

    // Same as above, but more specialized
    void update_duplicate_cache(Context *context, BtreeNodeProxy *node,
                        int slot, DeltaUpdate *du);

  private:
    friend struct TxnCursorFixture;
    friend class LocalDatabase; // TODO remove this

    // Returns the LocalEnvironment instance
    LocalEnvironment *lenv();

    // Returns true if a key was erased in a Transaction (or DeltaUpdate)
    bool is_key_erased(Context *context, ham_key_t *key);

    // Overloaded version; checks whether a DeltaUpdate key is erased
    bool is_key_erased(Context *context, DeltaUpdate *du);

    // Implementation of overwrite()
    virtual ham_status_t do_overwrite(ham_record_t *record, uint32_t flags);

    // Returns number of duplicates (ham_cursor_get_duplicate_count)
    virtual ham_status_t do_get_duplicate_count(uint32_t flags,
                                uint32_t *pcount);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t do_get_record_size(uint64_t *psize);

    // Implementation of get_duplicate_position()
    virtual ham_status_t do_get_duplicate_position(uint32_t *pposition);

    // Clears the DuplicateCache and disconnect the Cursor from any
    // duplicate key
    void clear_duplicate_cache() {
      m_duplicate_cache.clear();
      m_dupecache_index = -1;
    }

    // Compares btree and txn-cursor; stores result in lastcmp
    int compare(Context *context);

    // Returns true if this key has duplicates
    bool has_duplicates() const {
      return (m_duplicate_cache.size() > 0);
    }

    // Moves cursor to the first key
    ham_status_t move_first_key(Context *context, uint32_t flags);

    // Moves cursor to the last key
    ham_status_t move_last_key(Context *context, uint32_t flags);

    // Moves cursor to the next key
    ham_status_t move_next_key(Context *context, uint32_t flags);

    // Moves cursor to the previous key
    ham_status_t move_previous_key(Context *context, uint32_t flags);

    // Returns the last valid DeltaAction from a DeltaUpdate
    DeltaAction *locate_valid_action(DeltaUpdate *du);

    // A Cursor which can walk over Transaction trees
    TransactionCursor m_txn_cursor;

    // A Cursor which can walk over B+trees
    BtreeCursor m_btree_cursor;

    // A cache for all duplicates of the current key. needed for
    // ham_cursor_move, ham_find and other functions. The cache is
    // used to consolidate all duplicates of btree and txn.
    DuplicateCache m_duplicate_cache;

    /** The current position of the cursor in the cache. This is a
     * 0-based index. -1 means that the index is not used */
    int m_dupecache_index;

    // The last operation (insert/find or move); needed for
    // ham_cursor_move. Values can be HAM_CURSOR_NEXT,
    // HAM_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT
    uint32_t m_last_operation;

    // flags & state of the cursor
    uint32_t m_flags;

    // The result of the last compare operation
    int m_last_cmp;

    // true if this cursor was never used
    bool m_is_first_use;

    // kBtree: currently using the BtreeCursor; kDeltaUpdate: currently
    // using the DeltaUpdate
    int m_currently_using;

    // Helper flag to remember whether the B-tree cursor reached the end
    // of the Database
    bool m_btree_eof;
};

} // namespace hamsterdb

#endif /* HAM_CURSOR_LOCAL_H */
