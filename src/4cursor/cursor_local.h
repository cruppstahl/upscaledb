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

/*
 * Cursor implementation for local databases
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
struct DuplicateCacheLine {
  DuplicateCacheLine(bool use_btree = true, uint32_t btree_dupeidx = 0)
    : _btree_duplicate_index(btree_dupeidx), _op(0), _use_btree(use_btree) {
    assert(use_btree == true);
  }

  DuplicateCacheLine(bool use_btree, TxnOperation *op)
    : _btree_duplicate_index(0), _op(op), _use_btree(use_btree) {
    assert(use_btree == false);
  }

  // Returns true if this cache entry is a duplicate in the btree index
  // (otherwise it's a duplicate in the transaction index)
  bool use_btree() const {
    return _use_btree;
  }

  // Returns the btree duplicate index
  uint32_t btree_duplicate_index() {
    assert(_use_btree == true);
    return _btree_duplicate_index;
  }

  // Sets the btree duplicate index
  void set_btree_duplicate_index(uint32_t idx) {
    _use_btree = true;
    _btree_duplicate_index = idx;
    _op = 0;
  }

  // Returns the txn-op duplicate
  TxnOperation *txn_op() {
    assert(_use_btree == false);
    return _op;
  }

  // Sets the txn-op duplicate
  void set_txn_op(TxnOperation *op) {
    _use_btree = false;
    _op = op;
    _btree_duplicate_index = 0;
  }

  // The btree duplicate index (of the original btree dupe table)
  uint32_t _btree_duplicate_index;

  // The txn op structure that we refer to
  TxnOperation *_op;

  // using btree or txn duplicates?
  bool _use_btree;
};

//
// The DuplicateCache is a cache for duplicate keys
//
typedef std::vector<DuplicateCacheLine> DuplicateCache;

//
// the Database Cursor
//
struct LocalCursor : Cursor {
  // The flags have ranges:
  //  0 - 0x1000000-1:      btree_cursor
  //    > 0x1000000:        cursor
  enum {
    // Flags for set_to_nil, is_nil
    kBoth  = 0,
    kBtree = 1,
    kTxn   = 2,

    // Flag for synchronize(): do not use approx matching if the key
    // is not available
    kSyncOnlyEqualKeys = 0x200000,

    // Flag for synchronize(): do not load the key if there's an approx.
    // match. Only positions the cursor.
    kSyncDontLoadKey   = 0x100000,

    // Cursor flag: cursor is coupled to the txn-cursor
    kCoupledToTxn      = 0x1000000,

    // Flag for set_last_operation()
    kLookupOrInsert    = 0x10000
  };

  // Constructor; retrieves pointer to db and txn, initializes all members
  LocalCursor(LocalDb *db, Txn *txn = 0);

  // Copy constructor; used for cloning a Cursor
  LocalCursor(LocalCursor &other);

  // Destructor; sets cursor to nil
  ~LocalCursor() {
    close();
  }

  // Sets the cursor to nil
  void set_to_nil(int what = kBoth);

  // Returns true if a cursor is nil (Not In List - does not point to any
  // key)
  // |what| is one of the flags kBoth, kTxn, kBtree
  bool is_nil(int what = kBoth);

  // Couples the cursor to the btree key
  void activate_btree(bool exclusive = false) {
    state = kBtree;
    if (exclusive)
      set_to_nil(kTxn);
  }

  // Returns true if a cursor is coupled to the btree
  bool is_btree_active() const {
    return state == kBtree;
  }

  // Couples the cursor to the txn-op
  void activate_txn(TxnOperation *op = 0, bool exclusive = false) {
    state = kTxn;
    if (op)
      txn_cursor.couple_to(op);
    if (exclusive)
      set_to_nil(kBtree);
  }

  // Returns true if a cursor is coupled to a txn-op
  bool is_txn_active() const {
    return state == kTxn;
  }

  // Moves a Cursor (ups_cursor_move)
  ups_status_t move(Context *context, ups_key_t *key, ups_record_t *record,
                  uint32_t flags);

  // Implementation of overwrite()
  virtual ups_status_t overwrite(ups_record_t *record, uint32_t flags);

  // Returns number of duplicates (ups_cursor_get_duplicate_count)
  virtual uint32_t get_duplicate_count(uint32_t flags);

  // Get current record size (ups_cursor_get_record_size)
  virtual uint32_t get_record_size();

  // Implementation of get_duplicate_position()
  virtual uint32_t get_duplicate_position();

  // Closes the cursor (ups_cursor_close)
  virtual void close();

  // Couples the cursor to a duplicate in the dupe table
  // |duplicate_index| is a 1 based index!!
  void couple_to_duplicate(uint32_t duplicate_index);

  // Synchronizes txn- and btree-cursor
  //
  // If txn-cursor is nil then try to move the txn-cursor to the same key
  // as the btree cursor.
  // If btree-cursor is nil then try to move the btree-cursor to the same key
  // as the txn cursor.
  // If both are nil, or both are valid, then nothing happens
  //
  // |equal_key| is set to true if the keys in both cursors are equal.
  void synchronize(Context *context, uint32_t flags, bool *equal_keys = 0);

  // Returns the number of duplicates in the duplicate cache
  // The duplicate cache is updated if necessary
  uint32_t duplicate_cache_count(Context *context, bool clear_cache = false);


  // Moves cursor to the first duplicate
  ups_status_t move_first_duplicate(Context *context);

  // Moves cursor to the last duplicate
  ups_status_t move_last_duplicate(Context *context);

  // Moves cursor to the next duplicate
  ups_status_t move_next_duplicate(Context *context);

  // Moves cursor to the previous duplicate
  ups_status_t move_previous_duplicate(Context *context);

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

  // A Cursor which can walk over Txn trees
  TxnCursor txn_cursor;

  // A Cursor which can walk over B+trees
  BtreeCursor btree_cursor;

  // A cache for all duplicates of the current key. needed for
  // ups_cursor_move, ups_find and other functions. The cache is
  // used to consolidate all duplicates of btree and txn.
  DuplicateCache duplicate_cache;

  // Temporary copy of the duplicate_cache
  DuplicateCache old_duplicate_cache;

  // The current position of the cursor in the cache. This is a
  // 1-based index. 0 means that the cache is not in use.
  uint32_t duplicate_cache_index;

  // Temporary copy of the duplicate cache index
  uint32_t old_duplicate_cache_index;

  // The last operation (insert/find or move); needed for
  // ups_cursor_move. Values can be UPS_CURSOR_NEXT,
  // UPS_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT
  uint32_t last_operation;

  // The state of the cursor
  uint32_t state;

  // The result of the last compare operation
  int last_cmp;
};

} // namespace upscaledb

#endif /* UPS_CURSOR_LOCAL_H */
