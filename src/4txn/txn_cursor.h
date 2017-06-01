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
 * A cursor which can iterate over transaction nodes and operations
 *
 * A Txn Cursor can walk over Txn trees (TxnIndex).
 *
 * Txn Cursors are only used as part of the Cursor structure as defined
 * in cursor.h. Like all Txn operations it is in-memory only,
 * traversing the red-black tree that is implemented in txn.h, and
 * consolidating multiple operations in a node (i.e. if a Txn first
 * overwrites a record, and another transaction then erases the key).
 *
 * The Txn Cursor has two states: either it is coupled to a
 * Txn operation (TxnOperation) or it is unused.
 */

#ifndef UPS_TXN_CURSOR_H
#define UPS_TXN_CURSOR_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4txn/txn_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct LocalCursor;
struct Context;

struct TxnCursorState {
  // The parent cursor
  LocalCursor *parent;

  // A Cursor can either be coupled or nil ("not in list"). If it's
  // coupled, it directly points to a TxnOperation structure.
  // If it's nil then |m_coupled_op| is null.
  //
  // the txn operation to which we're pointing
  TxnOperation *coupled_op;

  // a double linked list with other cursors that are coupled
  // to the same Operation
  TxnCursor *coupled_next, *coupled_previous;
};

//
// An cursor which can iterate over Txn nodes
//
struct TxnCursor {
  // Constructor
  TxnCursor(LocalCursor *parent) {
    state_.parent = parent;
    state_.coupled_op = 0;
    state_.coupled_next = 0;
    state_.coupled_previous = 0;
  }

  // Destructor; sets the cursor to nil
  ~TxnCursor() {
    close();
  }

  // Clones another TxnCursor
  void clone(const TxnCursor *other);

  // Returns true if the cursor is nil (does not point to any item)
  bool is_nil() const {
    return state_.coupled_op == 0;
  }

  // Sets the cursor to nil
  void set_to_nil();

  // Couples this cursor to a TxnOperation structure
  void couple_to(TxnOperation *op);

  // Closes the cursor
  void close() {
    set_to_nil();
  }

  // Returns the parent cursor
  LocalCursor *parent() {
    return state_.parent;
  }

  // Returns the pointer to the coupled TxnOperation
  TxnOperation *get_coupled_op() const {
    return state_.coupled_op;
  }

  // Retrieves the key from the current item; creates a shallow copy.
  ups_key_t *coupled_key() {
    TxnNode *node = state_.coupled_op->node;
    return node->key();
  }

  // Retrieves the key from the current item; creates a deep copy.
  //
  // If the cursor is uncoupled, UPS_CURSOR_IS_NIL is returned. this
  // means that the item was already flushed to the btree, and the caller has
  // to use the btree lookup function to retrieve the key.
  void copy_coupled_key(ups_key_t *key);

  // Retrieves the record from the current item; creates a deep copy.
  //
  // If the cursor is uncoupled, UPS_CURSOR_IS_NIL will be returned. this
  // means that the item was already flushed to the btree, and the caller has
  // to use the btree lookup function to retrieve the record.
  void copy_coupled_record(ups_record_t *record);

  // Moves the cursor to first, last, previous or next
  ups_status_t move(uint32_t flags);

  // Looks up an item, places the cursor
  ups_status_t find(ups_key_t *key, uint32_t flags);

  // Retrieves the record size of the current item
  uint32_t record_size();

  // Returns the pointer to the next cursor in the linked list of coupled
  // cursors
  TxnCursor *next() {
    return state_.coupled_next;
  }

  TxnCursorState state_;
};

} // namespace upscaledb

#endif /* UPS_TXN_CURSOR_H */
