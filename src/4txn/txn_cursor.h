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
 * A cursor which can iterate over transaction nodes and operations
 *
 * A Transaction Cursor can walk over Transaction trees (TransactionIndex).
 *
 * Transaction Cursors are only used as part of the Cursor structure as defined
 * in cursor.h. Like all Transaction operations it is in-memory only,
 * traversing the red-black tree that is implemented in txn.h, and
 * consolidating multiple operations in a node (i.e. if a Transaction first
 * overwrites a record, and another transaction then erases the key).
 *
 * The Transaction Cursor has two states: either it is coupled to a
 * Transaction operation (TransactionOperation) or it is unused.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_TXN_CURSOR_H
#define UPS_TXN_CURSOR_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4txn/txn_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LocalCursor;
struct Context;

//
// An cursor which can iterate over Transaction nodes
//
class TransactionCursor
{
  public:
    // Constructor
    TransactionCursor(LocalCursor *parent)
      : m_parent(parent) {
      m_coupled_op = 0;
      m_coupled_next = 0;
      m_coupled_previous = 0;
    }

    // Destructor; asserts that the cursor is nil
    ~TransactionCursor() {
      ups_assert(is_nil());
    }

    // Clones another TransactionCursor
    void clone(const TransactionCursor *other);

    // Returns the parent cursor
    // TODO this should be private
    LocalCursor *get_parent() {
      return (m_parent);
    }

    // Couples this cursor to a TransactionOperation structure
    void couple_to_op(TransactionOperation *op);

    // Returns the pointer to the coupled TransactionOperation
    TransactionOperation *get_coupled_op() const {
      return (m_coupled_op);
    }

    // Sets the cursor to nil
    void set_to_nil();

    // Returns true if the cursor is nil (does not point to any item)
    bool is_nil() const {
      return (m_coupled_op == 0);
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

    // Overwrites the record of a cursor
    ups_status_t overwrite(Context *context, LocalTransaction *txn,
                    ups_record_t *record);

    // Looks up an item, places the cursor
    ups_status_t find(ups_key_t *key, uint32_t flags);

    // Retrieves the record size of the current item
    uint64_t get_record_size();

    // Returns the pointer to the next cursor in the linked list of coupled
    // cursors
    TransactionCursor *get_coupled_next() {
      return (m_coupled_next);
    }

    // Closes the cursor
    void close() {
      set_to_nil();
    }

  private:
    friend struct TxnCursorFixture;

    // Removes this cursor from this TransactionOperation
    void remove_cursor_from_op(TransactionOperation *op);

    // Inserts an item, places the cursor on the new item.
    // This function is only used in the unittests.
    ups_status_t test_insert(ups_key_t *key, ups_record_t *record,
                    uint32_t flags);

    // Returns the database pointer
    LocalDatabase *get_db();

    // Moves the cursor to the first valid Operation in a Node
    ups_status_t move_top_in_node(TransactionNode *node,
                    TransactionOperation *op, bool ignore_conflicts,
                    uint32_t flags);

    // The parent cursor
    LocalCursor *m_parent;

    // A Cursor can either be coupled or nil ("not in list"). If it's
    // coupled, it directly points to a TransactionOperation structure.
    // If it's nil then |m_coupled_op| is null.
    //
    // the txn operation to which we're pointing
    TransactionOperation *m_coupled_op;

    // a double linked list with other cursors that are coupled
    // to the same Operation
    TransactionCursor *m_coupled_next, *m_coupled_previous;
};

} // namespace hamsterdb

#endif /* UPS_TXN_CURSOR_H */
