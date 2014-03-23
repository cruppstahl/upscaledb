/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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
 */

#ifndef HAM_TXN_CURSOR_H__
#define HAM_TXN_CURSOR_H__

#include "txn_local.h"

namespace hamsterdb {

class Cursor;

//
// An cursor which can iterate over Transaction nodes
//
class TransactionCursor
{
  public:
    // Constructor
    TransactionCursor(Cursor *parent)
      : m_parent(parent) {
      m_coupled_op = 0;
      m_coupled_next = 0;
      m_coupled_previous = 0;
    }

    // Destructor; asserts that the cursor is nil
    ~TransactionCursor() {
      ham_assert(is_nil());
    }

    // Clones another TransactionCursor
    void clone(const TransactionCursor *other);

    // Returns the parent cursor
    // TODO this should be private
    Cursor *get_parent() {
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
    // If the cursor is uncoupled, HAM_CURSOR_IS_NIL is returned. this
    // means that the item was already flushed to the btree, and the caller has
    // to use the btree lookup function to retrieve the key.
    void copy_coupled_key(ham_key_t *key);

    // Retrieves the record from the current item; creates a deep copy.
    //
    // If the cursor is uncoupled, HAM_CURSOR_IS_NIL will be returned. this
    // means that the item was already flushed to the btree, and the caller has
    // to use the btree lookup function to retrieve the record.
    void copy_coupled_record(ham_record_t *record);

    // Moves the cursor to first, last, previous or next
    ham_status_t move(ham_u32_t flags);

    // Overwrites the record of a cursor
    ham_status_t overwrite(ham_record_t *record);

    // Looks up an item, places the cursor
    ham_status_t find(ham_key_t *key, ham_u32_t flags);

    // Retrieves the record size of the current item
    ham_u64_t get_record_size();

    // Erases the current item, then 'nil's the cursor
    ham_status_t erase();

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
    ham_status_t test_insert(ham_key_t *key, ham_record_t *record,
                    ham_u32_t flags);

    // Returns the database pointer
    LocalDatabase *get_db();

    // Checks if this cursor conflicts with another transaction
    bool has_conflict() const;

    // Moves the cursor to the first valid Operation in a Node
    ham_status_t move_top_in_node(TransactionNode *node,
                    TransactionOperation *op, bool ignore_conflicts,
                    ham_u32_t flags);

    // The parent cursor
    Cursor *m_parent;

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

#endif /* HAM_TXN_CURSOR_H__ */
