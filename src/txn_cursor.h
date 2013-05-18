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

/**
 * @brief A cursor which can iterate over transaction nodes and operations
 *
 * A Transaction Cursor can walk over Transaction trees (txn_tree_t).
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

#include "internal_fwd_decl.h"
#include "txn.h"

namespace hamsterdb {

/*
 * An cursor which can iterate over Transaction nodes
 */
class TransactionCursor
{
  public:
    /** Constructor; initializes the object */
    TransactionCursor(Cursor *parent);

    /** Copy constructor: for cloning a cursor */
    TransactionCursor(Cursor *parent, const TransactionCursor *other);

    /** Destructor; sets cursor to nil */
    ~TransactionCursor() {
      set_to_nil();
    }

    /** Sets a cursor to nil */
    void set_to_nil();

    /** Couples a txn cursor to a TransactionOperation structure */
    void couple(TransactionOperation *op);

    /** Moves the cursor to first, last, previous or next */
    ham_status_t move(ham_u32_t flags);

    /** Overwrites the record of a cursor */
    ham_status_t overwrite(ham_record_t *record);

    /** Returns true if the cursor points to a key that is erased */
    bool is_erased();

    /** Returns true if the cursor points to a duplicate key that is erased */
    bool is_erased_duplicate();

    /** Looks up an item, places the cursor */
    ham_status_t find(ham_key_t *key, ham_u32_t flags);

    /** Inserts an item, places the cursor on the new item
     * This function is only used in the unittests */
    ham_status_t insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * Retrieves the key from the current item
     *
     * If the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this
     * means that the item was already flushed to the btree, and the caller has
     * to use the btree lookup function to retrieve the key.
     */
    ham_status_t get_key(ham_key_t *key);

    /**
     * Retrieves the record from the current item
     *
     * If the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this
     * means that the item was already flushed to the btree, and the caller has
     * to use the btree lookup function to retrieve the record.
     */
    ham_status_t get_record(ham_record_t *record);

    /** Retrieves the record size of the current item */
    ham_status_t get_record_size(ham_u64_t *psize);

    /** Erases the current item, then 'nil's the cursor */
    ham_status_t erase();

    /** get the database pointer */
    Database *get_db();

    /** get the parent cursor */
    Cursor *get_parent() {
      return (m_parent);
    }

    /** get the pointer to the coupled txn_op */
    TransactionOperation *get_coupled_op() const {
      return (_coupled._op);
    }

    /** set the pointer to the coupled txn_op */
    void set_coupled_op(TransactionOperation *op) {
      _coupled._op = op;
    }

    /** get the pointer to the next cursor in the linked list of coupled
     * cursors */
    TransactionCursor *get_coupled_next() {
      return (_coupled._next);
    }

    /** set the pointer to the next cursor in the linked list of coupled
     * cursors */
    void set_coupled_next(TransactionCursor *next) {
      _coupled._next = next;
    }

    /** get the pointer to the previous cursor in the linked list of coupled
     * cursors */
    TransactionCursor *get_coupled_previous() {
      return (_coupled._previous);
    }

    /** set the pointer to the previous cursor in the linked list of coupled
     * cursors */
    void set_coupled_previous(TransactionCursor *prev) {
      _coupled._previous = prev;
    }

    /** returns true if the cursor is nil (does not point to any item) */
    bool is_nil() const {
      return (_coupled._op == 0);
    }

  private:
    /** checks if this cursor conflicts with another transaction */
    bool conflicts();

    /** moves the cursor to the first valid Operation in a Node */
    ham_status_t move_top_in_node(TransactionNode *node,
            TransactionOperation *op, bool ignore_conflicts,
            ham_u32_t flags);

    /** the parent cursor */
    Cursor *m_parent;

    /**
     * a Cursor can either be coupled or nil ("not in list"). If it's
     * coupled, it directly points to a TransactionOperation structure.
     * If it's nil then it basically is uninitialized.
     */
    struct txn_cursor_coupled_t {
      /* the txn operation to which we're pointing */
      TransactionOperation *_op;

      /** a double linked list with other cursors that are coupled
       * to the same txn_op */
      TransactionCursor *_next;

      /** a double linked list with other cursors that are coupled
       * to the same txn_op */
      TransactionCursor *_previous;
    } _coupled;
};

} // namespace hamsterdb

#endif /* HAM_TXN_CURSOR_H__ */
