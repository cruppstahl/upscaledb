/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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
 * Transaction operation (txn_op_t) or it is unused.
 */

#ifndef HAM_TXN_CURSOR_H__
#define HAM_TXN_CURSOR_H__

#include "internal_fwd_decl.h"
#include "txn.h"

namespace hamsterdb {

/*
 * An cursor which can iterate over Transaction nodes
 */
typedef struct txn_cursor_t
{
    txn_cursor_t() {
        _parent=0;
        _coupled._op=0;
        _coupled._next=0;
        _coupled._previous=0;
    }

    /** the parent cursor */
    Cursor *_parent;

    /**
     * a Cursor can either be coupled or nil ("not in list"). If it's
     * coupled, it directly points to a txn_op_t structure. If it's nil then it
     * basically is uninitialized.
     */
    struct txn_cursor_coupled_t {
        /* the txn operation to which we're pointing */
        txn_op_t *_op;

        /** a double linked list with other cursors that are coupled
         * to the same txn_op */
        struct txn_cursor_t *_next;

        /** a double linked list with other cursors that are coupled
         * to the same txn_op */
        struct txn_cursor_t *_previous;

    } _coupled;

} txn_cursor_t;

/** get the database pointer */
#define txn_cursor_get_db(c)                        (c)->_parent->get_db()

/** get the parent cursor */
#define txn_cursor_get_parent(c)                    (c)->_parent

/** set the parent cursor */
#define txn_cursor_set_parent(c, p)                 (c)->_parent=p

/** get the pointer to the coupled txn_op */
#define txn_cursor_get_coupled_op(c)                (c)->_coupled._op

/** set the pointer to the coupled txn_op */
#define txn_cursor_set_coupled_op(c, op)            (c)->_coupled._op=op

/** get the pointer to the next cursor in the linked list of coupled
 * cursors */
#define txn_cursor_get_coupled_next(c)              (c)->_coupled._next

/** set the pointer to the next cursor in the linked list of coupled
 * cursors */
#define txn_cursor_set_coupled_next(c, n)           (c)->_coupled._next=n

/** get the pointer to the previous cursor in the linked list of coupled
 * cursors */
#define txn_cursor_get_coupled_previous(c)          (c)->_coupled._previous

/** set the pointer to the previous cursor in the linked list of coupled
 * cursors */
#define txn_cursor_set_coupled_previous(c, p)       (c)->_coupled._previous=p

/**
 * Create a new txn cursor
 */
extern ham_status_t
txn_cursor_create(Database *db, Transaction *txn, ham_u32_t flags,
                txn_cursor_t *cursor, Cursor *parent);

/**
 * Returns true if the cursor is nil (does not point to any item)
 */
#define txn_cursor_is_nil(c)                            ((c)->_coupled._op==0)

/**
 * Sets a cursor to nil
 */
extern void
txn_cursor_set_to_nil(txn_cursor_t *cursor);

/**
 * Couples a txn cursor to an txn_op_t structure
 */
extern void
txn_cursor_couple(txn_cursor_t *cursor, txn_op_t *op);

/**
 * clones a cursor
 */
extern void
txn_cursor_clone(const txn_cursor_t *src, txn_cursor_t *dest,
                Cursor *parent);

/**
 * Closes a cursor
 */
extern void
txn_cursor_close(txn_cursor_t *cursor);

/**
 * Overwrites the record of a cursor
 */
extern ham_status_t
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record);

/**
 * Moves the cursor to first, last, previous or next
 */
extern ham_status_t
txn_cursor_move(txn_cursor_t *cursor, ham_u32_t flags);

/**
 * Returns true if the cursor points to a key that is erased
 */
extern ham_bool_t
txn_cursor_is_erased(txn_cursor_t *cursor);

/**
 * Returns true if the cursor points to a duplicate key that is erased
 */
extern ham_bool_t
txn_cursor_is_erased_duplicate(txn_cursor_t *cursor);

/**
 * Looks up an item, places the cursor
 */
extern ham_status_t
txn_cursor_find(txn_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * Inserts an item, places the cursor on the new item
 *
 * This function is only used in the unittests
 */
extern ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags);

/**
 * Retrieves the key from the current item
 *
 * If the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this means
 * that the item was already flushed to the btree, and the caller has to
 * use the btree lookup function to retrieve the key.
 */
extern ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key);

/**
 * Retrieves the record from the current item
 *
 * If the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this means
 * that the item was already flushed to the btree, and the caller has to
 * use the btree lookup function to retrieve the record.
 */
extern ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record);

/**
 * Retrieves the record size of the current item
 */
extern ham_status_t
txn_cursor_get_record_size(txn_cursor_t *cursor, ham_u64_t *psize);

/**
 * Erases the current item, then 'nil's the cursor
 */
extern ham_status_t
txn_cursor_erase(txn_cursor_t *cursor);

} // namespace hamsterdb

#endif /* HAM_TXN_CURSOR_H__ */
