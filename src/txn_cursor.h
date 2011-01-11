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
 */

#ifndef HAM_TXN_CURSOR_H__
#define HAM_TXN_CURSOR_H__

#include "internal_fwd_decl.h"
#include "txn.h"



#ifdef __cplusplus
extern "C" {
#endif 


/*
 * An cursor which can walk over Transaction nodes
 */
typedef struct txn_cursor_t
{
    /** the database on which this cursor operates */
    /* TODO - not required; we can access the db through the _parent pointer */
    ham_db_t *_db;

    /** the parent cursor */
    ham_cursor_t *_parent;

    /** 
     * a Cursor can either be coupled or uncoupled. If it's coupled, it 
     * directly points to a txn_op_t structure. If it's uncoupled, it
     * stores a copy of the key to which it points 
     */
    union txn_cursor_union_t {
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

        union txn_cursor_uncoupled_t {
            /* a copy of the key to which we're pointing */
            ham_key_t *_key;

        } _uncoupled;

    } _u;
    
    /* the flags store the state of the cursor - see below */
    ham_u32_t _flags;

} txn_cursor_t;

/** cursor flag - cursor is coupled */
#define TXN_CURSOR_FLAG_COUPLED                     1

/** cursor flag - cursor is uncoupled */
#define TXN_CURSOR_FLAG_UNCOUPLED                   2

/** get the database pointer */
#define txn_cursor_get_db(c)                        (c)->_db

/** set the database pointer */
#define txn_cursor_set_db(c, db)                    (c)->_db=db

/** get the parent cursor */
#define txn_cursor_get_parent(c)                    (c)->_parent

/** set the parent cursor */
#define txn_cursor_set_parent(c, p)                 (c)->_parent=p

/** get the cursor flags */
#define txn_cursor_get_flags(c)                     (c)->_flags

/** set the cursor flags */
#define txn_cursor_set_flags(c, f)                  (c)->_flags=f

/** get the pointer to the coupled txn_op */
#define txn_cursor_get_coupled_op(c)                (c)->_u._coupled._op

/** set the pointer to the coupled txn_op */
#define txn_cursor_set_coupled_op(c, op)            (c)->_u._coupled._op=op

/** get the pointer to the next cursor in the linked list of coupled
 * cursors */
#define txn_cursor_get_coupled_next(c)              (c)->_u._coupled._next

/** set the pointer to the next cursor in the linked list of coupled
 * cursors */
#define txn_cursor_set_coupled_next(c, n)           (c)->_u._coupled._next=n

/** get the pointer to the previous cursor in the linked list of coupled
 * cursors */
#define txn_cursor_get_coupled_previous(c)          (c)->_u._coupled._previous

/** set the pointer to the previous cursor in the linked list of coupled
 * cursors */
#define txn_cursor_set_coupled_previous(c, p)       (c)->_u._coupled._previous=p

/** get the pointer to the uncoupled key */
#define txn_cursor_get_uncoupled_key(c)             (c)->_u._uncoupled._key

/** set the pointer to the uncoupled key */
#define txn_cursor_set_uncoupled_key(c, k)          (c)->_u._uncoupled._key=k

/** 
 * returns true if the cursor is nil (does not point to any item) 
 */
extern ham_bool_t
txn_cursor_is_nil(txn_cursor_t *cursor);

/** 
 * sets a cursor to nil
 */
extern void
txn_cursor_set_to_nil(txn_cursor_t *cursor);

/**
 * clones a cursor
 *
 * returns NULL if out of memory
 */
extern txn_cursor_t *
txn_cursor_clone(txn_cursor_t *cursor);

/**
 * closes a cursor
 */
extern void
txn_cursor_close(txn_cursor_t *cursor);

/**
 * overwrites the record of a cursor
 */
extern ham_status_t
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record);

/**
 * moves the cursor to first, last, previous or next
 */
extern ham_status_t
txn_cursor_move(txn_cursor_t *cursor, ham_u32_t flags);

/**
 * looks up an item, places the cursor
 */
extern ham_status_t
txn_cursor_find(txn_cursor_t *cursor, ham_key_t *key);

/**
 * inserts an item, places the cursor on the new item
 */
extern ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags);

/**
 * retrieves the key from the current item
 *
 * if the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this means
 * that the item was already flushed to the btree, and the caller has to 
 * use the btree lookup function to retrieve the key.
 */
extern ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key);

/**
 * retrieves the record from the current item
 *
 * if the cursor is uncoupled, HAM_INTERNAL_ERROR will be returned. this means
 * that the item was already flushed to the btree, and the caller has to 
 * use the btree lookup function to retrieve the record.
 */
extern ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record);

/**
 * erases the current item, then 'nil's the cursor
 */
extern ham_status_t
txn_cursor_erase(txn_cursor_t *cursor);

/**
 * returns duplicate of the current item
 */
extern ham_status_t
txn_cursor_get_duplicate_count(txn_cursor_t *cursor, ham_u32_t *count);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TXN_CURSOR_H__ */
