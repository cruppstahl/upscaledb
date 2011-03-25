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

#include "internal_fwd_decl.h"
#include "txn_cursor.h"
#include "txn.h"
#include "db.h"
#include "env.h"
#include "mem.h"
#include "cursor.h"
#include "btree_cursor.h"

ham_bool_t
txn_cursor_is_nil(txn_cursor_t *cursor)
{
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED)
        return (HAM_FALSE);
    return (HAM_TRUE);
}

void
txn_cursor_set_to_nil(txn_cursor_t *cursor)
{
    /* uncoupled cursor? remove from the txn_op structure */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (op)
            txn_op_remove_cursor(op, cursor);
        txn_cursor_set_flags(cursor, 
                txn_cursor_get_flags(cursor)&(~TXN_CURSOR_FLAG_COUPLED));
        txn_cursor_set_coupled_op(cursor, 0);
    }

    /* otherwise cursor is already nil */
}

void
txn_cursor_couple(txn_cursor_t *cursor, txn_op_t *op)
{
    txn_cursor_set_to_nil(cursor);
    txn_cursor_set_coupled_op(cursor, op);
    txn_cursor_set_flags(cursor, 
                    txn_cursor_get_flags(cursor)|TXN_CURSOR_FLAG_COUPLED);

    txn_op_add_cursor(op, cursor);
}

void
txn_cursor_clone(const txn_cursor_t *src, txn_cursor_t *dest)
{
    txn_cursor_set_flags(dest, txn_cursor_get_flags(src));

    if (txn_cursor_get_flags(dest)&TXN_CURSOR_FLAG_COUPLED)
        txn_cursor_couple(dest, txn_cursor_get_coupled_op(src));
}

void
txn_cursor_close(txn_cursor_t *cursor)
{
    txn_cursor_set_to_nil(cursor);
}

ham_status_t
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_txn_t *txn=cursor_get_txn(txn_cursor_get_parent(cursor));
    txn_op_t *op;
    txn_opnode_t *node;

    /* an overwrite is actually an insert w/ HAM_OVERWRITE of the
     * current key */

    if (txn_cursor_is_nil(cursor))
        return (HAM_CURSOR_IS_NIL);

    op=txn_cursor_get_coupled_op(cursor);
    node=txn_op_get_node(op);

    return (db_insert_txn(db, txn, txn_opnode_get_key(node), 
                record, HAM_OVERWRITE, cursor));
}

static ham_status_t
__move_top_in_node(txn_cursor_t *cursor, txn_opnode_t *node, txn_op_t *op,
                ham_bool_t ignore_conflicts, ham_u32_t flags)
{
    txn_op_t *lastdup=0;
    ham_cursor_t *pc=txn_cursor_get_parent(cursor);

    if (!op)
        op=txn_opnode_get_newest_op(node);
    else
        goto next;

    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        /* only look at ops from the current transaction and from 
         * committed transactions */
        if ((optxn==cursor_get_txn(txn_cursor_get_parent(cursor)))
                || (txn_get_flags(optxn)&TXN_STATE_COMMITTED)) {
            /* a normal (overwriting) insert will return this key */
            if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)) {
                txn_cursor_couple(cursor, op);
                return (0);
            }
            /* retrieve a duplicate key */
            if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP) {
                /* the duplicates are handled by the caller. here we only
                 * couple to the first op */
                txn_cursor_couple(cursor, op);
                return (0);
            }
            /* a normal erase will return an error (but we still couple the
             * cursor because the caller might need to know WHICH key was
             * deleted!) */
            if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                txn_cursor_couple(cursor, op);
                return (HAM_KEY_ERASED_IN_TXN);
            }
            /* everything else is a bug! */
            ham_assert(txn_op_get_flags(op)==TXN_OP_NOP, (""));
        }
        else if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if (!ignore_conflicts) {
            return (HAM_TXN_CONFLICT);
        }

next:
        cursor_set_dupecache_index(pc, 0);
        op=txn_op_get_next_in_node(op);
    }

    /* did we find a duplicate key? then return it */
    if (lastdup) {
        txn_cursor_couple(cursor, op);
        return (0);
    }
 
    return (HAM_KEY_NOT_FOUND);
}

ham_status_t
txn_cursor_move(txn_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=txn_cursor_get_db(cursor);
    txn_opnode_t *node;

    if (flags&HAM_CURSOR_FIRST) {
        /* first set cursor to nil */
        txn_cursor_set_to_nil(cursor);

        node=txn_tree_get_first(db_get_optree(db));
        if (!node)
            return (HAM_KEY_NOT_FOUND);
        return (__move_top_in_node(cursor, node, 0, HAM_TRUE, flags));
    }
    else if (flags&HAM_CURSOR_LAST) {
        /* first set cursor to nil */
        txn_cursor_set_to_nil(cursor);

        node=txn_tree_get_last(db_get_optree(db));
        if (!node)
            return (HAM_KEY_NOT_FOUND);
        return (__move_top_in_node(cursor, node, 0, HAM_TRUE, flags));
    }
    else if (flags&HAM_CURSOR_NEXT) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (txn_cursor_is_nil(cursor))
            return (HAM_CURSOR_IS_NIL);

        node=txn_op_get_node(op);
        op=0;

        ham_assert(txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED, (""));

        /* first move to the next key in the current node; if we fail, 
         * then move to the next node. repeat till we've found a key or 
         * till we've reached the end of the tree */
        while (1) {
            node=txn_tree_get_next_node(txn_opnode_get_tree(node), node);
            if (!node)
                return (HAM_KEY_NOT_FOUND);
            st=__move_top_in_node(cursor, node, op, HAM_TRUE, flags); 
            if ((st==HAM_KEY_NOT_FOUND) || (st==HAM_KEY_ERASED_IN_TXN))
                continue;
            return (st);
        }
    }
    else if (flags&HAM_CURSOR_PREVIOUS) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (txn_cursor_is_nil(cursor))
            return (HAM_CURSOR_IS_NIL);

        node=txn_op_get_node(op);
        op=0;

        ham_assert(txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED, (""));

        /* first move to the previous key in the current node; if we fail, 
         * then move to the previous node. repeat till we've found a key or 
         * till we've reached the end of the tree */
        while (1) {
            node=txn_tree_get_previous_node(txn_opnode_get_tree(node), node);
            if (!node)
                return (HAM_KEY_NOT_FOUND);
            st=__move_top_in_node(cursor, node, op, HAM_TRUE, flags); 
            if ((st==HAM_KEY_NOT_FOUND) || (st==HAM_KEY_ERASED_IN_TXN))
                continue;
            return (st);
        }
    }
    else {
        ham_assert(!"this flag is not yet implemented", (""));
    }

    return (0);
}

ham_bool_t
txn_cursor_is_erased(txn_cursor_t *cursor)
{
    ham_status_t st;
    txn_op_t *op=txn_cursor_get_coupled_op(cursor);
    txn_opnode_t *node=txn_op_get_node(op);

    ham_assert(txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED, (""));

    /* move to the newest insert*-op and check if it erased the key */
    st=__move_top_in_node(cursor, node, 0, HAM_FALSE, 0);
    return (st==HAM_KEY_ERASED_IN_TXN);
}

ham_status_t
txn_cursor_find(txn_cursor_t *cursor, ham_key_t *key, ham_u32_t flags)
{
    txn_opnode_t *node;

    /* first set cursor to nil */
    txn_cursor_set_to_nil(cursor);

    /* then lookup the node */
    node=txn_opnode_get(txn_cursor_get_db(cursor), key, flags);
    if (!node)
        return (HAM_KEY_NOT_FOUND);

    while (1) {
        /* and then move to the newest insert*-op */
        ham_status_t st=__move_top_in_node(cursor, node, 0, HAM_FALSE, 0);
        if (st!=HAM_KEY_ERASED_IN_TXN)
            return (st);

        /* if the key was erased and approx. matching is enabled, then move
        * next/prev till we found a valid key. */
        if (flags&HAM_FIND_GT_MATCH)
            node=txn_tree_get_next_node(txn_opnode_get_tree(node), node);
        else if (flags&HAM_FIND_LT_MATCH)
            node=txn_tree_get_previous_node(txn_opnode_get_tree(node), node);
        else
            return (st);

        if (!node)
            return (HAM_KEY_NOT_FOUND);
    }

    ham_assert(!"should never reach this", (""));
    return (0);
}

ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_txn_t *txn=cursor_get_txn(txn_cursor_get_parent(cursor));

    return (db_insert_txn(db, txn, key, record, flags, cursor));
}

ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_key_t *source=0;

    /* coupled cursor? get key from the txn_op structure */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        txn_opnode_t *node=txn_op_get_node(op);

        ham_assert(db==txn_opnode_get_db(node), (""));
        source=txn_opnode_get_key(node);

        key->size=source->size;
        if (source->data && source->size) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                ham_status_t st=db_resize_key_allocdata(db, source->size);
                if (st)
                    return (st);
                key->data=db_get_key_allocdata(db);
            }
            memcpy(key->data, source->data, source->size);
        }
        else
            key->data=0;

    }
    /* 
     * otherwise cursor is nil and we cannot return a key 
     */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_record_t *source=0;

    /* coupled cursor? get record from the txn_op structure */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        source=txn_op_get_record(op);

        record->size=source->size;
        if (source->data && source->size) {
            if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                ham_status_t st=db_resize_record_allocdata(db, source->size);
                if (st)
                    return (st);
                record->data=db_get_record_allocdata(db);
            }
            memcpy(record->data, source->data, source->size);
        }
        else
            record->data=0;
    }
    /* 
     * otherwise cursor is nil and we cannot return a key 
     */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_erase(txn_cursor_t *cursor)
{
    ham_status_t st;
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_txn_t *txn=cursor_get_txn(txn_cursor_get_parent(cursor));
    txn_op_t *op;
    txn_opnode_t *node;

    /* don't continue if cursor is nil */
    if (bt_cursor_is_nil((ham_bt_cursor_t *)txn_cursor_get_parent(cursor))
            && txn_cursor_is_nil(cursor))
        return (HAM_CURSOR_IS_NIL);

    /*
     * !!
     * we have two cases:
     *
     * 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
     *    and the txn_cursor is nil; in that case, we have to 
     *      - uncouple the btree cursor
     *      - insert the erase-op for the key which is used by the btree cursor
     *
     * 2. the cursor is coupled to a txn-op; in this case, we have to
     *      - insert the erase-op for the key which is used by the txn-op
     */

    /* case 1 described above */
    if (txn_cursor_is_nil(cursor)) {
        ham_bt_cursor_t *btc=(ham_bt_cursor_t *)txn_cursor_get_parent(cursor);
        if (bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED) {
            st=bt_cursor_uncouple(btc, 0);
            if (st)
                return (st);
        }
        st=db_erase_txn(db, txn, bt_cursor_get_uncoupled_key(btc), 0);
        if (st)
            return (st);
    }
    /* case 2 described above */
    else {
        op=txn_cursor_get_coupled_op(cursor);
        node=txn_op_get_node(op);
        st=db_erase_txn(db, txn, txn_opnode_get_key(node), 0);
        if (st)
            return (st);
    }

    /* in any case we set the cursor to nil afterwards */
    bt_cursor_set_to_nil((ham_bt_cursor_t *)txn_cursor_get_parent(cursor));
    txn_cursor_set_to_nil(cursor);

    return (0);
}

ham_status_t
txn_cursor_get_duplicate_count(txn_cursor_t *cursor, ham_u32_t *count)
{
    /* TODO */
    return (0);
}
