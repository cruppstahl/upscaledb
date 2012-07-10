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

ham_status_t
txn_cursor_create(Database *db, Transaction *txn, ham_u32_t flags,
                txn_cursor_t *cursor, Cursor *parent)
{
    (void)db;
    (void)txn;
    (void)flags;
    txn_cursor_set_parent(cursor, parent);
    return (0);
}

void
txn_cursor_set_to_nil(txn_cursor_t *cursor)
{
    /* uncoupled cursor? remove from the txn_op structure */
    if (!txn_cursor_is_nil(cursor)) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (op)
            txn_op_remove_cursor(op, cursor);
        txn_cursor_set_coupled_op(cursor, 0);
    }

    /* otherwise cursor is already nil */
}

void
txn_cursor_couple(txn_cursor_t *cursor, txn_op_t *op)
{
    txn_cursor_set_to_nil(cursor);
    txn_cursor_set_coupled_op(cursor, op);
    txn_op_add_cursor(op, cursor);
}

void
txn_cursor_clone(const txn_cursor_t *src, txn_cursor_t *dest, 
                Cursor *parent)
{
    txn_cursor_set_parent(dest, parent);
    txn_cursor_set_coupled_op(dest, 0);

    if (!txn_cursor_is_nil(src))
        txn_cursor_couple(dest, txn_cursor_get_coupled_op(src));
}

void
txn_cursor_close(txn_cursor_t *cursor)
{
    txn_cursor_set_to_nil(cursor);
}

ham_status_t 
txn_cursor_conflicts(txn_cursor_t *cursor)
{
    Transaction *txn=txn_cursor_get_parent(cursor)->get_txn();
    txn_op_t *op=txn_cursor_get_coupled_op(cursor);

    if (txn_op_get_txn(op)!=txn) {
        ham_u32_t flags=txn_get_flags(txn_op_get_txn(op));
        if (!(flags&TXN_STATE_COMMITTED) && !(flags&TXN_STATE_ABORTED))
            return (HAM_TRUE);
    }

    return (HAM_FALSE);
}

ham_status_t
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record)
{
    Database *db=txn_cursor_get_db(cursor);
    Transaction *txn=txn_cursor_get_parent(cursor)->get_txn();
    txn_op_t *op;
    txn_opnode_t *node;

    /* an overwrite is actually an insert w/ HAM_OVERWRITE of the
     * current key */

    if (txn_cursor_is_nil(cursor))
        return (HAM_CURSOR_IS_NIL);

    op=txn_cursor_get_coupled_op(cursor);
    node=txn_op_get_node(op);

    /* check if the op is part of a conflicting txn */
    if (txn_cursor_conflicts(cursor))
        return (HAM_TXN_CONFLICT);

    return (db_insert_txn(db, txn, txn_opnode_get_key(node), 
                record, HAM_OVERWRITE, cursor));
}

static ham_status_t
__move_top_in_node(txn_cursor_t *cursor, txn_opnode_t *node, txn_op_t *op,
                ham_bool_t ignore_conflicts, ham_u32_t flags)
{
    txn_op_t *lastdup=0;
    Transaction *optxn=0;
    Cursor *pc=txn_cursor_get_parent(cursor);

    if (!op)
        op=txn_opnode_get_newest_op(node);
    else
        goto next;

    while (op) {
        optxn=txn_op_get_txn(op);
        /* only look at ops from the current transaction and from 
         * committed transactions */
        if ((optxn==txn_cursor_get_parent(cursor)->get_txn())
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
            ham_assert(txn_op_get_flags(op)==TXN_OP_NOP);
        }
        else if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if (!ignore_conflicts) {
            /* we still have to couple, because higher-level functions
             * will need to know about the op when consolidating the trees */
            txn_cursor_couple(cursor, op);
            return (HAM_TXN_CONFLICT);
        }

next:
        pc->set_dupecache_index(0);
        op=txn_op_get_previous_in_node(op);
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
    Database *db=txn_cursor_get_db(cursor);
    txn_opnode_t *node;

    if (flags&HAM_CURSOR_FIRST) {
        /* first set cursor to nil */
        txn_cursor_set_to_nil(cursor);

        node=txn_tree_get_first(db->get_optree());
        if (!node)
            return (HAM_KEY_NOT_FOUND);
        return (__move_top_in_node(cursor, node, 0, HAM_FALSE, flags));
    }
    else if (flags&HAM_CURSOR_LAST) {
        /* first set cursor to nil */
        txn_cursor_set_to_nil(cursor);

        node=txn_tree_get_last(db->get_optree());
        if (!node)
            return (HAM_KEY_NOT_FOUND);
        return (__move_top_in_node(cursor, node, 0, HAM_FALSE, flags));
    }
    else if (flags&HAM_CURSOR_NEXT) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (txn_cursor_is_nil(cursor))
            return (HAM_CURSOR_IS_NIL);

        node=txn_op_get_node(op);
        op=0;

        ham_assert(!txn_cursor_is_nil(cursor));

        /* first move to the next key in the current node; if we fail, 
         * then move to the next node. repeat till we've found a key or 
         * till we've reached the end of the tree */
        while (1) {
            node=txn_opnode_get_next_sibling(node);
            if (!node)
                return (HAM_KEY_NOT_FOUND);
            st=__move_top_in_node(cursor, node, op, HAM_TRUE, flags); 
            if (st==HAM_KEY_NOT_FOUND)
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

        ham_assert(!txn_cursor_is_nil(cursor));

        /* first move to the previous key in the current node; if we fail, 
         * then move to the previous node. repeat till we've found a key or 
         * till we've reached the end of the tree */
        while (1) {
            node=txn_opnode_get_previous_sibling(node);
            if (!node)
                return (HAM_KEY_NOT_FOUND);
            st=__move_top_in_node(cursor, node, op, HAM_TRUE, flags); 
            if (st==HAM_KEY_NOT_FOUND)
                continue;
            return (st);
        }
    }
    else {
        ham_assert(!"this flag is not yet implemented");
    }

    return (0);
}

ham_bool_t
txn_cursor_is_erased(txn_cursor_t *cursor)
{
    txn_op_t *op=txn_cursor_get_coupled_op(cursor);
    txn_opnode_t *node=txn_op_get_node(op);

    ham_assert(!txn_cursor_is_nil(cursor));

    /* move to the newest op and check if it erased the key */
    return (HAM_KEY_ERASED_IN_TXN
                ==__move_top_in_node(cursor, node, 0, HAM_FALSE, 0));
}

ham_bool_t
txn_cursor_is_erased_duplicate(txn_cursor_t *cursor)
{
    txn_op_t *op=txn_cursor_get_coupled_op(cursor);
    txn_opnode_t *node=txn_op_get_node(op);
    Cursor *pc=txn_cursor_get_parent(cursor);

    ham_assert(!txn_cursor_is_nil(cursor));
    ham_assert(pc->get_dupecache_index()!=0);

    op=txn_opnode_get_newest_op(node);

    while (op) {
        Transaction *optxn=txn_op_get_txn(op);
        /* only look at ops from the current transaction and from 
         * committed transactions */
        if ((optxn==txn_cursor_get_parent(cursor)->get_txn())
                || (txn_get_flags(optxn)&TXN_STATE_COMMITTED)) {
            /* a normal erase deletes ALL the duplicates */
            if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                ham_u32_t ref=txn_op_get_referenced_dupe(op);
                if (ref)
                    return (ref==pc->get_dupecache_index());
                return (HAM_TRUE);
            }
        }

        op=txn_op_get_previous_in_node(op);
    }

    return (HAM_FALSE);
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
            node=txn_opnode_get_next_sibling(node);
        else if (flags&HAM_FIND_LT_MATCH)
            node=txn_opnode_get_previous_sibling(node);
        else
            return (st);

        if (!node)
            return (HAM_KEY_NOT_FOUND);
    }

    ham_assert(!"should never reach this");
    return (0);
}

ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    Database *db=txn_cursor_get_db(cursor);
    Transaction *txn=txn_cursor_get_parent(cursor)->get_txn();

    return (db_insert_txn(db, txn, key, record, flags, cursor));
}

ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key)
{
    Database *db=txn_cursor_get_db(cursor);
    Transaction *txn=txn_cursor_get_parent(cursor)->get_txn();
    ham_key_t *source=0;

    ByteArray *arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &db->get_key_arena()
                        : &txn->get_key_arena();

    /* coupled cursor? get key from the txn_op structure */
    if (!txn_cursor_is_nil(cursor)) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        txn_opnode_t *node=txn_op_get_node(op);

        ham_assert(db==txn_opnode_get_db(node));
        source=txn_opnode_get_key(node);

        key->size=source->size;
        if (source->data && source->size) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                arena->resize(source->size);
                key->data=arena->get_ptr();
            }
            memcpy(key->data, source->data, source->size);
        }
        else
            key->data=0;
    }
    /* otherwise cursor is nil and we cannot return a key */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record)
{
    Database *db=txn_cursor_get_db(cursor);
    ham_record_t *source=0;
    Transaction *txn=txn_cursor_get_parent(cursor)->get_txn();

    ByteArray *arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &db->get_record_arena()
                        : &txn->get_record_arena();

    /* coupled cursor? get record from the txn_op structure */
    if (!txn_cursor_is_nil(cursor)) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        source=txn_op_get_record(op);

        record->size=source->size;
        if (source->data && source->size) {
            if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                arena->resize(source->size);
                record->data=arena->get_ptr();
            }
            memcpy(record->data, source->data, source->size);
        }
        else
            record->data=0;
    }
    /* otherwise cursor is nil and we cannot return a key */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_get_record_size(txn_cursor_t *cursor, ham_offset_t *psize)
{
    ham_record_t *record=0;

    /* coupled cursor? get record from the txn_op structure */
    if (!txn_cursor_is_nil(cursor)) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        record=txn_op_get_record(op);
        *psize=record->size;
    }
    /* otherwise cursor is nil and we cannot return a key */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_erase(txn_cursor_t *cursor)
{
    ham_status_t st;
    txn_op_t *op;
    txn_opnode_t *node;
    Database *db=txn_cursor_get_db(cursor);
    Cursor *parent=txn_cursor_get_parent(cursor);
    Transaction *txn=parent->get_txn();

    /* don't continue if cursor is nil */
    if (btree_cursor_is_nil(parent->get_btree_cursor())
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
        btree_cursor_t *btc=parent->get_btree_cursor();
        if (btree_cursor_is_coupled(btc)) {
            st=btree_cursor_uncouple(btc, 0);
            if (st)
                return (st);
        }
        st=db_erase_txn(db, txn, btree_cursor_get_uncoupled_key(btc), 0, 
                            cursor);
        if (st)
            return (st);
    }
    /* case 2 described above */
    else {
        op=txn_cursor_get_coupled_op(cursor);
        node=txn_op_get_node(op);
        st=db_erase_txn(db, txn, txn_opnode_get_key(node), 0, cursor);
        if (st)
            return (st);
    }

    return (0);
}

