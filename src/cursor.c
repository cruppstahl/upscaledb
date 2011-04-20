/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "cursor.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "btree_cursor.h"
#include "btree_key.h"

static ham_status_t
__dupecache_resize(dupecache_t *c, ham_size_t capacity)
{
    ham_env_t *env=db_get_env(cursor_get_db(dupecache_get_cursor(c)));
    dupecache_line_t *ptr=dupecache_get_elements(c);

    if (capacity==0) {
        dupecache_clear(c);
        return (0);
    }

    ptr=(dupecache_line_t *)allocator_realloc(env_get_allocator(env), 
                    ptr, sizeof(dupecache_line_t)*capacity);
    if (ptr) {
        dupecache_set_capacity(c, capacity);
        dupecache_set_elements(c, ptr);
        return (0);
    }

    return (HAM_OUT_OF_MEMORY);
}

ham_status_t
dupecache_create(dupecache_t *c, struct ham_cursor_t *cursor, 
                    ham_size_t capacity)
{
    memset(c, 0, sizeof(*c));
    dupecache_set_cursor(c, cursor);

    return (__dupecache_resize(c, capacity==0 ? 8 : capacity));
}

ham_status_t
dupecache_clone(dupecache_t *src, dupecache_t *dest)
{
    ham_status_t st;

    *dest=*src;
    dupecache_set_elements(dest, 0);

    if (!dupecache_get_capacity(src))
        return (0);

    st=__dupecache_resize(dest, dupecache_get_capacity(src));
    if (st)
        return (st);

    memcpy(dupecache_get_elements(dest), dupecache_get_elements(src),
            dupecache_get_count(dest)*sizeof(dupecache_line_t));
    return (0);
}

ham_status_t
dupecache_insert(dupecache_t *c, ham_u32_t position, dupecache_line_t *dupe)
{
    ham_status_t st;
    dupecache_line_t *e;

    ham_assert(position<=dupecache_get_count(c), (""));

    /* append or insert in the middle? */
    if (position==dupecache_get_count(c))
        return (dupecache_append(c, dupe));

    /* resize if necessary */
    if (dupecache_get_count(c)>=dupecache_get_capacity(c)-1) {
        st=__dupecache_resize(c, dupecache_get_capacity(c)*2);
        if (st)
            return (st);
    }

    e=dupecache_get_elements(c);

    /* shift elements to the "right" */
    memmove(&e[position+1], &e[position], 
                    sizeof(dupecache_line_t)*(dupecache_get_count(c)-position));
    e[position]=*dupe;
    dupecache_set_count(c, dupecache_get_count(c)+1);

    return (0);
}

ham_status_t
dupecache_append(dupecache_t *c, dupecache_line_t *dupe)
{
    ham_status_t st;
    dupecache_line_t *e;

    /* resize if necessary */
    if (dupecache_get_count(c)>=dupecache_get_capacity(c)-1) {
        st=__dupecache_resize(c, dupecache_get_capacity(c)*2);
        if (st)
            return (st);
    }

    e=dupecache_get_elements(c);

    e[dupecache_get_count(c)]=*dupe;
    dupecache_set_count(c, dupecache_get_count(c)+1);

    return (0);
}

ham_status_t
dupecache_erase(dupecache_t *c, ham_u32_t position)
{
    ham_assert(position<dupecache_get_count(c), (""));

    dupecache_line_t *e=dupecache_get_elements(c);

    if (position<dupecache_get_count(c)-1) {
        /* shift elements to the "left" */
        memmove(&e[position], &e[position+1], 
                sizeof(dupecache_line_t)*(dupecache_get_count(c)-position-1));
    }

    dupecache_set_count(c, dupecache_get_count(c)-1);

    return (0);
}

void
dupecache_clear(dupecache_t *c)
{
    ham_env_t *env;
    if (dupecache_get_cursor(c))
        env=db_get_env(cursor_get_db(dupecache_get_cursor(c)));

    if (dupecache_get_elements(c))
        allocator_free(env_get_allocator(env), dupecache_get_elements(c));

    dupecache_set_elements(c, 0);
    dupecache_set_capacity(c, 0);
    dupecache_set_count(c, 0);
}

void
dupecache_reset(dupecache_t *c)
{
    dupecache_set_count(c, 0);
}

ham_status_t
cursor_update_dupecache(ham_cursor_t *cursor, ham_u32_t what)
{
    ham_status_t st=0;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    dupecache_t *dc=cursor_get_dupecache(cursor);
    ham_bt_cursor_t *btc=(ham_bt_cursor_t *)cursor;
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);

    ham_assert(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES, (""));

    /* if the cache already exists: no need to continue, it should be
     * up to date */
    if (dupecache_get_count(dc)!=0)
        return (0);

    /* initialize the dupecache, if it was not yet initialized */
    if (dupecache_get_capacity(dc)==0) {
        st=dupecache_create(dc, cursor, 8);
        if (st)
            return (st);
    }

    /* first collect all duplicates from the btree. They're already sorted,
     * therefore we can just append them to our duplicate-cache. */
    if ((what&DUPE_CHECK_BTREE) && !bt_cursor_is_nil(btc)) {
        ham_size_t i;
        ham_bool_t needs_free=HAM_FALSE;
        dupe_table_t *table=0;
        st=bt_cursor_get_duplicate_table(btc, &table, &needs_free);
        if (st && st!=HAM_CURSOR_IS_NIL)
            return (st);
        st=0;
        if (table) {
            for (i=0; i<dupe_table_get_count(table); i++) {
                dupecache_line_t dcl={0};
                dupe_entry_t *e=dupe_table_get_entry(table, i);
                dupecache_line_set_btree(&dcl, HAM_TRUE);
                dupecache_line_set_btree_dupe_idx(&dcl, i);
                st=dupecache_append(dc, &dcl);
                if (st) {
                    allocator_free(env_get_allocator(env), table);
                    return (st);
                }
            }
            if (needs_free)
                allocator_free(env_get_allocator(env), table);
        }
        changeset_clear(env_get_changeset(env));
    }

    /* read duplicates from the txn-cursor? */
    if ((what&DUPE_CHECK_TXN) && !txn_cursor_is_nil(txnc)) {
        txn_op_t *op=txn_cursor_get_coupled_op(txnc);
        txn_opnode_t *node=txn_op_get_node(op);

        if (!node)
            goto bail;

        /* now start integrating the items from the transactions */
        op=txn_opnode_get_oldest_op(node);

        while (op) {
            ham_txn_t *optxn=txn_op_get_txn(op);
            /* only look at ops from the current transaction and from 
            * committed transactions */
            if ((optxn==cursor_get_txn(cursor))
                    || (txn_get_flags(optxn)&TXN_STATE_COMMITTED)) {
                /* a normal (overwriting) insert will overwrite ALL dupes,
                 * but an overwrite of a duplicate will only overwrite
                 * an entry in the dupecache */
                if (txn_op_get_flags(op)&TXN_OP_INSERT) {
                    dupecache_line_t dcl={0};
                    dupecache_line_set_btree(&dcl, HAM_FALSE);
                    dupecache_line_set_txn_op(&dcl, op);
                    /* all existing dupes are overwritten */
                    dupecache_reset(dc);
                    st=dupecache_append(dc, &dcl);
                    if (st)
                        return (st);
                }
                else if (txn_op_get_flags(op)&TXN_OP_INSERT_OW) {
                    dupecache_line_t *e=dupecache_get_elements(dc);
                    ham_u32_t ref=txn_op_get_referenced_dupe(op);
                    if (ref) {
                        ham_assert(ref<=dupecache_get_count(dc), (""));
                        dupecache_line_set_txn_op(&e[ref-1], op);
                        dupecache_line_set_btree(&e[ref-1], HAM_FALSE);
                    }
                    else {
                        dupecache_line_t dcl={0};
                        dupecache_line_set_btree(&dcl, HAM_FALSE);
                        dupecache_line_set_txn_op(&dcl, op);
                        /* all existing dupes are overwritten */
                        dupecache_reset(dc);
                        st=dupecache_append(dc, &dcl);
                        if (st)
                            return (st);
                    }
                }
                /* insert a duplicate key */
                else if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP) {
                    ham_u32_t of=txn_op_get_orig_flags(op);
                    ham_u32_t ref=txn_op_get_referenced_dupe(op)-1;
                    dupecache_line_t dcl={0};
                    dupecache_line_set_btree(&dcl, HAM_FALSE);
                    dupecache_line_set_txn_op(&dcl, op);
                    if (of&HAM_DUPLICATE_INSERT_FIRST)
                        st=dupecache_insert(dc, 0, &dcl);
                    else if (of&HAM_DUPLICATE_INSERT_BEFORE) {
                        st=dupecache_insert(dc, ref, &dcl);
                    }
                    else if (of&HAM_DUPLICATE_INSERT_AFTER) {
                        if (ref+1>=dupecache_get_count(dc))
                            st=dupecache_append(dc, &dcl);
                        else
                            st=dupecache_insert(dc, ref+1, &dcl);
                    }
                    else /* default is HAM_DUPLICATE_INSERT_LAST */
                        st=dupecache_append(dc, &dcl);
                    if (st)
                        return (st);
                }
                /* a normal erase will erase ALL duplicate keys */
                else if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                    ham_u32_t ref=txn_op_get_referenced_dupe(op);
                    if (ref) {
                        ham_assert(ref<=dupecache_get_count(dc), (""));
                        st=dupecache_erase(dc, ref-1);
                        if (st)
                            return (st);
                    }
                    else {
                        /* all existing dupes are erased */
                        dupecache_reset(dc);
                    }
                }
                else {
                    /* everything else is a bug! */
                    ham_assert(txn_op_get_flags(op)==TXN_OP_NOP, (""));
                }
            }
    
            /* continue with the previous/older operation */
            op=txn_op_get_next_in_node(op);
        }
    }

bail:

    return (0);
}

void
cursor_couple_to_dupe(ham_cursor_t *cursor, ham_u32_t dupe_id)
{
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);
    dupecache_t *dc=cursor_get_dupecache(cursor);
    dupecache_line_t *e=0;

    ham_assert(dc && dupecache_get_count(dc)>=dupe_id, (""));
    ham_assert(dupe_id>=1, (""));

    /* dupe-id is a 1-based index! */
    e=dupecache_get_elements(dc)+(dupe_id-1);
    if (dupecache_line_use_btree(e)) {
        ham_bt_cursor_t *btc=(ham_bt_cursor_t *)cursor;
        cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
        bt_cursor_set_dupe_id(btc, dupecache_line_get_btree_dupe_idx(e));
    }
    else {
        txn_cursor_couple(txnc, dupecache_line_get_txn_op(e));
        cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
    }
    cursor_set_dupecache_index(cursor, dupe_id);
}

ham_status_t
cursor_check_if_btree_key_is_erased_or_overwritten(ham_cursor_t *cursor)
{
    ham_key_t key={0};
    ham_cursor_t *clone;
    txn_op_t *op;
    ham_status_t st=ham_cursor_clone(cursor, &clone);
    txn_cursor_t *txnc=cursor_get_txn_cursor(clone);
    if (st)
        return (st);
    st=cursor->_fun_move(cursor, &key, 0, 0);
    if (st) {
        ham_cursor_close(clone);
        return (st);
    }

    st=txn_cursor_find(txnc, &key, 0);
    if (st) {
        ham_cursor_close(clone);
        return (st);
    }

    op=txn_cursor_get_coupled_op(txnc);
    if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)
        st=HAM_KEY_NOT_FOUND;
    ham_cursor_close(clone);
    return (st);
}

/* TODO TODO TODO
 * duplicate function is in db.c 
 */
static ham_bool_t
__btree_cursor_is_nil(ham_bt_cursor_t *btc)
{
    return (!(cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED) &&
            !(cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));
}

ham_status_t
cursor_sync(ham_cursor_t *cursor, ham_u32_t flags, ham_bool_t *equal_keys)
{
    ham_status_t st=0;
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);
    *equal_keys=HAM_FALSE;

    if (__btree_cursor_is_nil((ham_bt_cursor_t *)cursor)) {
        txn_opnode_t *node;
        if (!txn_cursor_get_coupled_op(txnc))
            return (0);
        node=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
        ham_key_t *k=txn_opnode_get_key(node);
        /* the flag DONT_LOAD_KEY does not load the key if there's an
         * approx match - it only positions the cursor */
        st=cursor->_fun_find(cursor, k, 0,
                BT_CURSOR_DONT_LOAD_KEY|(flags&HAM_CURSOR_NEXT 
                    ? HAM_FIND_GEQ_MATCH
                    : HAM_FIND_LEQ_MATCH));
        /* if we had a direct hit instead of an approx. match then
         * set fresh_start to false; otherwise do_local_cursor_move
         * will move the btree cursor again */
        if (st==0 && !ham_key_get_approximate_match_type(k))
            *equal_keys=HAM_TRUE;
    }
    else if (txn_cursor_is_nil(txnc)) {
        ham_cursor_t *clone;
        ham_key_t *k;
        ham_status_t st=ham_cursor_clone(cursor, &clone);
        if (st)
            goto bail;
        st=bt_cursor_uncouple((ham_bt_cursor_t *)clone, 0);
        if (st) {
            ham_cursor_close(clone);
            goto bail;
        }
        k=bt_cursor_get_uncoupled_key((ham_bt_cursor_t *)clone);
        st=txn_cursor_find(txnc, k,
                BT_CURSOR_DONT_LOAD_KEY|(flags&HAM_CURSOR_NEXT 
                    ? HAM_FIND_GEQ_MATCH
                    : HAM_FIND_LEQ_MATCH));
        /* if we had a direct hit instead of an approx. match then
        * set fresh_start to false; otherwise do_local_cursor_move
        * will move the btree cursor again */
        ham_cursor_close(clone);
        if (st==0 && !ham_key_get_approximate_match_type(k))
            *equal_keys=HAM_TRUE;
    }

bail:
    return (st);
}
