/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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


static ham_bool_t
__btree_cursor_is_nil(btree_cursor_t *btc)
{
    return (!btree_cursor_is_coupled(btc) && !btree_cursor_is_uncoupled(btc));
}

ham_status_t
cursor_update_dupecache(Cursor *cursor, ham_u32_t what)
{
    ham_status_t st=0;
    ham_db_t *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    DupeCache *dc=cursor->get_dupecache();
    btree_cursor_t *btc=cursor->get_btree_cursor();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    if (!(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES))
        return (0);

    /* if the cache already exists: no need to continue, it should be
     * up to date */
    if (dc->get_count()!=0)
        return (0);

    if ((what&CURSOR_BTREE) && (what&CURSOR_TXN)) {
        if (cursor_is_nil(cursor, CURSOR_BTREE) 
                && !cursor_is_nil(cursor, CURSOR_TXN)) {
            ham_bool_t equal_keys;
            (void)cursor_sync(cursor, 0, &equal_keys);
            if (!equal_keys)
                cursor_set_to_nil(cursor, CURSOR_BTREE);
        }
    }

    /* first collect all duplicates from the btree. They're already sorted,
     * therefore we can just append them to our duplicate-cache. */
    if ((what&CURSOR_BTREE)
            && !cursor_is_nil(cursor, CURSOR_BTREE)) {
        ham_size_t i;
        ham_bool_t needs_free=HAM_FALSE;
        dupe_table_t *table=0;
        st=btree_cursor_get_duplicate_table(btc, &table, &needs_free);
        if (st && st!=HAM_CURSOR_IS_NIL)
            return (st);
        st=0;
        if (table) {
            for (i=0; i<dupe_table_get_count(table); i++) {
                dc->append(DupeCacheLine(true, i));
            }
            if (needs_free)
                allocator_free(env_get_allocator(env), table);
        }
        env_get_changeset(env).clear();
    }

    /* read duplicates from the txn-cursor? */
    if ((what&CURSOR_TXN)
            && !cursor_is_nil(cursor, CURSOR_TXN)) {
        txn_op_t *op=txn_cursor_get_coupled_op(txnc);
        txn_opnode_t *node=txn_op_get_node(op);

        if (!node)
            goto bail;

        /* now start integrating the items from the transactions */
        op=txn_opnode_get_oldest_op(node);

        while (op) {
            ham_txn_t *optxn=txn_op_get_txn(op);
            /* collect all ops that are valid (even those that are 
             * from conflicting transactions) */
            if (!(txn_get_flags(optxn)&TXN_STATE_ABORTED)) {
                /* a normal (overwriting) insert will overwrite ALL dupes,
                 * but an overwrite of a duplicate will only overwrite
                 * an entry in the dupecache */
                if (txn_op_get_flags(op)&TXN_OP_INSERT) {
                    /* all existing dupes are overwritten */
                    dc->clear();
                    dc->append(DupeCacheLine(false, op));
                }
                else if (txn_op_get_flags(op)&TXN_OP_INSERT_OW) {
                    DupeCacheLine *e=dc->get_first_element();
                    ham_u32_t ref=txn_op_get_referenced_dupe(op);
                    if (ref) {
                        ham_assert(ref<=dc->get_count(), (""));
                        (&e[ref-1])->set_txn_op(op);
                    }
                    else {
                        /* all existing dupes are overwritten */
                        dc->clear();
                        dc->append(DupeCacheLine(false, op));
                    }
                }
                /* insert a duplicate key */
                else if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP) {
                    ham_u32_t of=txn_op_get_orig_flags(op);
                    ham_u32_t ref=txn_op_get_referenced_dupe(op)-1;
                    DupeCacheLine dcl(false, op);
                    if (of&HAM_DUPLICATE_INSERT_FIRST)
                        dc->insert(0, dcl);
                    else if (of&HAM_DUPLICATE_INSERT_BEFORE) {
                        dc->insert(ref, dcl);
                    }
                    else if (of&HAM_DUPLICATE_INSERT_AFTER) {
                        if (ref+1>=dc->get_count())
                            dc->append(dcl);
                        else
                            dc->insert(ref+1, dcl);
                    }
                    else /* default is HAM_DUPLICATE_INSERT_LAST */
                        dc->append(dcl);
                }
                /* a normal erase will erase ALL duplicate keys */
                else if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                    ham_u32_t ref=txn_op_get_referenced_dupe(op);
                    if (ref) {
                        ham_assert(ref<=dc->get_count(), (""));
                        dc->erase(ref-1);
                    }
                    else {
                        /* all existing dupes are erased */
                        dc->clear();
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
cursor_clear_dupecache(Cursor *cursor)
{
    cursor->get_dupecache()->clear();
    cursor->set_dupecache_index(0);
}

void
cursor_couple_to_dupe(Cursor *cursor, ham_u32_t dupe_id)
{
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    DupeCache *dc=cursor->get_dupecache();
    DupeCacheLine *e=0;

    ham_assert(dc->get_count()>=dupe_id, (""));
    ham_assert(dupe_id>=1, (""));

    /* dupe-id is a 1-based index! */
    e=dc->get_element(dupe_id-1);
    if (e->use_btree()) {
        btree_cursor_t *btc=cursor->get_btree_cursor();
        cursor_couple_to_btree(cursor);
        btree_cursor_set_dupe_id(btc, e->get_btree_dupe_idx());
    }
    else {
        ham_assert(e->get_txn_op()!=0, (""));
        txn_cursor_couple(txnc, e->get_txn_op());
        cursor_couple_to_txnop(cursor);
    }
    cursor->set_dupecache_index(dupe_id);
}

ham_status_t
cursor_check_if_btree_key_is_erased_or_overwritten(Cursor *cursor)
{
    ham_key_t key={0};
    Cursor *clone;
    txn_op_t *op;
    ham_status_t st=ham_cursor_clone((ham_cursor_t *)cursor, 
                            (ham_cursor_t **)&clone);
    txn_cursor_t *txnc=clone->get_txn_cursor();
    if (st)
        return (st);
    st=btree_cursor_move(cursor->get_btree_cursor(), &key, 0, 0);
    if (st) {
        ham_cursor_close((ham_cursor_t *)clone);
        return (st);
    }

    st=txn_cursor_find(txnc, &key, 0);
    if (st) {
        ham_cursor_close((ham_cursor_t *)clone);
        return (st);
    }

    op=txn_cursor_get_coupled_op(txnc);
    if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)
        st=HAM_KEY_NOT_FOUND;
    ham_cursor_close((ham_cursor_t *)clone);
    return (st);
}

ham_status_t
cursor_sync(Cursor *cursor, ham_u32_t flags, ham_bool_t *equal_keys)
{
    ham_status_t st=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    if (equal_keys)
        *equal_keys=HAM_FALSE;

    if (cursor_is_nil(cursor, CURSOR_BTREE)) {
        txn_opnode_t *node;
		ham_key_t *k;
        if (!txn_cursor_get_coupled_op(txnc))
            return (0);
        node=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
        k=txn_opnode_get_key(node);

        if (!(flags&CURSOR_SYNC_ONLY_EQUAL_KEY))
            flags=flags|((flags&HAM_CURSOR_NEXT)
                    ? HAM_FIND_GEQ_MATCH
                    : HAM_FIND_LEQ_MATCH);
        /* the flag DONT_LOAD_KEY does not load the key if there's an
         * approx match - it only positions the cursor */
        st=btree_cursor_find(cursor->get_btree_cursor(), k, 0, 
                CURSOR_SYNC_DONT_LOAD_KEY|flags);
        /* if we had a direct hit instead of an approx. match then
         * set fresh_start to false; otherwise do_local_cursor_move
         * will move the btree cursor again */
        if (st==0 && equal_keys && !ham_key_get_approximate_match_type(k))
            *equal_keys=HAM_TRUE;
    }
    else if (cursor_is_nil(cursor, CURSOR_TXN)) {
        Cursor *clone;
        ham_key_t *k;
        ham_status_t st=ham_cursor_clone((ham_cursor_t *)cursor, 
                            (ham_cursor_t **)&clone);
        if (st)
            goto bail;
        st=btree_cursor_uncouple(clone->get_btree_cursor(), 0);
        if (st) {
            ham_cursor_close((ham_cursor_t *)clone);
            goto bail;
        }
        k=btree_cursor_get_uncoupled_key(clone->get_btree_cursor());
        if (!(flags&CURSOR_SYNC_ONLY_EQUAL_KEY))
            flags=flags|((flags&HAM_CURSOR_NEXT)
                    ? HAM_FIND_GEQ_MATCH
                    : HAM_FIND_LEQ_MATCH);
        st=txn_cursor_find(txnc, k, CURSOR_SYNC_DONT_LOAD_KEY|flags);
        /* if we had a direct hit instead of an approx. match then
        * set fresh_start to false; otherwise do_local_cursor_move
        * will move the btree cursor again */
        if (st==0 && equal_keys && !ham_key_get_approximate_match_type(k))
            *equal_keys=HAM_TRUE;
        ham_cursor_close((ham_cursor_t *)clone);
    }

bail:
    return (st);
}

static ham_size_t
__cursor_has_duplicates(Cursor *cursor)
{
    return (cursor->get_dupecache()->get_count());
}

static ham_status_t
__cursor_move_next_dupe(Cursor *cursor)
{
    DupeCache *dc=cursor->get_dupecache();

    if (cursor->get_dupecache_index()) {
        if (cursor->get_dupecache_index()<dc->get_count()) {
            cursor->set_dupecache_index(cursor->get_dupecache_index()+1);
            cursor_couple_to_dupe(cursor, 
                        cursor->get_dupecache_index());
            return (0);
        }
    }
    return (HAM_LIMITS_REACHED);
}

static ham_status_t
__cursor_move_previous_dupe(Cursor *cursor, ham_u32_t flags)
{
    if (cursor->get_dupecache_index()) {
        if (cursor->get_dupecache_index()>1) {
            cursor->set_dupecache_index(cursor->get_dupecache_index()-1);
            cursor_couple_to_dupe(cursor, 
                        cursor->get_dupecache_index());
            return (0);
        }
    }
    return (HAM_LIMITS_REACHED);
}

static ham_status_t
__cursor_move_first_dupe(Cursor *cursor, ham_u32_t flags)
{
    DupeCache *dc=cursor->get_dupecache();

    if (dc->get_count()) {
        cursor->set_dupecache_index(1);
        cursor_couple_to_dupe(cursor, cursor->get_dupecache_index());
        return (0);
    }
    return (HAM_LIMITS_REACHED);
}

static ham_status_t
__cursor_move_last_dupe(Cursor *cursor, ham_u32_t flags)
{
    DupeCache *dc=cursor->get_dupecache();

    if (dc->get_count()) {
        cursor->set_dupecache_index(dc->get_count());
        cursor_couple_to_dupe(cursor, cursor->get_dupecache_index());
        return (0);
    }
    return (HAM_LIMITS_REACHED);
}

static ham_bool_t
__txn_cursor_is_erase(txn_cursor_t *txnc) 
{
    txn_op_t *op=txn_cursor_get_coupled_op(txnc);
    return (op ? (txn_op_get_flags(op)&TXN_OP_ERASE) : HAM_FALSE);
}

static ham_status_t
__compare_cursors(btree_cursor_t *btrc, txn_cursor_t *txnc, int *pcmp)
{
    Cursor *cursor=btree_cursor_get_parent(btrc);
    ham_db_t *db=cursor->get_db();

    txn_opnode_t *node=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
    ham_key_t *txnk=txn_opnode_get_key(node);

    ham_assert(!cursor_is_nil(cursor, 0), (""));
    ham_assert(!txn_cursor_is_nil(txnc), (""));

    if (btree_cursor_is_coupled(btrc)) {
        /* clone the cursor, then uncouple the clone; get the uncoupled key
         * and discard the clone again */
        
        /* 
         * TODO TODO TODO
         * this is all correct, but of course quite inefficient, because 
         *    a) new structures have to be allocated/released
         *    b) uncoupling fetches the whole extended key, which is often
         *      not necessary
         *  -> fix it!
         */
        int cmp;
        Cursor *clone;
        ham_status_t st=ham_cursor_clone((ham_cursor_t *)cursor, 
                                (ham_cursor_t **)&clone);
        if (st)
            return (st);
        st=btree_cursor_uncouple(clone->get_btree_cursor(), 0);
        if (st) {
            ham_cursor_close((ham_cursor_t *)clone);
            return (st);
        }
        /* TODO error codes are swallowed */
        cmp=db_compare_keys(db, 
                btree_cursor_get_uncoupled_key(clone->get_btree_cursor()), 
                txnk);
        ham_cursor_close((ham_cursor_t *)clone);
        *pcmp=cmp;
        return (0);
    }
    else if (btree_cursor_is_uncoupled(btrc)) {
        /* TODO error codes are swallowed */
        *pcmp=db_compare_keys(db, btree_cursor_get_uncoupled_key(btrc), txnk);
        return (0);
    }

    ham_assert(!"shouldn't be here", (""));
    return (0);
}

static ham_status_t
__cursor_move_next_key_singlestep(Cursor *cursor)
{
    ham_status_t st=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* if both cursors point to the same key: move next with both */
    if (cursor->get_lastcmp()==0) {
        if (!cursor_is_nil(cursor, CURSOR_BTREE)) {
            st=btree_cursor_move(btrc, 0, 0, 
                        HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
            if (st==HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
                cursor_set_to_nil(cursor, CURSOR_BTREE); // TODO muss raus
                if (txn_cursor_is_nil(txnc))
                    return (HAM_KEY_NOT_FOUND);
                else {
                    cursor_couple_to_txnop(cursor);
                    cursor->set_lastcmp(+1);
                }
            }
        }
        if (!txn_cursor_is_nil(txnc)) {
            st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
            if (st==HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
                cursor_set_to_nil(cursor, CURSOR_TXN); // TODO muss raus
                if (cursor_is_nil(cursor, CURSOR_BTREE))
                    return (HAM_KEY_NOT_FOUND);
                else {
                    cursor_couple_to_btree(cursor);
                    cursor->set_lastcmp(-1);

                    ham_status_t st2=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
                    if (st2==HAM_TXN_CONFLICT)
                        st=st2;
                }
            }
        }
    }
    /* if the btree-key is smaller: move it next */
    else if (cursor->get_lastcmp()<0) {
        st=btree_cursor_move(btrc, 0, 0, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
        if (st==HAM_KEY_NOT_FOUND) {
            cursor_set_to_nil(cursor, CURSOR_BTREE); // TODO Das muss raus!
            if (txn_cursor_is_nil(txnc))
                return (st);
            cursor_couple_to_txnop(cursor);
            cursor->set_lastcmp(+1);
        }
        else {
            ham_status_t st2=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st2==HAM_TXN_CONFLICT)
                st=st2;
        }
        if (txn_cursor_is_nil(txnc))
            cursor->set_lastcmp(-1);
    }
    /* if the txn-key is smaller OR if both keys are equal: move next
     * with the txn-key (which is chronologically newer) */
    else {
        st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
        if (st==HAM_KEY_NOT_FOUND) {
            cursor_set_to_nil(cursor, CURSOR_TXN); // TODO Das muss raus!
            if (cursor_is_nil(cursor, CURSOR_BTREE))
                return (st);
            cursor_couple_to_btree(cursor);
            cursor->set_lastcmp(-1);
        }
        if (cursor_is_nil(cursor, CURSOR_BTREE))
            cursor->set_lastcmp(+1);
    }

    /* compare keys again */
    if (!cursor_is_nil(cursor, CURSOR_BTREE) && !txn_cursor_is_nil(txnc)) {
        int cmp;
        __compare_cursors(btrc, txnc, &cmp);
        cursor->set_lastcmp(cmp);
    }

    /* if there's a txn conflict: move next */
    if (st==HAM_TXN_CONFLICT)
        return (__cursor_move_next_key_singlestep(cursor));

    /* btree-key is smaller */
    if (cursor->get_lastcmp()<0 || txn_cursor_is_nil(txnc)) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (0);
    }
    /* txn-key is smaller */
    else if (cursor->get_lastcmp()>0 || btree_cursor_is_nil(btrc)) {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (0);
    }
    /* both keys are equal */
    else {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN|CURSOR_BTREE);
        return (0);
    }
}

static ham_status_t
__cursor_move_next_key(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* are we in the middle of a duplicate list? if yes then move to the
     * next duplicate */
    if (cursor->get_dupecache_index()>0 && !(flags&HAM_SKIP_DUPLICATES)) {
        st=__cursor_move_next_dupe(cursor);
        if (st!=HAM_LIMITS_REACHED)
            return (st);
    }

    cursor_clear_dupecache(cursor);

    /* either there were no duplicates or we've reached the end of the 
     * duplicate list. move next till we found a new candidate */
    while (1) {
        st=__cursor_move_next_key_singlestep(cursor);
        if (st)
            return (st);

        /* check for duplicates. the dupecache was already updated in 
         * __cursor_move_next_key_singlestep() */
        if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
            /* are there any duplicates? if not then they were all erased and
             * we move to the previous key */
            if (!__cursor_has_duplicates(cursor))
                continue;

            /* otherwise move to the first duplicate */
            return (__cursor_move_first_dupe(cursor, 0));
        }

        /* no duplicates - make sure that we've not coupled to an erased 
         * item */
        if (cursor_is_coupled_to_txnop(cursor)) {
            if (__txn_cursor_is_erase(txnc))
                continue;
            else
                return (0);
        }
        if (cursor_is_coupled_to_btree(cursor)) {
            st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st==HAM_KEY_ERASED_IN_TXN)
                continue;
            else if (st==0) {
                cursor_couple_to_txnop(cursor);
                return (0);
            }
            else if (st==HAM_KEY_NOT_FOUND)
                return (0);
            else
                return (st);
        }
        else
            return (HAM_KEY_NOT_FOUND);
    }

    ham_assert(!"should never reach this", (""));
    return (HAM_INTERNAL_ERROR);
}

static ham_status_t
__cursor_move_previous_key_singlestep(Cursor *cursor)
{
    ham_status_t st=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* if both cursors point to the same key: move previous with both */
    if (cursor->get_lastcmp()==0) {
        if (!cursor_is_nil(cursor, CURSOR_BTREE)) {
            st=btree_cursor_move(btrc, 0, 0, 
                        HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
            if (st==HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
                cursor_set_to_nil(cursor, CURSOR_BTREE); // TODO muss raus
                if (txn_cursor_is_nil(txnc))
                    return (HAM_KEY_NOT_FOUND);
                else {
                    cursor_couple_to_txnop(cursor);
                    cursor->set_lastcmp(-1);
                }
            }
        }
        if (!txn_cursor_is_nil(txnc)) {
            st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
            if (st==HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
                cursor_set_to_nil(cursor, CURSOR_TXN); // TODO muss raus
                if (cursor_is_nil(cursor, CURSOR_BTREE))
                    return (HAM_KEY_NOT_FOUND);
                else {
                    cursor_couple_to_btree(cursor);
                    cursor->set_lastcmp(+1);
                }
            }
        }
    }
    /* if the btree-key is greater: move previous */
    else if (cursor->get_lastcmp()>0) {
        st=btree_cursor_move(btrc, 0, 0, 
                        HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
        if (st==HAM_KEY_NOT_FOUND) {
            cursor_set_to_nil(cursor, CURSOR_BTREE); // TODO Das muss raus!
            if (txn_cursor_is_nil(txnc))
                return (st);
            cursor_couple_to_txnop(cursor);
            cursor->set_lastcmp(-1);
        }
        else {
            ham_status_t st2=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st2==HAM_TXN_CONFLICT)
                st=st2;
        }
        if (txn_cursor_is_nil(txnc))
            cursor->set_lastcmp(+1);
    }
    /* if the txn-key is greater OR if both keys are equal: move previous
     * with the txn-key (which is chronologically newer) */
    else {
        st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
        if (st==HAM_KEY_NOT_FOUND) {
            cursor_set_to_nil(cursor, CURSOR_TXN); // TODO Das muss raus!
            if (cursor_is_nil(cursor, CURSOR_BTREE))
                return (st);
            cursor_couple_to_btree(cursor);
            cursor->set_lastcmp(+1);

            ham_status_t st2=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st2==HAM_TXN_CONFLICT)
                st=st2;
        }
        if (cursor_is_nil(cursor, CURSOR_BTREE))
            cursor->set_lastcmp(-1);
    }

    /* compare keys again */
    if (!cursor_is_nil(cursor, CURSOR_BTREE) && !txn_cursor_is_nil(txnc)) {
        int cmp;
        __compare_cursors(btrc, txnc, &cmp);
        cursor->set_lastcmp(cmp);
    }

    /* if there's a txn conflict: move previous */
    if (st==HAM_TXN_CONFLICT)
        return (__cursor_move_previous_key_singlestep(cursor));

    /* btree-key is greater */
    if (cursor->get_lastcmp()>0 || txn_cursor_is_nil(txnc)) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (0);
    }
    /* txn-key is greater */
    else if (cursor->get_lastcmp()<0 || btree_cursor_is_nil(btrc)) {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (0);
    }
    /* both keys are equal */
    else {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN|CURSOR_BTREE);
        return (0);
    }
}

static ham_status_t
__cursor_move_previous_key(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* are we in the middle of a duplicate list? if yes then move to the
     * previous duplicate */
    if (cursor->get_dupecache_index()>0 && !(flags&HAM_SKIP_DUPLICATES)) {
        st=__cursor_move_previous_dupe(cursor, flags);
        if (st!=HAM_LIMITS_REACHED)
            return (st);
    }

    cursor_clear_dupecache(cursor);

    /* either there were no duplicates or we've reached the end of the 
     * duplicate list. move previous till we found a new candidate */
    while (1) {
        st=__cursor_move_previous_key_singlestep(cursor);
        if (st)
            return (st);

        /* check for duplicates. the dupecache was already updated in 
         * __cursor_move_previous_key_singlestep() */
        if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
            /* are there any duplicates? if not then they were all erased and
             * we move to the previous key */
            if (!__cursor_has_duplicates(cursor))
                continue;

            /* otherwise move to the last duplicate */
            return (__cursor_move_last_dupe(cursor, 0));
        }

        /* no duplicates - make sure that we've not coupled to an erased 
         * item */
        if (cursor_is_coupled_to_txnop(cursor)) {
            if (__txn_cursor_is_erase(txnc))
                continue;
            else
                return (0);
        }
        if (cursor_is_coupled_to_btree(cursor)) {
            st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st==HAM_KEY_ERASED_IN_TXN)
                continue;
            else if (st==0) {
                cursor_couple_to_txnop(cursor);
                return (0);
            }
            else if (st==HAM_KEY_NOT_FOUND)
                return (0);
            else
                return (st);
        }
        else
            return (HAM_KEY_NOT_FOUND);
    }

    ham_assert(!"should never reach this", (""));
    return (HAM_INTERNAL_ERROR);
}

static ham_status_t
__cursor_move_first_key_singlestep(Cursor *cursor)
{
    ham_status_t st=0, btrs, txns;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* fetch the smallest key from the transaction tree. */
    txns=txn_cursor_move(txnc, HAM_CURSOR_FIRST);
    /* fetch the smallest key from the btree tree. */
    btrs=btree_cursor_move(btrc, 0, 0, HAM_CURSOR_FIRST|HAM_SKIP_DUPLICATES);
    /* now consolidate - if both trees are empty then return */
    if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) {
        return (HAM_KEY_NOT_FOUND);
    }
    /* if btree is empty but txn-tree is not: couple to txn */
    else if (btrs==HAM_KEY_NOT_FOUND && txns!=HAM_KEY_NOT_FOUND) {
        if (txns==HAM_TXN_CONFLICT)
            return (txns);
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (0);
    }
    /* if txn-tree is empty but btree is not: couple to btree */
    else if (txns==HAM_KEY_NOT_FOUND && btrs!=HAM_KEY_NOT_FOUND) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (0);
    }
    /* if both trees are not empty then compare them and couple to the 
     * smaller one */
    else {
        ham_assert(btrs==0 
            && (txns==0 
                || txns==HAM_KEY_ERASED_IN_TXN
                || txns==HAM_TXN_CONFLICT), (""));
        int cmp;
        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);
        cursor->set_lastcmp(cmp);

        /* both keys are equal - couple to txn; it's chronologically 
         * newer */
        if (cmp==0) {
            if (txns && txns!=HAM_KEY_ERASED_IN_TXN)
                return (txns);
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE|CURSOR_TXN);
        }
        /* couple to txn */
        else if (cmp>0) {
            if (txns && txns!=HAM_KEY_ERASED_IN_TXN)
                return (txns);
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_TXN);
        }
        /* couple to btree */
        else {
            cursor_couple_to_btree(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE);
        }
        return (0);
    }
}

static ham_status_t
__cursor_move_first_key(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* move to the very very first key */
    st=__cursor_move_first_key_singlestep(cursor);
    if (st)
        return (st);

    /* check for duplicates. the dupecache was already updated in 
     * __cursor_move_first_key_singlestep() */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        /* are there any duplicates? if not then they were all erased and we
         * move to the previous key */
        if (!__cursor_has_duplicates(cursor))
            return (__cursor_move_next_key(cursor, flags));

        /* otherwise move to the first duplicate */
        return (__cursor_move_first_dupe(cursor, 0));
    }

    /* no duplicates - make sure that we've not coupled to an erased 
     * item */
    if (cursor_is_coupled_to_txnop(cursor)) {
        if (__txn_cursor_is_erase(txnc))
            return (__cursor_move_next_key(cursor, flags));
        else
            return (0);
    }
    if (cursor_is_coupled_to_btree(cursor)) {
        st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
        if (st==HAM_KEY_ERASED_IN_TXN)
            return (__cursor_move_next_key(cursor, flags));
        else if (st==0) {
            cursor_couple_to_txnop(cursor);
            return (0);
        }
        else if (st==HAM_KEY_NOT_FOUND)
            return (0);
        else
            return (st);
    }
    else
        return (HAM_KEY_NOT_FOUND);
}

static ham_status_t
__cursor_move_last_key_singlestep(Cursor *cursor)
{
    ham_status_t st=0, btrs, txns;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* fetch the largest key from the transaction tree. */
    txns=txn_cursor_move(txnc, HAM_CURSOR_LAST);
    /* fetch the largest key from the btree tree. */
    btrs=btree_cursor_move(btrc, 0, 0, HAM_CURSOR_LAST|HAM_SKIP_DUPLICATES);
    /* now consolidate - if both trees are empty then return */
    if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) {
        return (HAM_KEY_NOT_FOUND);
    }
    /* if btree is empty but txn-tree is not: couple to txn */
    else if (btrs==HAM_KEY_NOT_FOUND && txns!=HAM_KEY_NOT_FOUND) {
        if (txns==HAM_TXN_CONFLICT)
            return (txns);
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (0);
    }
    /* if txn-tree is empty but btree is not: couple to btree */
    else if (txns==HAM_KEY_NOT_FOUND && btrs!=HAM_KEY_NOT_FOUND) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (0);
    }
    /* if both trees are not empty then compare them and couple to the 
     * greater one */
    else {
        ham_assert(btrs==0 
            && (txns==0 
                || txns==HAM_KEY_ERASED_IN_TXN
                || txns==HAM_TXN_CONFLICT), (""));
        int cmp;
        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);
        cursor->set_lastcmp(cmp);

        /* both keys are equal - couple to txn; it's chronologically 
         * newer */
        if (cmp==0) {
            if (txns && txns!=HAM_KEY_ERASED_IN_TXN)
                return (txns);
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE|CURSOR_TXN);
        }
        /* couple to txn */
        else if (cmp<1) {
            if (txns && txns!=HAM_KEY_ERASED_IN_TXN)
                return (txns);
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_TXN);
        }
        /* couple to btree */
        else {
            cursor_couple_to_btree(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE);
        }
        return (0);
    }
}

static ham_status_t
__cursor_move_last_key(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* move to the very very last key */
    st=__cursor_move_last_key_singlestep(cursor);
    if (st)
        return (st);

    /* check for duplicates. the dupecache was already updated in 
     * __cursor_move_last_key_singlestep() */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        /* are there any duplicates? if not then they were all erased and we
         * move to the previous key */
        if (!__cursor_has_duplicates(cursor))
            return (__cursor_move_previous_key(cursor, flags));

        /* otherwise move to the last duplicate */
        return (__cursor_move_last_dupe(cursor, 0));
    }

    /* no duplicates - make sure that we've not coupled to an erased 
     * item */
    if (cursor_is_coupled_to_txnop(cursor)) {
        if (__txn_cursor_is_erase(txnc))
            return (__cursor_move_previous_key(cursor, flags));
        else
            return (0);
    }
    if (cursor_is_coupled_to_btree(cursor)) {
        st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
        if (st==HAM_KEY_ERASED_IN_TXN)
            return (__cursor_move_previous_key(cursor, flags));
        else if (st==0) {
            cursor_couple_to_txnop(cursor);
            return (0);
        }
        else if (st==HAM_KEY_NOT_FOUND)
            return (0);
        else
            return (st);
    }
    else
        return (HAM_KEY_NOT_FOUND);
}

ham_status_t
cursor_move(Cursor *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    ham_status_t st=0;
    ham_bool_t changed_dir=HAM_FALSE;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* no movement requested? directly retrieve key/record */
    if (!flags)
        goto retrieve_key_and_record;

    /* synchronize the btree and transaction cursor if the last operation was
     * not a move next/previous OR if the direction changed */
    if ((cursor->get_lastop()==HAM_CURSOR_PREVIOUS)
            && (flags&HAM_CURSOR_NEXT))
        changed_dir=HAM_TRUE;
    else if ((cursor->get_lastop()==HAM_CURSOR_NEXT)
            && (flags&HAM_CURSOR_PREVIOUS))
        changed_dir=HAM_TRUE;
    if (((flags&HAM_CURSOR_NEXT) || (flags&HAM_CURSOR_PREVIOUS))
            && (cursor->get_lastop()==CURSOR_LOOKUP_INSERT
                || changed_dir)) {
        if (cursor_is_coupled_to_txnop(cursor))
            cursor_set_to_nil(cursor, CURSOR_BTREE);
        else
            cursor_set_to_nil(cursor, CURSOR_TXN);
        (void)cursor_sync(cursor, flags, 0);

        if (!txn_cursor_is_nil(txnc) && !cursor_is_nil(cursor, CURSOR_BTREE)) {
            int cmp;
            __compare_cursors(btrc, txnc, &cmp);
            cursor->set_lastcmp(cmp);
        }
    }

    /* should we move through the duplicate list? */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)
            && !(flags&HAM_CURSOR_FIRST) 
            && !(flags&HAM_CURSOR_LAST)) {
        if (flags&HAM_CURSOR_PREVIOUS) {
            st=__cursor_move_previous_dupe(cursor, flags);
            if (st==0)
                goto retrieve_key_and_record;
            if (st!=HAM_LIMITS_REACHED)
                return (st);
        }
    }

    /* we have either skipped duplicates or reached the end of the duplicate
     * list. btree cursor and txn cursor are synced and as close to 
     * each other as possible. Move the cursor in the requested direction. */
    if (flags&HAM_CURSOR_NEXT) {
        st=__cursor_move_next_key(cursor, flags);
    }
    else if (flags&HAM_CURSOR_PREVIOUS) {
        st=__cursor_move_previous_key(cursor, flags);
    }
    else if (flags&HAM_CURSOR_FIRST) {
        cursor_clear_dupecache(cursor);
        st=__cursor_move_first_key(cursor, flags);
    }
    else {
        ham_assert(flags&HAM_CURSOR_LAST, (""));
        cursor_clear_dupecache(cursor);
        st=__cursor_move_last_key(cursor, flags);
    }

    if (st)
        return (st);

retrieve_key_and_record:
    /* retrieve key/record, if requested */
    if (st==0) {
        if (cursor_is_coupled_to_txnop(cursor)) {
#ifdef HAM_DEBUG
            txn_op_t *op=txn_cursor_get_coupled_op(txnc);
            ham_assert(!(txn_op_get_flags(op)&TXN_OP_ERASE), (""));
#endif
            if (key) {
                st=txn_cursor_get_key(txnc, key);
                if (st)
                    goto bail;
            }
            if (record) {
                st=txn_cursor_get_record(txnc, record);
                if (st)
                    goto bail;
            }
        }
        else {
            st=btree_cursor_move(btrc, key, record, 0);
        }
    }

bail:
    return (st);
}

ham_size_t
cursor_get_dupecache_count(Cursor *cursor)
{
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    if (!(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES))
        return (HAM_FALSE);

    if (txn_cursor_get_coupled_op(txnc))
        cursor_update_dupecache(cursor, CURSOR_BTREE|CURSOR_TXN);
    else
        cursor_update_dupecache(cursor, CURSOR_BTREE);

    return (cursor->get_dupecache()->get_count());
}

ham_status_t
cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            Cursor **pcursor)
{
    Cursor *c=new Cursor(db, txn);

    *pcursor=0;

    c->set_flags(flags);

    txn_cursor_create(db, txn, flags, c->get_txn_cursor(), c);
    btree_cursor_create(db, txn, flags, c->get_btree_cursor(), c);

    *pcursor=c;
    return (0);
}

ham_status_t
cursor_clone(Cursor *src, Cursor **dest)
{
    ham_status_t st;
    ham_db_t *db=src->get_db();
    Cursor *c;

    *dest=0;

    c=new Cursor(*src);
    c->set_next_in_page(0);
    c->set_previous_in_page(0);

    st=btree_cursor_clone(src->get_btree_cursor(), c->get_btree_cursor(), c);
    if (st)
        return (st);

    /* always clone the txn-cursor, even if transactions are not required */
    txn_cursor_clone(src->get_txn_cursor(), c->get_txn_cursor(), c);

    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)
        src->get_dupecache()->clone(c->get_dupecache());

    *dest=c;
    return (0);
}

ham_bool_t
cursor_is_nil(Cursor *cursor, int what)
{
    ham_assert(cursor!=0, (""));

    switch (what) {
      case CURSOR_BTREE:
        return (__btree_cursor_is_nil(cursor->get_btree_cursor()));
      case CURSOR_TXN:
        return (txn_cursor_is_nil(cursor->get_txn_cursor()));
      default:
        ham_assert(what==0, (""));
        /* TODO btree_cursor_is_nil is different from __btree_cursor_is_nil
         * - refactor and clean up! */
        return (btree_cursor_is_nil(cursor->get_btree_cursor()));
    }
}

void
cursor_set_to_nil(Cursor *cursor, int what)
{
    switch (what) {
      case CURSOR_BTREE:
        btree_cursor_set_to_nil(cursor->get_btree_cursor());
        break;
      case CURSOR_TXN:
        txn_cursor_set_to_nil(cursor->get_txn_cursor());
        cursor_couple_to_btree(cursor); /* reset flag */
        break;
      default:
        ham_assert(what==0, (""));
        btree_cursor_set_to_nil(cursor->get_btree_cursor());
        txn_cursor_set_to_nil(cursor->get_txn_cursor());
        break;
    }
}

ham_status_t
cursor_erase(Cursor *cursor, ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;

    /* if transactions are enabled: add a erase-op to the txn-tree */
    if (txn) {
        /* if cursor is coupled to a btree item: set the txn-cursor to 
         * nil; otherwise txn_cursor_erase() doesn't know which cursor 
         * part is the valid one */
        if (cursor_is_coupled_to_btree(cursor))
            cursor_set_to_nil(cursor, CURSOR_TXN);
        st=txn_cursor_erase(cursor->get_txn_cursor());
    }
    else {
        st=btree_cursor_erase(cursor->get_btree_cursor(), flags);
    }

    if (st==0)
        cursor_set_to_nil(cursor, 0);
    return (st);
}

ham_status_t
cursor_get_duplicate_count(Cursor *cursor, ham_txn_t *txn, 
            ham_u32_t *pcount, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor->get_db();

    *pcount=0;

    if (txn) {
        if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
            ham_bool_t dummy;
            DupeCache *dc=cursor->get_dupecache();

            (void)cursor_sync(cursor, 0, &dummy);
            st=cursor_update_dupecache(cursor, CURSOR_TXN|CURSOR_BTREE);
            if (st)
                return (st);
            *pcount=dc->get_count();
        }
        else {
            /* obviously the key exists, since the cursor is coupled to
             * a valid item */
            *pcount=1;
        }
    }
    else {
        st=btree_cursor_get_duplicate_count(cursor->get_btree_cursor(), 
                    pcount, flags);
    }

    return (st);
}

ham_status_t
cursor_get_record_size(Cursor *cursor, ham_txn_t *txn, ham_offset_t *psize)
{
    ham_status_t st=0;

    *psize=0;

    if (txn) {
        if (cursor_is_coupled_to_txnop(cursor))
            st=txn_cursor_get_record_size(cursor->get_txn_cursor(), psize);
        else
            st=btree_cursor_get_record_size(cursor->get_btree_cursor(), psize);
    }
    else
        st=btree_cursor_get_record_size(cursor->get_btree_cursor(), psize);

    return (st);
}

ham_status_t 
cursor_overwrite(Cursor *cursor, ham_txn_t *txn, ham_record_t *record,
            ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor->get_db();

    /*
     * if we're in transactional mode then just append an "insert/OW" operation
     * to the txn-tree. 
     *
     * if the txn_cursor is already coupled to a txn-op, then we can use
     * txn_cursor_overwrite(). Otherwise we have to call db_insert_txn().
     *
     * If transactions are disabled then overwrite the item in the btree.
     */
    if (txn) {
        if (txn_cursor_is_nil(cursor->get_txn_cursor())
                && !(cursor_is_nil(cursor, 0))) {
            st=btree_cursor_uncouple(cursor->get_btree_cursor(), 0);
            if (st==0)
                st=db_insert_txn(db, txn, 
                    btree_cursor_get_uncoupled_key( cursor->get_btree_cursor()),
                    record, flags|HAM_OVERWRITE, 
                    cursor->get_txn_cursor());
        }
        else {
            st=txn_cursor_overwrite(cursor->get_txn_cursor(), record);
        }

        if (st==0)
            cursor_couple_to_txnop(cursor);
    }
    else {
        st=btree_cursor_overwrite(cursor->get_btree_cursor(), 
                        record, flags);
        if (st==0)
            cursor_couple_to_btree(cursor);
    }

    return (st);
}

void
cursor_close(Cursor *cursor)
{
    btree_cursor_close(cursor->get_btree_cursor());
    txn_cursor_close(cursor->get_txn_cursor());
    cursor->get_dupecache()->clear();
}

