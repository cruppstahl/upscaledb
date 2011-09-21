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
__cursor_move_next_dupe(Cursor *cursor, ham_u32_t flags)
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
__cursor_move_next_key(Cursor *cursor)
{
    ham_status_t st=0;
    int cmp=0; /* compare state; -1: btree < txn; +1: txn < btree; 0: equal */
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();
    ham_bool_t txnnil=txn_cursor_is_nil(txnc);
    ham_bool_t btrnil=cursor_is_nil(cursor, CURSOR_BTREE);

    /* txn cursor is nil, btree cursor is valid? then only move the btree
     * cursor */
    if (txnnil && !btrnil) {
        st=btree_cursor_move(btrc, 0, 0, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
        if (st==HAM_KEY_NOT_FOUND)
            cursor_set_to_nil(cursor, CURSOR_BTREE);
        if (st)
            goto bail;
        cursor_clear_dupecache(cursor);
        cmp=-1;
    }
    /* txn cursor is valid, btree cursor is nil? then only move the 
     * txn cursor */
    else if (!txnnil && btrnil) {
        do {
            st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
            if (st==HAM_KEY_NOT_FOUND)
                cursor_set_to_nil(cursor, CURSOR_TXN);
            if (st && st!=HAM_KEY_ERASED_IN_TXN)
                goto bail;
            if (st==HAM_KEY_ERASED_IN_TXN) {
                cursor_update_dupecache(cursor, CURSOR_BOTH);
                if (__cursor_has_duplicates(cursor)) {
                    st=0;
                    goto bail;
                }
            }
            cursor_clear_dupecache(cursor);
        } while (__txn_cursor_is_erase(txnc)); 
        cmp=+1;
    }
    /* if both cursors are valid: try to move any of them, but prefer the 
     * btree cursor (the btree index usually has more data and most likely
     * will therefore succeed). Then look for the smaller one */
    else {
        bool break_loop;
        /* TODO we might be able to cache this value and reduce
         * the number of calls to the compare function */
        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);

        if (cmp==0) {
            st=btree_cursor_move(btrc, 0, 0, 
                            HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
            if (st) {
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_BTREE);
                    cursor_couple_to_txnop(cursor);
                    return (__cursor_move_next_key(cursor));
                }
                else
                    return (st);
            }
            st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
            if (st) {
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_TXN);
                    cmp=-1; /* pretend that btree is smaller */
                    st=0;
                }
                if (st!=HAM_KEY_ERASED_IN_TXN)
                    goto bail;
            }
        }

        do {
            /* btree-cursor points to the smaller key? move next with
             * the btree cursor */
            if (cmp<0) {
                ham_bool_t erased;
                ham_bool_t overwritten;
                do {
                    erased=HAM_FALSE;
                    overwritten=HAM_FALSE;

                    st=btree_cursor_move(btrc, 0, 0, 
                                HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
                    if (st) {
                        if (st==HAM_KEY_NOT_FOUND) {
                            cursor_set_to_nil(cursor, CURSOR_BTREE);
                            if (!txn_cursor_is_nil(txnc)) {
                                st=0;
                                cmp=+1;
                            }
                        }
                        goto bail;
                    }
                    /* compare again; maybe the btree cursor is now larger
                     * than the txn-cursor */
                    if (!txn_cursor_is_nil(txnc)) {
                        (void)__compare_cursors(btrc, txnc, &cmp);
                        if (cmp>0)
                            break;
                    }

                    /* no, cursor order did not change. continue as usual */
                    cursor_clear_dupecache(cursor);
                    st=cursor_check_if_btree_key_is_erased_or_overwritten
                            (cursor);
                    if (st==HAM_KEY_ERASED_IN_TXN)
                        erased=HAM_TRUE;
                    else if (st==HAM_TXN_CONFLICT) {
                        cmp=0;
                        goto bail;
                    }
                    else if (st==0)
                        overwritten=HAM_TRUE;
                    st=0;
                    /* btree-key is erased: move txn-cursor to the next key */
                    if (erased) {
                        cursor_update_dupecache(cursor, CURSOR_BOTH);
                        if (__cursor_has_duplicates(cursor))
                            return (0);
                        if (HAM_KEY_NOT_FOUND==txn_cursor_move(txnc, 
                                HAM_CURSOR_NEXT))
                            cursor_set_to_nil(cursor, CURSOR_TXN);
                        cmp=-1;
                    }
                } while (erased && __cursor_has_duplicates(cursor)<=1);

                /* btree-key is overwritten: couple to txn */
                if (overwritten) {
                    cmp=+1;
                    goto bail;
                }
            }
            /* txn-cursor points to the smaller key? move next with
             * the txn-cursor */
            else if (cmp>0) {
                st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_TXN);
                    if (!cursor_is_nil(cursor, CURSOR_BTREE)) {
                        cmp=-1;
                        st=0;
                    }
                    goto bail;
                }
                cursor_clear_dupecache(cursor);
            }
            
            txnnil=txn_cursor_is_nil(txnc);
            btrnil=cursor_is_nil(cursor, CURSOR_BTREE);
            
            break_loop=true;
            if (!txnnil && !btrnil) {
                __compare_cursors(btrc, txnc, &cmp);
                if (cmp==0 && st==HAM_KEY_ERASED_IN_TXN)
                    st=0;
                else if (cmp==-1 && st==HAM_KEY_ERASED_IN_TXN)
                    st=0;
                else if ((cmp==+1 && st==HAM_KEY_ERASED_IN_TXN) ||
                         (cmp==+1 && __txn_cursor_is_erase(txnc))) {
                    cursor_update_dupecache(cursor, 
                            cmp==0 ? CURSOR_BOTH
                                   : (cmp < 0 ? CURSOR_BTREE
                                              : CURSOR_TXN));
                    if (!__cursor_has_duplicates(cursor))
                        break_loop=false;
                    else
                        st=0;
                }
            }
        
        /* if both keys are equal and the key was erased: continue; otherwise
         * return to caller */
        } while (break_loop==false);
    }

bail:
    if (st && st!=HAM_TXN_CONFLICT)
        return (st);

    ham_assert(st==HAM_TXN_CONFLICT ? cmp==0 : 1, (""));
    cursor_clear_dupecache(cursor);

    /* now finalize the results; either couple to one of the cursors or
     * move next, if both cursors are equal and the btree cursor is overwritten
     * in the txn-cursor */
    if (cmp<0) {
        /* btree is smaller - use it */
        cursor_couple_to_btree(cursor);
        st=cursor_update_dupecache(cursor, CURSOR_BTREE);
        if (st)
            return (st);
    }
    else if (cmp>0) {
        /* txn is smaller - use it */
        cursor_couple_to_txnop(cursor);
        st=cursor_update_dupecache(cursor, CURSOR_TXN);
        if (st)
            return (st);
    }
    else {
        ham_db_t *db=cursor->get_db();
        if ((st!=HAM_TXN_CONFLICT) && 
                ((cursor_get_dupecache_count(cursor)>1) ||
                    ((cursor_get_dupecache_count(cursor)==1) &&
                        (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)))) {
            cursor_couple_to_txnop(cursor);
            return (cursor_update_dupecache(cursor, CURSOR_TXN));
        }
        if (st==HAM_TXN_CONFLICT || __txn_cursor_is_erase(txnc)) {
            /* move btree and txn to next */
            st=txn_cursor_move(txnc, HAM_CURSOR_NEXT);
            if (st==HAM_KEY_NOT_FOUND)
                cursor_set_to_nil(cursor, CURSOR_TXN);
            else if (st && st!=HAM_TXN_CONFLICT && st!=HAM_KEY_ERASED_IN_TXN)
                return (st);
            st=__cursor_move_next_key(cursor);
            return (st);
        }
        else { 
            /* overwritten - couple to txn */
            cursor_couple_to_txnop(cursor);
            return (cursor_update_dupecache(cursor, CURSOR_TXN|CURSOR_BTREE));
        }
    }

    return (0);
}

static ham_status_t
__cursor_move_previous_key(Cursor *cursor)
{
    ham_status_t st=0;
    int cmp=0; /* compare state; -1: btree < txn; +1: txn < btree; 0: equal */
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();
    ham_bool_t txnnil=txn_cursor_is_nil(txnc);
    ham_bool_t btrnil=cursor_is_nil(cursor, CURSOR_BTREE);

    /* txn cursor is nil, btree cursor is valid? then only move the btree
     * cursor */
    if (txnnil && !btrnil) {
        st=btree_cursor_move(btrc, 0, 0, 
                        HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
        if (st==HAM_KEY_NOT_FOUND)
            cursor_set_to_nil(cursor, CURSOR_BTREE);
        if (st)
            goto bail;
        cursor_clear_dupecache(cursor);
        cmp=+1;
    }
    /* txn cursor is valid, btree cursor is nil? then only move the 
     * txn cursor */
    else if (!txnnil && btrnil) {
        do {
            st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
            if (st==HAM_KEY_NOT_FOUND)
                cursor_set_to_nil(cursor, CURSOR_TXN);
            if (st && st!=HAM_KEY_ERASED_IN_TXN)
                goto bail;
            if (st==HAM_KEY_ERASED_IN_TXN) {
                cursor_update_dupecache(cursor, CURSOR_BOTH);
                if (__cursor_has_duplicates(cursor)) {
                    st=0;
                    goto bail;
                }
            }
            cursor_clear_dupecache(cursor);
        } while (__txn_cursor_is_erase(txnc)); 
        cmp=-1;
    }
    /* if both cursors are valid: try to move any of them, but prefer the 
     * btree cursor (the btree index usually has more data and most likely
     * will therefore succeed). Then look for the larger one */
    else {
        bool break_loop;
        /* TODO we might be able to cache this value and reduce
         * the number of calls to the compare function */
        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);

        if (cmp==0) {
            st=btree_cursor_move(btrc, 0, 0, 
                        HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
            if (st) {
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_BTREE);
                    cursor_couple_to_txnop(cursor);
                    return (__cursor_move_previous_key(cursor));
                }
                else
                    return (st);
            }
            st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
            if (st) {
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_TXN);
                    cmp=+1; /* pretend that btree is larger */
                    st=0;
                }
                if (st!=HAM_KEY_ERASED_IN_TXN)
                    goto bail;
                goto bail;
            }
        }

        do {
            /* btree-cursor points to the larger key? move previous with
             * the btree cursor */
            if (cmp>0) {
                ham_bool_t erased;
                ham_bool_t overwritten;
                do {
                    erased=HAM_FALSE;
                    overwritten=HAM_FALSE;

                    st=btree_cursor_move(btrc, 0, 0, 
                                HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
                    if (st) {
                        if (st==HAM_KEY_NOT_FOUND) {
                            cursor_set_to_nil(cursor, CURSOR_BTREE);
                            if (!txn_cursor_is_nil(txnc)) {
                                st=0;
                                cmp=-1;
                            }
                        }
                        goto bail;
                    }
                    cursor_clear_dupecache(cursor);
                    st=cursor_check_if_btree_key_is_erased_or_overwritten
                            (cursor);
                    if (st==HAM_KEY_ERASED_IN_TXN)
                        erased=HAM_TRUE;
                    else if (st==HAM_TXN_CONFLICT) {
                        cmp=0;
                        goto bail;
                    }
                    else if (st==0)
                        overwritten=HAM_TRUE;
                    st=0;
                    /* btree-key is erased: move txn-cursor to the next key,
                     * unless there are duplicates */
                    if (erased) {
                        cursor_update_dupecache(cursor, CURSOR_BOTH);
                        if (__cursor_has_duplicates(cursor))
                            return (0);
                        if (HAM_KEY_NOT_FOUND==txn_cursor_move(txnc, 
                                HAM_CURSOR_NEXT)) {
                            cursor_set_to_nil(cursor, CURSOR_TXN);
                            cmp=+1;
                        }
                    }
                } while (erased && __cursor_has_duplicates(cursor)<=1);

                /* btree-key is overwritten: couple to txn */
                if (overwritten) {
                    cmp=-1;
                    goto bail;
                }
            }
            /* txn-cursor points to the greater key? move previous with
             * the txn-cursor */
            else if (cmp<0) {
                st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
                if (st==HAM_KEY_NOT_FOUND) {
                    cursor_set_to_nil(cursor, CURSOR_TXN);
                    if (!cursor_is_nil(cursor, CURSOR_BTREE)) {
                        cmp=+1;
                        st=0;
                    }
                    goto bail;
                }
                cursor_clear_dupecache(cursor);
            }

            txnnil=txn_cursor_is_nil(txnc);
            btrnil=cursor_is_nil(cursor, CURSOR_BTREE);
            
            break_loop=true;
            if (!txnnil && !btrnil) {
                __compare_cursors(btrc, txnc, &cmp);
                if (cmp==0 && st==HAM_KEY_ERASED_IN_TXN)
                    st=0;
                else if (cmp==+1 && st==HAM_KEY_ERASED_IN_TXN)
                    st=0;
                else if ((cmp==-1 && st==HAM_KEY_ERASED_IN_TXN) ||
                         (cmp==-1 && __txn_cursor_is_erase(txnc))) {
                    cursor_update_dupecache(cursor, 
                            cmp==0 ? CURSOR_BOTH
                                   : (cmp < 0 ? CURSOR_BTREE
                                              : CURSOR_TXN));
                    if (!__cursor_has_duplicates(cursor))
                        break_loop=false;
                    else
                        st=0;
                }
            }

        } while (break_loop==false);
    }

bail:
    if (st && st!=HAM_TXN_CONFLICT)
        return (st);

    ham_assert(st==HAM_TXN_CONFLICT ? cmp==0 : 1, (""));
    cursor_clear_dupecache(cursor);

    /* now finalize the results; either couple to one of the cursors or
     * move next, if both cursors are equal and the btree cursor is overwritten
     * in the txn-cursor */
    if (cmp>0) {
        /* btree is greater - use it */
        cursor_couple_to_btree(cursor);
        st=cursor_update_dupecache(cursor, CURSOR_BTREE);
        if (st)
            return (st);
    }
    else if (cmp<0) {
        /* txn is greater - use it */
        cursor_couple_to_txnop(cursor);
        st=cursor_update_dupecache(cursor, CURSOR_TXN);
        if (st)
            return (st);
    }
    else {
        ham_db_t *db=cursor->get_db();
        if ((st!=HAM_TXN_CONFLICT) && 
                ((cursor_get_dupecache_count(cursor)>1) ||
                    ((cursor_get_dupecache_count(cursor)==1) &&
                        (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)))) {
            cursor_couple_to_txnop(cursor);
            return (cursor_update_dupecache(cursor, CURSOR_TXN));
        }
        if (st==HAM_TXN_CONFLICT || __txn_cursor_is_erase(txnc)) {
            /* move btree and txn to previous */
            st=txn_cursor_move(txnc, HAM_CURSOR_PREVIOUS);
            if (st==HAM_KEY_NOT_FOUND)
                cursor_set_to_nil(cursor, CURSOR_TXN);
            else if (st && st!=HAM_TXN_CONFLICT && st!=HAM_KEY_ERASED_IN_TXN)
                return (st);
            st=__cursor_move_previous_key(cursor);
            return (st);
        }
        else { 
            /* overwritten - couple to txn */
            cursor_couple_to_txnop(cursor);
            return (cursor_update_dupecache(cursor, CURSOR_TXN|CURSOR_BTREE));
        }
    }

    return (0);
}

static ham_status_t
__cursor_move_first_key(Cursor *cursor)
{
    ham_status_t st=0, btrs, txns;
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    /* fetch the smallest/first key from the transaction tree. */
    txns=txn_cursor_move(txnc, HAM_CURSOR_FIRST);
    /* fetch the smallest/first key from the btree tree. */
    btrs=btree_cursor_move(btrc, 0, 0, HAM_CURSOR_FIRST|HAM_SKIP_DUPLICATES);
    /* now consolidate - if both trees are empty then return */
    if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) {
        return (HAM_KEY_NOT_FOUND);
    }
    /* if btree is empty but txn-tree is not: couple to txn */
    else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (st);
    }
    /* if txn-tree is empty but btree is not: couple to btree */
    else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (st);
    }
    /* btree-key is empty, txn was erased: move next in txn-tree (unless
     * duplicates are available) */
    else if (txns==HAM_KEY_ERASED_IN_TXN && btrs==HAM_KEY_NOT_FOUND) {
        cursor_set_to_nil(cursor, CURSOR_BTREE);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        if (__cursor_has_duplicates(cursor))
            return (0);
        return (__cursor_move_next_key(cursor));
    }
    /* if both trees are not empty then pick the smaller key, but make sure
     * that it was not erased in another transaction.
     *
     * !!
     * if the key has duplicates which were erased then return - dupes
     * are handled by the caller 
     *
     * !!
     * if both keys are equal: make sure that the btree key was not
     * erased in the transaction; otherwise couple to the txn-op
     * (it's chronologically newer and has faster access) 
     */
    else if (btrs==0 
            && (txns==0 
                || txns==HAM_KEY_ERASED_IN_TXN
                || txns==HAM_TXN_CONFLICT)) {
        int cmp;

        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);

        /* both keys are equal */
        if (cmp==0) {
            cursor_couple_to_txnop(cursor);

            /* we have duplicates? then return and let the caller
             * figure out which one to pick */
            cursor_update_dupecache(cursor, CURSOR_BOTH);
            if (__cursor_has_duplicates(cursor))
                return (0);
            /* txn has overwritten the btree key? */
            if (txns==HAM_SUCCESS && btrs==HAM_SUCCESS) {
                cursor_update_dupecache(cursor, CURSOR_BOTH);
                return (0);
            }
            /* otherwise (we do not have duplicates) */
            if (txns==HAM_KEY_ERASED_IN_TXN) {
                /* if this btree key was erased or overwritten then couple
                 * to the txn, but already move the btree cursor to the
                 * next item */
                (void)btree_cursor_move(btrc, 0, 0, 
                            HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES);
                /* if the key was erased: continue moving "next" till 
                 * we find a key or reach the end of the database */
                st=__cursor_move_next_key(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    cursor_set_to_nil(cursor, 0);
                    return (HAM_KEY_NOT_FOUND);
                }
                return (st);
            }
            if (txns==HAM_TXN_CONFLICT) {
                return (txns);
            }
            /* if the btree entry was overwritten in the txn: move the
             * btree entry to the next key */
            if (txns==HAM_SUCCESS) {
                st=__cursor_move_next_key(cursor);
                return (st);
            }
            else 
                return (txns ? txns : btrs);
        }
        else if (cmp<1) {
            /* couple to btree */
            cursor_couple_to_btree(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE);
            return (0);
        }
        else {
            if (txns==HAM_TXN_CONFLICT)
                return (txns);
            /* couple to txn */
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_TXN);
            return (0);
        }
    }

    /* every other error code is returned to the caller */
    if ((btrs==HAM_KEY_NOT_FOUND) && (txns==HAM_KEY_ERASED_IN_TXN))
        cursor_update_dupecache(cursor, CURSOR_TXN); /* TODO required? */
    return (txns ? txns : btrs);
}

static ham_status_t
__cursor_move_last_key(Cursor *cursor)
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
    else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
        cursor_couple_to_txnop(cursor);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        return (st);
    }
    /* if txn-tree is empty but btree is not: couple to btree */
    else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
        cursor_couple_to_btree(cursor);
        cursor_update_dupecache(cursor, CURSOR_BTREE);
        return (st);
    }
    /* btree-key is empty, txn was erased: move previous in txn-tree (unless
     * duplicates are available) */
    else if (txns==HAM_KEY_ERASED_IN_TXN && btrs==HAM_KEY_NOT_FOUND) {
        cursor_set_to_nil(cursor, CURSOR_BTREE);
        cursor_update_dupecache(cursor, CURSOR_TXN);
        if (__cursor_has_duplicates(cursor))
            return (0);
        return (__cursor_move_previous_key(cursor));
    }
    /* if both trees are not empty then pick the larger key, but make sure
     * that it was not erased in another transaction.
     *
     * !!
     * if the key has duplicates which were erased then return - dupes
     * are handled by the caller 
     *
     * !!
     * if both keys are equal: make sure that the btree key was not
     * erased in the transaction; otherwise couple to the txn-op
     * (it's chronologically newer and has faster access) 
     */
    else if (btrs==0 
            && (txns==0 
                || txns==HAM_KEY_ERASED_IN_TXN
                || txns==HAM_TXN_CONFLICT)) {
        int cmp;

        st=__compare_cursors(btrc, txnc, &cmp);
        if (st)
            return (st);

        /* both keys are equal */
        if (cmp==0) {
            cursor_couple_to_txnop(cursor);

            /* we have duplicates? then return and let the caller
             * figure out which one to pick */
            cursor_update_dupecache(cursor, CURSOR_BOTH);
            if (__cursor_has_duplicates(cursor))
                return (0);
            /* otherwise (we do not have duplicates) */
            if (txns==HAM_KEY_ERASED_IN_TXN) {
                /* if this btree key was erased or overwritten then couple
                 * to the txn, but already move the btree cursor to the
                 * previous item */
                (void)btree_cursor_move(btrc, 0, 0, 
                            HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
                /* if the key was erased: continue moving "previous" till 
                 * we find a key or reach the end of the database */
                st=__cursor_move_previous_key(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    cursor_set_to_nil(cursor, 0);
                    return (HAM_KEY_NOT_FOUND);
                }
                return (st);
            }
            if (txns==HAM_TXN_CONFLICT) {
                return (txns);
            }
            /* if the btree entry was overwritten in the txn: move the
             * btree entry to the previous key */
            if (txns==HAM_SUCCESS) {
                st=__cursor_move_previous_key(cursor);
                return (st);
            }
            else 
                return (txns ? txns : btrs);
        }
        else if (cmp<1) {
            if (txns==HAM_TXN_CONFLICT)
                return (txns);
            /* couple to txn */
            cursor_couple_to_txnop(cursor);
            cursor_update_dupecache(cursor, CURSOR_TXN);
            return (0);
        }
        else {
            /* couple to btree */
            cursor_couple_to_btree(cursor);
            cursor_update_dupecache(cursor, CURSOR_BTREE);
            return (0);
        }
    }

    /* every other error code is returned to the caller */
    if ((btrs==HAM_KEY_NOT_FOUND) && (txns==HAM_KEY_ERASED_IN_TXN))
        cursor_update_dupecache(cursor, CURSOR_TXN); /* TODO required? */
    return (txns ? txns : btrs);
}

ham_status_t
cursor_move(Cursor *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    ham_status_t st=0;
    ham_bool_t changed_dir=HAM_FALSE;
    ham_bool_t skip_duplicates=HAM_FALSE;
    ham_db_t *db=cursor->get_db();
    txn_cursor_t *txnc=cursor->get_txn_cursor();
    btree_cursor_t *btrc=cursor->get_btree_cursor();

    if (!(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)
            || (flags&HAM_SKIP_DUPLICATES))
        skip_duplicates=HAM_TRUE;

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
    }

    /* should we move through the duplicate list? */
    if (!skip_duplicates
            && !(flags&HAM_CURSOR_FIRST) 
            && !(flags&HAM_CURSOR_LAST)) {
        if (flags&HAM_CURSOR_NEXT)
            st=__cursor_move_next_dupe(cursor, flags);
        else if (flags&HAM_CURSOR_PREVIOUS)
            st=__cursor_move_previous_dupe(cursor, flags);
        if (st==0)
            goto retrieve_key_and_record;
        if (st!=HAM_LIMITS_REACHED)
            return (st);
    }

    /* we have either skipped duplicates or reached the end of the duplicate
     * list. btree cursor and txn cursor are synced and relative close to 
     * each other. Move the cursor in the requested direction. */
    cursor_clear_dupecache(cursor);
    if (flags&HAM_CURSOR_NEXT)
        st=__cursor_move_next_key(cursor);
    else if (flags&HAM_CURSOR_PREVIOUS)
        st=__cursor_move_previous_key(cursor);
    else if (flags&HAM_CURSOR_FIRST)
        st=__cursor_move_first_key(cursor);
    else {
        ham_assert(flags&HAM_CURSOR_LAST, (""));
        st=__cursor_move_last_key(cursor);
    }
    if (st)
        return (st);

    /* if ALL duplicates were erased: move next or previous */
    if ((db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) && 
            !__cursor_has_duplicates(cursor)) {
        goto move_next_or_previous;
    }

    /* now move once more through the duplicate list, if required. Since this
     * key is "fresh" and we have not yet returned any cursor we will start
     * at the beginning or at the end of the duplicate list. */
    if (__cursor_has_duplicates(cursor)) {
        if ((flags&HAM_CURSOR_NEXT) || (flags&HAM_CURSOR_FIRST))
            st=__cursor_move_first_dupe(cursor, flags);
        else {
            ham_assert((flags&HAM_CURSOR_LAST) || (flags&HAM_CURSOR_PREVIOUS), 
                            (""));
            st=__cursor_move_last_dupe(cursor, flags);
        }
        /* all duplicates were erased in a transaction? then move forward
         * or backwards */
        if (st==HAM_LIMITS_REACHED) {
move_next_or_previous:
            if (flags&HAM_CURSOR_FIRST) {
                flags&=~HAM_CURSOR_FIRST;
                flags|=HAM_CURSOR_NEXT;
            }
            else if (flags&HAM_CURSOR_LAST) {
                flags&=~HAM_CURSOR_LAST;
                flags|=HAM_CURSOR_PREVIOUS;
            }
            return (cursor_move(cursor, key, record, flags));
        }
        else if (st)
            return (st);
    }

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

