/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * btree searching
 *
 */

#include "config.h"

#include <string.h>

#include "btree.h"
#include "cursor.h"
#include "btree_cursor.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "util.h"
#include "btree_node.h"

using namespace ham;


ham_status_t
BtreeBackend::do_find(Transaction *txn, Cursor *hcursor, ham_key_t *key,
           ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Page *page = NULL;
    BtreeNode *node = NULL;
    BtreeKey *entry;
    ham_s32_t idx = -1;
    Database *db=get_db();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    BtreeStatistics::FindHints hints;
    hints = get_statistics()->get_find_hints(key, flags);
    btree_cursor_t *cursor=(btree_cursor_t *)hcursor;

    if (hints.key_is_out_of_bounds) {
        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                        hints.try_fast_track);
        return (HAM_KEY_NOT_FOUND);
    }

    if (hints.try_fast_track) {
        /*
         * see if we get a sure hit within this btree leaf; if not, revert to
         * regular scan
         *
         * As this is a speed-improvement hint re-using recent material, the
         * page should still sit in the cache, or we're using old info, which
         * should be discarded.
         */
        st = db_fetch_page(&page, db, hints.leaf_page_addr, DB_ONLY_FROM_CACHE);
        if (st)
            return st;
        if (page) {
            node=BtreeNode::from_page(page);
            ham_assert(node->is_leaf());

            /* we need at least 3 keys in the node: edges + middle match */
            if (node->get_count() < 3)
                goto no_fast_track;

            idx = find_leaf(page, key, hints.flags);
            /*
             * if we didn't hit a match OR a match at either edge, FAIL.
             * A match at one of the edges is very risky, as this can also
             * signal a match far away from the current node, so we need
             * the full tree traversal then.
             */
            if (idx <= 0 || idx >= node->get_count() - 1) {
                idx = -1;
            }
            /*
             * else: we landed in the middle of the node, so we don't need to
             * traverse the entire tree now.
             */
        }

        /* Reset any errors which may have been collected during the hinting
         * phase -- this is done by setting 'idx = -1' above as that effectively
         * clears the possible error code stored in there when (idx < -1)
         */
    }

no_fast_track:

    if (idx == -1) {
        /* get the address of the root page */
        if (!get_rootpage()) {
            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
            return HAM_KEY_NOT_FOUND;
        }

        /* load the root page */
        st=db_fetch_page(&page, db, get_rootpage(), 0);
        if (st) {
            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
            return (st);
        }

        /* now traverse the root to the leaf nodes, till we find a leaf */
        node=BtreeNode::from_page(page);
        if (!node->is_leaf()) {
            /* signal 'don't care' when we have multiple pages; we resolve
               this once we've got a hit further down */
            if (hints.flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
                hints.flags |= (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH);

            for (;;) {
                st=find_internal(page, key, &page);
                if (!page) {
                    get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                                    hints.try_fast_track);
                    return st ? st : HAM_KEY_NOT_FOUND;
                }

                node=BtreeNode::from_page(page);
                if (node->is_leaf())
                    break;
            }
        }

        /* check the leaf page for the key */
        idx=find_leaf(page, key, hints.flags);
        if (idx < -1) {
            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
            return (ham_status_t)idx;
        }
    }  /* end of regular search */

    /*
     * When we are performing an approximate match, the worst case
     * scenario is where we've picked the wrong side of the fence
     * while sitting at a page/node boundary: that's what this
     * next piece of code resolves:
     *
     * essentially it moves one record forwards or backward when
     * the flags tell us this is mandatory and we're not yet in the proper
     * position yet.
     *
     * The whole trick works, because the code above detects when
     * we need to traverse a multi-page btree -- where this worst-case
     * scenario can happen -- and adjusted the flags to accept
     * both LT and GT approximate matches so that find_leaf()
     * will be hard pressed to return a 'key not found' signal (idx==-1),
     * instead delivering the nearest LT or GT match; all we need to
     * do now is ensure we've got the right one and if not,
     * shift by one.
     */
    if (idx >= 0) {
        if ((ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE)
            && (hints.original_flags
                    & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
                != (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) {
            if ((ham_key_get_intflags(key) & BtreeKey::KEY_IS_GT)
                && (hints.original_flags & HAM_FIND_LT_MATCH)) {
                /*
                 * if the index-1 is still in the page, just decrement the
                 * index
                 */
                if (idx > 0) {
                    idx--;
                }
                else {
                    /*
                     * otherwise load the left sibling page
                     */
                    if (!node->get_left()) {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        ham_assert(node == BtreeNode::from_page(page));
                        get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                                    page, key, hints.original_flags, -1);
                        return HAM_KEY_NOT_FOUND;
                    }

                    st=db_fetch_page(&page, db, node->get_left(), 0);
                    if (st) {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        return (st);
                    }
                    node = BtreeNode::from_page(page);
                    idx = node->get_count() - 1;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
            }
            else if ((ham_key_get_intflags(key) & BtreeKey::KEY_IS_LT)
                    && (hints.original_flags & HAM_FIND_GT_MATCH)) {
                /*
                 * if the index+1 is still in the page, just increment the
                 * index
                 */
                if (idx + 1 < node->get_count()) {
                    idx++;
                }
                else {
                    /*
                     * otherwise load the right sibling page
                     */
                    if (!node->get_right())
                    {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        ham_assert(node == BtreeNode::from_page(page));
                        get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                                page, key, hints.original_flags, -1);
                        return HAM_KEY_NOT_FOUND;
                    }

                    st=db_fetch_page(&page, db, node->get_right(), 0);
                    if (st) {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        return (st);
                    }
                    node = BtreeNode::from_page(page);
                    idx = 0;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);
            }
        }
        else if (!(ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE)
                && !(hints.original_flags & HAM_FIND_EXACT_MATCH)
                && (hints.original_flags != 0)) {
            /*
             * 'true GT/LT' has been added @ 2009/07/18 to complete
             * the EQ/LEQ/GEQ/LT/GT functionality;
             *
             * 'true LT/GT' is simply an extension upon the already existing
             * LEQ/GEQ logic just above; all we do here is move one record
             * up/down as it just happens that we get an exact ('equal')
             * match here.
             *
             * The fact that the LT/GT constants share their bits with the
             * LEQ/GEQ flags so that LEQ==(LT|EXACT) and GEQ==(GT|EXACT)
             * ensures that we can restrict our work to a simple adjustment
             * right here; everything else has already been taken of by the
             * LEQ/GEQ logic in the section above when the key has been
             * flagged with the KEY_IS_APPROXIMATE flag.
             */
            if (hints.original_flags & HAM_FIND_LT_MATCH) {
                /*
                 * if the index-1 is still in the page, just decrement the
                 * index
                 */
                if (idx > 0) {
                    idx--;

                    ham_key_set_intflags(key, (ham_key_get_intflags(key)
                            & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
                }
                else {
                    /* otherwise load the left sibling page */
                    if (!node->get_left()) {
                        /* when an error is otherwise unavoidable, see if
                           we have an escape route through GT? */
                        if (hints.original_flags & HAM_FIND_GT_MATCH) {
                            /*
                             * if the index+1 is still in the page, just
                             * increment the index
                             */
                            if (idx + 1 < node->get_count())
                                idx++;
                            else {
                                /* otherwise load the right sibling page */
                                if (!node->get_right()) {
                                    get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                                        hints.try_fast_track);
                                    ham_assert(node==BtreeNode::from_page(page));
                                    get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                                            page, key, hints.original_flags, -1);
                                    return HAM_KEY_NOT_FOUND;
                                }

                                st=db_fetch_page(&page, db,
                                                node->get_right(), 0);
                                if (st) {
                                    get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                                        hints.try_fast_track);
                                    return (st);
                                }
                                node = BtreeNode::from_page(page);
                                idx = 0;
                            }
                            ham_key_set_intflags(key, (ham_key_get_intflags(key) &
                                            ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);
                        }
                        else {
                            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                                hints.try_fast_track);
                            ham_assert(node == BtreeNode::from_page(page));
                            get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                                    page, key, hints.original_flags, -1);
                            return HAM_KEY_NOT_FOUND;
                        }
                    }
                    else {
                        st=db_fetch_page(&page, db,
                                        node->get_left(), 0);
                        if (st) {
                            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                                hints.try_fast_track);
                            return (st);
                        }
                        node=BtreeNode::from_page(page);
                        idx=node->get_count()-1;

                        ham_key_set_intflags(key, (ham_key_get_intflags(key)
                                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
                    }
                }
            }
            else if (hints.original_flags&HAM_FIND_GT_MATCH) {
                /*
                 * if the index+1 is still in the page, just increment the
                 * index
                 */
                if (idx + 1 < node->get_count())
                    idx++;
                else {
                    /* otherwise load the right sibling page */
                    if (!node->get_right()) {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        ham_assert(node == BtreeNode::from_page(page));
                        get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                                page, key, hints.original_flags, -1);
                        return (HAM_KEY_NOT_FOUND);
                    }

                    st=db_fetch_page(&page, db,
                                node->get_right(), 0);
                    if (st) {
                        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                            hints.try_fast_track);
                        return (st);
                    }
                    node=BtreeNode::from_page(page);
                    idx=0;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key)
                                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);
            }
        }
    }

    if (idx<0) {
        get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                        hints.try_fast_track);
        ham_assert(node);
        ham_assert(page);
        ham_assert(node == BtreeNode::from_page(page));
        get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
                page, key, hints.original_flags, -1);
        return (HAM_KEY_NOT_FOUND);
    }

    /* load the entry, and store record ID and key flags */
    entry=node->get_key(db, idx);

    /* set the cursor-position to this key */
    if (cursor) {
        ham_assert(!btree_cursor_is_uncoupled(cursor));
        ham_assert(!btree_cursor_is_coupled(cursor));
        page->add_cursor(btree_cursor_get_parent(cursor));
        btree_cursor_set_flags(cursor,
                btree_cursor_get_flags(cursor)|BTREE_CURSOR_FLAG_COUPLED);
        btree_cursor_set_coupled_page(cursor, page);
        btree_cursor_set_coupled_index(cursor, idx);
    }

    /*
     * during read_key() and read_record() new pages might be needed,
     * and the page at which we're pointing could be moved out of memory;
     * that would mean that the cursor would be uncoupled, and we're losing
     * the 'entry'-pointer. therefore we 'lock' the page by incrementing
     * the reference counter
     */
    ham_assert(node->is_leaf());

    /* no need to load the key if we have an exact match, or if KEY_DONT_LOAD
     * is set: */
    if (key
            && (ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE)
            && !(flags & Cursor::CURSOR_SYNC_DONT_LOAD_KEY)) {
        ham_status_t st=be->read_key(txn, entry, key);
        if (st) {
            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                        hints.try_fast_track);
            return (st);
        }
    }

    if (record) {
        ham_status_t st;
        record->_intflags=entry->get_flags();
        record->_rid=entry->get_ptr();
        st=db->get_backend()->read_record(txn, record,
                        entry->get_rawptr(), flags);
        if (st) {
            get_statistics()->update_failed_oob(HAM_OPERATION_STATS_FIND,
                        hints.try_fast_track);
            return (st);
        }
    }

    ham_assert(node == BtreeNode::from_page(page));
    // TODO merge these two calls
    get_statistics()->update_succeeded(HAM_OPERATION_STATS_FIND,
            page, hints.try_fast_track);
    get_statistics()->update_any_bound(HAM_OPERATION_STATS_FIND,
            page, key, hints.original_flags, idx);

    return (0);
}


