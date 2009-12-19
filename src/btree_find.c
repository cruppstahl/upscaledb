/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include "db.h"
#include "error.h"
#include "btree.h"
#include "btree_cursor.h"
#include "keys.h"
#include "util.h" /* [i_a] */
#include "statistics.h"

ham_status_t 
btree_find_cursor(ham_btree_t *be, ham_bt_cursor_t *cursor, 
           ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_page_t *page = NULL;
    btree_node_t *node = NULL;
    int_key_t *entry;
    ham_s32_t idx = -1;
    ham_db_t *db=btree_get_db(be);
    find_hints_t hints = {flags, flags, 0, HAM_FALSE, HAM_FALSE, 1};

    db_set_error(db, 0);

    btree_find_get_hints(&hints, db, key);

    if (hints.key_is_out_of_bounds) {
        stats_update_find_fail_oob(db, &hints);
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
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
        page = db_fetch_page(db, hints.leaf_page_addr, DB_ONLY_FROM_CACHE);
        if (page) {
            node=ham_page_get_btree_node(page);
            ham_assert(btree_node_is_leaf(node), (0));

            idx = btree_node_search_by_key(db, page, key, hints.flags);
            /* 
             * if we didn't hit a match OR a match at either edge, FAIL.
             * A match at one of the edges is very risky, as this can also 
             * signal a match far away from the current node, so we need 
             * the full tree traversal then.
             */
            if (idx <= 0 || idx >= btree_node_get_count(node) - 1) {
                idx = -1;
            }
            /*
             * else: we landed in the middle of the node, so we don't need to
             * traverse the entire tree now.
             */
        }

        /* reset any errors which may have been collected during the hinting 
         * phase */
        db_set_error(db, 0);
    }

    if (idx == -1) {
        /* get the address of the root page */
        if (!btree_get_rootpage(be)) {
            stats_update_find_fail(db, &hints);
            return (db_set_error(db, HAM_KEY_NOT_FOUND));
        }

        /* load the root page */
        page=db_fetch_page(db, btree_get_rootpage(be), 0);
        if (!page) {
            ham_assert(db_get_error(db), (0));
            stats_update_find_fail(db, &hints);
            return (db_get_error(db));
        }

        /* now traverse the root to the leaf nodes, till we find a leaf */
        node=ham_page_get_btree_node(page);
        if (!btree_node_is_leaf(node)) {
            /* signal 'don't care' when we have multiple pages; we resolve 
               this once we've got a hit further down */
            if (hints.flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) 
                hints.flags |= (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH);

            for (;;) {
                hints.cost++;
                page=btree_traverse_tree(db, page, key, 0);
                if (!page) {
                    if (!db_get_error(db))
                        db_set_error(db, HAM_KEY_NOT_FOUND);
                    stats_update_find_fail(db, &hints);
                    return (db_get_error(db));
                }

                node=ham_page_get_btree_node(page);
                if (btree_node_is_leaf(node))
                    break;
            }
        }

        /* check the leaf page for the key */
        idx=btree_node_search_by_key(db, page, key, hints.flags);
        if (db_get_error(db)) {
            stats_update_find_fail(db, &hints);
            return (db_get_error(db));
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
     * both LT and GT approximate matches so that btree_node_search_by_key()
     * will be hard pressed to return a 'key not found' signal (idx==-1),
     * instead delivering the nearest LT or GT match; all we need to
     * do now is ensure we've got the right one and if not, 
     * shift by one.
     */
    if (idx >= 0) {
        if ((ham_key_get_intflags(key) & KEY_IS_APPROXIMATE) 
            && (hints.original_flags 
                    & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) 
                != (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) {
            if ((ham_key_get_intflags(key) & KEY_IS_GT) 
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
                    if (!btree_node_get_left(node)) {
                        stats_update_find_fail(db, &hints);
                        ham_assert(node == ham_page_get_btree_node(page), (0));
                        stats_update_any_bound(db, page, key, hints.original_flags, -1);
                        return db_set_error(db, HAM_KEY_NOT_FOUND);
                    }

                    hints.cost++;
                    page = db_fetch_page(db, btree_node_get_left(node), 0);
                    if (!page) {
                        ham_assert(db_get_error(db), (0));
                        stats_update_find_fail(db, &hints);
                        return (db_get_error(db));
                    }
                    node = ham_page_get_btree_node(page);
                    idx = btree_node_get_count(node) - 1;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key) 
                        & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
            }
            else if ((ham_key_get_intflags(key) & KEY_IS_LT) 
                    && (hints.original_flags & HAM_FIND_GT_MATCH)) {
                /*
                 * if the index+1 is still in the page, just increment the
                 * index
                 */
                if (idx + 1 < btree_node_get_count(node)) {
                    idx++;
                }
                else {
                    /*
                     * otherwise load the right sibling page
                     */
                    if (!btree_node_get_right(node))
                    {
                        stats_update_find_fail(db, &hints);
                        ham_assert(node == ham_page_get_btree_node(page), (0));
                        stats_update_any_bound(db, page, key, hints.original_flags, -1);
                        return db_set_error(db, HAM_KEY_NOT_FOUND);
                    }

                    hints.cost++;
                    page = db_fetch_page(db, btree_node_get_right(node), 0);
                    if (!page) {
                        ham_assert(db_get_error(db), (0));
                        stats_update_find_fail(db, &hints);
                        return (db_get_error(db));
                    }
                    node = ham_page_get_btree_node(page);
                    idx = 0;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key) 
                        & ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
            }
        }
        else if (!(ham_key_get_intflags(key) & KEY_IS_APPROXIMATE) 
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
            if (hints.original_flags & HAM_FIND_LT_MATCH)
            {
                /*
                 * if the index-1 is still in the page, just decrement the
                 * index
                 */
                if (idx > 0)
                {
                    idx--;

                    ham_key_set_intflags(key, (ham_key_get_intflags(key) 
                            & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
                }
                else
                {
                    /*
                     * otherwise load the left sibling page
                     */
                    if (!btree_node_get_left(node))
                    {
                        /* when an error is otherwise unavoidable, see if 
                           we have an escape route through GT? */

                        if (hints.original_flags & HAM_FIND_GT_MATCH)
                        {
                            /*
                             * if the index+1 is still in the page, just 
                             * increment the index
                             */
                            if (idx + 1 < btree_node_get_count(node))
                            {
                                idx++;
                            }
                            else
                            {
                                /*
                                 * otherwise load the right sibling page
                                 */
                                if (!btree_node_get_right(node))
                                {
                                    stats_update_find_fail(db, &hints);
                                    ham_assert(node == ham_page_get_btree_node(page), (0));
                                    stats_update_any_bound(db, page, key, hints.original_flags, -1);
                                    return db_set_error(db, HAM_KEY_NOT_FOUND);
                                }

                                hints.cost++;
                                page = db_fetch_page(db, btree_node_get_right(node), 0);
                                if (!page)
                                {
                                    ham_assert(db_get_error(db), (0));
                                    stats_update_find_fail(db, &hints);
                                    return (db_get_error(db));
                                }
                                node = ham_page_get_btree_node(page);
                                idx = 0;
                            }
                            ham_key_set_intflags(key, (ham_key_get_intflags(key) & 
                                            ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
                        }
                        else
                        {
                            stats_update_find_fail(db, &hints);
                            ham_assert(node == ham_page_get_btree_node(page), (0));
                            stats_update_any_bound(db, page, key, hints.original_flags, -1);
                            return db_set_error(db, HAM_KEY_NOT_FOUND);
                        }
                    }
                    else
                    {
                        hints.cost++;
                        page = db_fetch_page(db, btree_node_get_left(node), 0);
                        if (!page)
                        {
                            ham_assert(db_get_error(db), (0));
                            stats_update_find_fail(db, &hints);
                            return (db_get_error(db));
                        }
                        node = ham_page_get_btree_node(page);
                        idx = btree_node_get_count(node) - 1;

                        ham_key_set_intflags(key, (ham_key_get_intflags(key) 
                                        & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
                    }
                }
            }
            else if (hints.original_flags & HAM_FIND_GT_MATCH)
            {
                /*
                 * if the index+1 is still in the page, just increment the
                 * index
                 */
                if (idx + 1 < btree_node_get_count(node))
                {
                    idx++;
                }
                else
                {
                    /*
                     * otherwise load the right sibling page
                     */
                    if (!btree_node_get_right(node))
                    {
                        stats_update_find_fail(db, &hints);
                        ham_assert(node == ham_page_get_btree_node(page), (0));
                        stats_update_any_bound(db, page, key, hints.original_flags, -1);
                        return db_set_error(db, HAM_KEY_NOT_FOUND);
                    }

                    hints.cost++;
                    page = db_fetch_page(db, btree_node_get_right(node), 0);
                    if (!page)
                    {
                        ham_assert(db_get_error(db), (0));
                        stats_update_find_fail(db, &hints);
                        return (db_get_error(db));
                    }
                    node = ham_page_get_btree_node(page);
                    idx = 0;
                }
                ham_key_set_intflags(key, (ham_key_get_intflags(key) 
                                        & ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
            }
        }
    }

    if (idx<0) {
        stats_update_find_fail(db, &hints);
        ham_assert(node, (0));
        ham_assert(page, (0));
        ham_assert(node == ham_page_get_btree_node(page), (0));
        stats_update_any_bound(db, page, key, hints.original_flags, -1);
        return db_set_error(db, HAM_KEY_NOT_FOUND);
    }

    /* load the entry, and store record ID and key flags */
    entry=btree_node_get_key(db, node, idx);

    /* set the cursor-position to this key */
    if (cursor) {
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_UNCOUPLED), 
                ("coupling an uncoupled cursor, but need a nil-cursor"));
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_COUPLED), 
                ("coupling a coupled cursor, but need a nil-cursor"));
        page_add_cursor(page, (ham_cursor_t *)cursor);
        bt_cursor_set_flags(cursor, 
                bt_cursor_get_flags(cursor)|BT_CURSOR_FLAG_COUPLED);
        bt_cursor_set_coupled_page(cursor, page);
        bt_cursor_set_coupled_index(cursor, idx);
    }

    /*
     * during util_read_key and util_read_record, new pages might be needed,
     * and the page at which we're pointing could be moved out of memory; 
     * that would mean that the cursor would be uncoupled, and we're losing
     * the 'entry'-pointer. therefore we 'lock' the page by incrementing 
     * the reference counter
     */
    page_add_ref(page);
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));

    /* no need to load the key if we have an exact match: */
    if (key && (ham_key_get_intflags(key) & KEY_IS_APPROXIMATE)) 
    {
        ham_status_t st=util_read_key(db, entry, key);
        if (st) 
        {
            page_release_ref(page);
            stats_update_find_fail(db, &hints);
            return (st);
        }
    }

    if (record) 
    {
        ham_status_t st;
        record->_intflags=key_get_flags(entry);
        record->_rid=key_get_ptr(entry);
        st=util_read_record(db, record, flags&HAM_DIRECT_ACCESS);
        if (st) 
        {
            page_release_ref(page);
            stats_update_find_fail(db, &hints);
            return (st);
        }
    }

    page_release_ref(page);
    
    stats_update_find(db, page, &hints);
    ham_assert(node == ham_page_get_btree_node(page), (0));
    stats_update_any_bound(db, page, key, hints.original_flags, idx);

    return (0);
}

ham_status_t 
btree_find(ham_btree_t *be, ham_key_t *key,
           ham_record_t *record, ham_u32_t flags)
{
    return (btree_find_cursor(be, 0, key, record, flags));
}

