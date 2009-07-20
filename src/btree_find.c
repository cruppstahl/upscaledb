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


ham_status_t 
btree_find_cursor(ham_btree_t *be, ham_bt_cursor_t *cursor, 
           ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_page_t *page;
    btree_node_t *node;
    int_key_t *entry;
    ham_s32_t idx;
    ham_db_t *db=btree_get_db(be);
	ham_u32_t local_flags = flags;

    /* get the address of the root page */
    db_set_error(db, 0);
    if (!btree_get_rootpage(be))
        return (db_set_error(db, HAM_KEY_NOT_FOUND));

    /* load the root page */
    page=db_fetch_page(db, btree_get_rootpage(be), local_flags);
    if (!page)
        return (db_get_error(db));

    /* now traverse the root to the leaf nodes, till we find a leaf */
    node=ham_page_get_btree_node(page);
    if (!btree_node_is_leaf(node)) {
		/* signal 'don't care' when we have multiple pages; we resolve 
           this once we've got a hit further down */
		if (local_flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) 
			local_flags |= (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH);

		for (;;) {
			page=btree_traverse_tree(db, page, key, 0);
			if (!page) {
				if (!db_get_error(db))
					db_set_error(db, HAM_KEY_NOT_FOUND);
				return (db_get_error(db));
			}

			node=ham_page_get_btree_node(page);
			if (btree_node_is_leaf(node))
				break;
		}
	}

    /* check the leaf page for the key */
    idx=btree_node_search_by_key(db, page, key, local_flags);
    if (db_get_error(db))
        return (db_get_error(db));

	/*
	   When we are performing an approximate match, the worst case
	   scenario is where we've picked the wrong side of the fence
	   while sitting at a page/node boundary: that's what this
	   next piece of code resolves:

	   essentially it moves one record forwards or backward when
	   the flags tell us this is mandatory and we're not yet in the proper
	   position yet.

	   The whole trick works, because the code above detects when
	   we need to traverse a multi-page btree -- where this worst-case
	   scenario can happen -- and adjusted the flags to accept
	   both LT and GT approximate matches so that btree_node_search_by_key()
	   will be hard pressed to return a 'key not found' signal (idx==-1),
	   instead delivering the nearest LT or GT match; all we need to
	   do now is ensure we've got the right one and if not, 
	   shift by one.
     */
	if (idx >= 0)
	{
		if ((key_get_flags(key) & KEY_IS_APPROXIMATE) 
			&& (flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) 
                    != (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) 
		{
			if ((key_get_flags(key) & KEY_IS_GT) && (flags & HAM_FIND_LT_MATCH))
			{
				/*
				 * if the index-1 is still in the page, just decrement the
				 * index
				 */
				if (idx > 0)
				{
					idx--;
				}
				else
				{
					/*
					 * otherwise load the left sibling page
					 */
					if (!btree_node_get_left(node))
					{
						db_set_error(db, HAM_KEY_NOT_FOUND);
						return (db_get_error(db));
					}

					page = db_fetch_page(db, btree_node_get_left(node), 0);
					if (!page)
						return (db_get_error(db));
					node = ham_page_get_btree_node(page);
					idx = btree_node_get_count(node) - 1;
				}
				key_set_flags(key, 
                        (key_get_flags(key) & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
			}
			else if ((key_get_flags(key) & KEY_IS_LT) 
                    && (flags & HAM_FIND_GT_MATCH))
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
						db_set_error(db, HAM_KEY_NOT_FOUND);
						return (db_get_error(db));
					}

					page = db_fetch_page(db, btree_node_get_right(node), 0);
					if (!page)
						return (db_get_error(db));
					node = ham_page_get_btree_node(page);
					idx = 0;
				}
				key_set_flags(key, 
                        (key_get_flags(key) & ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
			}
		}
		else if (!(key_get_flags(key) & KEY_IS_APPROXIMATE) 
				&& !(flags & HAM_FIND_EXACT_MATCH) && (flags != 0))
		{
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
			if (flags & HAM_FIND_LT_MATCH)
			{
				/*
				 * if the index-1 is still in the page, just decrement the
				 * index
				 */
				if (idx > 0)
				{
					idx--;

					key_set_flags(key, 
                        (key_get_flags(key) & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
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

						if (flags & HAM_FIND_GT_MATCH)
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
									db_set_error(db, HAM_KEY_NOT_FOUND);
									return (db_get_error(db));
								}

								page = db_fetch_page(db, 
                                    btree_node_get_right(node), 0);
								if (!page)
									return (db_get_error(db));
								node = ham_page_get_btree_node(page);
								idx = 0;
							}
							key_set_flags(key, (key_get_flags(key) & 
                                            ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
						}
						else
						{
							db_set_error(db, HAM_KEY_NOT_FOUND);
							return (db_get_error(db));
						}
					}
					else
					{
						page = db_fetch_page(db, btree_node_get_left(node), 0);
						if (!page)
							return (db_get_error(db));
						node = ham_page_get_btree_node(page);
						idx = btree_node_get_count(node) - 1;

						key_set_flags(key, (key_get_flags(key) 
                                        & ~KEY_IS_APPROXIMATE) | KEY_IS_LT);
					}
				}
			}
			else if (flags & HAM_FIND_GT_MATCH)
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
						db_set_error(db, HAM_KEY_NOT_FOUND);
						return (db_get_error(db));
					}

					page = db_fetch_page(db, btree_node_get_right(node), 0);
					if (!page)
						return (db_get_error(db));
					node = ham_page_get_btree_node(page);
					idx = 0;
				}
				key_set_flags(key, (key_get_flags(key) 
                                        & ~KEY_IS_APPROXIMATE) | KEY_IS_GT);
			}
		}
	}

    if (idx<0) {
        db_set_error(db, HAM_KEY_NOT_FOUND);
        return (db_get_error(db));
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
	if (key && (key_get_flags(key) & KEY_IS_APPROXIMATE)) {
        ham_status_t st=util_read_key(db, entry, key);
        if (st) {
            page_release_ref(page);
            return (st);
        }
    }

    if (record) {
		ham_status_t st;
		record->_intflags=key_get_flags(entry);
        record->_rid=key_get_ptr(entry);
		st=util_read_record(db, record, 0);
        if (st) {
            page_release_ref(page);
            return (st);
        }
    }

    page_release_ref(page);
	
    return (0);
}

ham_status_t 
btree_find(ham_btree_t *be, ham_key_t *key,
           ham_record_t *record, ham_u32_t flags)
{
    return (btree_find_cursor(be, 0, key, record, flags));
}

