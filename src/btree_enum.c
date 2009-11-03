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
 * btree enumeration
 *
 */

#include "config.h"

#include <string.h>
#include <stdio.h>
#include "db.h"
#include "error.h"
#include "btree.h"

/*
 * enumerate a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_cb_status_t 
my_enumerate_level(ham_page_t *page, ham_u32_t level, 
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context);

/*
 * enumerate a single page
 */
static ham_cb_status_t
my_enumerate_page(ham_page_t *page, ham_u32_t level, ham_u32_t count, 
        ham_enumerate_cb_t cb, void *context);

ham_status_t
btree_enumerate(ham_btree_t *be, ham_enumerate_cb_t cb,
        void *context)
{
    ham_page_t *page;
    ham_u32_t level=0;
    ham_offset_t ptr_left;
    btree_node_t *node;
    ham_status_t st;
    ham_db_t *db=btree_get_db(be);
	ham_cb_status_t cb_st = CB_CONTINUE;

    ham_assert(btree_get_rootpage(be)!=0, ("invalid root page"));
    ham_assert(cb!=0, ("invalid parameter"));

    /* get the root page of the tree */
    page = db_fetch_page(db, btree_get_rootpage(be), 0);
    if (!page)
        return (db_get_error(db));

    /* while we found a page... */
    while (page) 
	{
		ham_size_t count;

        node=ham_page_get_btree_node(page);
        ptr_left = btree_node_get_ptr_left(node);

	    count=btree_node_get_count(node);
        st = cb(ENUM_EVENT_DESCEND, (void *)&level, (void *)&count, context);
        if (st)
            return (st);

        /*
         * enumerate the page and all its siblings
         */
        cb_st = my_enumerate_level(page, level, cb, (cb_st == CB_DO_NOT_DESCEND), context);
		if (cb_st == CB_STOP)
			break;

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left)
            page = db_fetch_page(db, ptr_left, 0);
        else
            page = 0;

        ++level;
    }

    return (db_get_error(db));
}

static ham_cb_status_t 
my_enumerate_level(ham_page_t *page, ham_u32_t level, 
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context)
{
    ham_size_t count=0;
    btree_node_t *node;
	ham_cb_status_t cb_st = CB_CONTINUE;

    while (page) 
	{
        /*
         * enumerate the page
         */
        cb_st = my_enumerate_page(page, level, count, cb, context);
		if (cb_st == CB_STOP)
			break;

        /* 
         * get the right sibling
         */
        node=ham_page_get_btree_node(page);
        if (btree_node_get_right(node))
            page=db_fetch_page(page_get_owner(page), 
                    btree_node_get_right(node), 0);
        else
            break;

        ++count;
    }

    return (cb_st);
}

ham_cb_status_t
my_enumerate_page(ham_page_t *page, ham_u32_t level, ham_u32_t sibcount, 
        ham_enumerate_cb_t cb, void *context)
{
    ham_size_t i;
	ham_size_t count;
    ham_db_t *db=page_get_owner(page);
    int_key_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_bool_t is_leaf;
	ham_cb_status_t cb_st;
	ham_cb_status_t cb_st2;

    if (btree_node_get_ptr_left(node))
        is_leaf=0;
    else
        is_leaf=1;

    count=btree_node_get_count(node);

    cb_st = cb(ENUM_EVENT_PAGE_START, (void *)page, &is_leaf, context);
    if (cb_st == CB_STOP)
        return (cb_st);

	for (i=0; (i < count) && (cb_st != CB_DO_NOT_DESCEND); i++) 
	{
		bte = btree_node_get_key(db, node, i);

		cb_st = cb(ENUM_EVENT_ITEM, (void *)bte, (void *)&count, context);
		if (cb_st == CB_STOP)
			break;
	}

    cb_st2 = cb(ENUM_EVENT_PAGE_STOP, (void *)page, &is_leaf, context);

	if (cb_st == CB_STOP)
	    return (CB_STOP);
	else
		return (cb_st2);
}

