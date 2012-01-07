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
 * @brief btree enumeration
 *
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "btree.h"
#include "db.h"
#include "env.h"
#include "device.h"
#include "error.h"
#include "mem.h"
#include "page.h"

/**
 * enumerate a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_status_t 
_enumerate_level(ham_btree_t *be, ham_page_t *page, ham_u32_t level, 
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context);

/**
 * enumerate a single page
 */
static ham_status_t
_enumerate_page(ham_btree_t *be, ham_page_t *page, ham_u32_t level, 
        ham_u32_t count, ham_enumerate_cb_t cb, void *context);

/**                                                                 
 * iterate the whole tree and enumerate every item.
 *
 * @note This is a B+-tree 'backend' method.
 */                                                                 
ham_status_t
btree_enumerate(ham_btree_t *be, ham_enumerate_cb_t cb, void *context)
{
    ham_page_t *page;
    ham_u32_t level=0;
    ham_offset_t ptr_left;
    btree_node_t *node;
    ham_status_t st;
    Database *db=be_get_db(be);
    ham_status_t cb_st = CB_CONTINUE;

    ham_assert(btree_get_rootpage(be)!=0, ("invalid root page"));
    ham_assert(cb!=0, ("invalid parameter"));

    /* get the root page of the tree */
    st = db_fetch_page(&page, db, btree_get_rootpage(be), 0);
    ham_assert(st ? !page : 1, (0));
    if (!page)
        return st ? st : HAM_INTERNAL_ERROR;
    /* hack: prior to 2.0, the type of btree root pages was not set
     * correctly */
    page_set_type(page, PAGE_TYPE_B_ROOT);

    /* while we found a page... */
    while (page) 
    {
        ham_size_t count;

        node=page_get_btree_node(page);
        ptr_left = btree_node_get_ptr_left(node);

        count=btree_node_get_count(node);

        /*
         * WARNING:WARNING:WARNING:WARNING:WARNING
         * 
         * the current Btree page must be 'pinned' during each callback 
         * invocation during the enumeration; if you don't (by temporarily 
         * bumping up its reference count) callback methods MAY flush the 
         * page from the page cache without us being aware of such until after 
         * the fact, when the hamster will CRASH as page pointers and content 
         * are invalidated.
         *    
         * To prevent such mishaps, all user-callback invocations in here 
         * are surrounded
         * by this page 'pinning' countermeasure.
         */
        st = cb(ENUM_EVENT_DESCEND, (void *)&level, (void *)&count, context);
        if (st != CB_CONTINUE)
            return (st);

        /*
         * enumerate the page and all its siblings
         */
        cb_st = _enumerate_level(be, page, level, cb, 
                        (cb_st == CB_DO_NOT_DESCEND), context);
        if (cb_st == CB_STOP || cb_st < 0 /* error */)
            break;

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left)
        {
            st = db_fetch_page(&page, db, ptr_left, 0);
            ham_assert(st ? !page : 1, (0));
            if (st)
                return st;
        }
        else
            page = 0;

        ++level;
    }

    return (cb_st < 0 ? cb_st : HAM_SUCCESS);
}

static ham_status_t 
_enumerate_level(ham_btree_t *be, ham_page_t *page, ham_u32_t level, 
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context)
{
    ham_status_t st;
    ham_size_t count=0;
    btree_node_t *node;
    ham_status_t cb_st = CB_CONTINUE;

    while (page) 
    {
        /*
         * enumerate the page
         */
        cb_st = _enumerate_page(be, page, level, count, cb, context);
        if (cb_st == CB_STOP || cb_st < 0 /* error */)
            break;

        /* 
         * get the right sibling
         */
        node=page_get_btree_node(page);
        if (btree_node_get_right(node))
        {
            st=db_fetch_page(&page, be_get_db(be), 
                    btree_node_get_right(node), 0);
            ham_assert(st ? !page : 1, (0));
            if (st)
                return st;
        }
        else
            break;

        ++count;
    }

    return (cb_st);
}

ham_status_t
_enumerate_page(ham_btree_t *be, ham_page_t *page, ham_u32_t level, 
        ham_u32_t sibcount, ham_enumerate_cb_t cb, void *context)
{
    ham_size_t i;
    ham_size_t count;
    Database *db=page_get_owner(page);
    btree_key_t *bte;
    btree_node_t *node=page_get_btree_node(page);
    ham_bool_t is_leaf;
    ham_status_t cb_st;
    ham_status_t cb_st2;

    if (btree_node_get_ptr_left(node))
        is_leaf=0;
    else
        is_leaf=1;

    count=btree_node_get_count(node);

    /*
     * WARNING:WARNING:WARNING:WARNING:WARNING
     * 
     * the current Btree page must be 'pinned' during each callback 
     * invocation during the enumeration; if you don't (by temporarily 
     * bumping up its reference count) callback methods MAY flush the 
     * page from the page cache without us being aware of such until after 
     * the fact, when the hamster will CRASH as page pointers and content 
     * are invalidated.
     *    
     * To prevent such mishaps, all user-callback invocations in here 
     * are surrounded
     * by this page 'pinning' countermeasure.
     */
    cb_st = cb(ENUM_EVENT_PAGE_START, (void *)page, &is_leaf, context);
    if (cb_st == CB_STOP || cb_st < 0 /* error */)
        return (cb_st);

    for (i=0; (i < count) && (cb_st != CB_DO_NOT_DESCEND); i++) 
    {
        bte = btree_node_get_key(db, node, i);

        cb_st = cb(ENUM_EVENT_ITEM, (void *)bte, (void *)&count, context);
        if (cb_st == CB_STOP || cb_st < 0 /* error */)
            break;
    }

    cb_st2 = cb(ENUM_EVENT_PAGE_STOP, (void *)page, &is_leaf, context);

    if (cb_st < 0 /* error */)
        return (cb_st);
    else if (cb_st == CB_STOP)
        return (CB_STOP);
    else
        return (cb_st2);
}

