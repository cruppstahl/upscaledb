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
#include "btree_node.h"

using namespace ham;


/**
 * enumerate a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_status_t
_enumerate_level(BtreeBackend *be, Page *page, ham_u32_t level,
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context);

/**
 * enumerate a single page
 */
static ham_status_t
_enumerate_page(BtreeBackend *be, Page *page, ham_u32_t level,
        ham_u32_t count, ham_enumerate_cb_t cb, void *context);

ham_status_t
BtreeBackend::do_enumerate(ham_enumerate_cb_t cb, void *context)
{
    Page *page;
    ham_u32_t level=0;
    ham_offset_t ptr_left;
    BtreeNode *node;
    ham_status_t st;
    Database *db=get_db();
    ham_status_t cb_st = HAM_ENUM_CONTINUE;

    ham_assert(get_rootpage()!=0);
    ham_assert(cb!=0);

    /* get the root page of the tree */
    st=db_fetch_page(&page, db, get_rootpage(), 0);
    if (st)
        return (st);

    /* while we found a page... */
    while (page) {
        ham_size_t count;

        node=BtreeNode::from_page(page);
        ptr_left = node->get_ptr_left();

        count=node->get_count();

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
        st = cb(HAM_ENUM_EVENT_DESCEND, (void *)&level, (void *)&count, context);
        if (st != HAM_ENUM_CONTINUE)
            return (st);

        /*
         * enumerate the page and all its siblings
         */
        cb_st = _enumerate_level(this, page, level, cb,
                        (cb_st == HAM_ENUM_DO_NOT_DESCEND), context);
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
            break;

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left)
        {
            st = db_fetch_page(&page, db, ptr_left, 0);
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
_enumerate_level(BtreeBackend *be, Page *page, ham_u32_t level,
        ham_enumerate_cb_t cb, ham_bool_t recursive, void *context)
{
    ham_status_t st;
    ham_size_t count=0;
    BtreeNode *node;
    ham_status_t cb_st = HAM_ENUM_CONTINUE;

    while (page) {
        /* enumerate the page */
        cb_st = _enumerate_page(be, page, level, count, cb, context);
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
            break;

        /*
         * get the right sibling
         */
        node=BtreeNode::from_page(page);
        if (node->get_right()) {
            st=db_fetch_page(&page, be->get_db(),
                    node->get_right(), 0);
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
_enumerate_page(BtreeBackend *be, Page *page, ham_u32_t level,
        ham_u32_t sibcount, ham_enumerate_cb_t cb, void *context)
{
    ham_size_t i;
    ham_size_t count;
    Database *db=page->get_db();
    btree_key_t *bte;
    BtreeNode *node=BtreeNode::from_page(page);
    ham_bool_t is_leaf;
    ham_status_t cb_st;
    ham_status_t cb_st2;

    if (node->get_ptr_left())
        is_leaf=0;
    else
        is_leaf=1;

    count=node->get_count();

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
    cb_st = cb(HAM_ENUM_EVENT_PAGE_START, (void *)page, &is_leaf, context);
    if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
        return (cb_st);

    for (i=0; (i < count) && (cb_st != HAM_ENUM_DO_NOT_DESCEND); i++)
    {
        bte = node->get_key(db, i);

        cb_st = cb(HAM_ENUM_EVENT_ITEM, (void *)bte, (void *)&count, context);
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
            break;
    }

    cb_st2 = cb(HAM_ENUM_EVENT_PAGE_STOP, (void *)page, &is_leaf, context);

    if (cb_st < 0 /* error */)
        return (cb_st);
    else if (cb_st == HAM_ENUM_STOP)
        return (HAM_ENUM_STOP);
    else
        return (cb_st2);
}

