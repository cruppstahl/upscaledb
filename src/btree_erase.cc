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
 * @brief btree erasing
 *
 */

#include "config.h"

#include <string.h>

#include "blob.h"
#include "btree.h"
#include "cache.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "cursor.h"


/*
 * the erase_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct erase_scratchpad_t
{
    /*
     * the backend pointer
     */
    ham_btree_t *be;

    /*
     * the flags of the ham_erase()-call
     */
    ham_u32_t flags;

    /*
     * the key which will be deleted
     */
    ham_key_t *key;

    /*
     * a page which needs rebalancing
     */
    Page *mergepage;

    /*
     * a coupled cursor (can be NULL)
     */
    btree_cursor_t *cursor;

    /*
     * a duplicate index - a +1 based index into the duplicate table. If
     * this index is set then only this duplicate is erased
     */
    ham_u32_t dupe_id;

} erase_scratchpad_t;

/**
 * recursively descend down the tree, delete the item and re-balance
 * the tree on the way back up
 *
 * returns the page which is deleted, if available
 */
static ham_status_t
my_erase_recursive(Page **page_ref, Page *page, ham_offset_t left,
                ham_offset_t right, ham_offset_t lanchor, ham_offset_t ranchor,
                Page *parent, erase_scratchpad_t *scratchpad,
                erase_hints_t *hints);

/*
 * collapse the root node
 */
static ham_status_t
__collapse_root(Page *root, erase_scratchpad_t *scratchpad);

/**
 * rebalance a page - either shifts elements to a sibling, or merges
 * the page with a sibling
 */
static ham_status_t
my_rebalance(Page **newpage_ref, Page *page, ham_offset_t left, ham_offset_t right,
             ham_offset_t lanchor, ham_offset_t ranchor, Page *parent,
             erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * merge two pages
 */
static ham_status_t
my_merge_pages(Page **newpage_ref, Page *page, Page *sibling, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * shift items from a sibling to this page, till both pages have an equal
 * number of items
 *
 * @remark @a newpage_ref reference will always be set to NULL.
 *
 * TODO has been checked twice against old code and this is true. Hence shift_pages
 *      does NOT need the newpage_ref and callers could set *newpage_ref=NULL
 *      themselves.
 */
static ham_status_t
my_shift_pages(Page **newpage_ref, Page *page, Page *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * copy a key
 */
static ham_status_t
my_copy_key(Database *db, btree_key_t *lhs, btree_key_t *rhs);

/*
 * replace two keys in a page
 */
static ham_status_t
my_replace_key(Page *page, ham_s32_t slot,
        btree_key_t *newentry, ham_u32_t flags, erase_hints_t *hints);

/*
 * remove an item from a page
 */
static ham_status_t
my_remove_entry(Page *page, ham_s32_t slot,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * flags for my_replace_key
 */
/* #define NOFLUSH 1  -- unused */
#define INTERNAL_KEY 2

static ham_status_t
btree_erase_impl(ham_btree_t *be, ham_key_t *key,
        btree_cursor_t *cursor, ham_u32_t dupe_id, ham_u32_t flags)
{
    ham_status_t st;
    Page *root;
    Page *p;
    ham_offset_t rootaddr;
    Database *db=be_get_db(be);
    erase_scratchpad_t scratchpad;
    erase_hints_t hints = {flags, flags,
                    cursor
                        ? (ham_cursor_t *)btree_cursor_get_parent(cursor)
                        : 0,
                    0, HAM_FALSE, HAM_FALSE, 0, NULL, -1};

    /*
     * initialize the scratchpad
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.key=key;
    scratchpad.flags=flags;
    scratchpad.cursor=cursor;
    scratchpad.dupe_id=dupe_id;

    btree_erase_get_hints(&hints, db, key);

    if (hints.key_is_out_of_bounds)
    {
        btree_stats_update_erase_fail_oob(db, &hints);
        return (HAM_KEY_NOT_FOUND);
    }

    if (hints.try_fast_track)
    {
        /* TODO */
        ham_assert(1,(0));
    }

    /*
     * get the root-page...
     */
    rootaddr=btree_get_rootpage(be);
    if (!rootaddr)
    {
        btree_stats_update_erase_fail(db, &hints);
        return HAM_KEY_NOT_FOUND;
    }
    st=db_fetch_page(&root, db, rootaddr, flags);
    ham_assert(st ? !root : 1, (0));
    if (!root)
    {
        btree_stats_update_erase_fail(db, &hints);
        return st ? st : HAM_INTERNAL_ERROR;
    }

    /*
     * ... and start the recursion
     */
    st=my_erase_recursive(&p, root, 0, 0, 0, 0, 0, &scratchpad, &hints);
    if (st)
    {
        btree_stats_update_erase_fail(db, &hints);
        return (st);
    }

    if (p)
    {
        ham_status_t st;

        /*
         * delete the old root page
         */
        st=btree_uncouple_all_cursors(root, 0);
        if (st)
        {
            btree_stats_update_erase_fail(db, &hints);
            return (st);
        }

        st=__collapse_root(p, &scratchpad);
        if (st) {
            btree_stats_update_erase_fail(db, &hints);
            return (st);
        }

        btree_stats_page_is_nuked(db, root, HAM_FALSE);
    }

    btree_stats_update_erase(db, hints.processed_leaf_page, &hints);
    btree_stats_update_any_bound(HAM_OPERATION_STATS_ERASE, db,
                    hints.processed_leaf_page, key,
                    hints.flags, hints.processed_slot);
    return (0);
}

static ham_status_t
my_erase_recursive(Page **page_ref, Page *page, ham_offset_t left, ham_offset_t right,
        ham_offset_t lanchor, ham_offset_t ranchor, Page *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_s32_t slot;
    ham_bool_t isfew;
    ham_status_t st;
    Page *newme;
    Page *child;
    Page *tempp=0;
    Database *db=page->get_db();
    btree_node_t *node=page_get_btree_node(page);
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);

    *page_ref = 0;

    /*
     * empty node? then most likely we're in the empty root page.
     */
    if (btree_node_get_count(node)==0) {
        return HAM_KEY_NOT_FOUND;
    }

    /*
     * mark the nodes which may need rebalancing
     */
    if (btree_get_rootpage(scratchpad->be)==page->get_self())
        isfew=(btree_node_get_count(node)<=1);
    else
        isfew=(btree_node_get_count(node)<btree_get_minkeys(maxkeys));

    if (!isfew) /* [i_a] name does not represent value; cf. code in btree_check */
        scratchpad->mergepage=0;
    else if (!scratchpad->mergepage)
        scratchpad->mergepage=page;

    if (!btree_node_is_leaf(node))
    {
        st=btree_traverse_tree(&child, &slot, db, page, scratchpad->key);
        ham_assert(child!=0, ("guru meditation error"));
        if (!child)
            return st ? st : HAM_INTERNAL_ERROR;
    }
    else
    {
        hints->cost++;
        st=btree_get_slot(db, page, scratchpad->key, &slot, 0);
        if (st) {
            return st;
        }
        child=0;
    }

    /*
     * if this page is not a leaf: recursively descend down the tree
     */
    if (!btree_node_is_leaf(node))
    {
        ham_offset_t next_lanchor;
        ham_offset_t next_ranchor;
        ham_offset_t next_left;
        ham_offset_t next_right;

        /*
         * calculate neighbor and anchor nodes
         */
        if (slot==-1) {
            if (!left)
                next_left=0;
            else {
                btree_key_t *bte;
                btree_node_t *n;
                st=db_fetch_page(&tempp, db, left, 0);
                if (!tempp)
                    return st ? st : HAM_INTERNAL_ERROR;
                n=page_get_btree_node(tempp);
                bte=btree_node_get_key(db, n, btree_node_get_count(n)-1);
                next_left=key_get_ptr(bte);
            }
            next_lanchor=lanchor;
        }
        else {
            if (slot==0)
                next_left=btree_node_get_ptr_left(node);
            else {
                btree_key_t *bte;
                bte=btree_node_get_key(db, node, slot-1);
                next_left=key_get_ptr(bte);
            }
            next_lanchor=page->get_self();
        }

        if (slot==btree_node_get_count(node)-1) {
            if (!right)
                next_right=0;
            else {
                btree_key_t *bte;
                btree_node_t *n;
                st=db_fetch_page(&tempp, db, right, 0);
                ham_assert(st ? !tempp : 1, (0));
                if (!tempp)
                    return st ? st : HAM_INTERNAL_ERROR;
                n=page_get_btree_node(tempp);
                bte=btree_node_get_key(db, n, 0);
                next_right=key_get_ptr(bte);
            }
            next_ranchor=ranchor;
        }
        else {
            btree_key_t *bte;
            bte=btree_node_get_key(db, node, slot+1);
            next_right=key_get_ptr(bte);
            next_ranchor=page->get_self();
        }

        st=my_erase_recursive(&newme, child, next_left, next_right, next_lanchor,
                    next_ranchor, page, scratchpad, hints);
        if (st)
            return st;
    }
    else
    {
        /*
         * otherwise (page is a leaf) delete the key...
         *
         * first, check if this entry really exists
         */
        newme=0;
        if (slot!=-1)
        {
            int cmp=btree_compare_keys(db, page, scratchpad->key, slot);
            if (cmp < -1)
                return (ham_status_t)cmp;
            
            if (cmp==0) {
                newme=page;
            }
            else {
                return HAM_KEY_NOT_FOUND;
            }
        }
        if (!newme)
        {
            scratchpad->mergepage=0;
            return HAM_KEY_NOT_FOUND;
        }
    }

    /*
     * ... and rebalance the tree, if necessary
     */
    if (newme) {
        if (slot==-1)
            slot=0;
        st=my_remove_entry(page, slot, scratchpad, hints);
        if (st)
            return st;
    }

    /*
     * no need to rebalance in case of an error
     */
    ham_assert(!st, (0));
    return my_rebalance(page_ref, page, left, right, lanchor, ranchor, parent,
                scratchpad, hints);
}

static ham_status_t
__collapse_root(Page *newroot, erase_scratchpad_t *scratchpad)
{
    Environment *env;

    btree_set_rootpage(scratchpad->be, newroot->get_self());
    be_set_dirty(scratchpad->be, HAM_TRUE);
    scratchpad->be->_fun_flush(scratchpad->be);
    ham_assert(newroot->get_db(), (0));

    env=newroot->get_db()->get_env();
    ham_assert(env!=0, (""));
    env->set_dirty(true);

    /* add the page to the changeset to make sure that the changes are
     * logged */
    if (env->get_flags()&HAM_ENABLE_RECOVERY)
        env->get_changeset().add_page(env->get_header_page());

    newroot->set_type(Page::TYPE_B_ROOT);

    return (0);
}

static ham_status_t
my_rebalance(Page **newpage_ref, Page *page, ham_offset_t left, ham_offset_t right,
        ham_offset_t lanchor, ham_offset_t ranchor, Page *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_status_t st;
    btree_node_t *node=page_get_btree_node(page);
    Page *leftpage=0;
    Page *rightpage=0;
    btree_node_t *leftnode=0;
    btree_node_t *rightnode=0;
    ham_bool_t fewleft=HAM_FALSE;
    ham_bool_t fewright=HAM_FALSE;
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);
    ham_size_t minkeys=btree_get_minkeys(maxkeys);

    ham_assert(page->get_db(), (0));

    *newpage_ref = 0;
    if (!scratchpad->mergepage)
        return (0);

    /*
     * get the left and the right sibling of this page
     */
    if (left)
    {
        st = db_fetch_page(&leftpage, page->get_db(),
                        btree_node_get_left(node), 0);
        if (st)
            return st;
        if (leftpage) {
            leftnode =page_get_btree_node(leftpage);
            fewleft  =(btree_node_get_count(leftnode)<=minkeys);
        }
    }
    if (right)
    {
        st = db_fetch_page(&rightpage, page->get_db(),
                        btree_node_get_right(node), 0);
        if (st)
            return st;
        if (rightpage) {
            rightnode=page_get_btree_node(rightpage);
            fewright =(btree_node_get_count(rightnode)<=minkeys);
        }
    }

    /*
     * if we have no siblings, then we're rebalancing the root page
     */
    if (!leftpage && !rightpage) {
        if (btree_node_is_leaf(node)) {
            return (0);
        }
        else {
            return (db_fetch_page(newpage_ref,
                        page->get_db(),
                        btree_node_get_ptr_left(node), 0));
        }
    }

    /*
     * if one of the siblings is missing, or both of them are
     * too empty, we have to merge them
     */
    if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (parent && lanchor!=parent->get_self()) {
            return (my_merge_pages(newpage_ref, page, rightpage, ranchor,
                        scratchpad, hints));
        }
        else {
            return (my_merge_pages(newpage_ref, leftpage, page, lanchor,
                        scratchpad, hints));
        }
    }

    /*
     * otherwise choose the better of a merge or a shift
     */
    if (leftpage && fewleft && rightpage && !fewright) {
        if (parent && (!(ranchor==parent->get_self()) &&
                (page->get_self()==scratchpad->mergepage->get_self()))) {
            return (my_merge_pages(newpage_ref, leftpage, page, lanchor,
                        scratchpad, hints));
        }
        else {
            return (my_shift_pages(newpage_ref, page, rightpage, ranchor,
                        scratchpad, hints));
        }
    }

    /*
     * ... still choose the better of a merge or a shift...
     */
    if (leftpage && !fewleft && rightpage && fewright) {
        if (parent && (!(lanchor==parent->get_self()) &&
                (page->get_self()==scratchpad->mergepage->get_self()))) {
            return (my_merge_pages(newpage_ref, page, rightpage, ranchor,
                        scratchpad, hints));
        }
        else {
            return (my_shift_pages(newpage_ref, leftpage, page, lanchor,
                        scratchpad, hints));
        }
    }

    /*
     * choose the more effective of two shifts
     */
    if (lanchor==ranchor) {
        if (leftnode!=0 && rightnode!=0
                && btree_node_get_count(leftnode)
                    <=btree_node_get_count(rightnode)) {
            return (my_shift_pages(newpage_ref, page, rightpage,
                        ranchor, scratchpad, hints));
        }
        else {
            return (my_shift_pages(newpage_ref, leftpage, page,
                        lanchor, scratchpad, hints));
        }
    }

    /*
     * choose the shift with more local effect
     */
    if (parent && lanchor==parent->get_self()) {
        return (my_shift_pages(newpage_ref, leftpage, page, lanchor,
                        scratchpad, hints));
    }
    else {
        return (my_shift_pages(newpage_ref, page, rightpage, ranchor,
                        scratchpad, hints));
    }
}

static ham_status_t
my_merge_pages(Page **newpage_ref, Page *page, Page *sibpage,
            ham_offset_t anchor, erase_scratchpad_t *scratchpad,
            erase_hints_t *hints)
{
    ham_status_t st;
    ham_s32_t slot;
    ham_size_t c, keysize;
    Database *db=page->get_db();
    Page *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    btree_key_t *bte_lhs, *bte_rhs;

    ham_assert(db, (0));

    keysize=db_get_keysize(db);
    node   =page_get_btree_node(page);
    sibnode=page_get_btree_node(sibpage);

    if (anchor) {
        st=db_fetch_page(&ancpage, page->get_db(), anchor, 0);
        ham_assert(st ? !ancpage : 1, (0));
        if (!ancpage)
            return st ? st : HAM_INTERNAL_ERROR;
        ancnode=page_get_btree_node(ancpage);
    }
    else {
        ancpage=0;
        ancnode=0;
    }

    *newpage_ref = 0;

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;
    if ((st=btree_uncouple_all_cursors(sibpage, 0)))
        return st;
    if (ancpage)
        if ((st=btree_uncouple_all_cursors(ancpage, 0)))
            return st;

    /*
     * internal node: append the anchornode separator value to
     * this node
     */
    if (!btree_node_is_leaf(node)) {
        btree_key_t *bte;
        ham_key_t key;

        bte =btree_node_get_key(db, sibnode, 0);
        memset(&key, 0, sizeof(key));
        key._flags=key_get_flags(bte);
        key.data  =key_get_key(bte);
        key.size  =key_get_size(bte);

        hints->cost++;
        st=btree_get_slot(db, ancpage, &key, &slot, 0);
        if (st) {
            return st;
        }

        bte_lhs=btree_node_get_key(db, node,
            btree_node_get_count(node));
        bte_rhs=btree_node_get_key(db, ancnode, slot);

        hints->cost++;
        st=my_copy_key(db, bte_lhs, bte_rhs);
        if (st) {
            return st;
        }
        key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
        btree_node_set_count(node, btree_node_get_count(node)+1);
    }

    c=btree_node_get_count(sibnode);
    bte_lhs=btree_node_get_key(db, node, btree_node_get_count(node));
    bte_rhs=btree_node_get_key(db, sibnode, 0);

    /*
     * shift items from the sibling to this page
     */
    hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()+
                        keysize)*c);
    memcpy(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*c);
            
    /*
     * as sibnode is merged into node, we will also need to ensure that our
     * statistics node/page tracking is corrected accordingly: what was in
     * sibnode, is now in node. And sibnode will be destroyed at the end.
     */
    if (sibpage == hints->processed_leaf_page) {
        /* sibnode slot 0 has become node slot 'bte_lhs' */
        hints->processed_slot += btree_node_get_count(node);
        hints->processed_leaf_page = page;
    }

    page->set_dirty(true);
    sibpage->set_dirty(true);
    ham_assert(btree_node_get_count(node)+c <= 0xFFFF, (0));
    btree_node_set_count(node, btree_node_get_count(node)+c);
    btree_node_set_count(sibnode, 0);

    /*
     * update the linked list of pages
     */
    if (btree_node_get_left(node)==sibpage->get_self()) {
        if (btree_node_get_left(sibnode)) {
            Page *p;
            btree_node_t *n;

            st=db_fetch_page(&p, page->get_db(),
                    btree_node_get_left(sibnode), 0);
            if (!p)
                return st ? st : HAM_INTERNAL_ERROR;
            n=page_get_btree_node(p);
            btree_node_set_right(n, btree_node_get_right(sibnode));
            btree_node_set_left(node, btree_node_get_left(sibnode));
            p->set_dirty(true);
        }
        else
            btree_node_set_left(node, 0);
    }
    else if (btree_node_get_right(node)==sibpage->get_self()) {
        if (btree_node_get_right(sibnode)) {
            Page *p;
            btree_node_t *n;
            
            st=db_fetch_page(&p, page->get_db(),
                    btree_node_get_right(sibnode), 0);
            if (!p)
                return st ? st : HAM_INTERNAL_ERROR;
            n=page_get_btree_node(p);

            btree_node_set_right(node, btree_node_get_right(sibnode));
            btree_node_set_left(n, btree_node_get_left(sibnode));
            p->set_dirty(true);
        }
        else
            btree_node_set_right(node, 0);
    }
    
    /*
     * return this page for deletion
     */
    if (scratchpad->mergepage &&
           (scratchpad->mergepage->get_self()==page->get_self() ||
            scratchpad->mergepage->get_self()==sibpage->get_self()))
        scratchpad->mergepage=0;

    btree_stats_page_is_nuked(db, sibpage, HAM_FALSE);

    /*
     * delete the page
     * TODO
     */
    ham_assert(hints->processed_leaf_page != sibpage, (0));

    *newpage_ref = sibpage;
    return (HAM_SUCCESS);
}

static ham_status_t
my_shift_pages(Page **newpage_ref, Page *page, Page *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_s32_t slot=0;
    ham_status_t st;
    ham_bool_t intern;
    ham_size_t s;
    ham_size_t c;
    ham_size_t keysize;
    Database *db=page->get_db();
    Page *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    btree_key_t *bte_lhs, *bte_rhs;

    node   =page_get_btree_node(page);
    sibnode=page_get_btree_node(sibpage);
    keysize=db_get_keysize(db);
    intern =!btree_node_is_leaf(node);
    st=db_fetch_page(&ancpage, db, anchor, 0);
    if (!ancpage)
        return st ? st : HAM_INTERNAL_ERROR;
    ancnode=page_get_btree_node(ancpage);

    ham_assert(btree_node_get_count(node)!=btree_node_get_count(sibnode), (0));

    *newpage_ref = 0;

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;
    if ((st=btree_uncouple_all_cursors(sibpage, 0)))
        return st;
    if (ancpage)
        if ((st=btree_uncouple_all_cursors(ancpage, 0)))
            return st;

    /*
     * shift from sibling to this node
     */
    if (btree_node_get_count(sibnode)>=btree_node_get_count(node))
    {
        /*
         * internal node: insert the anchornode separator value to
         * this node
         */
        if (intern)
        {
            btree_key_t *bte;
            ham_key_t key;

            bte=btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
            hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                return st;
            }
    
            /*
             * append the anchor node to the page
             */
            bte_rhs=btree_node_get_key(db, ancnode, slot);
            bte_lhs=btree_node_get_key(db, node,
                btree_node_get_count(node));

            hints->cost++;
            st=my_copy_key(db, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            /*
             * the pointer of this new node is ptr_left of the sibling
             */
            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));

            /*
             * new pointer left of the sibling is sibling[0].ptr
             */
            btree_node_set_ptr_left(sibnode, key_get_ptr(bte));

            /*
             * update the anchor node with sibling[0]
             */
            (void)my_replace_key(ancpage, slot, bte, INTERNAL_KEY, hints);

            /*
             * shift the remainder of sibling to the left
             */
            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                        + keysize) * (btree_node_get_count(sibnode)-1));
            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, sibnode, 1);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)
                    * (btree_node_get_count(sibnode)-1));

            /*
             * adjust counters
             */
            btree_node_set_count(node, btree_node_get_count(node)+1);
            btree_node_set_count(sibnode, btree_node_get_count(sibnode)-1);
        }

        c=(btree_node_get_count(sibnode)-btree_node_get_count(node))/2;
        if (c==0)
            goto cleanup;
        if (intern)
            c--;
        if (c==0)
            goto cleanup;

        /*
         * internal node: append the anchor key to the page
         */
        if (intern)
        {
            bte_lhs=btree_node_get_key(db, node,
                    btree_node_get_count(node));
            bte_rhs=btree_node_get_key(db, ancnode, slot);

            hints->cost++;
            st=my_copy_key(db, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(node, btree_node_get_count(node)+1);
        }

        /*
         * shift items from the sibling to this page, then
         * delete the shifted items
         */
        hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                + keysize)*(btree_node_get_count(sibnode) + c));

        bte_lhs=btree_node_get_key(db, node,
                btree_node_get_count(node));
        bte_rhs=btree_node_get_key(db, sibnode, 0);

        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*c);

        bte_lhs=btree_node_get_key(db, sibnode, 0);
        bte_rhs=btree_node_get_key(db, sibnode, c);
        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*
                (btree_node_get_count(sibnode)-c));

        /*
         * internal nodes: don't forget to set ptr_left of the sibling, and
         * replace the anchor key
         */
        if (intern)
        {
            btree_key_t *bte;
            bte=btree_node_get_key(db, sibnode, 0);
            btree_node_set_ptr_left(sibnode, key_get_ptr(bte));
            if (anchor)
            {
                ham_key_t key;
                memset(&key, 0, sizeof(key));
                key._flags=key_get_flags(bte);
                key.data  =key_get_key(bte);
                key.size  =key_get_size(bte);
                hints->cost++;
                st=btree_get_slot(db, ancpage, &key, &slot, 0);
                if (st) {
                    return st;
                }
                /* replace the key */
                st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY, hints);
                if (st) {
                    return st;
                }
            }
            /*
             * shift once more
             */
            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                    + keysize)*(btree_node_get_count(sibnode)-1));
            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, sibnode, 1);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*
                    (btree_node_get_count(sibnode)-1));
        }
        else
        {
            /*
             * in a leaf - update the anchor
             */
            ham_key_t key;
            btree_key_t *bte;
            bte=btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
            hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                return st;
            }
            /* replace the key */
            st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY, hints);
            if (st) {
                return st;
            }
        }

        /*
         * update the page counter
         */
        ham_assert(btree_node_get_count(node)+c <= 0xFFFF, (0));
        ham_assert(btree_node_get_count(sibnode)-c-(intern ? 1 : 0) <= 0xFFFF, (0));
        btree_node_set_count(node,
                btree_node_get_count(node)+c);
        btree_node_set_count(sibnode,
                btree_node_get_count(sibnode)-c-(intern ? 1 : 0));
    }
    else
    {
        /*
         * shift from this node to the sibling
         */

        /*
        * internal node: insert the anchornode separator value to
        * this node
        */
        if (intern)
        {
            btree_key_t *bte;
            ham_key_t key;
    
            bte =btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
            hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                return st;
            }

            /*
             * shift entire sibling by 1 to the right
             */
            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                    + keysize) * (btree_node_get_count(sibnode)));
            bte_lhs=btree_node_get_key(db, sibnode, 1);
            bte_rhs=btree_node_get_key(db, sibnode, 0);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)
                    * (btree_node_get_count(sibnode)));

            /*
             * copy the old anchor element to sibling[0]
             */
            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, ancnode, slot);

            hints->cost++;
            st=my_copy_key(db, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            /*
             * sibling[0].ptr = sibling.ptr_left
             */
            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));

            /*
             * sibling.ptr_left = node[node.count-1].ptr
             */
            bte_lhs=btree_node_get_key(db, node,
            btree_node_get_count(node)-1);
            btree_node_set_ptr_left(sibnode, key_get_ptr(bte_lhs));

            /*
             * new anchor element is node[node.count-1].key
             */
            st=my_replace_key(ancpage, slot, bte_lhs, INTERNAL_KEY, hints);
            if (st) {
                return st;
            }

            /*
             * page: one item less; sibling: one item more
             */
            btree_node_set_count(node, btree_node_get_count(node)-1);
            btree_node_set_count(sibnode, btree_node_get_count(sibnode)+1);
        }

        c=(btree_node_get_count(node)-btree_node_get_count(sibnode))/2;
        if (c==0)
            goto cleanup;
        if (intern)
            c--;
        if (c==0)
            goto cleanup;

        /*
         * internal pages: insert the anchor element
         */
        if (intern) {
            /*
             * shift entire sibling by 1 to the right
             */
            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                    + keysize) * (btree_node_get_count(sibnode)));
            bte_lhs=btree_node_get_key(db, sibnode, 1);
            bte_rhs=btree_node_get_key(db, sibnode, 0);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)
                    * (btree_node_get_count(sibnode)));

            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, ancnode, slot);

            /* clear the key - we don't want my_replace_key to free
             * an extended block which is still used by sibnode[1] */
            memset(bte_lhs, 0, sizeof(*bte_lhs));

            st=my_replace_key(sibpage, 0, bte_rhs,
                    (btree_node_is_leaf(node) ? 0 : INTERNAL_KEY),
                    hints);
            if (st) {
                return st;
            }

            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(sibnode, btree_node_get_count(sibnode)+1);
        }

        s=btree_node_get_count(node)-c-1;

        /*
         * shift items from this page to the sibling, then delete the
         * items from this page
         */
        hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                + keysize)*(btree_node_get_count(sibnode)+c));
        bte_lhs=btree_node_get_key(db, sibnode, c);
        bte_rhs=btree_node_get_key(db, sibnode, 0);
        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*
                btree_node_get_count(sibnode));

        bte_lhs=btree_node_get_key(db, sibnode, 0);
        bte_rhs=btree_node_get_key(db, node, s+1);
        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*c);

        ham_assert(btree_node_get_count(node)-c <= 0xFFFF, (0));
        ham_assert(btree_node_get_count(sibnode)+c <= 0xFFFF, (0));
        btree_node_set_count(node,
                btree_node_get_count(node)-c);
        btree_node_set_count(sibnode,
                btree_node_get_count(sibnode)+c);

        /*
         * internal nodes: the pointer of the highest item
         * in the node will become the ptr_left of the sibling
         */
        if (intern) {
            bte_lhs=btree_node_get_key(db, node,
                    btree_node_get_count(node)-1);
            btree_node_set_ptr_left(sibnode, key_get_ptr(bte_lhs));

            /*
             * free the extended blob of this key
             */
            if (key_get_flags(bte_lhs)&KEY_IS_EXTENDED) {
                ham_offset_t blobid=key_get_extended_rid(db, bte_lhs);
                ham_assert(blobid, (""));

                st=extkey_remove(db, blobid);
                if (st)
                    return st;
            }
            btree_node_set_count(node, btree_node_get_count(node)-1);
        }

        /*
         * replace the old anchor key with the new anchor key
         */
        if (anchor) {
            btree_key_t *bte;
            ham_key_t key;
            memset(&key, 0, sizeof(key));

            if (intern)
                bte =btree_node_get_key(db, node, s);
            else
                bte =btree_node_get_key(db, sibnode, 0);

            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);

            hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                return st;
            }

            st=my_replace_key(ancpage, slot+1, bte, INTERNAL_KEY, hints);
            if (st) {
                return st;
            }
        }
    }

cleanup:
    /*
     * mark pages as dirty
     */
    page->set_dirty(true);
    ancpage->set_dirty(true);
    sibpage->set_dirty(true);

    scratchpad->mergepage=0;

    return st;
}

static ham_status_t
my_copy_key(Database *db, btree_key_t *lhs, btree_key_t *rhs)
{
    memcpy(lhs, rhs, db_get_int_key_header_size()+db_get_keysize(db));

    /*
     * if the key is extended, we copy the extended blob; otherwise, we'd
     * have to add reference counting to the blob, because two keys are now
     * using the same blobid. this would be too complicated.
     */
    if (key_get_flags(rhs)&KEY_IS_EXTENDED)
    {
        ham_status_t st;
        ham_record_t record;
        ham_offset_t rhsblobid, lhsblobid;

        memset(&record, 0, sizeof(record));

        rhsblobid=key_get_extended_rid(db, rhs);
        st=blob_read(db, rhsblobid, &record, 0);
        if (st)
            return (st);

        st=blob_allocate(db->get_env(), db, &record, 0, &lhsblobid);
        if (st)
            return (st);
        key_set_extended_rid(db, lhs, lhsblobid);
    }

    return (0);
}

static ham_status_t
my_replace_key(Page *page, ham_s32_t slot,
        btree_key_t *rhs, ham_u32_t flags, erase_hints_t *hints)
{
    btree_key_t *lhs;
    ham_status_t st;
    Database *db=page->get_db();
    btree_node_t *node=page_get_btree_node(page);

    hints->cost++;

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;

    lhs=btree_node_get_key(db, node, slot);

    /*
     * if we overwrite an extended key: delete the existing extended blob
     */
    if (key_get_flags(lhs)&KEY_IS_EXTENDED) {
        ham_offset_t blobid=key_get_extended_rid(db, lhs);
        ham_assert(blobid, (""));

        st=extkey_remove(db, blobid);
        if (st)
            return (st);
    }

    key_set_flags(lhs, key_get_flags(rhs));
    memcpy(key_get_key(lhs), key_get_key(rhs), db_get_keysize(db));

    /*
     * internal keys are not allowed to have blob-flags, because only the
     * leaf-node can manage the blob. Therefore we have to disable those
     * flags if we modify an internal key.
     */
    if (flags&INTERNAL_KEY)
        key_set_flags(lhs, key_get_flags(lhs)&
                ~(KEY_BLOB_SIZE_TINY
                    |KEY_BLOB_SIZE_SMALL
                    |KEY_BLOB_SIZE_EMPTY
                    |KEY_HAS_DUPLICATES));

    /*
     * if this key is extended, we copy the extended blob; otherwise, we'd
     * have to add reference counting to the blob, because two keys are now
     * using the same blobid. this would be too complicated.
     */
    if (key_get_flags(rhs)&KEY_IS_EXTENDED) {
        ham_status_t st;
        ham_record_t record;
        ham_offset_t rhsblobid, lhsblobid;

        memset(&record, 0, sizeof(record));

        rhsblobid=key_get_extended_rid(db, rhs);
        st=blob_read(db, rhsblobid, &record, 0);
        if (st)
            return (st);

        st=blob_allocate(db->get_env(), db, &record, 0, &lhsblobid);
        if (st)
            return (st);
        key_set_extended_rid(db, lhs, lhsblobid);
    }

    key_set_size(lhs, key_get_size(rhs));

    page->set_dirty(true);

    return (HAM_SUCCESS);
}

static ham_status_t
my_remove_entry(Page *page, ham_s32_t slot,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_status_t st;
    btree_key_t *bte_lhs, *bte_rhs, *bte;
    btree_node_t *node;
    ham_size_t keysize;
    Database *db;
    btree_cursor_t *btc=0;

    db=page->get_db();
    node=page_get_btree_node(page);
    keysize=db_get_keysize(db);
    bte=btree_node_get_key(db, node, slot);

    if (hints)
        hints->cost++;

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;

    ham_assert(slot>=0, ("invalid slot %ld", slot));
    ham_assert(slot<btree_node_get_count(node), ("invalid slot %ld", slot));

    /*
     * leaf page: get rid of the record
     *
     * if duplicates are enabled and a cursor exists: remove the duplicate
     *
     * otherwise remove the full key with all duplicates
     */
    if (btree_node_is_leaf(node)) {
        Cursor *cursors=db->get_cursors();
        ham_u32_t dupe_id=0;

        if (cursors)
            btc=cursors->get_btree_cursor();

        if (hints) {
            hints->processed_leaf_page = page;
            hints->processed_slot = slot;
        }

        if (scratchpad->cursor)
            dupe_id=btree_cursor_get_dupe_id(scratchpad->cursor)+1;
        else if (scratchpad->dupe_id) /* +1-based index */
            dupe_id=scratchpad->dupe_id;

        if (key_get_flags(bte)&KEY_HAS_DUPLICATES && dupe_id) {
            st=key_erase_record(db, bte, dupe_id-1, 0);
            if (st)
                return st;

            /*
             * if the last duplicate was erased (ptr and flags==0):
             * remove the entry completely
             */
            if (key_get_ptr(bte)==0 && key_get_flags(bte)==0)
                goto free_all;

            /*
             * make sure that no cursor is pointing to this dupe, and shift
             * all other cursors
             *
             * TODO why? all cursors on this page were uncoupled above!
             */
            while (btc && scratchpad->cursor) {
                btree_cursor_t *next=0;
                if (cursors->get_next()) {
                    cursors=cursors->get_next();
                    next=cursors->get_btree_cursor();
                }
                if (btc!=scratchpad->cursor) {
                    if (btree_cursor_get_dupe_id(btc)
                            ==btree_cursor_get_dupe_id(scratchpad->cursor)) {
                        if (btree_cursor_points_to(btc, bte))
                            btree_cursor_set_to_nil(btc);
                    }
                    else if (btree_cursor_get_dupe_id(btc)>
                            btree_cursor_get_dupe_id(scratchpad->cursor)) {
                        btree_cursor_set_dupe_id(btc,
                                btree_cursor_get_dupe_id(btc)-1);
                        memset(btree_cursor_get_dupe_cache(btc), 0,
                                sizeof(dupe_entry_t));
                    }
                }
                btc=next;
            }
    
            /*
             * return immediately
             *
             * TODO why? all cursors on this page were uncoupled above!
             */
            return (0);
        }
        else {
            st=key_erase_record(db, bte, 0, HAM_ERASE_ALL_DUPLICATES);
            if (st)
                return (st);

free_all:
            if (cursors) {
                btc=cursors->get_btree_cursor();

                /*
                 * make sure that no cursor is pointing to this key
                 */
                while (btc) {
                    btree_cursor_t *cur=btc;
                    btree_cursor_t *next=0;
                    if (cursors->get_next()) {
                        cursors=cursors->get_next();
                        next=cursors->get_btree_cursor();
                    }
                    if (btc!=scratchpad->cursor) {
                        if (btree_cursor_points_to(cur, bte))
                            btree_cursor_set_to_nil(cur);
                    }
                    btc=next;
                }
            }
        }
    }

    /*
     * get rid of the extended key (if there is one)
     *
     * also remove the key from the cache
     */
    if (key_get_flags(bte)&KEY_IS_EXTENDED) {
        ham_offset_t blobid=key_get_extended_rid(db, bte);
        ham_assert(blobid, (""));

        st=extkey_remove(db, blobid);
        if (st)
            return (st);
    }

    /*
     * if we delete the last item, it's enough to decrement the item
     * counter and return...
     */
    if (slot != btree_node_get_count(node)-1) {
        if (hints)
            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()
                + keysize)*(btree_node_get_count(node)-slot-1));
        bte_lhs=btree_node_get_key(db, node, slot);
        bte_rhs=btree_node_get_key(db, node, slot+1);
        memmove(bte_lhs, bte_rhs, ((db_get_int_key_header_size()+keysize))*
                (btree_node_get_count(node)-slot-1));
    }

    btree_node_set_count(node, btree_node_get_count(node)-1);

    page->set_dirty(true);

    return (0);
}

ham_status_t
btree_erase(ham_btree_t *be, ham_key_t *key, ham_u32_t flags)
{
    return (btree_erase_impl(be, key, 0, 0, flags));
}

ham_status_t
btree_erase_duplicate(ham_btree_t *be, ham_key_t *key, ham_u32_t dupe_id,
        ham_u32_t flags)
{
    return (btree_erase_impl(be, key, 0, dupe_id, flags));
}

ham_status_t
btree_erase_cursor(ham_btree_t *be, ham_key_t *key,
        btree_cursor_t *cursor, ham_u32_t flags)
{
    return (btree_erase_impl(be, key, cursor, 0, flags));
}

ham_status_t
btree_cursor_erase_fasttrack(ham_btree_t *be, btree_cursor_t *cursor)
{
    erase_scratchpad_t scratchpad;

    ham_assert(btree_cursor_is_coupled(cursor), (""));

    /* initialize the scratchpad */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.cursor=cursor;

    return (my_remove_entry(btree_cursor_get_coupled_page(cursor),
                btree_cursor_get_coupled_index(cursor),
                &scratchpad, 0));
}
