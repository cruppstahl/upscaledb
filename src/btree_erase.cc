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
#include "btree_node.h"

namespace ham {

/*
 * the erase_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct erase_scratchpad_t
{
    /*
     * the backend pointer
     */
    BtreeBackend *be;

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

    /* the current transaction */
    Transaction *txn;

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
                BtreeStatistics::EraseHints *hints);

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
             erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints);

/*
 * merge two pages
 */
static ham_status_t
my_merge_pages(Page **newpage_ref, Page *page, Page *sibling, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints);

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
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints);

/*
 * copy a key
 */
static ham_status_t
my_copy_key(Database *db, Transaction *txn, BtreeKey *lhs, BtreeKey *rhs);

/*
 * replace two keys in a page
 */
static ham_status_t
my_replace_key(Page *page, ham_s32_t slot, BtreeKey *newentry,
        ham_u32_t flags, erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints);

/*
 * remove an item from a page
 */
static ham_status_t
my_remove_entry(Page *page, ham_s32_t slot,
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints);

/*
 * flags for my_replace_key
 */
/* #define NOFLUSH 1  -- unused */
#define INTERNAL_KEY 2

static ham_status_t
btree_erase_impl(BtreeBackend *be, Transaction *txn, ham_key_t *key,
        btree_cursor_t *cursor, ham_u32_t dupe_id, ham_u32_t flags)
{
    ham_status_t st;
    Page *root;
    Page *p;
    ham_offset_t rootaddr;
    Database *db=be->get_db();
    erase_scratchpad_t scratchpad;

    /*
     * initialize the scratchpad
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.key=key;
    scratchpad.flags=flags;
    scratchpad.cursor=cursor;
    scratchpad.dupe_id=dupe_id;
    scratchpad.txn=txn;

    BtreeStatistics::EraseHints hints = be->get_statistics()->get_erase_hints(
                    flags, key);

    if (hints.key_is_out_of_bounds)
    {
        be->get_statistics()->update_failed_oob(HAM_OPERATION_STATS_ERASE,
                    hints.try_fast_track);
        return (HAM_KEY_NOT_FOUND);
    }

    if (hints.try_fast_track)
    {
        /* TODO */
    }

    /*
     * get the root-page...
     */
    rootaddr=be->get_rootpage();
    if (!rootaddr) {
        be->get_statistics()->update_failed(HAM_OPERATION_STATS_ERASE,
                    hints.try_fast_track);
        return HAM_KEY_NOT_FOUND;
    }
    st=db_fetch_page(&root, db, rootaddr, 0);
    if (st)
        return (st);

    /* ... and start the recursion */
    st=my_erase_recursive(&p, root, 0, 0, 0, 0, 0, &scratchpad, &hints);
    if (st) {
        be->get_statistics()->update_failed(HAM_OPERATION_STATS_ERASE,
                    hints.try_fast_track);
        return (st);
    }

    if (p) {
        ham_status_t st;

        /*
         * delete the old root page
         */
        st=btree_uncouple_all_cursors(root, 0);
        if (st)
        {
            be->get_statistics()->update_failed(HAM_OPERATION_STATS_ERASE,
                        hints.try_fast_track);
            return (st);
        }

        st=__collapse_root(p, &scratchpad);
        if (st) {
            be->get_statistics()->update_failed(HAM_OPERATION_STATS_ERASE,
                        hints.try_fast_track);
            return (st);
        }

        be->get_statistics()->reset_page(root, false);
    }

    // TODO merge the following two calls
    be->get_statistics()->update_succeeded(HAM_OPERATION_STATS_ERASE,
                    hints.processed_leaf_page, hints.try_fast_track);
    be->get_statistics()->update_any_bound(HAM_OPERATION_STATS_ERASE,
                    hints.processed_leaf_page, key,
                    hints.flags, hints.processed_slot);
    return (0);
}

static ham_status_t
my_erase_recursive(Page **page_ref, Page *page, ham_offset_t left, ham_offset_t right,
        ham_offset_t lanchor, ham_offset_t ranchor, Page *parent,
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints)
{
    ham_s32_t slot;
    ham_bool_t isfew;
    ham_status_t st;
    Page *newme;
    Page *child;
    Page *tempp=0;
    Database *db=page->get_db();
    BtreeNode *node=BtreeNode::from_page(page);

    *page_ref = 0;

    /*
     * empty node? then most likely we're in the empty root page.
     */
    if (node->get_count()==0)
        return HAM_KEY_NOT_FOUND;

    /*
     * mark the nodes which may need rebalancing
     */
    if (scratchpad->be->get_rootpage()==page->get_self())
        isfew=(node->get_count()<=1);
    else
        isfew=(node->get_count()<scratchpad->be->get_minkeys());

    if (!isfew) /* [i_a] name does not represent value; cf. code in btree_check */
        scratchpad->mergepage=0;
    else if (!scratchpad->mergepage)
        scratchpad->mergepage=page;

    if (!node->is_leaf()) {
        st=scratchpad->be->find_internal(page, scratchpad->key, &child, &slot);
        if (st)
            return (st);
    }
    else {
        st=scratchpad->be->get_slot(page, scratchpad->key, &slot);
        if (st)
            return st;
        child=0;
    }

    /*
     * if this page is not a leaf: recursively descend down the tree
     */
    if (!node->is_leaf())
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
                BtreeKey *bte;
                BtreeNode *n;
                st=db_fetch_page(&tempp, db, left, 0);
                if (st)
                    return (st);
                n=BtreeNode::from_page(tempp);
                bte=n->get_key(db, n->get_count()-1);
                next_left=bte->get_ptr();
            }
            next_lanchor=lanchor;
        }
        else {
            if (slot==0)
                next_left=node->get_ptr_left();
            else {
                BtreeKey *bte;
                bte=node->get_key(db, slot-1);
                next_left=bte->get_ptr();
            }
            next_lanchor=page->get_self();
        }

        if (slot==node->get_count()-1) {
            if (!right)
                next_right=0;
            else {
                BtreeKey *bte;
                BtreeNode *n;
                st=db_fetch_page(&tempp, db, right, 0);
                if (st)
                    return (st);
                n=BtreeNode::from_page(tempp);
                bte=n->get_key(db, 0);
                next_right=bte->get_ptr();
            }
            next_ranchor=ranchor;
        }
        else {
            BtreeKey *bte;
            bte=node->get_key(db, slot+1);
            next_right=bte->get_ptr();
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
         * TODO is this compare_keys() call really necessary??
         */
        newme=0;
        if (slot!=-1)
        {
            int cmp=scratchpad->be->compare_keys(page, scratchpad->key, slot);
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
    ham_assert(!st);
    return my_rebalance(page_ref, page, left, right, lanchor, ranchor, parent,
                scratchpad, hints);
}

static ham_status_t
__collapse_root(Page *newroot, erase_scratchpad_t *scratchpad)
{
    Environment *env;

    scratchpad->be->set_rootpage( newroot->get_self());
    scratchpad->be->do_flush_indexdata();
    ham_assert(newroot->get_db());

    env=newroot->get_db()->get_env();
    ham_assert(env!=0);
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
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints)
{
    ham_status_t st;
    BtreeNode *node=BtreeNode::from_page(page);
    Page *leftpage=0;
    Page *rightpage=0;
    BtreeNode *leftnode=0;
    BtreeNode *rightnode=0;
    ham_bool_t fewleft=HAM_FALSE;
    ham_bool_t fewright=HAM_FALSE;
    ham_size_t minkeys=scratchpad->be->get_minkeys();

    ham_assert(page->get_db());

    *newpage_ref = 0;
    if (!scratchpad->mergepage)
        return (0);

    /*
     * get the left and the right sibling of this page
     */
    if (left)
    {
        st = db_fetch_page(&leftpage, page->get_db(),
                        node->get_left(), 0);
        if (st)
            return st;
        if (leftpage) {
            leftnode =BtreeNode::from_page(leftpage);
            fewleft  =(leftnode->get_count()<=minkeys);
        }
    }
    if (right)
    {
        st = db_fetch_page(&rightpage, page->get_db(),
                        node->get_right(), 0);
        if (st)
            return st;
        if (rightpage) {
            rightnode=BtreeNode::from_page(rightpage);
            fewright =(rightnode->get_count()<=minkeys);
        }
    }

    /*
     * if we have no siblings, then we're rebalancing the root page
     */
    if (!leftpage && !rightpage) {
        if (node->is_leaf()) {
            return (0);
        }
        else {
            return (db_fetch_page(newpage_ref,
                        page->get_db(),
                        node->get_ptr_left(), 0));
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
                && leftnode->get_count()
                    <=rightnode->get_count()) {
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
            BtreeStatistics::EraseHints *hints)
{
    ham_status_t st;
    ham_s32_t slot;
    ham_size_t c, keysize;
    Database *db=page->get_db();
    Page *ancpage;
    BtreeNode *node, *sibnode, *ancnode;
    BtreeKey *bte_lhs, *bte_rhs;

    ham_assert(db);

    keysize=db_get_keysize(db);
    node   =BtreeNode::from_page(page);
    sibnode=BtreeNode::from_page(sibpage);

    if (anchor) {
        st=db_fetch_page(&ancpage, page->get_db(), anchor, 0);
        if (st)
            return (st);
        ancnode=BtreeNode::from_page(ancpage);
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
    if (!node->is_leaf()) {
        BtreeKey *bte;
        ham_key_t key;

        bte =sibnode->get_key(db, 0);
        memset(&key, 0, sizeof(key));
        key._flags=bte->get_flags();
        key.data  =bte->get_key();
        key.size  =bte->get_size();

        st=scratchpad->be->get_slot(ancpage, &key, &slot);
        if (st) {
            return st;
        }

        bte_lhs=node->get_key(db, node->get_count());
        bte_rhs=ancnode->get_key(db, slot);

        st=my_copy_key(db, scratchpad->txn, bte_lhs, bte_rhs);
        if (st) {
            return st;
        }
        bte_lhs->set_ptr(sibnode->get_ptr_left());
        node->set_count(node->get_count()+1);
    }

    c=sibnode->get_count();
    bte_lhs=node->get_key(db, node->get_count());
    bte_rhs=sibnode->get_key(db, 0);

    /*
     * shift items from the sibling to this page
     */
    memcpy(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*c);

    /*
     * as sibnode is merged into node, we will also need to ensure that our
     * statistics node/page tracking is corrected accordingly: what was in
     * sibnode, is now in node. And sibnode will be destroyed at the end.
     */
    if (sibpage == hints->processed_leaf_page) {
        /* sibnode slot 0 has become node slot 'bte_lhs' */
        hints->processed_slot += node->get_count();
        hints->processed_leaf_page = page;
    }

    page->set_dirty(true);
    sibpage->set_dirty(true);
    ham_assert(node->get_count()+c <= 0xFFFF);
    node->set_count(node->get_count()+c);
    sibnode->set_count(0);

    /*
     * update the linked list of pages
     */
    if (node->get_left()==sibpage->get_self()) {
        if (sibnode->get_left()) {
            Page *p;
            BtreeNode *n;

            st=db_fetch_page(&p, page->get_db(),
                    sibnode->get_left(), 0);
            if (st)
                return (st);
            n=BtreeNode::from_page(p);
            n->set_right(sibnode->get_right());
            node->set_left(sibnode->get_left());
            p->set_dirty(true);
        }
        else
            node->set_left(0);
    }
    else if (node->get_right()==sibpage->get_self()) {
        if (sibnode->get_right()) {
            Page *p;
            BtreeNode *n;

            st=db_fetch_page(&p, page->get_db(),
                    sibnode->get_right(), 0);
            if (st)
                return (st);
            n=BtreeNode::from_page(p);

            node->set_right(sibnode->get_right());
            n->set_left(sibnode->get_left());
            p->set_dirty(true);
        }
        else
            node->set_right(0);
    }

    /*
     * return this page for deletion
     */
    if (scratchpad->mergepage &&
           (scratchpad->mergepage->get_self()==page->get_self() ||
            scratchpad->mergepage->get_self()==sibpage->get_self()))
        scratchpad->mergepage=0;

    scratchpad->be->get_statistics()->reset_page(sibpage, false);

    /*
     * delete the page
     * TODO
     */
    ham_assert(hints->processed_leaf_page != sibpage);

    *newpage_ref = sibpage;
    return (HAM_SUCCESS);
}

static ham_status_t
my_shift_pages(Page **newpage_ref, Page *page, Page *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints)
{
    ham_s32_t slot=0;
    ham_status_t st;
    ham_bool_t intern;
    ham_size_t s;
    ham_size_t c;
    ham_size_t keysize;
    Database *db=page->get_db();
    Page *ancpage;
    BtreeNode *node, *sibnode, *ancnode;
    BtreeKey *bte_lhs, *bte_rhs;

    node   =BtreeNode::from_page(page);
    sibnode=BtreeNode::from_page(sibpage);
    keysize=db_get_keysize(db);
    intern =!node->is_leaf();
    st=db_fetch_page(&ancpage, db, anchor, 0);
    if (st)
        return (st);
    ancnode=BtreeNode::from_page(ancpage);

    ham_assert(node->get_count()!=sibnode->get_count());

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
    if (sibnode->get_count()>=node->get_count())
    {
        /*
         * internal node: insert the anchornode separator value to
         * this node
         */
        if (intern)
        {
            BtreeKey *bte;
            ham_key_t key;

            bte=sibnode->get_key(db, 0);
            memset(&key, 0, sizeof(key));
            key._flags=bte->get_flags();
            key.data  =bte->get_key();
            key.size  =bte->get_size();
            st=scratchpad->be->get_slot(ancpage, &key, &slot);
            if (st) {
                return st;
            }

            /*
             * append the anchor node to the page
             */
            bte_rhs=ancnode->get_key(db, slot);
            bte_lhs=node->get_key(db, node->get_count());

            st=my_copy_key(db, scratchpad->txn, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            /*
             * the pointer of this new node is ptr_left of the sibling
             */
            bte_lhs->set_ptr(sibnode->get_ptr_left());

            /*
             * new pointer left of the sibling is sibling[0].ptr
             */
            sibnode->set_ptr_left(bte->get_ptr());

            /*
             * update the anchor node with sibling[0]
             */
            (void)my_replace_key(ancpage, slot, bte, INTERNAL_KEY, scratchpad, hints);

            /*
             * shift the remainder of sibling to the left
             */
            bte_lhs=sibnode->get_key(db, 0);
            bte_rhs=sibnode->get_key(db, 1);
            memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)
                    * (sibnode->get_count()-1));

            /*
             * adjust counters
             */
            node->set_count(node->get_count()+1);
            sibnode->set_count(sibnode->get_count()-1);
        }

        c=(sibnode->get_count()-node->get_count())/2;
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
            bte_lhs=node->get_key(db, node->get_count());
            bte_rhs=ancnode->get_key(db, slot);

            st=my_copy_key(db, scratchpad->txn, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            bte_lhs->set_ptr(sibnode->get_ptr_left());
            node->set_count(node->get_count()+1);
        }

        /*
         * shift items from the sibling to this page, then
         * delete the shifted items
         */
        bte_lhs=node->get_key(db, node->get_count());
        bte_rhs=sibnode->get_key(db, 0);

        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*c);

        bte_lhs=sibnode->get_key(db, 0);
        bte_rhs=sibnode->get_key(db, c);
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*
                (sibnode->get_count()-c));

        /*
         * internal nodes: don't forget to set ptr_left of the sibling, and
         * replace the anchor key
         */
        if (intern)
        {
            BtreeKey *bte;
            bte=sibnode->get_key(db, 0);
            sibnode->set_ptr_left(bte->get_ptr());
            if (anchor)
            {
                ham_key_t key;
                memset(&key, 0, sizeof(key));
                key._flags=bte->get_flags();
                key.data  =bte->get_key();
                key.size  =bte->get_size();
                st=scratchpad->be->get_slot(ancpage, &key, &slot);
                if (st) {
                    return st;
                }
                /* replace the key */
                st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY,  scratchpad,hints);
                if (st) {
                    return st;
                }
            }
            /*
             * shift once more
             */
            bte_lhs=sibnode->get_key(db, 0);
            bte_rhs=sibnode->get_key(db, 1);
            memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*
                    (sibnode->get_count()-1));
        }
        else
        {
            /*
             * in a leaf - update the anchor
             */
            ham_key_t key;
            BtreeKey *bte;
            bte=sibnode->get_key(db, 0);
            memset(&key, 0, sizeof(key));
            key._flags=bte->get_flags();
            key.data  =bte->get_key();
            key.size  =bte->get_size();
            st=scratchpad->be->get_slot(ancpage, &key, &slot);
            if (st) {
                return st;
            }
            /* replace the key */
            st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY, scratchpad, hints);
            if (st) {
                return st;
            }
        }

        /*
         * update the page counter
         */
        ham_assert(node->get_count()+c <= 0xFFFF);
        ham_assert(sibnode->get_count()-c-(intern ? 1 : 0) <= 0xFFFF);
        node->set_count(node->get_count()+c);
        sibnode->set_count(sibnode->get_count()-c-(intern ? 1 : 0));
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
            BtreeKey *bte;
            ham_key_t key;

            bte =sibnode->get_key(db, 0);
            memset(&key, 0, sizeof(key));
            key._flags=bte->get_flags();
            key.data  =bte->get_key();
            key.size  =bte->get_size();
            st=scratchpad->be->get_slot(ancpage, &key, &slot);
            if (st) {
                return st;
            }

            /*
             * shift entire sibling by 1 to the right
             */
            bte_lhs=sibnode->get_key(db, 1);
            bte_rhs=sibnode->get_key(db, 0);
            memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)
                    * (sibnode->get_count()));

            /*
             * copy the old anchor element to sibling[0]
             */
            bte_lhs=sibnode->get_key(db, 0);
            bte_rhs=ancnode->get_key(db, slot);

            st=my_copy_key(db, scratchpad->txn, bte_lhs, bte_rhs);
            if (st) {
                return st;
            }

            /*
             * sibling[0].ptr = sibling.ptr_left
             */
            bte_lhs->set_ptr(sibnode->get_ptr_left());

            /*
             * sibling.ptr_left = node[node.count-1].ptr
             */
            bte_lhs=node->get_key(db, node->get_count()-1);
            sibnode->set_ptr_left(bte_lhs->get_ptr());

            /*
             * new anchor element is node[node.count-1].key
             */
            st=my_replace_key(ancpage, slot, bte_lhs, INTERNAL_KEY, scratchpad, hints);
            if (st) {
                return st;
            }

            /*
             * page: one item less; sibling: one item more
             */
            node->set_count(node->get_count()-1);
            sibnode->set_count(sibnode->get_count()+1);
        }

        c=(node->get_count()-sibnode->get_count())/2;
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
            bte_lhs=sibnode->get_key(db, 1);
            bte_rhs=sibnode->get_key(db, 0);
            memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)
                    * (sibnode->get_count()));

            bte_lhs=sibnode->get_key(db, 0);
            bte_rhs=ancnode->get_key(db, slot);

            /* clear the key - we don't want my_replace_key to free
             * an extended block which is still used by sibnode[1] */
            memset(bte_lhs, 0, sizeof(*bte_lhs));

            st=my_replace_key(sibpage, 0, bte_rhs,
                    (node->is_leaf() ? 0 : INTERNAL_KEY),
                    scratchpad, hints);
            if (st) {
                return st;
            }

            bte_lhs->set_ptr(sibnode->get_ptr_left());
            sibnode->set_count(sibnode->get_count()+1);
        }

        s=node->get_count()-c-1;

        /*
         * shift items from this page to the sibling, then delete the
         * items from this page
         */
        bte_lhs=sibnode->get_key(db, c);
        bte_rhs=sibnode->get_key(db, 0);
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*
                sibnode->get_count());

        bte_lhs=sibnode->get_key(db, 0);
        bte_rhs=node->get_key(db, s+1);
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)*c);

        ham_assert(node->get_count()-c <= 0xFFFF);
        ham_assert(sibnode->get_count()+c <= 0xFFFF);
        node->set_count(node->get_count()-c);
        sibnode->set_count(sibnode->get_count()+c);

        /*
         * internal nodes: the pointer of the highest item
         * in the node will become the ptr_left of the sibling
         */
        if (intern) {
            bte_lhs=node->get_key(db, node->get_count()-1);
            sibnode->set_ptr_left(bte_lhs->get_ptr());

            /*
             * free the extended blob of this key
             */
            if (bte_lhs->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
                ham_offset_t blobid=bte_lhs->get_extended_rid(db);
                ham_assert(blobid);

                st=db->remove_extkey(blobid);
                if (st)
                    return st;
            }
            node->set_count(node->get_count()-1);
        }

        /*
         * replace the old anchor key with the new anchor key
         */
        if (anchor) {
            BtreeKey *bte;
            ham_key_t key;
            memset(&key, 0, sizeof(key));

            if (intern)
                bte =node->get_key(db, s);
            else
                bte =sibnode->get_key(db, 0);

            key._flags=bte->get_flags();
            key.data  =bte->get_key();
            key.size  =bte->get_size();

            st=scratchpad->be->get_slot(ancpage, &key, &slot);
            if (st) {
                return st;
            }

            st=my_replace_key(ancpage, slot+1, bte, INTERNAL_KEY, scratchpad, hints);
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
my_copy_key(Database *db, Transaction *txn, BtreeKey *lhs, BtreeKey *rhs)
{
    memcpy(lhs, rhs, BtreeKey::ms_sizeof_overhead+db_get_keysize(db));

    /*
     * if the key is extended, we copy the extended blob; otherwise, we'd
     * have to add reference counting to the blob, because two keys are now
     * using the same blobid. this would be too complicated.
     */
    if (rhs->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
        ham_status_t st;
        ham_record_t record;
        ham_offset_t rhsblobid, lhsblobid;

        memset(&record, 0, sizeof(record));

        rhsblobid=rhs->get_extended_rid(db);
        st=db->get_env()->get_blob_manager()->read(db, txn, rhsblobid,
                                &record, 0);
        if (st)
            return (st);

        st=db->get_env()->get_blob_manager()->allocate(db, &record,
                                0, &lhsblobid);
        if (st)
            return (st);
        lhs->set_extended_rid(db, lhsblobid);
    }

    return (0);
}

static ham_status_t
my_replace_key(Page *page, ham_s32_t slot, BtreeKey *rhs,
        ham_u32_t flags, erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints)
{
    BtreeKey *lhs;
    ham_status_t st;
    Database *db=page->get_db();
    BtreeNode *node=BtreeNode::from_page(page);

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;

    lhs=node->get_key(db, slot);

    /*
     * if we overwrite an extended key: delete the existing extended blob
     */
    if (lhs->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
        ham_offset_t blobid=lhs->get_extended_rid(db);
        ham_assert(blobid);

        st=db->remove_extkey(blobid);
        if (st)
            return (st);
    }

    lhs->set_flags(rhs->get_flags());
    memcpy(lhs->get_key(), rhs->get_key(), db_get_keysize(db));

    /*
     * internal keys are not allowed to have blob-flags, because only the
     * leaf-node can manage the blob. Therefore we have to disable those
     * flags if we modify an internal key.
     */
    if (flags&INTERNAL_KEY)
        lhs->set_flags(lhs->get_flags() &
                ~(BtreeKey::KEY_BLOB_SIZE_TINY
                    |BtreeKey::KEY_BLOB_SIZE_SMALL
                    |BtreeKey::KEY_BLOB_SIZE_EMPTY
                    |BtreeKey::KEY_HAS_DUPLICATES));

    /*
     * if this key is extended, we copy the extended blob; otherwise, we'd
     * have to add reference counting to the blob, because two keys are now
     * using the same blobid. this would be too complicated.
     */
    if (rhs->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
        ham_status_t st;
        ham_record_t record;
        ham_offset_t rhsblobid, lhsblobid;

        memset(&record, 0, sizeof(record));

        rhsblobid=rhs->get_extended_rid(db);
        st=db->get_env()->get_blob_manager()->read(db, scratchpad->txn,
                                rhsblobid, &record, 0);
        if (st)
            return (st);

        st=db->get_env()->get_blob_manager()->allocate(db, &record, 0,
                                &lhsblobid);
        if (st)
            return (st);
        lhs->set_extended_rid(db, lhsblobid);
    }

    lhs->set_size(rhs->get_size());

    page->set_dirty(true);

    return (HAM_SUCCESS);
}

static ham_status_t
my_remove_entry(Page *page, ham_s32_t slot,
        erase_scratchpad_t *scratchpad, BtreeStatistics::EraseHints *hints)
{
    ham_status_t st;
    BtreeKey *bte_lhs, *bte_rhs, *bte;
    BtreeNode *node;
    ham_size_t keysize;
    Database *db;
    btree_cursor_t *btc=0;

    db=page->get_db();
    node=BtreeNode::from_page(page);
    keysize=db_get_keysize(db);
    bte=node->get_key(db, slot);

    /*
     * uncouple all cursors
     */
    if ((st=btree_uncouple_all_cursors(page, 0)))
        return st;

    ham_assert(slot>=0);
    ham_assert(slot<node->get_count());

    /*
     * leaf page: get rid of the record
     *
     * if duplicates are enabled and a cursor exists: remove the duplicate
     *
     * otherwise remove the full key with all duplicates
     */
    if (node->is_leaf()) {
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

        if (bte->get_flags()&BtreeKey::KEY_HAS_DUPLICATES && dupe_id) {
            st=bte->erase_record(db, scratchpad->txn, dupe_id-1, false);
            if (st)
                return st;

            /*
             * if the last duplicate was erased (ptr and flags==0):
             * remove the entry completely
             */
            if (bte->get_ptr()==0 && bte->get_flags()==0)
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
            st=bte->erase_record(db, scratchpad->txn, 0, true);
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
    if (bte->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
        ham_offset_t blobid=bte->get_extended_rid(db);
        ham_assert(blobid);

        st=db->remove_extkey(blobid);
        if (st)
            return (st);
    }

    /*
     * if we delete the last item, it's enough to decrement the item
     * counter and return...
     */
    if (slot != node->get_count()-1) {
        bte_lhs=node->get_key(db, slot);
        bte_rhs=node->get_key(db, slot+1);
        memmove(bte_lhs, bte_rhs, ((BtreeKey::ms_sizeof_overhead+keysize))*
                (node->get_count()-slot-1));
    }

    node->set_count(node->get_count()-1);

    page->set_dirty(true);

    return (0);
}

ham_status_t
BtreeBackend::do_erase(Transaction *txn, ham_key_t *key, ham_u32_t flags)
{
    return (btree_erase_impl(this, txn, key, 0, 0, flags));
}

ham_status_t
BtreeBackend::erase_duplicate(Transaction *txn, ham_key_t *key,
        ham_u32_t dupe_id, ham_u32_t flags)
{
    return (btree_erase_impl(this, txn, key, 0, dupe_id, flags));
}

ham_status_t
BtreeBackend::do_erase_cursor(Transaction *txn, ham_key_t *key,
        Cursor *cursor, ham_u32_t flags)
{
    return (btree_erase_impl(this, txn, key, cursor->get_btree_cursor(),
                        0, flags));
}

ham_status_t
BtreeBackend::cursor_erase_fasttrack(Transaction *txn,
        btree_cursor_t *cursor)
{
    erase_scratchpad_t scratchpad;

    ham_assert(btree_cursor_is_coupled(cursor));

    /* initialize the scratchpad */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=this;
    scratchpad.txn=txn;
    scratchpad.cursor=cursor;

    return (my_remove_entry(btree_cursor_get_coupled_page(cursor),
                btree_cursor_get_coupled_index(cursor),
                &scratchpad, 0));
}

} // namespace ham
