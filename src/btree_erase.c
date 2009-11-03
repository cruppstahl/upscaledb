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
 * btree erasing
 *
 */

#include "config.h"

#include <string.h>
#include "db.h"
#include "error.h"
#include "page.h"
#include "btree.h"
#include "mem.h"
#include "util.h"
#include "keys.h"
#include "extkeys.h"
#include "blob.h"

/*
 * the erase_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct
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
    ham_page_t *mergepage;

    /*
     * a coupled cursor (can be NULL)
     */
    ham_bt_cursor_t *cursor;

} erase_scratchpad_t;

/**
 * recursively descend down the tree, delete the item and re-balance 
 * the tree on the way back up
 *
 * returns the page which is deleted, if available
 */
static ham_page_t *
my_erase_recursive(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * collapse the root node
 */
static ham_status_t
my_collapse_root(ham_page_t *root, erase_scratchpad_t *scratchpad);

/**
 * rebalance a page - either shifts elements to a sibling, or merges 
 * the page with a sibling
 */
static ham_page_t *
my_rebalance(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * merge two pages
 */
static ham_page_t *
my_merge_pages(ham_page_t *page, ham_page_t *sibling, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * shift items from a sibling to this page, till both pages have an equal 
 * number of items
 */
static ham_page_t *
my_shift_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * copy a key
 */
static ham_status_t
my_copy_key(ham_db_t *db, int_key_t *lhs, int_key_t *rhs);

/*
 * replace two keys in a page 
 */
static ham_status_t
my_replace_key(ham_page_t *page, ham_s32_t slot, 
        int_key_t *newentry, ham_u32_t flags, erase_hints_t *hints);

/*
 * remove an item from a page 
 */
static ham_status_t
my_remove_entry(ham_page_t *page, ham_s32_t slot, 
        erase_scratchpad_t *scratchpad, erase_hints_t *hints);

/*
 * flags for my_replace_key
 */
/* #define NOFLUSH 1  -- unused */
#define INTERNAL_KEY 2


ham_status_t
btree_erase(ham_btree_t *be, ham_key_t *key, ham_u32_t flags)
{
    return (btree_erase_cursor(be, key, 0, flags));
}

ham_status_t
btree_erase_cursor(ham_btree_t *be, ham_key_t *key, 
        ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    ham_page_t *root;
	ham_page_t *p;
    ham_offset_t rootaddr;
    ham_db_t *db=btree_get_db(be);
    erase_scratchpad_t scratchpad;
	erase_hints_t hints = {flags, flags, cursor, 0, HAM_FALSE, HAM_FALSE, 0, 
                            NULL, -1};

    /* 
     * initialize the scratchpad 
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.key=key;
    scratchpad.flags=flags;
    scratchpad.cursor=cursor;

	btree_erase_get_hints(&hints, db, key);

	if (hints.key_is_out_of_bounds)
	{
		stats_update_erase_fail_oob(db, &hints);
		return (db_set_error(db, HAM_KEY_NOT_FOUND));
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
		stats_update_erase_fail(db, &hints);
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
	}
    root=db_fetch_page(db, rootaddr, flags);
    if (!root)
	{
		stats_update_erase_fail(db, &hints);
        return (db_get_error(db));
	}

    db_set_error(db, 0);

    /* 
     * ... and start the recursion 
     */
    p=my_erase_recursive(root, 0, 0, 0, 0, 0, &scratchpad, &hints);
    if (db_get_error(db))
	{
		stats_update_erase_fail(db, &hints);
        return (db_get_error(db));
	}

    if (p) 
	{
		ham_status_t st;

        /* 
         * delete the old root page 
         */
        st=db_uncouple_all_cursors(root, 0);
        if (st)
		{
			stats_update_erase_fail(db, &hints);
            return (st);
		}

        st=my_collapse_root(p, &scratchpad);
        if (st)
		{
			stats_update_erase_fail(db, &hints);
            return (st);
		}

		stats_page_is_nuked(db, root, HAM_FALSE);

        st=txn_free_page(db_get_txn(db), root);
        if (st)
		{
			stats_update_erase_fail(db, &hints);
            return (st);
		}
    }

	stats_update_erase(db, hints.processed_leaf_page, &hints);
	stats_update_any_bound(db, hints.processed_leaf_page, key, hints.flags, 
                    hints.processed_slot);
    return (0);
}

static ham_page_t *
my_erase_recursive(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_s32_t slot;
    ham_bool_t isfew;
    ham_status_t st;
    ham_page_t *newme;
	ham_page_t *child;
	ham_page_t *tempp=0;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);

    /* 
     * empty node? then most likely we're in the empty root page.
     */
    if (btree_node_get_count(node)==0) {
        db_set_error(db, HAM_KEY_NOT_FOUND);
        return 0;
    }

    /*
     * mark the nodes which may need rebalancing
     */
    if (btree_get_rootpage(scratchpad->be)==page_get_self(page))
        isfew=(btree_node_get_count(node)<=1);
    else
        isfew=(btree_node_get_count(node)<btree_get_minkeys(maxkeys)); 

    if (!isfew) /* [i_a] name does not represent value; cf. code in btree_check */
        scratchpad->mergepage=0;
    else if (!scratchpad->mergepage)
        scratchpad->mergepage=page;

    if (!btree_node_is_leaf(node)) 
	{
        child=btree_traverse_tree(db, page, scratchpad->key, &slot);
        ham_assert(child!=0, ("guru meditation error"));
    }
    else 
	{
		hints->cost++;
        st=btree_get_slot(db, page, scratchpad->key, &slot, 0);
        if (st) {
            db_set_error(db, st);
            return 0;
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
                int_key_t *bte; 
                btree_node_t *n;
                tempp=db_fetch_page(db, left, 0);
                n=ham_page_get_btree_node(tempp);
                bte=btree_node_get_key(db, n, btree_node_get_count(n)-1);
                next_left=key_get_ptr(bte);
            }
            next_lanchor=lanchor;
        }
        else {
            if (slot==0)
                next_left=btree_node_get_ptr_left(node);
            else {
                int_key_t *bte; 
                bte=btree_node_get_key(db, node, slot-1);
                next_left=key_get_ptr(bte);
            }
            next_lanchor=page_get_self(page);
        }

        if (slot==btree_node_get_count(node)-1) {
            if (!right)
                next_right=0;
            else {
                int_key_t *bte; 
                btree_node_t *n;
                tempp=db_fetch_page(db, right, 0);
                n=ham_page_get_btree_node(tempp);
                bte=btree_node_get_key(db, n, 0);
                next_right=key_get_ptr(bte);
            }
            next_ranchor=ranchor;
        }
        else {
            int_key_t *bte; 
            bte=btree_node_get_key(db, node, slot+1);
            next_right=key_get_ptr(bte);
            next_ranchor=page_get_self(page);
        }

        newme=my_erase_recursive(child, next_left, next_right, next_lanchor, 
                    next_ranchor, page, scratchpad, hints);
    }
    else 
	{
		/*
		 * otherwise (page is a leaf) delete the key...
		 */

        /*
         * check if this entry really exists
         */
        newme=0;
        if (slot!=-1) 
		{
            int cmp;
            int_key_t *bte;
			ham_key_t rhs;

            bte=btree_node_get_key(db, node, slot);

			st = db_prepare_ham_key_for_compare(db, bte, &rhs);
			if (st)
				return 0;

			cmp=db_compare_keys(db, scratchpad->key, &rhs);

			db_release_ham_key_after_compare(db, &rhs);
			if (db_get_error(db))
				return 0;
			
            if (cmp==0)
			{
                newme=page;
			}
            else 
			{
                db_set_error(db, HAM_KEY_NOT_FOUND);
                return (0);
            }
        }
        if (!newme) 
		{
            db_set_error(db, HAM_KEY_NOT_FOUND);
            scratchpad->mergepage=0;
            return (0);
        }
    }

    /*
     * ... and rebalance the tree, if necessary
     */
    if (newme) 
	{
        if (slot==-1)
            slot=0;
        st=my_remove_entry(page, slot, scratchpad, hints);
        if (st)
            return (0);
    }

    /*
     * no need to rebalance in case of an error
     */
    if (!db_get_error(db))
        return (my_rebalance(page, left, right, lanchor, ranchor, parent, 
                scratchpad, hints));
    else
        return (0);
}

static ham_status_t
my_collapse_root(ham_page_t *newroot, erase_scratchpad_t *scratchpad)
{
	ham_db_t *db;

    btree_set_rootpage(scratchpad->be, page_get_self(newroot));
    be_set_dirty(scratchpad->be, HAM_TRUE);
	db = page_get_owner(newroot);
    db_set_dirty(db);
	if (db_get_cache(db) && (page_get_type(newroot) != PAGE_TYPE_B_ROOT))
	{
		ham_cache_t *cache = db_get_cache(db);
        /*
         * As we re-purpose a page, we will reset its pagecounter as
         * well to signal its first use as the new type assigned here.
         */
		ham_assert(cache, (0));
	    page_set_cache_cntr(newroot, cache->_timeslot++);
		cache_update_page_access_counter(newroot, cache); /* bump up */
	}
    page_set_type(newroot, PAGE_TYPE_B_ROOT);
    return (0);
}

static ham_page_t *
my_rebalance(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_page_t *leftpage, *rightpage;
    btree_node_t *leftnode=0, *rightnode=0;
    ham_bool_t fewleft=HAM_FALSE, fewright=HAM_FALSE;
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);
    ham_size_t minkeys=btree_get_minkeys(maxkeys);

    if (!scratchpad->mergepage)
        return (0);

    /*
     * get the left and the right sibling of this page
     */
    leftpage =left
                ? db_fetch_page(page_get_owner(page), 
                        btree_node_get_left(node), 0)
                : 0;
    if (leftpage) {
        leftnode =ham_page_get_btree_node(leftpage);
        fewleft  =(btree_node_get_count(leftnode)<=minkeys);
    }
    rightpage=right
                ? db_fetch_page(page_get_owner(page), 
                        btree_node_get_right(node), 0)
                : 0;
    if (rightpage) {
        rightnode=ham_page_get_btree_node(rightpage);
        fewright =(btree_node_get_count(rightnode)<=minkeys);
    }

    /*
     * if we have no siblings, then we're rebalancing the root page
     */
    if (!leftpage && !rightpage) {
        if (btree_node_is_leaf(node))
            return (0);
        else 
            return (db_fetch_page(page_get_owner(page), 
                        btree_node_get_ptr_left(node), 0));
    }

    /*
     * if one of the siblings is missing, or both of them are 
     * too empty, we have to merge them
     */
    if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (parent && lanchor!=page_get_self(parent)) {
            return (my_merge_pages(page, rightpage, ranchor, scratchpad, hints));
        }
        else {
            return (my_merge_pages(leftpage, page, lanchor, scratchpad, hints));
        }
    }

    /*
     * otherwise choose the better of a merge or a shift
     */
    if (leftpage && fewleft && rightpage && !fewright) {
        if (parent && (!(ranchor==page_get_self(parent)) && 
                (page_get_self(page)==page_get_self(scratchpad->mergepage)))) {
            return (my_merge_pages(leftpage, page, lanchor, scratchpad, hints));
        }
        else {
            return (my_shift_pages(page, rightpage, ranchor, scratchpad, hints));
        }
    }

    /*
     * ... still choose the better of a merge or a shift...
     */
    if (leftpage && !fewleft && rightpage && fewright) {
        if (parent && (!(lanchor==page_get_self(parent)) &&
                (page_get_self(page)==page_get_self(scratchpad->mergepage)))) {
            return (my_merge_pages(page, rightpage, ranchor, scratchpad, hints));
        }
        else {
            return (my_shift_pages(leftpage, page, lanchor, scratchpad, hints));
        }
    }

    /*
     * choose the more effective of two shifts
     */
    if (lanchor==ranchor) {
        if (btree_node_get_count(leftnode)<=btree_node_get_count(rightnode)) {
            return (my_shift_pages(page, rightpage, ranchor, scratchpad, hints));
        }
        else {
            return (my_shift_pages(leftpage, page, lanchor, scratchpad, hints));
        }
    }

    /*
     * choose the shift with more local effect
     */
    if (parent && lanchor==page_get_self(parent)) {
        return (my_shift_pages(leftpage, page, lanchor, scratchpad, hints));
    }
    else {
        return (my_shift_pages(page, rightpage, ranchor, scratchpad, hints));
    }
}

static ham_page_t *
my_merge_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor, 
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_status_t st;
    ham_s32_t slot;
    ham_size_t c, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_page_t *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    int_key_t *bte_lhs, *bte_rhs;

    keysize=db_get_keysize(db);
    node   =ham_page_get_btree_node(page);
    sibnode=ham_page_get_btree_node(sibpage);

    if (anchor) {
        ancpage=db_fetch_page(db, anchor, 0);
        ancnode=ham_page_get_btree_node(ancpage);
    }
    else {
        ancpage=0;
        ancnode=0;
    }

    /*
     * prepare all pages for the log
     */
    if ((st=ham_log_add_page_before(page)))
        return (0);
    if ((st=ham_log_add_page_before(sibpage)))
        return (0);
    if (ancpage)
        if ((st=ham_log_add_page_before(ancpage)))
            return (0);

    /*
     * uncouple all cursors
     */
    if ((st=db_uncouple_all_cursors(page, 0)))
        return (0);
    if ((st=db_uncouple_all_cursors(sibpage, 0)))
        return (0);
    if (ancpage)
        if ((st=db_uncouple_all_cursors(ancpage, 0)))
            return (0);

    /*
     * internal node: append the anchornode separator value to 
     * this node
     */
    if (!btree_node_is_leaf(node)) {
        int_key_t *bte;
        ham_key_t key; 

        bte =btree_node_get_key(db, sibnode, 0);
        memset(&key, 0, sizeof(key));
        key._flags=key_get_flags(bte);
        key.data  =key_get_key(bte);
        key.size  =key_get_size(bte);

		hints->cost++;
        st=btree_get_slot(db, ancpage, &key, &slot, 0);
        if (st) {
            db_set_error(db, st);
            return 0;
        }

        bte_lhs=btree_node_get_key(db, node,
            btree_node_get_count(node));
        bte_rhs=btree_node_get_key(db, ancnode, slot);

		hints->cost++;
        st=my_copy_key(db, bte_lhs, bte_rhs);
        if (st) {
            db_set_error(db, st);
            return 0;
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
	hints->cost += stats_memmove_cost((db_get_int_key_header_size()+keysize)*c);
    memcpy(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*c);
            
	/*
	 * as sibnode is merged into node, we will also need to ensure that our 
     * statistics node/page tracking is corrected accordingly: what was in sibnode, is now in 
     * node. And sibnode will be destroyed at the end.
	 */
	if (sibpage == hints->processed_leaf_page)
	{
		/* sibnode slot 0 has become node slot 'bte_lhs' */
		hints->processed_slot += btree_node_get_count(node);
		hints->processed_leaf_page = page;
	}

    page_set_dirty(page);
    page_set_dirty(sibpage);
    btree_node_set_count(node, btree_node_get_count(node)+c);
    btree_node_set_count(sibnode, 0);

    /*
     * update the linked list of pages
     */
    if (btree_node_get_left(node)==page_get_self(sibpage)) {
        if (btree_node_get_left(sibnode)) {
            ham_page_t *p=db_fetch_page(db, 
                    btree_node_get_left(sibnode), 0);
            btree_node_t *n=ham_page_get_btree_node(p);
            btree_node_set_right(n, btree_node_get_right(sibnode));
            btree_node_set_left(node, btree_node_get_left(sibnode));
            page_set_dirty(p);
        }
        else
            btree_node_set_left(node, 0);
    }
    else if (btree_node_get_right(node)==page_get_self(sibpage)) {
        if (btree_node_get_right(sibnode)) {
            ham_page_t *p=db_fetch_page(db, 
                    btree_node_get_right(sibnode), 0);
            btree_node_t *n=ham_page_get_btree_node(p);

            btree_node_set_right(node, btree_node_get_right(sibnode));
            btree_node_set_left(n, btree_node_get_left(sibnode));
            page_set_dirty(p);
        }
        else
            btree_node_set_right(node, 0);
    }
    
    /*
     * return this page for deletion
     */
    if (scratchpad->mergepage && 
           (page_get_self(scratchpad->mergepage)==page_get_self(page) ||
            page_get_self(scratchpad->mergepage)==page_get_self(sibpage))) 
        scratchpad->mergepage=0;

	stats_page_is_nuked(db, sibpage, HAM_FALSE);

    /* 
     * delete the page
     */
	ham_assert(hints->processed_leaf_page != sibpage, (0));
    st=txn_free_page(db_get_txn(db), sibpage);
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    return (sibpage);
}

static ham_page_t *
my_shift_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_s32_t slot=0;
    ham_status_t st;
    ham_bool_t intern;
	ham_size_t s;
	ham_size_t c;
	ham_size_t keysize;
    ham_db_t *db=page_get_owner(page);
    ham_page_t *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    int_key_t *bte_lhs, *bte_rhs;

    node   =ham_page_get_btree_node(page);
    sibnode=ham_page_get_btree_node(sibpage);
    keysize=db_get_keysize(db);
    intern =!btree_node_is_leaf(node);
    ancpage=db_fetch_page(db, anchor, 0);
    ancnode=ham_page_get_btree_node(ancpage);

    ham_assert(btree_node_get_count(node)!=btree_node_get_count(sibnode), (""));

    /*
     * prepare all pages for the log
     */
    if ((st=ham_log_add_page_before(page)))
        return (0);
    if ((st=ham_log_add_page_before(sibpage)))
        return (0);
    if (ancpage)
        if ((st=ham_log_add_page_before(ancpage)))
            return (0);

    /*
     * uncouple all cursors
     */
    if ((st=db_uncouple_all_cursors(page, 0)))
        return (0);
    if ((st=db_uncouple_all_cursors(sibpage, 0)))
        return (0);
    if (ancpage)
        if ((st=db_uncouple_all_cursors(ancpage, 0)))
            return (0);

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
            int_key_t *bte;
            ham_key_t key;

            bte=btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
			hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                db_set_error(db, st);
                return 0;
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
                db_set_error(db, st);
                return 0;
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
             * shift the whole sibling to the left
             */
			hints->cost += stats_memmove_cost((db_get_int_key_header_size()
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
                db_set_error(db, st);
                return (0);
            }

            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(node, btree_node_get_count(node)+1);
        }

        /*
         * shift items from the sibling to this page, then 
         * delete the shifted items
         */
		hints->cost += stats_memmove_cost((db_get_int_key_header_size()
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
            int_key_t *bte;
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
                    db_set_error(db, st);
                    return (0);
                }
                /* replace the key */
                st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY, hints);
                if (st) {
                    db_set_error(db, st);
                    return (0);
                }
            }
            /*
             * shift once more
             */
			hints->cost += stats_memmove_cost((db_get_int_key_header_size()
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
            int_key_t *bte;
            bte=btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
			hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
            /* replace the key */
            st=my_replace_key(ancpage, slot, bte, INTERNAL_KEY, hints);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }

        /*
         * update the page counter
         */
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
            int_key_t *bte;
            ham_key_t key;
    
            bte =btree_node_get_key(db, sibnode, 0);
            memset(&key, 0, sizeof(key));
            key._flags=key_get_flags(bte);
            key.data  =key_get_key(bte);
            key.size  =key_get_size(bte);
			hints->cost++;
            st=btree_get_slot(db, ancpage, &key, &slot, 0);
            if (st) {
                db_set_error(db, st);
                return 0;
            }

            /*
             * shift sibling by 1 to the right 
             */
			hints->cost += stats_memmove_cost((db_get_int_key_header_size()
                    + keysize) * (btree_node_get_count(sibnode)-1));
            bte_lhs=btree_node_get_key(db, sibnode, 1);
            bte_rhs=btree_node_get_key(db, sibnode, 0);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize) 
                    * (btree_node_get_count(sibnode)-1));

            /*
             * copy the old anchor element to sibling[0]
             */
            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, ancnode, slot);

			hints->cost++;
            st=my_copy_key(db, bte_lhs, bte_rhs);
            if (st) {
                db_set_error(db, st);
                return (0);
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
                db_set_error(db, st);
                return (0);
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
        if (intern) 
		{
            /* shift sibling by 1 to the right */
			hints->cost += stats_memmove_cost((db_get_int_key_header_size()
                    + keysize) * (btree_node_get_count(sibnode)-1));
            bte_lhs=btree_node_get_key(db, sibnode, 1);
            bte_rhs=btree_node_get_key(db, sibnode, 0);
            memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize) 
                    * (btree_node_get_count(sibnode)-1));

            bte_lhs=btree_node_get_key(db, sibnode, 0);
            bte_rhs=btree_node_get_key(db, ancnode, slot);

            /* clear the key - we don't want my_replace_key to free
             * an extended block which is still used by sibnode[1] */
            memset(bte_lhs, 0, sizeof(*bte_lhs));

            st=my_replace_key(sibpage, 0, bte_rhs, 
                    (btree_node_is_leaf(node) ? 0 : INTERNAL_KEY),
					hints);
            if (st) {
                db_set_error(db, st);
                return (0);
            }

            key_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(sibnode, btree_node_get_count(sibnode)+1);
        }

        s=btree_node_get_count(node)-c-1;

        /*
         * shift items from this page to the sibling, then delete the
         * items from this page
         */
		hints->cost += stats_memmove_cost((db_get_int_key_header_size()
                + keysize)*(btree_node_get_count(sibnode)+c));
        bte_lhs=btree_node_get_key(db, sibnode, c);
        bte_rhs=btree_node_get_key(db, sibnode, 0);
        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*
                btree_node_get_count(sibnode));

        bte_lhs=btree_node_get_key(db, sibnode, 0);
        bte_rhs=btree_node_get_key(db, node, s+1);
        memmove(bte_lhs, bte_rhs, (db_get_int_key_header_size()+keysize)*c);

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
                    return (0);
            }
            btree_node_set_count(node, btree_node_get_count(node)-1);
        }

        /*
         * replace the old anchor key with the new anchor key 
         */
        if (anchor) {
            int_key_t *bte;
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
                db_set_error(db, st);
                return (0);
            }

            st=my_replace_key(ancpage, slot+1, bte, INTERNAL_KEY, hints);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
    }

cleanup:
    /*
     * mark pages as dirty
     */
    page_set_dirty(page);
    page_set_dirty(ancpage);
    page_set_dirty(sibpage);

    scratchpad->mergepage=0;

    return (0);
}

static ham_status_t
my_copy_key(ham_db_t *db, int_key_t *lhs, int_key_t *rhs)
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

        st=blob_allocate(db, record.data, record.size, 0, &lhsblobid);
        if (st)
            return (st);
        key_set_extended_rid(db, lhs, lhsblobid);
    }

    return (0);
}

static ham_status_t
my_replace_key(ham_page_t *page, ham_s32_t slot, 
        int_key_t *rhs, ham_u32_t flags, erase_hints_t *hints)
{
    int_key_t *lhs;
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);

	hints->cost++;

    /*
     * prepare page for the log
     */
    if ((st=ham_log_add_page_before(page)))
        return (st);

    /*
     * uncouple all cursors
     */
    if ((st=db_uncouple_all_cursors(page, 0)))
        return (db_set_error(db, st));

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

        st=blob_allocate(db, record.data, record.size, 0, &lhsblobid);
        if (st)
            return (st);
        key_set_extended_rid(db, lhs, lhsblobid);
    }

    key_set_size(lhs, key_get_size(rhs));

    page_set_dirty(page);

    return (HAM_SUCCESS);
}

static ham_status_t
my_remove_entry(ham_page_t *page, ham_s32_t slot, 
        erase_scratchpad_t *scratchpad, erase_hints_t *hints)
{
    ham_status_t st;
    int_key_t *bte_lhs, *bte_rhs, *bte;
    btree_node_t *node;
    ham_size_t keysize;
    ham_db_t *db;

    db=page_get_owner(page);
    node=ham_page_get_btree_node(page);
    keysize=db_get_keysize(db);
    bte=btree_node_get_key(db, node, slot);

	hints->cost++;

    /*
     * prepare page for the log
     */
    if ((st=ham_log_add_page_before(page)))
        return (st);

    /*
     * uncouple all cursors
     */
    if ((st=db_uncouple_all_cursors(page, 0)))
        return (db_set_error(db, st));

    ham_assert(slot>=0, ("invalid slot %ld", slot));
    ham_assert(slot<btree_node_get_count(node), ("invalid slot %ld", slot));

    /*
     * leaf page: get rid of the record
     *
     * if duplicates are enabled and a cursor exists: remove the duplicate
     * 
     * otherwise remove the full key with all duplicates
     */
    if (btree_node_is_leaf(node)) 
	{
        ham_bt_cursor_t *c=(ham_bt_cursor_t *)db_get_cursors(db);
        ham_bt_cursor_t *cursor=(ham_bt_cursor_t *)scratchpad->cursor;

		hints->processed_leaf_page = page;
		hints->processed_slot = slot;

        if (key_get_flags(bte)&KEY_HAS_DUPLICATES && scratchpad->cursor) 
		{
            st=key_erase_record(db, bte, bt_cursor_get_dupe_id(cursor), 0);
            if (st)
                return (db_set_error(db, st));

            /*
             * if the last duplicate was erased (ptr and flags==0): 
             * remove the entry completely
             */
            if (key_get_ptr(bte)==0 && key_get_flags(bte)==0)
                goto free_all;

            /*
             * make sure that no cursor is pointing to this dupe, and shift
             * all other cursors
             */
            while (c && cursor) {
                ham_bt_cursor_t *next=(ham_bt_cursor_t *)cursor_get_next(c);
                if (c!=cursor) {
                    if (bt_cursor_get_dupe_id(c)
                            ==bt_cursor_get_dupe_id(cursor)) {
                        if (bt_cursor_points_to(c, bte))
                            bt_cursor_set_to_nil((ham_bt_cursor_t *)c);
                    }
                    else if (bt_cursor_get_dupe_id(c)>
                            bt_cursor_get_dupe_id(cursor)) {
                        bt_cursor_set_dupe_id(c, 
                                bt_cursor_get_dupe_id(c)-1);
                        memset(bt_cursor_get_dupe_cache(c), 0,
                                sizeof(dupe_entry_t));
                    }
                }
                c=next;
            }
    
            /*
             * return immediately
             */
            return (0);
        }
        else 
		{
            ham_bt_cursor_t *c;

            st=key_erase_record(db, bte, 0, BLOB_FREE_ALL_DUPES);
            if (st)
                return (db_set_error(db, st));

free_all:
            c=(ham_bt_cursor_t *)db_get_cursors(db);

            /*
             * make sure that no cursor is pointing to this key
             */
            while (c) {
                ham_bt_cursor_t *next=(ham_bt_cursor_t *)cursor_get_next(c);
                if (c!=cursor) {
                    if (bt_cursor_points_to(c, bte))
                        bt_cursor_set_to_nil((ham_bt_cursor_t *)c);
                }
                c=next;
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
    if (slot != btree_node_get_count(node)-1) 
	{
		hints->cost += stats_memmove_cost((db_get_int_key_header_size()
                + keysize)*(btree_node_get_count(node)-slot-1));
        bte_lhs=btree_node_get_key(db, node, slot);
        bte_rhs=btree_node_get_key(db, node, slot+1);
        memmove(bte_lhs, bte_rhs, ((db_get_int_key_header_size()+keysize))*
                (btree_node_get_count(node)-slot-1));
    }

    btree_node_set_count(node, btree_node_get_count(node)-1);

    page_set_dirty(page);

    return (0);
}

