/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * btree erasing
 *
 */

#include <string.h>
#include <ham/config.h>
#include "db.h"
#include "error.h"
#include "page.h"
#include "btree.h"
#include "mem.h"
#include "util.h"

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
     * the transaction object 
     */
    ham_txn_t *txn;

    /*
     * the key which will be deleted
     */
    ham_key_t *key;

    /*
     * a pointer to the record id of the deleted key
     */
    ham_offset_t *rid;

    /*
     * a page which needs rebalancing
     */
    ham_page_t *mergepage;

} erase_scratchpad_t;

/*
 * recursively descend down the tree, delete the item and re-balance 
 * the tree on the way back up
 * returns the page which is deleted, if available
 */
static ham_page_t *
my_erase_recursive(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad);

/*
 * collapse the root node
 */
static ham_status_t
my_collapse_root(ham_page_t *root, erase_scratchpad_t *scratchpad);

/*
 * rebalance a page - either shifts elements to a sibling, or merges 
 * the page with a sibling
 */
static ham_page_t *
my_rebalance(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad);

/*
 * merge two pages
 */
static ham_page_t *
my_merge_pages(ham_page_t *page, ham_page_t *sibling, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad);

/*
 * shift items from a sibling to this page, till both pages have an equal 
 * number of items
 */
static ham_page_t *
my_shift_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad);

/*
 * replace two keys in a page
 */
static ham_status_t
my_replace_key(ham_page_t *page, long slot, btree_entry_t *newentry,
        ham_u32_t flags);

/*
 * remove an item from a page
 */
static ham_status_t
my_remove_entry(ham_page_t *page, long slot, erase_scratchpad_t *scratchpad);

/*
 * flags for my_replace_key
 */
#define NOFLUSH 1


ham_status_t
btree_erase(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key, 
        ham_offset_t *rid, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_page_t *root, *p;
    ham_offset_t rootaddr;
    ham_db_t *db=btree_get_db(be);
    erase_scratchpad_t scratchpad;

    /* 
     * initialize the scratchpad 
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.txn=txn;
    scratchpad.key=key;
    scratchpad.rid=rid;
    scratchpad.flags=flags;

    /* 
     * get the root-page...
     */
    rootaddr=btree_get_rootpage(be);
    if (!rootaddr)
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
    root=txn_fetch_page(scratchpad.txn, rootaddr, flags);

    db_set_error(db, 0);

    /* 
     * ... and start the recursion 
     */
    p=my_erase_recursive(root, 0, 0, 0, 0, 0, &scratchpad);
    if (db_get_error(db))
        return (db_get_error(db));
    if (p) {
        st=my_collapse_root(p, &scratchpad);
        /* 
         * delete the old root page 
         * see below for notes about page_io_free()!
         */
        txn_remove_page(scratchpad.txn, root);
        (void)page_io_free(scratchpad.txn, root);
        (void)cm_move_to_garbage(db_get_cm(db), root);
    }

    return (st);
}

static ham_page_t *
my_erase_recursive(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad)
{
    long slot;
    ham_bool_t isfew;
    ham_status_t st;
    ham_offset_t next_left, next_right, next_lanchor, next_ranchor;
    ham_page_t *newme, *child, *tempp=0;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_size_t maxkeys=db_get_maxkeys(db);

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
        isfew=(btree_node_get_count(node)>1);
    else
        isfew=(btree_node_get_count(node)>btree_get_minkeys(maxkeys)); 

    if (isfew)
        scratchpad->mergepage=0;
    else if (!scratchpad->mergepage)
        scratchpad->mergepage=page;

    if (!btree_node_is_leaf(node)) {
        child=btree_find_child2(db, scratchpad->txn, page, 
                scratchpad->key, &slot);
        ham_assert(child!=0, "guru meditation error", 0);
    }
    else {
        slot=btree_get_slot(db, page, scratchpad->key);
        child=0;
    }

    /*
     * if this page is not a leaf: recursively descend down the tree
     */
    if (!btree_node_is_leaf(node)) {
        /*
         * calculate neighbor and anchor nodes
         */
        if (slot==-1) {
            if (!left)
                next_left=0;
            else {
                btree_entry_t *bte; 
                btree_node_t *n;
                tempp=txn_fetch_page(scratchpad->txn, left, 0);
                n=ham_page_get_btree_node(tempp);
                bte=btree_node_get_entry(db, n, btree_node_get_count(n)-1);
                next_left=btree_entry_get_ptr(bte);
            }
            next_lanchor=lanchor;
        }
        else {
            if (slot==0)
                next_left=btree_node_get_ptr_left(node);
            else {
                btree_entry_t *bte; 
                bte=btree_node_get_entry(db, node, slot-1);
                next_left=btree_entry_get_ptr(bte);
            }
            next_lanchor=page_get_self(page);
        }

        if (slot==btree_node_get_count(node)-1) {
            if (!right)
                next_right=0;
            else {
                btree_entry_t *bte; 
                btree_node_t *n;
                tempp=txn_fetch_page(scratchpad->txn, right, 0);
                n=ham_page_get_btree_node(tempp);
                bte=btree_node_get_entry(db, n, 0);
                next_right=btree_entry_get_ptr(bte);
            }
            next_ranchor=ranchor;
        }
        else {
            btree_entry_t *bte; 
            bte=btree_node_get_entry(db, node, slot+1);
            next_right=btree_entry_get_ptr(bte);
            next_ranchor=page_get_self(page);
        }

        newme=my_erase_recursive(child, next_left, next_right, next_lanchor, 
                    next_ranchor, page, scratchpad);
    }
    /*
     * otherwise (page is a leaf) delete the key...
     */
    else {
        /*
         * check if this entry really exists
         */
        newme=0;
        if (slot!=-1) {
            int cmp;
            btree_entry_t *bte;

            bte=btree_node_get_entry(db, node, slot);

            cmp=db_compare_keys(db, page, 
                    -1, scratchpad->key->_flags, scratchpad->key->data, 
                    scratchpad->key->size, scratchpad->key->size,
                    slot, btree_entry_get_flags(bte), btree_entry_get_key(bte), 
                    btree_entry_get_size(bte), btree_entry_get_size(bte));
            if (db_get_error(db))
                return (0);
            if (cmp==0) {
                *scratchpad->rid=btree_entry_get_ptr(bte);
                newme=page;
            }
            else {
                db_set_error(db, HAM_KEY_NOT_FOUND);
                return (0);
            }
        }
        if (!newme) {
            db_set_error(db, HAM_KEY_NOT_FOUND);
            scratchpad->mergepage=0;
            return (0);
        }
    }

    /*
     * ... and rebalance the tree, if necessary
     */
    if (newme) {
        if (slot==-1)
            slot=0;
        st=my_remove_entry(page, slot, scratchpad);
        if (st) /* TODO log */
            return (0);
    }

    /*
     * no need to rebalance in case of an error
     */
    if (!db_get_error(db))
        return (my_rebalance(page, left, right, lanchor, ranchor, parent, 
                scratchpad));
    else
        return (0);
}

static ham_status_t
my_collapse_root(ham_page_t *newroot, erase_scratchpad_t *scratchpad)
{
    btree_set_rootpage(scratchpad->be, page_get_self(newroot));
    db_set_dirty(page_get_owner(newroot), 1);
    return (0);
}

static ham_page_t *
my_rebalance(ham_page_t *page, ham_offset_t left, ham_offset_t right, 
        ham_offset_t lanchor, ham_offset_t ranchor, ham_page_t *parent,
        erase_scratchpad_t *scratchpad)
{
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_page_t *leftpage, *rightpage;
    btree_node_t *leftnode=0, *rightnode=0;
    ham_bool_t fewleft=HAM_FALSE, fewright=HAM_FALSE;
    ham_size_t maxkeys=db_get_maxkeys(page_get_owner(page));
    ham_size_t minkeys=btree_get_minkeys(maxkeys);

    if (!scratchpad->mergepage)
        return (0);

    /*
     * get the left and the right sibling of this page
     */
    leftpage =left
                ? txn_fetch_page(scratchpad->txn, btree_node_get_left(node), 0)
                : 0;
    if (leftpage) {
        leftnode =ham_page_get_btree_node(leftpage);
        fewleft  =(btree_node_get_count(leftnode)<=minkeys) 
                ? HAM_TRUE : HAM_FALSE;
    }
    rightpage=right
                ? txn_fetch_page(scratchpad->txn, btree_node_get_right(node), 0)
                : 0;
    if (rightpage) {
        rightnode=ham_page_get_btree_node(rightpage);
        fewright =(btree_node_get_count(rightnode)<=minkeys) 
                ? HAM_TRUE : HAM_FALSE;
    }

    /*
     * if we have no siblings, then we're rebalancing the root page
     */
    if (!leftpage && !rightpage) {
        if (btree_node_is_leaf(node))
            return (0);
        else 
            return (txn_fetch_page(scratchpad->txn, 
                        btree_node_get_ptr_left(node), 0));
    }

    /*
     * if one of the siblings is missing, or both of them are 
     * too empty, we have to merge them
     */
    if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (lanchor!=page_get_self(parent))
            return (my_merge_pages(page, rightpage, ranchor, scratchpad));
        else
            return (my_merge_pages(leftpage, page, lanchor, scratchpad));
    }

    /*
     * otherwise choose the better of a merge or a shift
     */
    if (leftpage && fewleft && rightpage && !fewright) {
        if (!(ranchor==page_get_self(parent)) && 
                (page_get_self(page)==page_get_self(scratchpad->mergepage))) 
            return (my_merge_pages(leftpage, page, lanchor, scratchpad));
        else
            return (my_shift_pages(page, rightpage, ranchor, scratchpad));
    }

    /*
     * ... still choose the better of a merge or a shift...
     */
    if (leftpage && !fewleft && rightpage && fewright) {
        if (!(lanchor==page_get_self(parent)) &&
                (page_get_self(page)==page_get_self(scratchpad->mergepage)))
            return (my_merge_pages(page, rightpage, ranchor, scratchpad));
        else
            return (my_shift_pages(leftpage, page, lanchor, scratchpad));
    }

    /*
     * choose the more effective of two shifts
     */
    if (lanchor==ranchor) {
        if (btree_node_get_count(leftnode)<=btree_node_get_count(rightnode))
            return (my_shift_pages(page, rightpage, ranchor, scratchpad));
        else
            return (my_shift_pages(leftpage, page, lanchor, scratchpad));
    }

    /*
     * choose the shift with more local effect
     */
    if (lanchor==page_get_self(parent))
        return (my_shift_pages(leftpage, page, lanchor, scratchpad));
    else
        return (my_shift_pages(page, rightpage, ranchor, scratchpad));
}

static ham_page_t *
my_merge_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor, 
        erase_scratchpad_t *scratchpad)
{
    ham_size_t c, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_page_t *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    btree_entry_t *bte_lhs, *bte_rhs;

    keysize=db_get_keysize(db);
    node   =ham_page_get_btree_node(page);
    sibnode=ham_page_get_btree_node(sibpage);

    if (anchor) {
        ancpage=txn_fetch_page(scratchpad->txn, anchor, 0);
        ancnode=ham_page_get_btree_node(ancpage);
    }
    else {
        ancpage=0;
        ancnode=0;
    }

    /*
     * internal node: insert the anchornode separator value to 
     * this node
     */
    if (!btree_node_is_leaf(node)) {
        long slot;
        btree_entry_t *bte;
        ham_key_t key; 

        bte =btree_node_get_entry(db, sibnode, 0);
        key.data=btree_entry_get_key(bte);
        key.size=btree_entry_get_size(bte);

        slot=btree_get_slot(db, ancpage, &key);
        bte_rhs=btree_node_get_entry(db, ancnode, slot);
        bte_lhs=btree_node_get_entry(db, node,
            btree_node_get_count(node));
        /* TODO we could also copy the cached extkey... */
        memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
        btree_entry_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
        btree_node_set_count(node, btree_node_get_count(node)+1);
    }

    c=btree_node_get_count(sibnode);
    bte_lhs=btree_node_get_entry(db, node, 
            btree_node_get_count(node));
    bte_rhs=btree_node_get_entry(db, sibnode, 0);

    /*
     * shift items from the sibling to this page
     * TODO we could also shift the extkeys... 
     */
    memcpy(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*c);
            
    page_set_dirty(page, 1);
    page_set_dirty(sibpage, 1);
    btree_node_set_count(node, btree_node_get_count(node)+c);
    btree_node_set_count(sibnode, 0);

    /*
     * update the linked list of pages
     */
    if (btree_node_get_left(node)==page_get_self(sibpage)) {
        if (btree_node_get_left(sibnode)) {
            ham_page_t *p=txn_fetch_page(scratchpad->txn, 
                    btree_node_get_left(sibnode), 0);
            btree_node_t *n=ham_page_get_btree_node(p);
            btree_node_set_right(n, btree_node_get_right(sibnode));
            btree_node_set_left(node, btree_node_get_left(sibnode));
            page_set_dirty(p, 1);
        }
        else
            btree_node_set_left(node, 0);
    }
    else if (btree_node_get_right(node)==page_get_self(sibpage)) {
        if (btree_node_get_right(sibnode)) {
            ham_page_t *p=txn_fetch_page(scratchpad->txn, 
                    btree_node_get_right(sibnode), 0);
            btree_node_t *n=ham_page_get_btree_node(p);

            btree_node_set_right(node, btree_node_get_right(sibnode));
            btree_node_set_left(n, btree_node_get_left(sibnode));
            page_set_dirty(p, 1);
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

    /* 
     * delete the page
     *
     * it is important that page_io_free() is called BEFORE
     * cm_move_to_garbage(). page_io_free() adds the page to the 
     * freelist. In rare cases, it can happen that the freelist page is 
     * full, and the freelist allocates another page. If the page was already
     * moved to the garbage bin (with cm_move_to_garbage()), the page 
     * might be reused, before it's freed, which leads to troubles...
     */
    txn_remove_page(scratchpad->txn, sibpage);
    (void)page_io_free(scratchpad->txn, sibpage);
    (void)cm_move_to_garbage(db_get_cm(db), sibpage);

    /* TODO it's no problem to return an invalid pointer to a deleted page,
     * but we need only a boolean value. maybe the return type should
     * be replaced with ham_bool_t. */
    return (sibpage);
}

static ham_page_t *
my_shift_pages(ham_page_t *page, ham_page_t *sibpage, ham_offset_t anchor,
        erase_scratchpad_t *scratchpad)
{
    long slot=0;
    ham_bool_t intern;
    ham_size_t i, s, c, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_page_t *ancpage;
    btree_node_t *node, *sibnode, *ancnode;
    btree_entry_t *bte_lhs, *bte_rhs;

    node   =ham_page_get_btree_node(page);
    sibnode=ham_page_get_btree_node(sibpage);
    if (btree_node_get_count(node)==btree_node_get_count(sibnode))
        return (0);
    keysize=db_get_keysize(db);
    intern =!btree_node_is_leaf(node);
    ancpage=txn_fetch_page(scratchpad->txn, anchor, 0);
    ancnode=ham_page_get_btree_node(ancpage);

    /*
     * shift from sibling to this node
     */
    if (btree_node_get_count(sibnode)>=btree_node_get_count(node)) {
        /*
         * TODO this is slow: we just delete all extkeys of the sibling.
         */
        page_delete_ext_keys(sibpage);

        /*
         * internal node: insert the anchornode separator value to 
         * this node
         */
        if (intern) {
            btree_entry_t *bte;
            ham_key_t key;

            bte=btree_node_get_entry(db, sibnode, 0);
            key.data=btree_entry_get_key(bte);
            key.size=btree_entry_get_size(bte);
            slot=btree_get_slot(db, ancpage, &key);
    
            /*
             * append the anchor node to the page
             */
            bte_rhs=btree_node_get_entry(db, ancnode, slot);
            bte_lhs=btree_node_get_entry(db, node,
                btree_node_get_count(node));
            memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            /*
             * the pointer of this new node is ptr_left of the sibling
             */
            btree_entry_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            /*
             * new pointer left of the sibling is sibling[0].ptr
             */
            btree_node_set_ptr_left(sibnode, btree_entry_get_ptr(bte));
            /*
             * update the anchor node with sibling[0]
             */
            (void)my_replace_key(ancpage, slot, bte, 0);
            /*
             * shift the whole sibling to the left
             */
            for (i=0; i<btree_node_get_count(sibnode)-1; i++) {
                bte_lhs=btree_node_get_entry(db, sibnode, i);
                bte_rhs=btree_node_get_entry(db, sibnode, i+1);
                memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            }
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

        /*
         * internal node: append the anchor key to the page 
         */
        if (intern) {
            bte_lhs=btree_node_get_entry(db, node, 
                    btree_node_get_count(node));
            bte_rhs=btree_node_get_entry(db, ancnode, slot);
            memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            btree_entry_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(node, btree_node_get_count(node)+1);
        }

        /*
         * shift items from the sibling to this page, then 
         * delete the shifted items
         */
        bte_lhs=btree_node_get_entry(db, node, 
                btree_node_get_count(node));
        bte_rhs=btree_node_get_entry(db, sibnode, 0);

        memmove(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*c);

        bte_lhs=btree_node_get_entry(db, sibnode, 0);
        bte_rhs=btree_node_get_entry(db, sibnode, c);
        memmove(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*
                (btree_node_get_count(sibnode)-c));

        /*
         * internal nodes: don't forget to set ptr_left of the sibling, and
         * replace the anchor key
         */
        if (intern) {
            btree_entry_t *bte;
            bte=btree_node_get_entry(db, sibnode, 0);
            btree_node_set_ptr_left(sibnode, btree_entry_get_ptr(bte));
            if (anchor) {
                long slot;
                ham_key_t key;
                key.data=btree_entry_get_key(bte);
                slot=btree_get_slot(db, ancpage, &key);
                (void)my_replace_key(ancpage, slot, bte, 0);
            }
            /*
             * shift once more
             */
            bte_lhs=btree_node_get_entry(db, sibnode, 0);
            bte_rhs=btree_node_get_entry(db, sibnode, 1);
            memmove(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*
                    (btree_node_get_count(sibnode)-1));
        }
        /*
         * in a leaf - update the anchor
         */
        else {
            long slot;
            ham_key_t key;
            btree_entry_t *bte;
            bte=btree_node_get_entry(db, sibnode, 0);
            key.data=btree_entry_get_key(bte);
            key.size=btree_entry_get_size(bte);
            slot=btree_get_slot(db, ancpage, &key);
            (void)my_replace_key(ancpage, slot, bte, 0);
        }

        /*
         * update the page counter
         */
        btree_node_set_count(node, 
                btree_node_get_count(node)+c);
        btree_node_set_count(sibnode, 
                btree_node_get_count(sibnode)-c-(intern ? 1 : 0));
    }
    /*
     * shift from this node to the sibling
     */
    else {
        /*
         * TODO this is slow: we just delete all extkeys of the page and
         * of the sibling.
         */
        page_delete_ext_keys(page);
        page_delete_ext_keys(sibpage);

        /*
        * internal node: insert the anchornode separator value to 
        * this node
        */
        if (intern) {
            ham_size_t i;
            btree_entry_t *bte;
            ham_key_t key;
    
            bte =btree_node_get_entry(db, sibnode, 0);
            key.data=btree_entry_get_key(bte);
            key.size=btree_entry_get_size(bte);
            slot=btree_get_slot(db, ancpage, &key);
            /*
             * shift sibling by 1 to the right 
             */
            for (i=btree_node_get_count(sibnode); i>0; i--) {
                bte_lhs=btree_node_get_entry(db, sibnode, i);
                bte_rhs=btree_node_get_entry(db, sibnode, i-1);
                memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            }
            /*
             * copy the old anchor element to sibling[0]
             */
            bte_lhs=btree_node_get_entry(db, sibnode, 0);
            bte_rhs=btree_node_get_entry(db, ancnode, slot);
            memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            /*
             * sibling[0].ptr = sibling.ptr_left
             */
            btree_entry_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            /*
             * sibling.ptr_left = node[node.count-1].ptr
             */
            bte_lhs=btree_node_get_entry(db, node, 
            btree_node_get_count(node)-1);
            btree_node_set_ptr_left(sibnode, btree_entry_get_ptr(bte_lhs));
            /*
             * new anchor element is node[node.count-1].key
             */
            (void)my_replace_key(ancpage, slot, bte_lhs, NOFLUSH);
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

        /*
         * internal pages: insert the anchor element
         */
        if (intern) {
            ham_size_t i;
            /*
             * shift sibling by 1 to the right 
             */
            for (i=btree_node_get_count(sibnode); i>0; i--) {
                bte_lhs=btree_node_get_entry(db, sibnode, i);
                bte_rhs=btree_node_get_entry(db, sibnode, i-1);
                memcpy(bte_lhs, bte_rhs, sizeof(btree_entry_t)-1+keysize);
            }
            bte_lhs=btree_node_get_entry(db, sibnode, 0);
            bte_rhs=btree_node_get_entry(db, ancnode, slot);
            (void)my_replace_key(sibpage, 0, bte_rhs, NOFLUSH);
            btree_entry_set_ptr(bte_lhs, btree_node_get_ptr_left(sibnode));
            btree_node_set_count(sibnode, btree_node_get_count(sibnode)+1);
        }

        s=btree_node_get_count(node)-c-1;

        /*
         * shift items from this page to the sibling, then delete the
         * items from this page
         */
        bte_lhs=btree_node_get_entry(db, sibnode, c);
        bte_rhs=btree_node_get_entry(db, sibnode, 0);
        memmove(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*
                btree_node_get_count(sibnode));

        bte_lhs=btree_node_get_entry(db, sibnode, 0);
        bte_rhs=btree_node_get_entry(db, node, s+1);
        memmove(bte_lhs, bte_rhs, (sizeof(btree_entry_t)-1+keysize)*c);

        btree_node_set_count(node,
                btree_node_get_count(node)-c);
        btree_node_set_count(sibnode, 
                btree_node_get_count(sibnode)+c);

        /*
         * internal nodes: the pointer of the highest item
         * in the node will become the ptr_left of the sibling
         */
        if (intern) {
            bte_lhs=btree_node_get_entry(db, node, 
                    btree_node_get_count(node)-1);
            btree_node_set_ptr_left(sibnode, btree_entry_get_ptr(bte_lhs));
            btree_node_set_count(node, btree_node_get_count(node)-1);
        }

        /*
         * replace the old anchor key with the new anchor key 
         */
        if (anchor) {
            long slot;
            btree_entry_t *bte;
            ham_key_t key;

            if (intern)
                bte =btree_node_get_entry(db, node, s);
            else
                bte =btree_node_get_entry(db, sibnode, 0);

            key.data=btree_entry_get_key(bte);
            slot=btree_get_slot(db, ancpage, &key)+1;

            if (my_replace_key(ancpage, slot, bte, 0))
                return (0); /* TODO Log */
        }
    }

cleanup:
    /*
     * mark pages as dirty
     */
    page_set_dirty(page, 1);
    page_set_dirty(ancpage, 1);
    page_set_dirty(sibpage, 1);

    scratchpad->mergepage=0;

    return (0);
}

static ham_status_t
my_replace_key(ham_page_t *page, long slot, btree_entry_t *newbte,
        ham_u32_t flags)
{
    btree_entry_t *oldbte;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_ext_key_t *extkeys=page_get_extkeys(page);

    oldbte=btree_node_get_entry(db, node, slot);
    memcpy(btree_entry_get_key(oldbte), btree_entry_get_key(newbte), 
            db_get_keysize(db));
    page_set_dirty(page, 1);

    /* 
     * we could actually copy the extended key, not just delete it.
     * TODO 
     */
    if (extkeys && extkeys[slot].data) {
        ham_mem_free(extkeys[slot].data);
        extkeys[slot].data=0;
        extkeys[slot].size=0;
    }

    return (HAM_SUCCESS);
}

static ham_status_t
my_remove_entry(ham_page_t *page, long slot, erase_scratchpad_t *scratchpad)
{
    btree_entry_t *bte, *bte_lhs, *bte_rhs;
    btree_node_t *node;
    ham_size_t keysize;
    ham_db_t *db;
    ham_ext_key_t *extkeys;

    db=page_get_owner(page);
    node=ham_page_get_btree_node(page);
    keysize=db_get_keysize(db);
    extkeys=page_get_extkeys(page);

    ham_assert(slot>=0, "invalid slot %ld", slot);
    ham_assert(slot<btree_node_get_count(node), "invalid slot %ld", slot);

    /*
     * remove extended key?
     */
    if (extkeys) {
        if (extkeys[slot].data) {
            ham_status_t st;
            ham_offset_t blobid;
            ham_u8_t *prefix;
            ham_mem_free(extkeys[slot].data);
            extkeys[slot].data=0;
            extkeys[slot].size=0;
            bte=btree_node_get_entry(db, node, slot);
            prefix=btree_entry_get_key(bte);
            blobid=*(ham_offset_t*)&prefix[db_get_keysize(db)-
                    sizeof(ham_offset_t)-1];
            blobid=ham_db2h_offset(blobid);
            
            st=db_ext_key_erase(db, scratchpad->txn, blobid);
            if (st) {
                ham_trace("failed to delete extended key blob at "
                        "offset 0x%llx: status 0x%x", blobid, st);
                /* fall through */
            }
        }
    }

    /*
     * if we delete the last item, it's enough to decrement the item 
     * counter and return...
     */
    if (slot!=btree_node_get_count(node)-1) {
        bte_lhs=btree_node_get_entry(db, node, slot);
        bte_rhs=btree_node_get_entry(db, node, slot+1);
        memmove(bte_lhs, bte_rhs, ((sizeof(btree_entry_t)-1+keysize))*
                (btree_node_get_count(node)-slot-1));
        if (extkeys) {
            memmove(&extkeys[slot], &extkeys[slot+1], 
                    sizeof(ham_ext_key_t)*(btree_node_get_count(node)-slot-1));
        }
    }

    btree_node_set_count(node, btree_node_get_count(node)-1);
    page_set_dirty(page, 1);
    return (0);
}
