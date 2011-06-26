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
 * @brief btree verification
 *
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "btree.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "keys.h"
#include "mem.h"
#include "page.h"

/**
 * the check_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct
{
    /**
     * the backend pointer
     */
    ham_btree_t *be;

    /**   
     * the flags of the ham_check_integrity()-call
     */
    ham_u32_t flags;

} check_scratchpad_t;

/**
 * verify a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_status_t 
__verify_level(ham_page_t *parent, ham_page_t *page, 
        ham_u32_t level, check_scratchpad_t *scratchpad);

/**
 * verify a single page
 */
static ham_status_t
__verify_page(ham_page_t *parent, ham_page_t *leftsib, ham_page_t *page, 
        ham_u32_t level, ham_u32_t count, check_scratchpad_t *scratchpad);
    
/**                                                                 
 * verify the whole tree                                            
 *                                                                  
 * @note This is a B+-tree 'backend' method.
 */                                                                 
ham_status_t 
btree_check_integrity(ham_btree_t *be)
{
    ham_page_t *page, *parent=0; 
    ham_u32_t level=0;
    btree_node_t *node;
    ham_status_t st=0;
    ham_offset_t ptr_left;
    ham_db_t *db=be_get_db(be);
    check_scratchpad_t scratchpad;

    ham_assert(btree_get_rootpage(be)!=0, ("invalid root page"));

    scratchpad.be=be;
    scratchpad.flags=0;

    /* get the root page of the tree */
    st=db_fetch_page(&page, db, btree_get_rootpage(be), 0);
    ham_assert(st ? !page : 1, (0));
    if (!page)
        return (st ? st : HAM_INTERNAL_ERROR);

    /* while we found a page... */
    while (page) {
        node=ham_page_get_btree_node(page);
        ptr_left=btree_node_get_ptr_left(node);

        /*
         * verify the page and all its siblings
         */
        st=__verify_level(parent, page, level, &scratchpad);
        if (st)
            break;
        parent=page;

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left) {
            st=db_fetch_page(&page, db, ptr_left, 0);
            ham_assert(st ? !page : 1, (0));
            if (st)
                return (st);
        }
        else
            page=0;

        ++level;
    }

    return (st);
}

static ham_status_t 
__verify_level(ham_page_t *parent, ham_page_t *page, 
        ham_u32_t level, check_scratchpad_t *scratchpad)
{
    int cmp;
    ham_u32_t count=0;
    ham_page_t *child, *leftsib=0;
    ham_status_t st=0;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_db_t *db=page_get_owner(page);

    /* 
     * assert that the parent page's smallest item (item 0) is bigger
     * than the largest item in this page
     */
    if (parent && btree_node_get_left(node)) {
        btree_node_t *cnode =ham_page_get_btree_node(page);

        cmp=key_compare_int_to_int(db, page, 0,
                    (ham_u16_t)(btree_node_get_count(cnode)-1));
        if (cmp < -1)
            return (ham_status_t)cmp;
        if (cmp<0) {
            ham_log(("integrity check failed in page 0x%llx: parent item #0 "
                    "< item #%d\n", page_get_self(page), 
                    btree_node_get_count(cnode)-1));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    while (page) {
        /*
         * verify the page
         */
        st=__verify_page(parent, leftsib, page, level, count, scratchpad);
        if (st)
            break;

        /* 
         * get the right sibling
         */
        node=ham_page_get_btree_node(page);
        if (btree_node_get_right(node)) {
            st=db_fetch_page(&child, db, btree_node_get_right(node), 0);
            ham_assert(st ? !child : 1, (0));
            if (st)
                return st;
        }
        else
            child=0;

        leftsib=page;
        page=child;

        ++count;
    }

    return (st);
}

static ham_status_t
__verify_page(ham_page_t *parent, ham_page_t *leftsib, ham_page_t *page, 
        ham_u32_t level, ham_u32_t sibcount, check_scratchpad_t *scratchpad)
{
    int cmp;
    ham_size_t i=0;
    ham_size_t count;
    ham_size_t maxkeys;
    ham_db_t *db=page_get_owner(page);
    int_key_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    maxkeys=btree_get_maxkeys(scratchpad->be);
    count=btree_node_get_count(node);

    if (count==0) {
        /*
         * a rootpage can be empty! check if this page is the 
         * rootpage.
         */
        ham_btree_t *be=(ham_btree_t *)db_get_backend(db);
        if (page_get_self(page)==btree_get_rootpage(be))
            return (0);

        ham_log(("integrity check failed in page 0x%llx: empty page!\n",
                page_get_self(page)));
        return (HAM_INTEGRITY_VIOLATED);
    }

    /*
     * previous hamsterdb versions verified that at least "minkeys" keys
     * are in the page. newer hamsterdb versions relaxed these rules and 
     * performed late splits and maybe will even avoid merges if pages 
     * underflow. 
     */

    /*
     * check if the largest item of the left sibling is smaller than
     * the smallest item of this page
     */
    if (leftsib) {
        btree_node_t *sibnode=ham_page_get_btree_node(leftsib);
        int_key_t *sibentry=btree_node_get_key(db, sibnode, 
                btree_node_get_count(sibnode)-1);

        bte=btree_node_get_key(db, node, 0);

        if ((key_get_flags(bte)!=0 && key_get_flags(bte)!=KEY_IS_EXTENDED) 
                && !btree_node_is_leaf(node)) 
        {
            ham_log(("integrity check failed in page 0x%llx: item #0 "
                    "has flags, but it's not a leaf page", 
                    page_get_self(page), i));
            return (HAM_INTEGRITY_VIOLATED);
        }
        else {
            ham_status_t st;
            ham_key_t lhs;
            ham_key_t rhs;

            st = db_prepare_ham_key_for_compare(db, 0, sibentry, &lhs);
            if (st)
                return (st);
            st = db_prepare_ham_key_for_compare(db, 1, bte, &rhs);
            if (st)
                return (st);

            cmp = db_compare_keys(db, &lhs, &rhs);
            if (cmp < -1)
                return ((ham_status_t)cmp);
        }

        if (cmp >= 0) {
            ham_log(("integrity check failed in page 0x%llx: item #0 "
                    "< left sibling item #%d\n", page_get_self(page), 
                    btree_node_get_count(sibnode)-1));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    if (count==1)
        return (0);

    for (i=0; i<count-1; i++) {
        /* 
         * if this is an extended key: check for a blob-id
         */
        bte=btree_node_get_key(db, node, i);
        if (key_get_flags(bte)&KEY_IS_EXTENDED) {
            ham_offset_t blobid=key_get_extended_rid(db, bte);
            if (!blobid) {
                ham_log(("integrity check failed in page 0x%llx: item #%d "
                        "is extended, but has no blob", 
                        page_get_self(page), i));
                return (HAM_INTEGRITY_VIOLATED);
            }
        }
        
        cmp=key_compare_int_to_int(db, page, (ham_u16_t)i, (ham_u16_t)(i+1));

        if (cmp < -1)
            return (ham_status_t)cmp;
        if (cmp>=0) {
            ham_log(("integrity check failed in page 0x%llx: item #%d "
                    "< item #%d", page_get_self(page), i, i+1));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    return (0);
}

