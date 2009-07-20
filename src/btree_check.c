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
 * btree verification
 *
 */

#ifdef HAM_ENABLE_INTERNAL

#include "config.h"

#include <string.h>
#include <stdio.h>
#include "db.h"
#include "error.h"
#include "keys.h"
#include "btree.h"

/*
 * the check_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct
{
    /*
     * the backend pointer
     */
    ham_btree_t *be;

    /*
     * the flags of the ham_check_integrity()-call
     */
    ham_u32_t flags;

} check_scratchpad_t;

/*
 * verify a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_status_t 
my_verify_level(ham_page_t *parent, ham_page_t *page, 
        ham_u32_t level, check_scratchpad_t *scratchpad);

/*
 * verify a single page
 */
static ham_status_t
my_verify_page(ham_page_t *parent, ham_page_t *leftsib, ham_page_t *page, 
        ham_u32_t level, ham_u32_t count, check_scratchpad_t *scratchpad);
    
/*
 * compare two internal keys
 */
static int
__key_compare_int_to_int(ham_page_t *page, 
        ham_u16_t lhs, ham_u16_t rhs)
{
    int_key_t *l, *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);
    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), key_get_size(l), 
                rhs, key_get_flags(r), key_get_key(r), key_get_size(r)));
}

ham_status_t 
btree_check_integrity(ham_btree_t *be)
{
    ham_page_t *page, *parent=0; 
    ham_u32_t level=0;
    btree_node_t *node;
    ham_status_t st=0;
    ham_offset_t ptr_left;
    ham_db_t *db=btree_get_db(be);
    check_scratchpad_t scratchpad;

    ham_assert(btree_get_rootpage(be)!=0, ("invalid root page"));

    scratchpad.be=be;
    scratchpad.flags=0;

    /* get the root page of the tree */
    page=db_fetch_page(db, btree_get_rootpage(be), 0);
    if (!page)
        return (db_get_error(db));

    /* while we found a page... */
    while (page) {
        node=ham_page_get_btree_node(page);
        ptr_left=btree_node_get_ptr_left(node);

        /*
         * verify the page and all its siblings
         */
        st=my_verify_level(parent, page, level, &scratchpad);
        if (st)
            break;
        parent=page;

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left) {
            page=db_fetch_page(db, ptr_left, 0);
        }
        else
            page=0;

        ++level;
    }

    return (st);
}

static ham_status_t 
my_verify_level(ham_page_t *parent, ham_page_t *page, 
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

        cmp=__key_compare_int_to_int(page, 0,
                    (ham_u16_t)(btree_node_get_count(cnode)-1));
        if (db_get_error(db))
            return (db_get_error(db));
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
        st=my_verify_page(parent, leftsib, page, level, count, scratchpad);
        if (st)
            break;

        /* 
         * get the right sibling
         */
        node=ham_page_get_btree_node(page);
        if (btree_node_get_right(node)) {
            child=db_fetch_page(db, btree_node_get_right(node), 0);
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
my_verify_page(ham_page_t *parent, ham_page_t *leftsib, ham_page_t *page, 
        ham_u32_t level, ham_u32_t sibcount, check_scratchpad_t *scratchpad)
{
    int cmp;
    ham_size_t i=0, count, maxkeys;
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
     * check if we've at least "minkeys" entries in this node.
     * a root page can have ANY number of keys, and we relax the "minkeys"
     * for internal pages.
     *
     * !! recno databases have relaxed SMO rules
     */
    if ((btree_node_get_left(node)!=0 
        || btree_node_get_right(node)!=0)
        && !(db_get_rt_flags(db)&HAM_RECORD_NUMBER)) {
        ham_bool_t isfew=HAM_FALSE;
        if (btree_node_get_ptr_left(node))
            isfew=btree_node_get_count(node)<btree_get_minkeys(maxkeys)-1;
        else
            isfew=btree_node_get_count(node)<btree_get_minkeys(maxkeys);
        if (isfew) {
            ham_log(("integrity check failed in page 0x%llx: not enough keys "
                    " (need %d, got %d)\n", page_get_self(page),
                    btree_get_minkeys(maxkeys), btree_node_get_count(node)));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    /*
     * check if the largest item of the left sibling is smaller than
     * the smallest item of this page
     */
    if (leftsib) {
        btree_node_t *sibnode=ham_page_get_btree_node(leftsib);

        int_key_t *sibentry=btree_node_get_key(db, sibnode, 
                btree_node_get_count(sibnode)-1);
        bte=btree_node_get_key(db, node, 0);

        if ((key_get_flags(bte)!=0 && key_get_flags(bte)!=KEY_IS_EXTENDED) && 
            !btree_node_is_leaf(node)) {
            ham_log(("integrity check failed in page 0x%llx: item #0 "
                    "has flags, but it's not a leaf page", 
                    page_get_self(page), i));
            return (HAM_INTEGRITY_VIOLATED);
        }

        cmp=db_compare_keys(db, page,
                btree_node_get_count(sibnode)-1,
                key_get_flags(sibentry), key_get_key(sibentry), key_get_size(sibentry),
                0, 
                key_get_flags(bte), key_get_key(bte), key_get_size(bte));
        if (db_get_error(db))
            return (db_get_error(db));
        if (cmp>=0) {
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
        
        cmp=__key_compare_int_to_int(page, (ham_u16_t)i, (ham_u16_t)(i+1));

        if (db_get_error(db))
            return (db_get_error(db));
        if (cmp>=0) {
            ham_log(("integrity check failed in page 0x%llx: item #%d "
                    "< item #%d", page_get_self(page), i, i+1));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    return (0);
}

#endif /* HAM_ENABLE_INTERNAL */
