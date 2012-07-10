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
#include "btree_key.h"
#include "mem.h"
#include "page.h"

class BtreeCheckAction
{
  public:
    BtreeCheckAction(BtreeBackend *backend)
      : m_backend(backend) {
    }

    ham_status_t run() {
        Page *page, *parent=0; 
        ham_u32_t level=0;
        btree_node_t *node;
        ham_status_t st=0;
        ham_offset_t ptr_left;
        Database *db=m_backend->get_db();

        ham_assert(m_backend->get_rootpage()!=0);

        /* get the root page of the tree */
        st=db_fetch_page(&page, db, m_backend->get_rootpage(), 0);
        if (st)
            return (st);

        /* while we found a page... */
        while (page) {
            node=page_get_btree_node(page);
            ptr_left=btree_node_get_ptr_left(node);

            /* verify the page and all its siblings */
            st=verify_level(parent, page, level);
            if (st)
                break;
            parent=page;

            /* follow the pointer to the smallest child */
            if (ptr_left) {
                st=db_fetch_page(&page, db, ptr_left, 0);
                if (st)
                    return (st);
            }
            else
                page=0;

            ++level;
        }

        return (st);
    }

  private:
    /**
     * verify a whole level in the tree - start with "page" and traverse
     * the linked list of all the siblings
     */
    ham_status_t verify_level(Page *parent, Page *page, ham_u32_t level) {
        int cmp;
        ham_u32_t count=0;
        Page *child, *leftsib=0;
        ham_status_t st=0;
        btree_node_t *node=page_get_btree_node(page);
        Database *db=page->get_db();

        /* 
         * assert that the parent page's smallest item (item 0) is bigger
         * than the largest item in this page
         */
        if (parent && btree_node_get_left(node)) {
            btree_node_t *cnode=page_get_btree_node(page);

            cmp=compare_keys(db, page, 0,
                        (ham_u16_t)(btree_node_get_count(cnode)-1));
            if (cmp < -1)
                return ((ham_status_t)cmp);
            if (cmp<0) {
                ham_log(("integrity check failed in page 0x%llx: parent item "
                        "#0 < item #%d\n", page->get_self(), 
                        btree_node_get_count(cnode)-1));
                return (HAM_INTEGRITY_VIOLATED);
            }
        }

        while (page) {
            /* verify the page */
            st=verify_page(parent, leftsib, page, level, count);
            if (st)
                break;

            /* get the right sibling */
            node=page_get_btree_node(page);
            if (btree_node_get_right(node)) {
                st=db_fetch_page(&child, db, btree_node_get_right(node), 0);
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

    /** verify a single page */
    ham_status_t verify_page(Page *parent, Page *leftsib, Page *page,
                ham_u32_t level, ham_u32_t count) {
        int cmp;
        ham_size_t i=0;
        Database *db=page->get_db();
        btree_key_t *bte;
        btree_node_t *node=page_get_btree_node(page);

        count=btree_node_get_count(node);
        if (count==0) {
            /* a rootpage can be empty! check if this page is the rootpage */
            BtreeBackend *be=(BtreeBackend *)db->get_backend();
            if (page->get_self()==be->get_rootpage())
                return (0);

            ham_log(("integrity check failed in page 0x%llx: empty page!\n",
                    page->get_self()));
            return (HAM_INTEGRITY_VIOLATED);
        }

        /*
         * check if the largest item of the left sibling is smaller than
         * the smallest item of this page
         */
        if (leftsib) {
            btree_node_t *sibnode=page_get_btree_node(leftsib);
            btree_key_t *sibentry=btree_node_get_key(db, sibnode, 
                    btree_node_get_count(sibnode)-1);

            bte=btree_node_get_key(db, node, 0);

            if ((key_get_flags(bte)!=0 && key_get_flags(bte)!=KEY_IS_EXTENDED) 
                    && !btree_node_is_leaf(node)) {
                ham_log(("integrity check failed in page 0x%llx: item #0 "
                        "has flags, but it's not a leaf page", 
                        page->get_self(), i));
                return (HAM_INTEGRITY_VIOLATED);
            }
            else {
                ham_status_t st;
                ham_key_t lhs;
                ham_key_t rhs;

                st=btree_prepare_key_for_compare(db, 0, sibentry, &lhs);
                if (st)
                    return (st);
                st=btree_prepare_key_for_compare(db, 1, bte, &rhs);
                if (st)
                    return (st);

                cmp=db->compare_keys(&lhs, &rhs);

                /* error is detected, but ensure keys are always released */
                if (cmp < -1)
                    return ((ham_status_t)cmp);
            }

            if (cmp >= 0) {
                ham_log(("integrity check failed in page 0x%llx: item #0 "
                        "< left sibling item #%d\n", page->get_self(), 
                        btree_node_get_count(sibnode)-1));
                return (HAM_INTEGRITY_VIOLATED);
            }
        }

        if (count==1)
            return (0);

        for (i=0; i<count-1; i++) {
            /* if this is an extended key: check for a blob-id */
            bte=btree_node_get_key(db, node, i);
            if (key_get_flags(bte)&KEY_IS_EXTENDED) {
                ham_offset_t blobid=key_get_extended_rid(db, bte);
                if (!blobid) {
                    ham_log(("integrity check failed in page 0x%llx: item #%d "
                            "is extended, but has no blob", 
                            page->get_self(), i));
                    return (HAM_INTEGRITY_VIOLATED);
                }
            }
        
            cmp=compare_keys(db, page, (ham_u16_t)i, (ham_u16_t)(i+1));
            if (cmp < -1)
                return (ham_status_t)cmp;
            if (cmp>=0) {
                ham_log(("integrity check failed in page 0x%llx: item #%d "
                        "< item #%d", page->get_self(), i, i+1));
                return (HAM_INTEGRITY_VIOLATED);
            }
        }

        return (0);
    }

    int compare_keys(Database *db, Page *page, 
            ham_u16_t lhs_int, ham_u16_t rhs_int) {
        btree_key_t *l;
	    btree_key_t *r;
        btree_node_t *node=page_get_btree_node(page);
	    ham_key_t lhs;
	    ham_key_t rhs;
	    ham_status_t st;

        l=btree_node_get_key(page->get_db(), node, lhs_int);
        r=btree_node_get_key(page->get_db(), node, rhs_int);

	    st=btree_prepare_key_for_compare(db, 0, l, &lhs);
	    if (st) {
		    ham_assert(st < -1);
		    return (st);
	    }
	    st=btree_prepare_key_for_compare(db, 1, r, &rhs);
	    if (st) {
		    ham_assert(st < -1);
		    return (st);
	    }

	    return (page->get_db()->compare_keys(&lhs, &rhs));
    }

    BtreeBackend *m_backend;
};

ham_status_t
BtreeBackend::do_check_integrity()
{
    BtreeCheckAction bta(this);
    return (bta.run());
}
