/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "btree_node.h"

namespace ham {

class BtreeCheckAction
{
  public:
    BtreeCheckAction(BtreeIndex *btree)
      : m_btree(btree) {
    }

    ham_status_t run() {
      Page *page, *parent = 0;
      ham_u32_t level = 0;
      LocalDatabase *db = m_btree->get_db();

      ham_assert(m_btree->get_rootpage() != 0);

      /* get the root page of the tree */
      ham_status_t st = db->fetch_page(&page, m_btree->get_rootpage());
      if (st)
        return (st);

      /* for each level... */
      while (page) {
        BtreeNode *node = BtreeNode::from_page(page);
        ham_offset_t ptr_left = node->get_ptr_left();

        /* verify the page and all its siblings */
        st = verify_level(parent, page, level);
        if (st)
          break;
        parent = page;

        /* follow the pointer to the smallest child */
        if (ptr_left) {
          st = db->fetch_page(&page, ptr_left);
          if (st)
            return (st);
        }
        else
          page = 0;

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
      Page *child, *leftsib = 0;
      LocalDatabase *db = m_btree->get_db();
      BtreeNode *node = BtreeNode::from_page(page);

      /*
       * assert that the parent page's smallest item (item 0) is bigger
       * than the largest item in this page
       */
      if (parent && node->get_left()) {
        int cmp = compare_keys(db, page, 0, (ham_u16_t)(node->get_count() - 1));
        if (cmp < -1)
          return ((ham_status_t)cmp);
        if (cmp < 0) {
          ham_log(("integrity check failed in page 0x%llx: parent item "
                  "#0 < item #%d\n", page->get_self(), node->get_count() - 1));
          return (HAM_INTEGRITY_VIOLATED);
        }
      }

      while (page) {
        /* verify the page */
        ham_status_t st = verify_page(parent, leftsib, page, level);
        if (st)
          break;

        /* get the right sibling */
        BtreeNode *node = BtreeNode::from_page(page);
        if (node->get_right()) {
          st = db->fetch_page(&child, node->get_right());
          if (st)
            return (st);
        }
        else
          child = 0;

        leftsib = page;
        page = child;
      }

      return (0);
    }

    /** verify a single page */
    ham_status_t verify_page(Page *parent, Page *leftsib, Page *page,
                ham_u32_t level) {
      int cmp;
      LocalDatabase *db = m_btree->get_db();
      BtreeNode *node = BtreeNode::from_page(page);

      if (node->get_count() == 0) {
        /* a rootpage can be empty! check if this page is the rootpage */
        if (page->get_self() == m_btree->get_rootpage())
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
        BtreeNode *sibnode = BtreeNode::from_page(leftsib);
        BtreeKey *sibentry = sibnode->get_key(db, sibnode->get_count() - 1);
        BtreeKey *bte = node->get_key(db, 0);

        if ((bte->get_flags() != 0
            && bte->get_flags() != BtreeKey::KEY_IS_EXTENDED)
            && !node->is_leaf()) {
          ham_log(("integrity check failed in page 0x%llx: item #0 "
                  "has flags, but it's not a leaf page", page->get_self()));
          return (HAM_INTEGRITY_VIOLATED);
        }

        ham_status_t st;
        ham_key_t lhs;
        ham_key_t rhs;

        // TODO rewrite using BtreeIndex::compare_keys

        st = m_btree->prepare_key_for_compare(0, sibentry, &lhs);
        if (st)
          return (st);
        st = m_btree->prepare_key_for_compare(1, bte, &rhs);
        if (st)
          return (st);

        cmp = db->compare_keys(&lhs, &rhs);

        /* error is detected, but ensure keys are always released */
        if (cmp < -1)
          return ((ham_status_t)cmp);

        if (cmp >= 0) {
          ham_log(("integrity check failed in page 0x%llx: item #0 "
                  "< left sibling item #%d\n", page->get_self(),
                  sibnode->get_count() - 1));
          return (HAM_INTEGRITY_VIOLATED);
        }
      }

      if (node->get_count() == 1)
        return (0);

      for (ham_u16_t i = 0; i < node->get_count() - 1; i++) {
        /* if this is an extended key: check for a blob-id */
        BtreeKey *bte = node->get_key(db, i);
        if (bte->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
          ham_offset_t blobid = bte->get_extended_rid(db);
          if (!blobid) {
            ham_log(("integrity check failed in page 0x%llx: item #%d "
                    "is extended, but has no blob", page->get_self(), i));
            return (HAM_INTEGRITY_VIOLATED);
          }
        }

        cmp = compare_keys(db, page, (ham_u16_t)i, (ham_u16_t)(i + 1));
        if (cmp < -1)
          return (ham_status_t)cmp;
        if (cmp >= 0) {
          ham_log(("integrity check failed in page 0x%llx: item #%d "
                  "< item #%d", page->get_self(), i, i + 1));
          return (HAM_INTEGRITY_VIOLATED);
        }
      }

      return (0);
    }

    int compare_keys(LocalDatabase *db, Page *page,
            ham_u16_t lhs_int, ham_u16_t rhs_int) {
      ham_key_t lhs;
      ham_key_t rhs;
      BtreeNode *node = BtreeNode::from_page(page);

      BtreeKey *l = node->get_key(page->get_db(), lhs_int);
      BtreeKey *r = node->get_key(page->get_db(), rhs_int);

      // TODO rewrite using BtreeIndex::compare_keys

      ham_status_t st = m_btree->prepare_key_for_compare(0, l, &lhs);
      if (st)
        return (st);
      st = m_btree->prepare_key_for_compare(1, r, &rhs);
      if (st)
        return (st);

      return (page->get_db()->compare_keys(&lhs, &rhs));
    }

    BtreeIndex *m_btree;
};

ham_status_t
BtreeIndex::check_integrity()
{
  BtreeCheckAction bta(this);
  return (bta.run());
}

} // namespace ham
