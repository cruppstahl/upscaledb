/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "db.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"
#include "page_manager.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

/*
 * btree verification
 */
class BtreeCheckAction
{
  public:
    // Constructor
    BtreeCheckAction(BtreeIndex *btree)
      : m_btree(btree) {
    }

    // This is the main method; it starts the verification.
    void run() {
      Page *page, *parent = 0;
      ham_u32_t level = 0;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      ham_assert(m_btree->get_root_address() != 0);

      // get the root page of the tree
      page = env->get_page_manager()->fetch_page(db,
              m_btree->get_root_address());

      // for each level...
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        ham_u64_t ptr_down = node->get_ptr_down();

        // verify the page and all its siblings
        verify_level(parent, page, level);
        parent = page;

        // follow the pointer to the smallest child
        if (ptr_down)
          page = env->get_page_manager()->fetch_page(db, ptr_down);
        else
          page = 0;

        ++level;
    }
  }

  private:
    // Verifies a whole level in the tree - start with "page" and traverse
    // the linked list of all the siblings
    void verify_level(Page *parent, Page *page, ham_u32_t level) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      Page *child, *leftsib = 0;
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      // assert that the parent page's smallest item (item 0) is bigger
      // than the largest item in this page
      if (parent && node->get_left()) {
        int cmp = compare_keys(db, page, 0, node->get_count() - 1);
        if (cmp < 0) {
          ham_log(("integrity check failed in page 0x%llx: parent item "
                  "#0 < item #%d\n", page->get_address(),
                  node->get_count() - 1));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }
      }

      while (page) {
        // verify the page
        verify_page(parent, leftsib, page, level);

        // follow the right sibling
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        if (node->get_right())
          child = env->get_page_manager()->fetch_page(db, node->get_right());
        else
          child = 0;

        leftsib = page;
        page = child;
      }
    }

    // Verifies a single page
    void verify_page(Page *parent, Page *leftsib, Page *page, ham_u32_t level) {
      LocalDatabase *db = m_btree->get_db();
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      if (node->get_count() == 0) {
        // a rootpage can be empty! check if this page is the rootpage
        if (page->get_address() == m_btree->get_root_address())
          return;

        ham_log(("integrity check failed in page 0x%llx: empty page!\n",
                page->get_address()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      // check if the largest item of the left sibling is smaller than
      // the smallest item of this page
      if (leftsib) {
        BtreeNodeProxy *sibnode = m_btree->get_node_from_page(leftsib);
        ham_key_t key1 = {0};
        ham_key_t key2 = {0};

        node->check_integrity();

        sibnode->get_key(sibnode->get_count() - 1, &m_barray1, &key1);
        node->get_key(0, &m_barray2, &key2);

        int cmp = node->compare(&key1, &key2);
        if (cmp >= 0) {
          ham_log(("integrity check failed in page 0x%llx: item #0 "
                  "< left sibling item #%d\n", page->get_address(),
                  sibnode->get_count() - 1));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }
      }

      if (node->get_count() == 1)
        return;

      node->check_integrity();

      for (ham_u16_t i = 0; i < node->get_count() - 1; i++) {
        int cmp = compare_keys(db, page, (ham_u16_t)i, (ham_u16_t)(i + 1));
        if (cmp >= 0) {
          ham_log(("integrity check failed in page 0x%llx: item #%d "
                  "< item #%d", page->get_address(), i, i + 1));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }
      }
    }

    int compare_keys(LocalDatabase *db, Page *page, int lhs, int rhs) {
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      ham_key_t key1 = {0};
      ham_key_t key2 = {0};

      node->get_key(lhs, &m_barray1, &key1);
      node->get_key(rhs, &m_barray2, &key2);

      return (node->compare(&key1, &key2));
    }

    BtreeIndex *m_btree;
    ByteArray m_barray1;
    ByteArray m_barray2;
};

void
BtreeIndex::check_integrity()
{
  BtreeCheckAction bta(this);
  bta.run();
}

} // namespace hamsterdb
