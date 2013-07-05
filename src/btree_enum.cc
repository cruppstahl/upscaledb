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
#include "btree_enum.h"
#include "page_manager.h"

namespace hamsterdb {

class BtreeEnumAction
{
  public:
    BtreeEnumAction(BtreeIndex *btree, BtreeVisitor *visitor)
      : m_btree(btree), m_visitor(visitor) {
      ham_assert(m_btree->get_root_address() != 0);
      ham_assert(m_visitor != 0);
    }

    ham_status_t run() {
      Page *page;
      LocalDatabase *db = m_btree->get_db();
      ham_status_t st;

      // get the root page of the tree
      st = db->get_env()->get_page_manager()->fetch_page(&page,
                      db, m_btree->get_root_address());
      if (st)
        return (st);

      // go down to the leaf
      while (page) {
        PBtreeNode *node = PBtreeNode::from_page(page);
        ham_u64_t ptr_left = node->get_ptr_left();

        // visit internal nodes as well?
        if (ptr_left != 0 && m_visitor->visit_internal_nodes()) {
          while (page) {
            node = PBtreeNode::from_page(page);
            st = visit_node(node);
            if (st)
              return (st);

            // load the right sibling
            ham_u64_t right = node->get_right();
            if (right) {
              st = db->get_env()->get_page_manager()->fetch_page(&page,
                              db, right);
              if (st)
                return (st);
            }
            else
              page = 0;
          }
        }

        // follow the pointer to the smallest child
        if (ptr_left) {
          st = db->get_env()->get_page_manager()->fetch_page(&page,
                      db, ptr_left);
          if (st)
            return (st);
        }
        else
          break;
      }

      ham_assert(page != 0);

      // now enumerate all leaf nodes
      while (page) {
        PBtreeNode *node = PBtreeNode::from_page(page);
        ham_u64_t right = node->get_right();

        st = visit_node(node);
        if (st)
          return (st);

        /* follow the pointer to the right sibling */
        if (right) {
          st = db->get_env()->get_page_manager()->fetch_page(&page, db, right);
          if (st)
            return (st);
        }
        else
          break;
      }

      return (st);
    }

  private:
    ham_status_t visit_node(PBtreeNode *node) {
      LocalDatabase *db = m_btree->get_db();
      ham_status_t st;

      // 'visit' each key
      for (ham_size_t i = 0; i < node->get_count(); i++) {
        PBtreeKey *bte = node->get_key(db, i);
        st = m_visitor->item(node, bte);
        if (st == BtreeVisitor::kSkipPage) {
          st = 0;
          break;
        }
        else if (st)
          return (st);
      }

      return (0);
    }

    BtreeIndex *m_btree;
    BtreeVisitor *m_visitor;
};

ham_status_t
BtreeIndex::enumerate(BtreeVisitor *visitor)
{
  BtreeEnumAction bea(this, visitor);
  return (bea.run());
}

} // namespace hamsterdb

