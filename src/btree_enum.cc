/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include "page_manager.h"
#include "btree_index.h"
#include "btree_node_proxy.h"


namespace hamsterdb {

class BtreeEnumAction
{
  public:
    BtreeEnumAction(BtreeIndex *btree, BtreeVisitor &visitor,
                    bool visit_internal_nodes)
      : m_btree(btree), m_visitor(visitor),
        m_visit_internal_nodes(visit_internal_nodes) {
      ham_assert(m_btree->get_root_address() != 0);
    }

    void run() {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      // get the root page of the tree
      Page *page = env->get_page_manager()->fetch_page(db,
                    m_btree->get_root_address());

      // go down to the leaf
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        ham_u64_t ptr_down = node->get_ptr_down();

        // visit internal nodes as well?
        if (ptr_down != 0 && m_visit_internal_nodes) {
          while (page) {
            node = m_btree->get_node_from_page(page);
            node->enumerate(m_visitor);

            // load the right sibling
            ham_u64_t right = node->get_right();
            if (right)
              page = env->get_page_manager()->fetch_page(db, right);
            else
              page = 0;
          }
        }

        // follow the pointer to the smallest child
        if (ptr_down)
          page = env->get_page_manager()->fetch_page(db, ptr_down);
        else
          break;
      }

      ham_assert(page != 0);

      // now enumerate all leaf nodes
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        ham_u64_t right = node->get_right();

        node->enumerate(m_visitor);

        /* follow the pointer to the right sibling */
        if (right)
          page = env->get_page_manager()->fetch_page(db, right);
        else
          break;
      }
    }

  private:
    BtreeIndex *m_btree;
    BtreeVisitor &m_visitor;
    bool m_visit_internal_nodes;
};

void
BtreeIndex::enumerate(BtreeVisitor &visitor, bool visit_internal_nodes)
{
  BtreeEnumAction bea(this, visitor, visit_internal_nodes);
  bea.run();
}

} // namespace hamsterdb

