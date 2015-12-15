/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * btree enumeration; visits each node
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_visitor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class BtreeVisitAction
{
  public:
    BtreeVisitAction(BtreeIndex *btree, Context *context, BtreeVisitor &visitor,
                    bool visit_internal_nodes)
      : m_btree(btree), m_context(context), m_visitor(visitor),
        m_visit_internal_nodes(visit_internal_nodes) {
    }

    void run() {
      LocalDatabase *db = m_btree->db();
      LocalEnvironment *env = db->lenv();

      uint32_t pm_flags = 0;
      if (m_visitor.is_read_only())
        pm_flags = PageManager::kReadOnly;

      // get the root page of the tree
      Page *page = env->page_manager()->fetch(m_context,
                    m_btree->root_address(), pm_flags);

      // go down to the leaf
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        uint64_t ptr_down = node->get_ptr_down();

        // visit internal nodes as well?
        if (ptr_down != 0 && m_visit_internal_nodes) {
          while (page) {
            node = m_btree->get_node_from_page(page);
            uint64_t right = node->get_right();

            m_visitor(m_context, node);

            // load the right sibling
            if (right)
              page = env->page_manager()->fetch(m_context, right, pm_flags);
            else
              page = 0;
          }
        }

        // follow the pointer to the smallest child
        if (ptr_down)
          page = env->page_manager()->fetch(m_context, ptr_down, pm_flags);
        else
          break;
      }

      ups_assert(page != 0);

      // now visit all leaf nodes
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        uint64_t right = node->get_right();

        m_visitor(m_context, node);

        /* follow the pointer to the right sibling */
        if (right)
          page = env->page_manager()->fetch(m_context, right, pm_flags);
        else
          break;
      }
    }

  private:
    BtreeIndex *m_btree;
    Context *m_context;
    BtreeVisitor &m_visitor;
    bool m_visit_internal_nodes;
};

void
BtreeIndex::visit_nodes(Context *context, BtreeVisitor &visitor,
                bool visit_internal_nodes)
{
  BtreeVisitAction bva(this, context, visitor, visit_internal_nodes);
  bva.run();
}

} // namespace upscaledb

