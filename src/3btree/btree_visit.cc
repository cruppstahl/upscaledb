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

struct BtreeVisitAction
{
  BtreeVisitAction(BtreeIndex *btree_, Context *context_,
                  BtreeVisitor &visitor_, bool visit_internal_nodes_)
    : btree(btree_), context(context_), visitor(visitor_),
      visit_internal_nodes(visit_internal_nodes_) {
  }

  void run() {
    LocalEnvironment *env = (LocalEnvironment *)btree->db()->env;

    uint32_t page_manager_flags = 0;
    if (visitor.is_read_only())
      page_manager_flags = PageManager::kReadOnly;

    // get the root page of the tree
    Page *page = btree->root_page(context);

    // go down to the leaf
    while (page) {
      BtreeNodeProxy *node = btree->get_node_from_page(page);
      uint64_t left_child = node->left_child();

      // visit internal nodes as well?
      if (left_child != 0 && visit_internal_nodes) {
        while (page) {
          node = btree->get_node_from_page(page);
          uint64_t right = node->right_sibling();

          visitor(context, node);

          // load the right sibling
          if (likely(right))
            page = env->page_manager()->fetch(context, right,
                            page_manager_flags);
          else
            page = 0;
        }
      }

      // follow the pointer to the smallest child
      if (likely(left_child))
        page = env->page_manager()->fetch(context, left_child,
                        page_manager_flags);
      else
        break;
    }

    assert(page != 0);

    // now visit all leaf nodes
    while (page) {
      BtreeNodeProxy *node = btree->get_node_from_page(page);
      uint64_t right = node->right_sibling();

      visitor(context, node);

      /* follow the pointer to the right sibling */
      if (likely(right))
        page = env->page_manager()->fetch(context, right, page_manager_flags);
      else
        break;
    }
  }

  BtreeIndex *btree;
  Context *context;
  BtreeVisitor &visitor;
  bool visit_internal_nodes;
};

void
BtreeIndex::visit_nodes(Context *context, BtreeVisitor &visitor,
              bool visit_internal_nodes)
{
  BtreeVisitAction bva(this, context, visitor, visit_internal_nodes);
  bva.run();
}

} // namespace upscaledb

