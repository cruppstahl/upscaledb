/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    LocalEnv *env = (LocalEnv *)btree->db()->env;

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
            page = env->page_manager->fetch(context, right, page_manager_flags);
          else
            page = 0;
        }
      }

      // follow the pointer to the smallest child
      if (likely(left_child))
        page = env->page_manager->fetch(context, left_child, page_manager_flags);
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
        page = env->page_manager->fetch(context, right, page_manager_flags);
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

