/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * btree enumeration; visits each node
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class BtreeVisitAction
{
  public:
    BtreeVisitAction(BtreeIndex *btree, BtreeVisitor &visitor,
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
        uint64_t ptr_down = node->get_ptr_down();

        // visit internal nodes as well?
        if (ptr_down != 0 && m_visit_internal_nodes) {
          while (page) {
            node = m_btree->get_node_from_page(page);
            m_visitor(node);

            // load the right sibling
            uint64_t right = node->get_right();
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

      // now visit all leaf nodes
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        uint64_t right = node->get_right();

        m_visitor(node);

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
BtreeIndex::visit_nodes(BtreeVisitor &visitor, bool visit_internal_nodes)
{
  BtreeVisitAction bva(this, visitor, visit_internal_nodes);
  bva.run();
}

} // namespace hamsterdb

