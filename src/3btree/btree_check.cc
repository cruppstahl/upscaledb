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
 * btree verification
 */

#include "0root/root.h"

#include <set>
#include <string.h>
#include <stdio.h>
#ifndef NDEBUG
#  include <sstream>
#  include <fstream>
#endif

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BtreeCheckAction
{
  // Constructor
  BtreeCheckAction(BtreeIndex *btree_, Context *context_, uint32_t flags_)
    : btree(btree_), context(context_), flags(flags_) {
  }

  // This is the main method; it starts the verification.
  void run() {
    Page *page, *parent = 0;
    uint32_t level = 0;
    LocalDb *db = btree->db();
    LocalEnv *env = (LocalEnv *)db->env;

    // get the root page of the tree
    page = btree->root_page(context);

    if (ISSET(flags, UPS_PRINT_GRAPH)) {
      graph << "digraph g {" << std::endl
            << "  graph [" << std::endl
            << "    rankdir = \"TD\"" << std::endl
            << "  ];" << std::endl
            << "  node [" << std::endl
            << "    fontsize = \"8\"" << std::endl
            << "    shape = \"ellipse\"" << std::endl
            << "  ];" << std::endl
            << "  edge [" << std::endl
            << "  ];" << std::endl;
    }

    // for each level...
    while (page) {
      BtreeNodeProxy *node = btree->get_node_from_page(page);
      uint64_t ptr_down = node->left_child();

      // verify the page and all its siblings
      verify_level(parent, page, level);
      parent = page;
      page = 0;

      // follow the pointer to the smallest child
      if (ptr_down)
        page = env->page_manager->fetch(context, ptr_down,
                              PageManager::kReadOnly);

      ++level;
    }

    if (ISSET(flags, UPS_PRINT_GRAPH)) {
      graph << "}" << std::endl;

      std::ofstream file;
      file.open("graph.dot");
      file << graph.str();
    }
  }

  // Verifies a whole level in the tree - start with "page" and traverse
  // the linked list of all the siblings
  void verify_level(Page *parent, Page *page, uint32_t level) {
    LocalDb *db = btree->db();
    LocalEnv *env = (LocalEnv *)db->env;
    Page *child, *leftsib = 0;
    BtreeNodeProxy *node = btree->get_node_from_page(page);

    // assert that the parent page's smallest item (item 0) is bigger
    // than the largest item in this page
    if (parent && node->left_sibling()) {
      int cmp = compare_keys(db, page, 0, node->length() - 1);
      if (unlikely(cmp <= 0)) {
        ups_log(("integrity check failed in page 0x%llx: parent item "
                "#0 <= item #%d\n", page->address(),
                node->length() - 1));
        throw Exception(UPS_INTEGRITY_VIOLATED);
      }
    }

    children.clear();

    while (page) {
      // verify the page
      verify_page(parent, leftsib, page, level);

      // follow the right sibling
      BtreeNodeProxy *node = btree->get_node_from_page(page);
      child = 0;
      if (node->right_sibling())
        child = env->page_manager->fetch(context, node->right_sibling(),
                        PageManager::kReadOnly);

      if (leftsib) {
        BtreeNodeProxy *leftnode = btree->get_node_from_page(leftsib);
        if (unlikely(leftnode->is_leaf() != node->is_leaf())) {
          ups_log(("integrity check failed in page 0x%llx: left sibling is "
                  "leaf %d, page is leaf %d\n", (int)leftnode->is_leaf(),
                  (int)node->is_leaf()));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
      }

      leftsib = page;
      page = child;
    }
  }

  // Verifies a single page
  void verify_page(Page *parent, Page *leftsib, Page *page, uint32_t level) {
    LocalDb *db = btree->db();
    LocalEnv *env = (LocalEnv *)db->env;
    BtreeNodeProxy *node = btree->get_node_from_page(page);

    if (ISSET(flags, UPS_PRINT_GRAPH)) {
      std::stringstream ss;
      ss << "node" << page->address();
      graph << "  \"" << ss.str() << "\" [" << std::endl
              << "    label = \"";
      graph << "<fl>L|<fd>D|";
      for (uint32_t i = 0; i < node->length(); i++) {
        graph << "<f" << i << ">" << i << "|";
      }
      graph << "<fr>R\"" << std::endl
              << "    shape = \"record\"" << std::endl
              << "  ];" << std::endl;
#if 0
      // edge to the left sibling
      if (node->left_sibling())
        graph << "\"" << ss.str() << "\":fl -> \"node"
              << node->left_sibling() << "\":fr [" << std::endl
              << "  ];" << std::endl;
      // to the right sibling
      if (node->right_sibling())
        graph << "  \"" << ss.str() << "\":fr -> \"node"
              << node->right_sibling() << "\":fl [" << std::endl
              << "  ];" << std::endl;
#endif
      // to ptr_down
      if (node->left_child())
        graph << "  \"" << ss.str() << "\":fd -> \"node"
              << node->left_child() << "\":fd [" << std::endl
              << "  ];" << std::endl;
      // to all children
      if (!node->is_leaf()) {
        for (uint32_t i = 0; i < node->length(); i++) {
          graph << "  \"" << ss.str() << "\":f" << i << " -> \"node"
                  << node->record_id(context, i) << "\":fd ["
                  << std::endl << "  ];" << std::endl;
        }
      }
    }

    if (unlikely(node->length() == 0)) {
      // a rootpage can be empty! check if this page is the rootpage
      if (page->address() == btree->root_page(context)->address())
        return;

      // for internal nodes: ptr_down HAS to be set!
      if (unlikely(!node->is_leaf() && node->left_child() == 0)) {
        ups_log(("integrity check failed in page 0x%llx: empty page!\n",
                page->address()));
        throw Exception(UPS_INTEGRITY_VIOLATED);
      }
    }

    // check if the largest item of the left sibling is smaller than
    // the smallest item of this page
    if (leftsib) {
      BtreeNodeProxy *sibnode = btree->get_node_from_page(leftsib);
      ups_key_t key1 = {0};
      ups_key_t key2 = {0};

      node->check_integrity(context);

      if (node->length() > 0 && sibnode->length() > 0) {
        sibnode->key(context, sibnode->length() - 1, &barray1, &key1);
        node->key(context, 0, &barray2, &key2);

        if (unlikely(node->compare(&key1, &key2) >= 0)) {
          ups_log(("integrity check failed in page 0x%llx: item #0 "
                  "< left sibling item #%d\n", page->address(),
                  sibnode->length() - 1));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
      }
    }

    node->check_integrity(context);

    if (likely(node->length() > 1)) {
      for (uint32_t i = 0; i < node->length() - 1; i++) {
        int cmp = compare_keys(db, page, (uint32_t)i, (uint32_t)(i + 1));
        if (unlikely(cmp >= 0)) {
          ups_log(("integrity check failed in page 0x%llx: item #%d "
                  "< item #%d", page->address(), i, i + 1));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
      }
    }

    // internal nodes: make sure that all record IDs are unique
    if (!node->is_leaf()) {
      if (unlikely(children.find(node->left_child()) != children.end())) {
        ups_log(("integrity check failed in page 0x%llx: record of item "
                "-1 is not unique", page->address()));
        throw Exception(UPS_INTEGRITY_VIOLATED);
      }

      children.insert(node->left_child());

      for (uint32_t i = 0; i < node->length(); i++) {
        uint64_t child_id = node->record_id(context, i);
        if (unlikely(children.find(child_id) != children.end())) {
          ups_log(("integrity check failed in page 0x%llx: record of item "
                  "#%d is not unique", page->address(), i));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }

        // TODO replace this line with a "real" function
        if (unlikely(env->page_manager->state->freelist.has(child_id))) {
          ups_log(("integrity check failed in page 0x%llx: record of item "
                  "#%d is in freelist", page->address(), i));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }

        children.insert(child_id);
      }
    }
  }

  int compare_keys(LocalDb *db, Page *page, int lhs, int rhs) {
    BtreeNodeProxy *node = btree->get_node_from_page(page);
    ups_key_t key1 = {0};
    ups_key_t key2 = {0};

    node->key(context, lhs, &barray1, &key1);
    node->key(context, rhs, &barray2, &key2);
    return node->compare(&key1, &key2);
  }

  // The BtreeIndex on which we operate
  BtreeIndex *btree;

  // The current Context
  Context *context;

  // The flags as specified when calling ups_db_check_integrity
  uint32_t flags;

  // ByteArrays to avoid frequent memory allocations
  ByteArray barray1;
  ByteArray barray2;

  // For checking uniqueness of record IDs on an internal level
  std::set<uint64_t> children;

  // For printing the graph
  std::ostringstream graph;
};

void
BtreeIndex::check_integrity(Context *context, uint32_t flags)
{
  BtreeCheckAction bta(this, context, flags);
  bta.run();
}

} // namespace upscaledb
