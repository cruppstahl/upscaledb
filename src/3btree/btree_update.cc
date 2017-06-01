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

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "3page_manager/page_manager.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_update.h"
#include "3btree/btree_node_proxy.h"
#include "4cursor/cursor_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/* a unittest hook triggered when a page is split */
void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

// Calculates the pivot index of a split.
//
// For databases with sequential access (this includes recno databases):
// do not split in the middle, but at the very end of the page.
//
// If this page is the right-most page in the index, and the new key is
// inserted at the very end, then we select the same pivot as for
// sequential access.
static inline int
pivot_position(BtreeUpdateAction &state, BtreeNodeProxy *old_node,
                const ups_key_t *key, BtreeStatistics::InsertHints &hints)
{
  uint32_t old_count = old_node->length();
  assert(old_count > 2);

  bool pivot_at_end = false;
  if (ISSET(hints.flags, UPS_HINT_APPEND) && hints.append_count > 5)
    pivot_at_end = true;
  else if (old_node->right_sibling() == 0) {
    int cmp = old_node->compare(state.context, key, old_node->length() - 1);
    if (cmp > 0)
      pivot_at_end = true;
  }

  /* The position of the pivot key depends on the previous inserts; if most
   * of them were appends then pick a pivot key at the "end" of the node */
  int pivot;
  if (pivot_at_end || hints.append_count > 30)
    pivot = old_count - 2;
  else if (hints.append_count > 10)
    pivot = (int)(old_count / 100.f * 66);
  else if (hints.prepend_count > 10)
    pivot = (int)(old_count / 100.f * 33);
  else if (hints.prepend_count > 30)
    pivot = 2;
  else
    pivot = old_count / 2;

  assert(pivot > 0 && pivot <= (int)old_count - 2);

  return pivot;
}

// Allocates a new root page and sets it up in the btree
static inline Page *
allocate_new_root(BtreeUpdateAction &state, Page *old_root)
{
  LocalEnv *env = (LocalEnv *)state.btree->db()->env;

  Page *new_root = env->page_manager->alloc(state.context, Page::kTypeBroot);
  BtreeNodeProxy *new_node = state.btree->get_node_from_page(new_root);
  new_node->set_left_child(old_root->address());

  state.btree->set_root_page(new_root);
  Page *header = env->page_manager->fetch(state.context, 0);
  header->set_dirty(true);

  old_root->set_type(Page::kTypeBindex);

  return new_root;
}

/* Merges the |sibling| into |page|, returns the merged page and moves
 * the sibling to the freelist */ 
static inline Page *
merge_page(BtreeUpdateAction &state, Page *page, Page *sibling)
{
  LocalEnv *env = (LocalEnv *)state.btree->db()->env;
  BtreeNodeProxy *node = state.btree->get_node_from_page(page);
  BtreeNodeProxy *sib_node = state.btree->get_node_from_page(sibling);

  if (sib_node->is_leaf())
    BtreeCursor::uncouple_all_cursors(state.context, sibling, 0);

  node->merge_from(state.context, sib_node);
  page->set_dirty(true);

  // fix the linked list
  node->set_right_sibling(sib_node->right_sibling());
  if (node->right_sibling()) {
    Page *p = env->page_manager->fetch(state.context, node->right_sibling());
    BtreeNodeProxy *new_right_node = state.btree->get_node_from_page(p);
    new_right_node->set_left_sibling(page->address());
    p->set_dirty(true);
  }

  env->page_manager->del(state.context, sibling);

  Globals::ms_btree_smo_merge++;
  return page;
}

/* collapse the root node; returns the new root */
static inline Page *
collapse_root(BtreeUpdateAction &state, Page *root_page)
{
  LocalEnv *env = (LocalEnv *)state.btree->db()->env;
  BtreeNodeProxy *node = state.btree->get_node_from_page(root_page);
  assert(node->length() == 0);

  Page *header = env->page_manager->fetch(state.context, 0);
  header->set_dirty(true);

  Page *new_root = env->page_manager->fetch(state.context,
                  node->left_child());
  state.btree->set_root_page(new_root);
  env->page_manager->del(state.context, root_page);
  return new_root;
}

// Traverses the tree, looking for the leaf with the specified |key|. Will
// split or merge nodes while descending.
// Returns the leaf page and the |parent| of the leaf (can be null if
// there is no parent).
Page *
BtreeUpdateAction::traverse_tree(Context *context, const ups_key_t *key,
                BtreeStatistics::InsertHints &hints, Page **parent)
{
  LocalEnv *env = (LocalEnv *)btree->db()->env;

  Page *page = btree->root_page(context);
  BtreeNodeProxy *node = btree->get_node_from_page(page);

  *parent = 0;

  // if the root page is empty with children then collapse it
  if (unlikely(node->length() == 0 && !node->is_leaf())) {
    page = collapse_root(*this, page);
    node = btree->get_node_from_page(page);
  }

  int slot;

  // now walk down the tree
  while (!node->is_leaf()) {
    // is a split required?
    if (node->requires_split(context)) {
      page = split_page(page, *parent, key, hints);
      node = btree->get_node_from_page(page);
    }

    // get the child page
    Page *sibling = 0;
    Page *child_page = btree->find_lower_bound(context, page, key, 0, &slot);
    BtreeNodeProxy *child_node = btree->get_node_from_page(child_page);

    // We can merge this child with the RIGHT sibling iff...
    // 1. it's not the right-most slot (and therefore the right sibling has
    //      the same parent as the child)
    // 2. the child is a leaf!
    // 3. it's empty or has too few elements
    // 4. its right sibling is also empty
    if (unlikely(slot < (int)node->length() - 1
            && child_node->is_leaf()
            && child_node->requires_merge()
            && child_node->right_sibling() != 0)) {
      sibling = env->page_manager->fetch(context, child_node->right_sibling(),
                        PageManager::kOnlyFromCache);
      if (sibling != 0) {
        BtreeNodeProxy *sib_node = btree->get_node_from_page(sibling);
        if (sib_node->requires_merge()) {
          merge_page(*this, child_page, sibling);
          // also remove the link to the sibling from the parent
          node->erase(context, slot + 1);
          page->set_dirty(true);
        }
      }
    }

    // We can also merge this child with the LEFT sibling iff...
    // 1. it's not the left-most slot
    // 2. the child is a leaf!
    // 3. it's empty or has too few elements
    // 4. its left sibling is also empty
    else if (unlikely(slot > 0
                && child_node->is_leaf()
                && child_node->requires_merge()
                && child_node->left_sibling() != 0)) {
      sibling = env->page_manager->fetch(context, child_node->left_sibling(),
                            PageManager::kOnlyFromCache);
      if (sibling != 0) {
        BtreeNodeProxy *sib_node = btree->get_node_from_page(sibling);
        if (sib_node->requires_merge()) {
          merge_page(*this, sibling, child_page);
          // also remove the link to the sibling from the parent
          node->erase(context, slot);
          page->set_dirty(true);
          // continue traversal with the sibling
          child_page = sibling;
          child_node = sib_node;
        }
      }
    }

    *parent = page;

    // go down one level in the tree
    page = child_page;
    node = child_node;
  }

  return page;
}

Page *
BtreeUpdateAction::split_page(Page *old_page, Page *parent,
                const ups_key_t *key, BtreeStatistics::InsertHints &hints)
{
  LocalEnv *env = (LocalEnv *)btree->db()->env;
  BtreeNodeProxy *old_node = btree->get_node_from_page(old_page);

  /* allocate a new page and initialize it */
  Page *new_page = env->page_manager->alloc(context, Page::kTypeBindex);
  {
    PBtreeNode *node = PBtreeNode::from_page(new_page);
    node->set_flags(old_node->is_leaf() ? PBtreeNode::kLeafNode : 0);
  }
  BtreeNodeProxy *new_node = btree->get_node_from_page(new_page);

  /* no parent page? then we're splitting the root page. allocate
   * a new root page */
  if (unlikely(!parent))
    parent = allocate_new_root(*this, old_page);

  Page *to_return = 0;
  ByteArray pivot_key_arena;
  ups_key_t pivot_key = {0};

  /* if the key is appended then don't split the page; simply allocate
   * a new page and insert the new key. */
  int pivot = 0;
  if (ISSET(hints.flags, UPS_HINT_APPEND) && old_node->is_leaf()) {
    int cmp = old_node->compare(context, key, old_node->length() - 1);
    if (likely(cmp == +1)) {
      to_return = new_page;
      pivot_key = *key;
      pivot = old_node->length();
    }
  }

  /* no append? then calculate the pivot key and perform the split */
  if (pivot != (int)old_node->length()) {
    pivot = pivot_position(*this, old_node, key, hints);

    /* and store the pivot key for later */
    old_node->key(context, pivot, &pivot_key_arena, &pivot_key);

    /* leaf page: uncouple all cursors */
    if (old_node->is_leaf())
      BtreeCursor::uncouple_all_cursors(context, old_page, pivot);
    /* internal page: fix the ptr_down of the new page
     * (it must point to the ptr of the pivot key) */
    else
      new_node->set_left_child(old_node->record_id(context, pivot));

    /* now move some of the key/rid-tuples to the new page */
    old_node->split(context, new_node, pivot);

    // if the new key is >= the pivot key then continue with the right page,
    // otherwise continue with the left page
    to_return = btree->compare_keys((ups_key_t *)key, &pivot_key) >= 0
                      ? new_page
                      : old_page;
  }

  /* update the parent page */
  BtreeNodeProxy *parent_node = btree->get_node_from_page(parent);
  uint64_t rid = new_page->address();
  ups_record_t record = ups_make_record(&rid, sizeof(rid));
  ups_status_t st = insert_in_page(parent, &pivot_key, &record, hints);
  if (st)
    throw Exception(st);

  /* new root page? then also set the cuild pointer */
  if (parent_node->length() == 0)
    parent_node->set_left_child(old_page->address());

  /* fix the double-linked list of pages, and mark the pages as dirty */
  if (old_node->right_sibling()) {
    Page *sib_page = env->page_manager->fetch(context,
                    old_node->right_sibling());
    BtreeNodeProxy *sib_node = btree->get_node_from_page(sib_page);
    sib_node->set_left_sibling(new_page->address());
    sib_page->set_dirty(true);
  }
  new_node->set_left_sibling(old_page->address());
  new_node->set_right_sibling(old_node->right_sibling());
  old_node->set_right_sibling(new_page->address());
  new_page->set_dirty(true);
  old_page->set_dirty(true);

  Globals::ms_btree_smo_split++;

  if (unlikely(g_BTREE_INSERT_SPLIT_HOOK != 0))
    g_BTREE_INSERT_SPLIT_HOOK();

  return to_return;
}

ups_status_t
BtreeUpdateAction::insert_in_page(Page *page, ups_key_t *key,
                ups_record_t *record, BtreeStatistics::InsertHints &hints,
                bool force_prepend, bool force_append)
{
  bool exists = false;

  BtreeNodeProxy *node = btree->get_node_from_page(page);

  int flags = 0;
  if (force_prepend)
    flags |= PBtreeNode::kInsertPrepend;
  if (force_append)
    flags |= PBtreeNode::kInsertAppend;

  PBtreeNode::InsertResult result = node->insert(context, key, flags);
  switch (result.status) {
    case UPS_DUPLICATE_KEY:
      if (ISSET(hints.flags, UPS_OVERWRITE)) {
        /* key already exists; only overwrite the data */
        if (!node->is_leaf())
          return UPS_SUCCESS;
      }
      else if (NOTSET(hints.flags, UPS_DUPLICATE))
        return UPS_DUPLICATE_KEY;
      /* do NOT shift keys up to make room; just overwrite the
       * current [slot] */
      exists = true;
      break;
    case UPS_SUCCESS:
      break;
    default:
      return result.status;
  }

  uint32_t new_duplicate_id = 0;
  if (exists) {
    if (node->is_leaf()) {
      // overwrite record blob
      node->set_record(context, result.slot, record, duplicate_index,
                      hints.flags, &new_duplicate_id);

      hints.processed_leaf_page = page;
      hints.processed_slot = result.slot;
    }
    else {
      // overwrite record id
      assert(record->size == sizeof(uint64_t));
      node->set_record_id(context, result.slot, *(uint64_t *)record->data);
    }
  }
  // key does not exist and has to be inserted or appended
  else {
    try {
      if (node->is_leaf()) {
        // allocate record id
        node->set_record(context, result.slot, record, duplicate_index,
                        hints.flags, &new_duplicate_id);

        hints.processed_leaf_page = page;
        hints.processed_slot = result.slot;
      }
      else {
        // set the internal record id
        assert(record->size == sizeof(uint64_t));
        node->set_record_id(context, result.slot, *(uint64_t *)record->data);
      }
    }
    // In case of an error: undo the insert. This happens very rarely but
    // it's possible, i.e. if the BlobManager fails to allocate storage.
    catch (Exception &ex) {
      if (result.slot < (int)node->length())
        node->erase(context, result.slot);
      throw ex;
    }
  }

  page->set_dirty(true);

  // if this update was triggered with a cursor (and this is a leaf node):
  // couple it to the inserted key
  // TODO only when performing an insert(), not an erase()!
  if (cursor && node->is_leaf())
    cursor->couple_to(page, result.slot, new_duplicate_id);

  return 0;
}

} // namespace upscaledb
