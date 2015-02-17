/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/* a unittest hook triggered when a page is split */
void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

// Traverses the tree, looking for the leaf with the specified |key|. Will
// split or merge nodes while descending.
// Returns the leaf page and the |parent| of the leaf (can be null if
// there is no parent).
Page *
BtreeUpdateAction::traverse_tree(const ham_key_t *key,
                        BtreeStatistics::InsertHints &hints,
                        Page **parent)
{
  LocalDatabase *db = m_btree->get_db();
  LocalEnvironment *env = db->lenv();

  Page *page = env->page_manager()->fetch(m_context,
                m_btree->get_root_address());
  BtreeNodeProxy *node = m_btree->get_node_from_page(page);

  *parent = 0;

  // if the root page is empty with children then collapse it
  if (node->get_count() == 0 && !node->is_leaf()) {
    page = collapse_root(page);
    node = m_btree->get_node_from_page(page);
  }

  int slot;

  // now walk down the tree
  while (!node->is_leaf()) {
    // is a split required?
    if (node->requires_split(m_context)) {
      page = split_page(page, *parent, key, hints);
      node = m_btree->get_node_from_page(page);
    }

    // get the child page
    Page *sib_page = 0;
    Page *child_page = m_btree->find_child(m_context, page, key, 0, &slot);
    BtreeNodeProxy *child_node = m_btree->get_node_from_page(child_page);

    // We can merge this child with the RIGHT sibling iff...
    // 1. it's not the right-most slot (and therefore the right sibling has
    //      the same parent as the child)
    // 2. the child is a leaf!
    // 3. it's empty or has too few elements
    // 4. its right sibling is also empty
    if (slot < (int)node->get_count() - 1
        && child_node->is_leaf()
        && child_node->requires_merge()
        && child_node->get_right() != 0) {
      sib_page = env->page_manager()->fetch(m_context,
                            child_node->get_right(),
                        PageManager::kOnlyFromCache);
      if (sib_page != 0) {
        BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
        if (sib_node->requires_merge()) {
          merge_page(child_page, sib_page);
          // also remove the link to the sibling from the parent
          node->erase(m_context, slot + 1);
          page->set_dirty(true);
        }
      }
    }

    // We can also merge this child with the LEFT sibling iff...
    // 1. it's not the left-most slot
    // 2. the child is a leaf!
    // 3. it's empty or has too few elements
    // 4. its left sibling is also empty
    else if (slot > 0
        && child_node->is_leaf()
        && child_node->requires_merge()
        && child_node->get_left() != 0) {
      sib_page = env->page_manager()->fetch(m_context,
                            child_node->get_left(),
                            PageManager::kOnlyFromCache);
      if (sib_page != 0) {
        BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
        if (sib_node->requires_merge()) {
          merge_page(sib_page, child_page);
          // also remove the link to the sibling from the parent
          node->erase(m_context, slot);
          page->set_dirty(true);
          // continue traversal with the sibling
          child_page = sib_page;
          child_node = sib_node;
        }
      }
    }

    *parent = page;

    // go down one level in the tree
    page = child_page;
    node = child_node;
  }

  return (page);
}

Page *
BtreeUpdateAction::merge_page(Page *page, Page *sibling)
{
  LocalDatabase *db = m_btree->get_db();
  LocalEnvironment *env = db->lenv();

  BtreeNodeProxy *node = m_btree->get_node_from_page(page);
  BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sibling);

  if (sib_node->is_leaf())
    BtreeCursor::uncouple_all_cursors(m_context, sibling, 0);

  node->merge_from(m_context, sib_node);
  page->set_dirty(true);

  // fix the linked list
  node->set_right(sib_node->get_right());
  if (node->get_right()) {
    Page *new_right = env->page_manager()->fetch(m_context, node->get_right());
    BtreeNodeProxy *new_right_node = m_btree->get_node_from_page(new_right);
    new_right_node->set_left(page->get_address());
    new_right->set_dirty(true);
  }

  m_btree->get_statistics()->reset_page(sibling);
  m_btree->get_statistics()->reset_page(page);
  env->page_manager()->del(m_context, sibling);

  BtreeIndex::ms_btree_smo_merge++;
  return (page);
}

Page *
BtreeUpdateAction::collapse_root(Page *root_page)
{
  LocalEnvironment *env = root_page->get_db()->lenv();
  BtreeNodeProxy *node = m_btree->get_node_from_page(root_page);
  ham_assert(node->get_count() == 0);

  m_btree->get_statistics()->reset_page(root_page);
  m_btree->set_root_address(m_context, node->get_ptr_down());
  Page *header = env->page_manager()->fetch(m_context, 0);
  header->set_dirty(true);

  Page *new_root = env->page_manager()->fetch(m_context,
                        m_btree->get_root_address());
  new_root->set_type(Page::kTypeBroot);
  env->page_manager()->del(m_context, root_page);
  return (new_root);
}

Page *
BtreeUpdateAction::split_page(Page *old_page, Page *parent,
                                const ham_key_t *key,
                                BtreeStatistics::InsertHints &hints)
{
  LocalDatabase *db = m_btree->get_db();
  LocalEnvironment *env = db->lenv();

  m_btree->get_statistics()->reset_page(old_page);
  BtreeNodeProxy *old_node = m_btree->get_node_from_page(old_page);

  /* allocate a new page and initialize it */
  Page *new_page = env->page_manager()->alloc(m_context, Page::kTypeBindex);
  {
    PBtreeNode *node = PBtreeNode::from_page(new_page);
    node->set_flags(old_node->is_leaf() ? PBtreeNode::kLeafNode : 0);
  }
  BtreeNodeProxy *new_node = m_btree->get_node_from_page(new_page);

  /* no parent page? then we're splitting the root page. allocate
   * a new root page */
  if (!parent)
    parent = allocate_new_root(old_page);

  Page *to_return = 0;
  ByteArray pivot_key_arena;
  ham_key_t pivot_key = {0};

  /* if the key is appended then don't split the page; simply allocate
   * a new page and insert the new key. */
  int pivot = 0;
  if (hints.flags & HAM_HINT_APPEND && old_node->is_leaf()) {
    int cmp = old_node->compare(m_context, key, old_node->get_count() - 1);
    if (cmp == +1) {
      to_return = new_page;
      pivot_key = *key;
      pivot = old_node->get_count();
    }
  }

  /* no append? then calculate the pivot key and perform the split */
  if (pivot != (int)old_node->get_count()) {
    pivot = get_pivot(old_node, key, hints);

    /* and store the pivot key for later */
    old_node->get_key(m_context, pivot, &pivot_key_arena, &pivot_key);

    /* leaf page: uncouple all cursors */
    if (old_node->is_leaf())
      BtreeCursor::uncouple_all_cursors(m_context, old_page, pivot);
    /* internal page: fix the ptr_down of the new page
     * (it must point to the ptr of the pivot key) */
    else
      new_node->set_ptr_down(old_node->get_record_id(m_context, pivot));

    /* now move some of the key/rid-tuples to the new page */
    old_node->split(m_context, new_node, pivot);

    // if the new key is >= the pivot key then continue with the right page,
    // otherwise continue with the left page
    to_return = m_btree->compare_keys((ham_key_t *)key, &pivot_key) >= 0
                      ? new_page
                      : old_page;
  }

  /* update the parent page */
  BtreeNodeProxy *parent_node = m_btree->get_node_from_page(parent);
  uint64_t rid = new_page->get_address();
  ham_record_t record = ham_make_record(&rid, sizeof(rid));
  ham_status_t st = insert_in_page(parent, &pivot_key, &record, hints);
  if (st)
    throw Exception(st);
  /* new root page? then also set ptr_down! */
  if (parent_node->get_count() == 0)
    parent_node->set_ptr_down(old_page->get_address());

  /* fix the double-linked list of pages, and mark the pages as dirty */
  if (old_node->get_right()) {
    Page *sib_page = env->page_manager()->fetch(m_context,
                    old_node->get_right());
    BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
    sib_node->set_left(new_page->get_address());
    sib_page->set_dirty(true);
  }
  new_node->set_left(old_page->get_address());
  new_node->set_right(old_node->get_right());
  old_node->set_right(new_page->get_address());
  new_page->set_dirty(true);
  old_page->set_dirty(true);

  BtreeIndex::ms_btree_smo_split++;

  if (g_BTREE_INSERT_SPLIT_HOOK)
    g_BTREE_INSERT_SPLIT_HOOK();

  return (to_return);
}

Page *
BtreeUpdateAction::allocate_new_root(Page *old_root)
{
  LocalDatabase *db = m_btree->get_db();
  LocalEnvironment *env = db->lenv();

  Page *new_root = env->page_manager()->alloc(m_context, Page::kTypeBroot);

  /* insert the pivot element and set ptr_down */
  BtreeNodeProxy *new_node = m_btree->get_node_from_page(new_root);
  new_node->set_ptr_down(old_root->get_address());

  m_btree->set_root_address(m_context, new_root->get_address());
  Page *header = env->page_manager()->fetch(m_context, 0);
  header->set_dirty(true);

  old_root->set_type(Page::kTypeBindex);

  return (new_root);
}

int
BtreeUpdateAction::get_pivot(BtreeNodeProxy *old_node, const ham_key_t *key,
                            BtreeStatistics::InsertHints &hints) const
{
  uint32_t old_count = old_node->get_count();
  ham_assert(old_count > 2);

  bool pivot_at_end = false;
  if (hints.flags & HAM_HINT_APPEND && hints.append_count > 5)
    pivot_at_end = true;
  else if (old_node->get_right() == 0) {
    int cmp = old_node->compare(m_context, key, old_node->get_count() - 1);
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

  ham_assert(pivot > 0 && pivot <= (int)old_count - 2);

  return (pivot);
}

ham_status_t
BtreeUpdateAction::insert_in_page(Page *page, ham_key_t *key,
                            ham_record_t *record,
                            BtreeStatistics::InsertHints &hints,
                            bool force_prepend, bool force_append)
{
  bool exists = false;

  BtreeNodeProxy *node = m_btree->get_node_from_page(page);

  int flags = 0;
  if (force_prepend)
    flags |= PBtreeNode::kInsertPrepend;
  if (force_append)
    flags |= PBtreeNode::kInsertAppend;

  PBtreeNode::InsertResult result = node->insert(m_context, key, flags);
  switch (result.status) {
    case HAM_DUPLICATE_KEY:
      if (hints.flags & HAM_OVERWRITE) {
        /* key already exists; only overwrite the data */
        if (!node->is_leaf())
          return (HAM_SUCCESS);
      }
      else if (!(hints.flags & HAM_DUPLICATE))
        return (HAM_DUPLICATE_KEY);
      /* do NOT shift keys up to make room; just overwrite the
       * current [slot] */
      exists = true;
      break;
    case HAM_SUCCESS:
      break;
    default:
      return (result.status);
  }

  uint32_t new_duplicate_id = 0;
  if (exists) {
    if (node->is_leaf()) {
      // overwrite record blob
      node->set_record(m_context, result.slot, record, m_duplicate_index,
                      hints.flags, &new_duplicate_id);

      hints.processed_leaf_page = page;
      hints.processed_slot = result.slot;
    }
    else {
      // overwrite record id
      ham_assert(record->size == sizeof(uint64_t));
      node->set_record_id(m_context, result.slot, *(uint64_t *)record->data);
    }
  }
  // key does not exist and has to be inserted or appended
  else {
    try {
      if (node->is_leaf()) {
        // allocate record id
        node->set_record(m_context, result.slot, record, m_duplicate_index,
                        hints.flags, &new_duplicate_id);

        hints.processed_leaf_page = page;
        hints.processed_slot = result.slot;
      }
      else {
        // set the internal record id
        ham_assert(record->size == sizeof(uint64_t));
        node->set_record_id(m_context, result.slot, *(uint64_t *)record->data);
      }
    }
    // In case of an error: undo the insert. This happens very rarely but
    // it's possible, i.e. if the BlobManager fails to allocate storage.
    catch (Exception &ex) {
      if (result.slot < (int)node->get_count())
        node->erase(m_context, result.slot);
      throw ex;
    }
  }

  page->set_dirty(true);

  // if this update was triggered with a cursor (and this is a leaf node):
  // couple it to the inserted key
  // TODO only when performing an insert(), not an erase()!
  if (m_cursor && node->is_leaf()) {
    m_cursor->get_parent()->set_to_nil(Cursor::kBtree);
    ham_assert(m_cursor->get_state() == BtreeCursor::kStateNil);
    m_cursor->couple_to_page(page, result.slot, new_duplicate_id);
  }

  return (HAM_SUCCESS);
}

} // namespace hamsterdb
