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
#include "1globals/callbacks.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_index_factory.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4env/env.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

Page *
BtreeIndex::root_page(Context *context)
{
  if (unlikely(state.root_page == 0))
    state.root_page = state.page_manager->fetch(context,
                            state.btree_header->root_address);
  else
    context->changeset.put(state.root_page);
  return state.root_page;
}

void
BtreeIndex::create(Context *context, PBtreeHeader *btree_header,
                    DbConfig *dbconfig)
{
  LocalEnv *env = (LocalEnv *)state.db->env;
  state.page_manager = env->page_manager.get();
  state.btree_header = btree_header;
  state.leaf_traits.reset(BtreeIndexFactory::create(state.db, true));
  state.internal_traits.reset(BtreeIndexFactory::create(state.db, false));

  /* allocate a new root page */
  set_root_page(state.page_manager->alloc(context, Page::kTypeBroot,
                        PageManager::kClearWithZero));

  /* initialize the root page */
  PBtreeNode *node = PBtreeNode::from_page(state.root_page);
  node->set_flags(PBtreeNode::kLeafNode);

  persist_configuration(context, dbconfig);
}

void
BtreeIndex::open(PBtreeHeader *btree_header, DbConfig *dbconfig)
{
  LocalEnv *env = (LocalEnv *)state.db->env;
  state.page_manager = env->page_manager.get();
  state.btree_header = btree_header;

  /* merge the non-persistent database flag with the persistent flags from
   * the btree index */
  dbconfig->flags |= btree_header->flags;
  dbconfig->key_size = btree_header->key_size;
  dbconfig->key_type = btree_header->key_type;
  dbconfig->key_compressor = btree_header->key_compression();
  dbconfig->record_type = btree_header->record_type;
  dbconfig->record_size = btree_header->record_size;
  dbconfig->record_compressor = btree_header->record_compression();

  assert(dbconfig->key_size > 0);

  state.leaf_traits.reset(BtreeIndexFactory::create(state.db, true));
  state.internal_traits.reset(BtreeIndexFactory::create(state.db, false));
}

void
BtreeIndex::persist_configuration(Context *context, const DbConfig *dbconfig)
{
  if (unlikely(ISSET(dbconfig->flags, UPS_READ_ONLY)))
    return;

  state.btree_header->dbname = state.db->name();
  state.btree_header->key_size = dbconfig->key_size;
  state.btree_header->key_type = dbconfig->key_type;
  state.btree_header->record_size = dbconfig->record_size;
  state.btree_header->record_type = dbconfig->record_type;
  state.btree_header->flags = dbconfig->flags; // TODO nur die "interessanten"!
  state.btree_header->compare_hash
          = CallbackManager::hash(dbconfig->compare_name);
  state.btree_header->set_record_compression(dbconfig->record_compressor);
  state.btree_header->set_key_compression(dbconfig->key_compressor);
}

Page *
BtreeIndex::find_lower_bound(Context *context, Page *page, const ups_key_t *key,
                uint32_t page_manager_flags, int *idxptr)
{
  BtreeNodeProxy *node = get_node_from_page(page);

  // make sure that we're not in a leaf page, and that the
  // page is not empty
  assert(node->left_child() != 0);

  uint64_t record_id;
  int slot = node->find_lower_bound(context, (ups_key_t *)key, &record_id);

  if (idxptr)
    *idxptr = slot;

  return state.page_manager->fetch(context, record_id, page_manager_flags);
}

//
// visitor object for estimating / counting the number of keys
///
struct CalcKeysVisitor : public BtreeVisitor
{
  CalcKeysVisitor(LocalDb *db_, bool distinct_)
    : db(db_), distinct(distinct_), count(0) {
  }

  virtual bool is_read_only() const {
    return true;
  }

  virtual void operator()(Context *context, BtreeNodeProxy *node) {
    size_t length = node->length();

    if (distinct || NOTSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      count += length;
      return;
    }

    for (size_t i = 0; i < length; i++)
      count += node->record_count(context, i);
  }

  LocalDb *db;
  bool distinct;
  uint64_t count;
};

uint64_t
BtreeIndex::count(Context *context, bool distinct)
{
  CalcKeysVisitor visitor(state.db, distinct);
  visit_nodes(context, visitor, false);
  return visitor.count;
}

//
// visitor object to free all allocated blobs
///
struct FreeBlobsVisitor : public BtreeVisitor
{
  FreeBlobsVisitor(PageManager *page_manager_)
    : page_manager(page_manager_) {
  }

  virtual void operator()(Context *context, BtreeNodeProxy *node) {
    node->erase_everything(context);
    page_manager->del(context, node->page, 1);
  }

  virtual bool is_read_only() const {
    return false;
  }

  PageManager *page_manager;
};

void
BtreeIndex::drop(Context *context)
{
  FreeBlobsVisitor visitor(state.page_manager);
  visit_nodes(context, visitor, true);
}

} // namespace upscaledb
