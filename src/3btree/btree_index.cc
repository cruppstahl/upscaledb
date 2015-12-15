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

uint64_t BtreeIndex::ms_btree_smo_split = 0;
uint64_t BtreeIndex::ms_btree_smo_merge = 0;
uint64_t BtreeIndex::ms_btree_smo_shift = 0;

void
BtreeIndex::create(Context *context, PBtreeHeader *btree_header,
                    DatabaseConfiguration *dbconfig,
                    const std::string &compare_name)
{
  m_btree_header = btree_header;
  m_leaf_traits = BtreeIndexFactory::create(m_db, true);
  m_internal_traits = BtreeIndexFactory::create(m_db, false);

  /* allocate a new root page */
  Page *root = m_db->lenv()->page_manager()->alloc(context,
                    Page::kTypeBroot, PageManager::kClearWithZero);

  /* initialize the root page */
  PBtreeNode *node = PBtreeNode::from_page(root);
  node->set_flags(PBtreeNode::kLeafNode);

  m_root_address = root->get_address();
  m_compare_hash = CallbackManager::hash(compare_name);

  persist_configuration(context, dbconfig);
}

void
BtreeIndex::open(PBtreeHeader *btree_header, DatabaseConfiguration *dbconfig)
{
  m_btree_header = btree_header;

  /* merge the non-persistent database flag with the persistent flags from
   * the btree index */
  dbconfig->flags |= m_btree_header->flags;
  dbconfig->key_size = m_btree_header->key_size;
  dbconfig->key_type = m_btree_header->key_type;
  dbconfig->record_type = m_btree_header->record_type;
  dbconfig->flags = m_btree_header->flags;
  dbconfig->record_size = m_btree_header->record_size;
  dbconfig->record_compressor = m_btree_header->record_compression();
  dbconfig->key_compressor = m_btree_header->key_compression();

  m_root_address = m_btree_header->root_address;

  ups_assert(dbconfig->key_size > 0);
  ups_assert(m_root_address > 0);

  m_leaf_traits = BtreeIndexFactory::create(m_db, true);
  m_internal_traits = BtreeIndexFactory::create(m_db, false);
}

void
BtreeIndex::persist_configuration(Context *context,
                    const DatabaseConfiguration *dbconfig)
{
  if (dbconfig->flags & UPS_READ_ONLY)
    return;

  m_btree_header->dbname = m_db->name();
  m_btree_header->key_size = dbconfig->key_size;
  m_btree_header->key_type = dbconfig->key_type;
  m_btree_header->record_size = dbconfig->record_size;
  m_btree_header->record_type = dbconfig->record_type;
  m_btree_header->flags = dbconfig->flags; // TODO nur die "interessanten"!
  m_btree_header->root_address = m_root_address;
  m_btree_header->set_record_compression(dbconfig->record_compressor);
  m_btree_header->set_key_compression(dbconfig->key_compressor);
}

Page *
BtreeIndex::find_lower_bound(Context *context, Page *page, const ups_key_t *key,
                uint32_t page_manager_flags, int *idxptr)
{
  BtreeNodeProxy *node = get_node_from_page(page);

  // make sure that we're not in a leaf page, and that the
  // page is not empty
  ups_assert(node->get_ptr_down() != 0);

  uint64_t record_id;
  int slot = node->find_lower_bound(context, (ups_key_t *)key, &record_id);

  if (idxptr)
    *idxptr = slot;

  return (m_db->lenv()->page_manager()->fetch(context,
                    record_id, page_manager_flags));
}

//
// visitor object for estimating / counting the number of keys
///
class CalcKeysVisitor : public BtreeVisitor {
  public:
    CalcKeysVisitor(LocalDatabase *db, bool distinct)
      : m_db(db), m_distinct(distinct), m_count(0) {
    }

    virtual bool is_read_only() const {
      return (true);
    }

    virtual void operator()(Context *context, BtreeNodeProxy *node) {
      size_t node_count = node->get_count();

      if (m_distinct
          || (m_db->get_flags() & UPS_ENABLE_DUPLICATE_KEYS) == 0) {
        m_count += node_count;
        return;
      }

      for (size_t i = 0; i < node_count; i++)
        m_count += node->get_record_count(context, i);
    }

    uint64_t get_result() const {
      return (m_count);
    }

  private:
    LocalDatabase *m_db;
    bool m_distinct;
    uint64_t m_count;
};

uint64_t
BtreeIndex::count(Context *context, bool distinct)
{
  CalcKeysVisitor visitor(m_db, distinct);
  visit_nodes(context, visitor, false);
  return (visitor.get_result());
}

//
// visitor object to free all allocated blobs
///
class FreeBlobsVisitor : public BtreeVisitor {
  public:
    virtual void operator()(Context *context, BtreeNodeProxy *node) {
      node->remove_all_entries(context);

      PageManager *pm = context->env->page_manager();
      pm->del(context, node->get_page(), 1);
    }

    virtual bool is_read_only() const {
      return (false);
    }
};

void
BtreeIndex::drop(Context *context)
{
  FreeBlobsVisitor visitor;
  visit_nodes(context, visitor, true);
}

} // namespace upscaledb
