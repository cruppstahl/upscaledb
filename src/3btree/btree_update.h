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

#ifndef UPS_BTREE_UPDATE_H
#define UPS_BTREE_UPDATE_H

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
struct BtreeIndex;
struct BtreeCursor;

/*
 * Base class for updates; derived for erasing and inserting keys.
 */
struct BtreeUpdateAction
{
  // Constructor
  BtreeUpdateAction(BtreeIndex *btree_, Context *context_,
                  BtreeCursor *cursor_, uint32_t duplicate_index_)
    : btree(btree_), context(context_), cursor(cursor_),
      duplicate_index(duplicate_index_) {
  }

  // Traverses the tree, looking for the leaf with the specified |key|. Will
  // split or merge nodes while descending.
  // Returns the leaf page and the |parent| of the leaf (can be null if
  // there is no parent).
  Page *traverse_tree(Context *context, const ups_key_t *key,
                      BtreeStatistics::InsertHints &hints, Page **parent);

  // Splits |page| and updates the |parent|. If |parent| is null then
  // it's assumed that |page| is the root node.
  // Returns the new page in the path for |key|; caller can immediately
  // continue the traversal.
  Page *split_page(Page *old_page, Page *parent, const ups_key_t *key,
                      BtreeStatistics::InsertHints &hints);

  // Inserts a key in a page
  ups_status_t insert_in_page(Page *page, ups_key_t *key,
                      ups_record_t *record,
                      BtreeStatistics::InsertHints &hints,
                      bool force_prepend = false, bool force_append = false);

  // the current btree
  BtreeIndex *btree;

  // The caller's Context
  Context *context;

  // the current cursor
  BtreeCursor *cursor;

  // the duplicate index (in case the update is for a duplicate key)
  // 1-based (if 0 then this update is not for a duplicate)
  uint32_t duplicate_index;
};

} // namespace upscaledb

#endif // UPS_BTREE_UPDATE_H
