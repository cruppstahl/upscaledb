/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef UPS_BTREE_UPDATE_H
#define UPS_BTREE_UPDATE_H

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
class BtreeIndex;
class BtreeCursor;

/*
 * Base class for updates; derived for erasing and inserting keys.
 */
class BtreeUpdateAction
{
  public:
    // Constructor
    BtreeUpdateAction(BtreeIndex *btree, Context *context, BtreeCursor *cursor,
                    uint32_t duplicate_index)
      : m_btree(btree), m_context(context), m_cursor(cursor),
        m_duplicate_index(duplicate_index) {
    }

    // Traverses the tree, looking for the leaf with the specified |key|. Will
    // split or merge nodes while descending.
    // Returns the leaf page and the |parent| of the leaf (can be null if
    // there is no parent).
    Page *traverse_tree(const ups_key_t *key,
                        BtreeStatistics::InsertHints &hints, Page **parent);

    // Calculates the pivot index of a split.
    //
    // For databases with sequential access (this includes recno databases):
    // do not split in the middle, but at the very end of the page.
    //
    // If this page is the right-most page in the index, and the new key is
    // inserted at the very end, then we select the same pivot as for
    // sequential access.
    int get_pivot(BtreeNodeProxy *old_node, const ups_key_t *key,
                        BtreeStatistics::InsertHints &hints) const;

    // Splits |page| and updates the |parent|. If |parent| is null then
    // it's assumed that |page| is the root node.
    // Returns the new page in the path for |key|; caller can immediately
    // continue the traversal.
    Page *split_page(Page *old_page, Page *parent, const ups_key_t *key,
                        BtreeStatistics::InsertHints &hints);

    // Allocates a new root page and sets it up in the btree
    Page *allocate_new_root(Page *old_root);

    // Inserts a key in a page
    ups_status_t insert_in_page(Page *page, ups_key_t *key,
                        ups_record_t *record,
                        BtreeStatistics::InsertHints &hints,
                        bool force_prepend = false, bool force_append = false);

  protected:
    // the current btree
    BtreeIndex *m_btree;

    // The caller's Context
    Context *m_context;

    // the current cursor
    BtreeCursor *m_cursor;

    // the duplicate index (in case the update is for a duplicate key)
    // 1-based (if 0 then this update is not for a duplicate)
    uint32_t m_duplicate_index;

  private:
    /* Merges the |sibling| into |page|, returns the merged page and moves
     * the sibling to the freelist */ 
    Page *merge_page(Page *page, Page *sibling);

    /* collapse the root node; returns the new root */
    Page *collapse_root(Page *root_page);
};

} // namespace hamsterdb

#endif // UPS_BTREE_UPDATE_H
