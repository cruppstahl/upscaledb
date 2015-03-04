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

/*
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_BTREE_UPDATE_H
#define HAM_BTREE_UPDATE_H

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
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
    Page *traverse_tree(const ham_key_t *key,
                        BtreeStatistics::InsertHints &hints, Page **parent);

    // Calculates the pivot index of a split.
    //
    // For databases with sequential access (this includes recno databases):
    // do not split in the middle, but at the very end of the page.
    //
    // If this page is the right-most page in the index, and the new key is
    // inserted at the very end, then we select the same pivot as for
    // sequential access.
    int get_pivot(BtreeNodeProxy *old_node, const ham_key_t *key,
                        BtreeStatistics::InsertHints &hints) const;

    // Splits |page| and updates the |parent|. If |parent| is null then
    // it's assumed that |page| is the root node.
    // Returns the new page in the path for |key|; caller can immediately
    // continue the traversal.
    Page *split_page(Page *old_page, Page *parent, const ham_key_t *key,
                        BtreeStatistics::InsertHints &hints);

    // Allocates a new root page and sets it up in the btree
    Page *allocate_new_root(Page *old_root);

    // Inserts a key in a page
    ham_status_t insert_in_page(Page *page, ham_key_t *key,
                        ham_record_t *record,
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

#endif // HAM_BTREE_UPDATE_H
