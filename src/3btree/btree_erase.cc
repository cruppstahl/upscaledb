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
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_update.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4cursor/cursor_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * Erases key/value pairs from a btree
 */
struct BtreeEraseAction : public BtreeUpdateAction
{
  BtreeEraseAction(BtreeIndex *btree_, Context *context_,
                  BtreeCursor *cursor_, ups_key_t *key_,
                  int duplicate_index_, uint32_t /* flags (not used) */)
    : BtreeUpdateAction(btree_, context_, cursor_, duplicate_index_),
      key(key_) {
    if (cursor)
      duplicate_index = cursor->duplicate_index() + 1;
  }

  // This is the entry point for the erase operation
  ups_status_t run() {
    // Coupled cursor: try to remove the key directly from the page
    if (cursor) {
      if (cursor->state() == BtreeCursor::kStateCoupled) {
        Page *coupled_page;
        int coupled_index;
        cursor->coupled_key(&coupled_page, &coupled_index);

        BtreeNodeProxy *node = btree->get_node_from_page(coupled_page);
        assert(node->is_leaf());

        // Now try to delete the key. This can require a page split if the
        // KeyList is not "delete-stable" (some compressed lists can
        // grow when keys are deleted).
        try {
          remove_entry(coupled_page, 0, coupled_index);
        }
        catch (Exception &ex) {
          if (ex.code != UPS_LIMITS_REACHED)
            throw ex;
          goto fall_through;
        }
        // TODO if the page is empty then move it to the freelist
        return 0;

fall_through:
        cursor->uncouple_from_page(context);
      }

      if (cursor->state() == BtreeCursor::kStateUncoupled)
        key = cursor->uncoupled_key();
    }

    return erase();
  }

  ups_status_t erase() {
    // traverse the tree to the leaf, splitting/merging nodes as required
    Page *parent;
    BtreeStatistics::InsertHints hints;
    Page *page = traverse_tree(key, hints, &parent);
    BtreeNodeProxy *node = btree->get_node_from_page(page);

    // we have reached the leaf; search the leaf for the key
    int slot = node->find(context, key);
    if (slot < 0) {
      btree->statistics()->erase_failed();
      return UPS_KEY_NOT_FOUND;
    }

    // remove the key from the leaf
    return remove_entry(page, parent, slot);
  }

  ups_status_t remove_entry(Page *page, Page *parent, int slot) {
    LocalDatabase *db = btree->db();
    BtreeNodeProxy *node = btree->get_node_from_page(page);

    assert(slot >= 0);
    assert(slot < (int)node->length());

    // delete the record, but only on leaf nodes! internal nodes don't have
    // records; they point to pages instead, and we do not want to delete
    // those.
    bool has_duplicates_left = false;
    if (node->is_leaf()) {
      // only delete a duplicate?
      if (duplicate_index > 0)
        node->erase_record(context, slot, duplicate_index - 1, false,
                      &has_duplicates_left);
      else
        node->erase_record(context, slot, 0, true, 0);
    }

    page->set_dirty(true);

    // still got duplicates left? then adjust all cursors
    if (node->is_leaf() && has_duplicates_left && db->cursor_list()) {
      LocalCursor *cursors = (LocalCursor *)db->cursor_list();
      BtreeCursor *btcur = cursors->get_btree_cursor();

      int dupidx =
              cursor
                  ? cursor->duplicate_index()
                  : duplicate_index;

      while (btcur) {
        BtreeCursor *next = 0;
        if (cursors->get_next()) {
          cursors = (LocalCursor *)cursors->get_next();
          next = cursors->get_btree_cursor();
        }

        if (btcur != cursor && btcur->points_to(context, page, slot)) {
          if (btcur->duplicate_index() == dupidx)
              btcur->set_to_nil();
          else if (btcur->duplicate_index() > dupidx)
            btcur->set_duplicate_index(btcur->duplicate_index() - 1);
        }
        btcur = next;
      }
      // all cursors were adjusted, the duplicate was deleted. return
      // to caller!
      return 0;
    }

    // no duplicates left, the key was deleted; all cursors pointing to
    // this key are set to nil, all cursors pointing to a key in the same
    // page are adjusted, if necessary
    if (node->is_leaf() && !has_duplicates_left && db->cursor_list()) {
      LocalCursor *cursors = (LocalCursor *)db->cursor_list();
      BtreeCursor *btcur = cursors->get_btree_cursor();

      /* 'nil' every cursor which points to the deleted key, and adjust
       * other cursors attached to the same page */
      while (btcur) {
        BtreeCursor *cur = btcur;
        BtreeCursor *next = 0;
        if (cursors->get_next()) {
          cursors = (LocalCursor *)cursors->get_next();
          next = cursors->get_btree_cursor();
        }
        if (btcur != cursor && cur->points_to(context, page, slot))
          cur->set_to_nil();
        else if (btcur != cursor
                && ISSET(cur->state(), BtreeCursor::kStateCoupled)) {
          Page *coupled_page;
          int coupled_slot;
          cur->coupled_key(&coupled_page, &coupled_slot);
          if (coupled_page == page && coupled_slot > slot)
            cur->uncouple_from_page(context);
        }
        btcur = next;
      }
    }

    if (has_duplicates_left)
      return 0;

    // We've reached the leaf; it's still possible that we have to
    // split the page, therefore this case has to be handled
    try {
      node->erase(context, slot);
    }
    catch (Exception &ex) {
      if (ex.code != UPS_LIMITS_REACHED)
        throw ex;

      // Split the page in the middle. This will invalidate the |node| pointer
      // and the |slot| of the key, therefore restart the whole operation
      BtreeStatistics::InsertHints hints = {0};
      split_page(page, parent, key, hints);
      return erase();
    }

    return 0;
  }

  // the key that is retrieved
  ups_key_t *key;
};

ups_status_t
BtreeIndex::erase(Context *context, LocalCursor *cursor, ups_key_t *key,
              int duplicate_index, uint32_t flags)
{
  context->db = db();

  BtreeEraseAction bea(this, context, cursor ? cursor->get_btree_cursor() : 0,
                key, duplicate_index, flags);
  return bea.run();
}

} // namespace upscaledb
