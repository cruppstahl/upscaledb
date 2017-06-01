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
 * btree cursors
 *
 * A Btree-Cursor is an object which is used to traverse a Btree.
 * It is a random access iterator.
 *
 * Btree-Cursors are used in Cursor structures as defined in cursor.h. But
 * some routines use them directly, mostly for performance reasons. Over
 * time these layers will be cleaned up and the separation will be improved.
 *
 * The cursor implementation is very fast. Most of the operations (i.e.
 * move previous/next) will not cause any disk access but are O(1) and
 * in-memory only. That's because a cursor is directly "coupled" to a
 * btree page (Page) that resides in memory. If the page is removed
 * from memory (i.e. because the cache decides that it needs to purge the
 * cache, or if there's a page split) then the cursor is "uncoupled", and a
 * copy of the current key is stored in the cursor. On first access, the
 * cursor is "coupled" again and basically performs a normal lookup of the key.
 *
 * The three states of a BtreeCursor("nil", "coupled", "uncoupled") can be
 * retrieved with the method state(), and can be modified with
 * set_to_nil(), couple_to_page() and uncouple_from_page().
 */

#ifndef UPS_BTREE_CURSOR_H
#define UPS_BTREE_CURSOR_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "1base/error.h"
#include "1base/intrusive_list.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
struct LocalCursor;
struct BtreeIndex;
struct BtreeCursor;
class Page;

struct BtreeCursorState {
  // the parent cursor
  LocalCursor *parent;

  // The BtreeIndex instance
  BtreeIndex *btree;

  // "coupled" or "uncoupled" states; coupled means that the
  // cursor points into a Page object, which is in
  // memory. "uncoupled" means that the cursor has a copy
  // of the key on which it points (i.e. because the coupled page was
  // flushed to disk and removed from the cache)
  int state;

  // the id of the duplicate key to which this cursor is coupled
  int duplicate_index;

  // for coupled cursors: the page we're pointing to
  Page *coupled_page;

  // ... and the index of the key in that page
  int coupled_index;

  // for uncoupled cursors: a copy of the key at which we're pointing
  ups_key_t uncoupled_key;

  // a ByteArray which backs |uncoupled_key.data|
  ByteArray uncoupled_arena;
};


//
// The Cursor structure for a b+tree cursor
//
struct BtreeCursor {
  enum {
    // Cursor does not point to any key
    kStateNil       = 0,
    // Cursor flag: the cursor is coupled
    kStateCoupled   = 1,
    // Cursor flag: the cursor is uncoupled
    kStateUncoupled = 2
  };

  // Constructor
  BtreeCursor(LocalCursor *parent = 0);

  // Destructor; asserts that the cursor is nil
  ~BtreeCursor() {
    close();
  }

  // Clones another BtreeCursor
  void clone(BtreeCursor *other);

  // Closes the cursor
  void close() {
    set_to_nil();
  }

  // Compares the current key against |key|
  int compare(Context *context, ups_key_t *key);

  // Returns true if the cursor is nil
  bool is_nil() const {
    return st_.state == kStateNil;
  }

  // Reset's the cursor's state and uninitializes it. After this call
  // the cursor no longer points to any key.
  void set_to_nil();

  // Couples the cursor to a key directly in a page. Also sets the
  // duplicate index.
  void couple_to(Page *page, uint32_t index, int duplicate_index = 0);

  // Returns true if this cursor is coupled to a btree key
  bool is_coupled() const {
    return st_.state == kStateCoupled;
  }

  // Returns true if this cursor is uncoupled
  bool is_uncoupled() const {
    return st_.state == kStateUncoupled;
  }

  // Returns the duplicate index that this cursor points to.
  int duplicate_index() const {
    return st_.duplicate_index;
  }

  // Sets the duplicate key we're pointing to
  void set_duplicate_index(int duplicate_index) {
    st_.duplicate_index = duplicate_index;
  }

  Page *coupled_page() const {
    assert(st_.state == kStateCoupled);
    return st_.coupled_page;
  }

  int coupled_slot() const {
    assert(st_.state == kStateCoupled);
    return st_.coupled_index;
  }

  // Returns the current key of the cursor. Can be a shallow copy!
  ups_key_t *uncoupled_key() {
    assert(st_.state == kStateUncoupled);
    return &st_.uncoupled_key;
  }

  // Uncouples the cursor
  void uncouple_from_page(Context *context);

  // Returns true if a cursor points to this btree key
  bool points_to(Context *context, Page *page, int slot);

  // Returns true if a cursor points to this external key
  bool points_to(Context *context, ups_key_t *key);

  // Moves the btree cursor to the next page
  ups_status_t move_to_next_page(Context *context);

  // Positions the cursor on a key and retrieves the record (if |record|
  // is a valid pointer)
  ups_status_t find(Context *context, ups_key_t *key, ByteArray *key_arena,
                  ups_record_t *record, ByteArray *record_arena,
                  uint32_t flags);

  // Moves the cursor to the first, last, next or previous element
  ups_status_t move(Context *context, ups_key_t *key, ByteArray *key_arena,
                  ups_record_t *record, ByteArray *record_arena,
                  uint32_t flags);

  // Overwrite the record of this cursor
  void overwrite(Context *context, ups_record_t *record, uint32_t flags);

  // Returns the number of records of the referenced key
  int record_count(Context *context, uint32_t flags);

  // retrieves the record size of the current record
  uint32_t record_size(Context *context);

  // Uncouples all cursors from a page
  // This method is called whenever the page is deleted or becomes invalid
  static void uncouple_all_cursors(Context *context, Page *page,
                  int start = 0);

  // The cursor's current state
  BtreeCursorState st_;

  // Linked list of cursors which point to the same page
  IntrusiveListNode<BtreeCursor> list_node;
};

} // namespace upscaledb

#endif // UPS_BTREE_CURSOR_H
