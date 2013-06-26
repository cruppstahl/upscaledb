/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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
 * retrieved with the method get_state(), and can be modified with
 * set_to_nil(), couple_to_page() and uncouple_from_page().
 */

#ifndef HAM_BTREE_CURSORS_H__
#define HAM_BTREE_CURSORS_H__

#include "blob_manager.h"
#include "duplicates.h"
#include "error.h"

namespace hamsterdb {

class Cursor;
struct PBtreeKey;

//
// The Cursor structure for a b+tree cursor
//
class BtreeCursor
{
  public:
    enum {
      // Cursor does not point to any key
      kStateNil       = 0,
      // Cursor flag: the cursor is coupled
      kStateCoupled   = 1,
      // Cursor flag: the cursor is uncoupled
      kStateUncoupled = 2
    };

    // Constructor
    BtreeCursor(Cursor *parent = 0)
      : m_parent(parent), m_state(0), m_duplicate_index(0), m_dupe_cache(),
        m_coupled_page(0), m_coupled_index(0), m_uncoupled_key(0),
        m_next_in_page(0), m_previous_in_page(0) {
    }

    // Returns the parent cursor
    Cursor *get_parent() {
      return (m_parent);
    }

    // Clones another BtreeCursor
    void clone(BtreeCursor *other);

    // Returns the cursor's state (kStateCoupled, kStateUncoupled, kStateNil)
    ham_u32_t get_state() const {
      return (m_state);
    }

    // Reset's the cursor's state and uninitializes it. After this call
    // the cursor no longer points to any key.
    void set_to_nil();

    // Returns the page, index in this page and the duplicate index that this
    // cursor is coupled to. This is used by Btree functions to optimize
    // certain algorithms, i.e. when erasing the current key.
    // Asserts that the cursor is coupled.
    void get_coupled_key(Page **page, ham_u32_t *index = 0,
                    ham_u32_t *duplicate_index = 0) const {
      ham_assert(m_state == kStateCoupled);
      if (page)
        *page = m_coupled_page;
      if (index)
        *index = m_coupled_index;
      if (duplicate_index)
        *duplicate_index = m_duplicate_index;
    }

    // Returns the uncoupled key of this cursor.
    // Asserts that the cursor is uncoupled.
    ham_key_t *get_uncoupled_key() {
      ham_assert(m_state == kStateUncoupled);
      return (m_uncoupled_key);
    }

    // Couples the cursor to a key directly in a page. Also sets the
    // duplicate index.
    void couple_to_page(Page *page, ham_size_t index,
                    ham_size_t duplicate_index) {
      couple_to_page(page, index);
      m_duplicate_index = duplicate_index;
    }

    // Returns the duplicate index that this cursor points to.
    ham_u32_t get_duplicate_index() const {
      return (m_duplicate_index);
    }

    // Sets the duplicate key we're pointing to
    void set_duplicate_index(ham_size_t duplicate_index) {
      m_duplicate_index = duplicate_index;
      memset(&m_dupe_cache, 0, sizeof(m_dupe_cache));
    }

    // Uncouples the cursor
    // Asserts that the cursor is coupled
    ham_status_t uncouple_from_page();

    // Returns true if a cursor points to this btree key
    bool points_to(PBtreeKey *key);

    // Returns true if a cursor points to this external key
    bool points_to(ham_key_t *key);

    // Positions the cursor on a key and retrieves the record (if |record|
    // is a valid pointer)
    ham_status_t find(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Inserts a key/record pair with a cursor
    ham_status_t insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Erases the key from the index; afterwards, the cursor points to NIL
    ham_status_t erase(ham_u32_t flags);

    // Moves the cursor to the first, last, next or previous element
    ham_status_t move(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Returns the number of duplicates of the referenced key
    ham_status_t get_duplicate_count(ham_size_t *count, ham_u32_t flags);

    // Overwrite the record of this cursor
    ham_status_t overwrite(ham_record_t *record, ham_u32_t flags);

    // retrieves the record size of the current record
    ham_status_t get_record_size(ham_u64_t *size);

    // Closes the cursor
    void close() {
      set_to_nil();
    }

    // Uncouples all cursors from a page
    // This method is called whenever the page is deleted or becomes invalid
    static ham_status_t uncouple_all_cursors(Page *page, ham_size_t start = 0);

  private:
    // Sets the key we're pointing to - if the cursor is coupled. Also
    // links the Cursor with |page| (and vice versa).
    void couple_to_page(Page *page, ham_size_t index);

    // Removes this cursor from a page
    void remove_cursor_from_page(Page *page);

    // Couples the cursor to the current page/key
    // Asserts that the cursor is uncoupled. After this call the cursor
    // will be coupled.
    ham_status_t couple();

    // move cursor to the very first key
    ham_status_t move_first(ham_u32_t flags);

    // move cursor to the very last key
    ham_status_t move_last(ham_u32_t flags);

    // move cursor to the next key
    ham_status_t move_next(ham_u32_t flags);

    // move cursor to the previous key
    ham_status_t move_previous(ham_u32_t flags);

    // the parent cursor
    Cursor *m_parent;

    // "coupled" or "uncoupled" states; coupled means that the
    // cursor points into a Page object, which is in
    // memory. "uncoupled" means that the cursor has a copy
    // of the key on which it points (i.e. because the coupled page was
    // flushed to disk and removed from the cache)
    int m_state;

    // the id of the duplicate key to which this cursor is coupled
    ham_size_t m_duplicate_index;

    // cached flags and record ID of the current duplicate
    PDupeEntry m_dupe_cache;

    // for coupled cursors: the page we're pointing to
    Page *m_coupled_page;

    // ... and the index of the key in that page
    ham_size_t m_coupled_index;

    // for uncoupled cursors: a copy of the key at which we're pointing
    ham_key_t *m_uncoupled_key;

    // Linked list of cursors which point to the same page
    BtreeCursor *m_next_in_page, *m_previous_in_page;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_CURSORS_H__ */
