/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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

#include "error.h"
#include "util.h"

namespace hamsterdb {

class Cursor;
class BtreeIndex;
class Page;

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
    BtreeCursor(Cursor *parent = 0);

    // Destructor; asserts that the cursor is nil
    ~BtreeCursor() {
      ham_assert(m_state == kStateNil);
    }

    // Returns the parent cursor
    // TODO this should be private
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
      return (&m_uncoupled_key);
    }

    // Couples the cursor to a key directly in a page. Also sets the
    // duplicate index.
    void couple_to_page(Page *page, ham_u32_t index,
                    ham_u32_t duplicate_index) {
      couple_to_page(page, index);
      m_duplicate_index = duplicate_index;
    }

    // Returns the duplicate index that this cursor points to.
    ham_u32_t get_duplicate_index() const {
      return (m_duplicate_index);
    }

    // Sets the duplicate key we're pointing to
    void set_duplicate_index(ham_u32_t duplicate_index) {
      m_duplicate_index = duplicate_index;
    }

    // Uncouples the cursor
    // Asserts that the cursor is coupled
    void uncouple_from_page();

    // Returns true if a cursor points to this btree key
    bool points_to(Page *page, ham_u32_t slot);

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

    // Returns the number of records of the referenced key
    ham_u32_t get_record_count(ham_u32_t flags);

    // Overwrite the record of this cursor
    void overwrite(ham_record_t *record, ham_u32_t flags);

    // retrieves the record size of the current record
    ham_u64_t get_record_size();

    // Closes the cursor
    void close() {
      set_to_nil();
    }

    // Uncouples all cursors from a page
    // This method is called whenever the page is deleted or becomes invalid
    static void uncouple_all_cursors(Page *page, ham_u32_t start = 0);

  private:
    // Sets the key we're pointing to - if the cursor is coupled. Also
    // links the Cursor with |page| (and vice versa).
    void couple_to_page(Page *page, ham_u32_t index);

    // Removes this cursor from a page
    void remove_cursor_from_page(Page *page);

    // Couples the cursor to the current page/key
    // Asserts that the cursor is uncoupled. After this call the cursor
    // will be coupled.
    void couple();

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

    // The BtreeIndex instance
    BtreeIndex *m_btree;

    // "coupled" or "uncoupled" states; coupled means that the
    // cursor points into a Page object, which is in
    // memory. "uncoupled" means that the cursor has a copy
    // of the key on which it points (i.e. because the coupled page was
    // flushed to disk and removed from the cache)
    int m_state;

    // the id of the duplicate key to which this cursor is coupled
    ham_u32_t m_duplicate_index;

    // for coupled cursors: the page we're pointing to
    Page *m_coupled_page;

    // ... and the index of the key in that page
    ham_u32_t m_coupled_index;

    // for uncoupled cursors: a copy of the key at which we're pointing
    ham_key_t m_uncoupled_key;

    // a ByteArray which backs |m_uncoupled_key.data|
    ByteArray m_uncoupled_arena;

    // Linked list of cursors which point to the same page
    BtreeCursor *m_next_in_page, *m_previous_in_page;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_CURSORS_H__ */
