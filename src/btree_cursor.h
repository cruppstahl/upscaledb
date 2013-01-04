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

/**
 * @brief btree cursors
 *
 * A Btree-Cursor is an object which is used to traverse a Btree.
 *
 * Btree-Cursors are used in Cursor structures as defined in cursor.h. But
 * some routines still use them directly. Over time these layers will be
 * cleaned up and the separation will be improved.
 *
 * The cursor implementation is very fast. Most of the operations (i.e.
 * move previous/next) will not cause any disk access but are O(1) and
 * in-memory only. That's because a cursor is directly "coupled" to a
 * btree page (Page) that resides in memory. If the page is removed
 * from memory (i.e. because the cache decides that it needs to purge the
 * cache, or if there's a page split) then the page is "uncoupled", and a
 * copy of the current key is stored in the cursor. On first access, the
 * cursor is "coupled" again and basically performs a normal lookup of the key.
 */

#ifndef HAM_BTREE_CURSORS_H__
#define HAM_BTREE_CURSORS_H__

#include "internal_fwd_decl.h"
#include "blob.h"
#include "duplicates.h"
#include "error.h"

namespace hamsterdb {

/**
 * the Cursor structure for a b+tree cursor
 */
class BtreeCursor
{
  public:
    enum {
      /** cursor does not point to any key */
      STATE_NIL       = 0,
      /** cursor flag: the cursor is coupled */
      STATE_COUPLED   = 1,
      /** cursor flag: the cursor is uncoupled */
      STATE_UNCOUPLED = 2
    };

    BtreeCursor(Cursor *parent = 0)
      : m_parent(parent), m_state(0), m_dupe_id(0), m_dupe_cache(),
        m_coupled_page(0), m_coupled_index(0), m_uncoupled_key(0) {
    }

    /** get the parent cursor */
    Cursor *get_parent() {
      return (m_parent);
    }

    /** clone another BtreeCursor */
    void clone(BtreeCursor *other);

    /** set the cursor to NIL */
    void set_to_nil();

    /** returns true if the cursor is nil, otherwise false */
    bool is_nil();

    /** set the cursor state */
    void set_state(int state) {
      m_state = state;
    }

    /** get the database pointer */
    Database *get_db();

    /** set the key we're pointing to - if the cursor is coupled */
    void couple_to(Page *page, ham_size_t index) {
      m_coupled_page = page;
      m_coupled_index = index;
      m_state = STATE_COUPLED;
    }

    /** get the page we're pointing to - if the cursor is coupled */
    Page *get_coupled_page() const {
      return (m_coupled_page);
    }

    /** get the key index we're pointing to - if the cursor is coupled */
    ham_size_t get_coupled_index() const {
      return (m_coupled_index);
    }

    /** get the duplicate key we're pointing to - if the cursor is coupled */
    ham_size_t get_dupe_id() const {
      return (m_dupe_id);
    }

    /** set the duplicate key we're pointing to - if the cursor is coupled */
    void set_dupe_id(ham_size_t dupe_id) {
      m_dupe_id = dupe_id;
    }

    /** get the duplicate key's cache */
    dupe_entry_t *get_dupe_cache() {
      return (&m_dupe_cache);
    }

    /** get the key we're pointing to - if the cursor is uncoupled */
    ham_key_t *get_uncoupled_key() {
      return (m_uncoupled_key);
    }

    /** set the key we're pointing to - if the cursor is uncoupled */
    void set_uncoupled_key(ham_key_t *key) {
      m_uncoupled_key = key;
    }

    /** check if the cursor is coupled */
    bool is_coupled() const {
      return (m_state == BtreeCursor::STATE_COUPLED);
    }

    /** check if the cursor is uncoupled */
    bool is_uncoupled() const {
      return (m_state == BtreeCursor::STATE_UNCOUPLED);
    }

    /**
     * Couple the cursor to the same item as another (coupled!) cursor
     *
     * @remark will assert that the other cursor is coupled; will set the
     * current cursor to nil
     */
    void couple_to_other(const BtreeCursor *other) {
      ham_assert(other->is_coupled());
      set_to_nil();
      couple_to(other->get_coupled_page(), other->get_coupled_index());
      set_dupe_id(other->get_dupe_id());
    }

    /**
     * Uncouple the cursor
     * @remark to uncouple a page the cursor HAS to be coupled!
     */
    ham_status_t uncouple(ham_u32_t flags = 0);

    /**
     * returns true if a cursor points to this btree key, otherwise false
     * TODO make this private
     */
    bool points_to(BtreeKey *key);

    /**
     * returns true if a cursor points to this external key, otherwise false
     * TODO make this private
     */
    bool points_to(ham_key_t *key);

    /**
     * Positions the cursor on a key and retrieves the record (if @a record
     * is a valid pointer)
     */
    ham_status_t find(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * Inserts a key/record pair with a cursor
     */
    ham_status_t insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * Erases the key from the index; afterwards, the cursor points to NIL
     */
    ham_status_t erase(ham_u32_t flags);

    /**
     * Moves the cursor to the first, last, next or previous element
     */
    ham_status_t move(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * Returns the number of duplicates of the referenced key
     */
    ham_status_t get_duplicate_count(ham_size_t *count, ham_u32_t flags);

    /**
     * Overwrite the record of this cursor
     */
    ham_status_t overwrite(ham_record_t *record, ham_u32_t flags);

    /**
     * retrieves the record size of the current record
     */
    ham_status_t get_record_size(ham_u64_t *size);

    /**
     * retrieves the duplicate table of the current key; memory in ptable has
     * to be released by the caller.
     *
     * if key has no duplicates, *ptable is NULL.
     *
     * @warning memory has to be freed by the caller IF needs_free is true!
     * TODO make this private
     */
    ham_status_t get_duplicate_table(dupe_table_t **ptable, bool *needs_free);

    /** closes a cursor */
    void close() {
      set_to_nil();
    }

  private:
    /** couple the cursor */
    ham_status_t couple();

    /** move cursor to the very first key */
    ham_status_t move_first(ham_u32_t flags);

    /** move cursor to the very last key */
    ham_status_t move_last(ham_u32_t flags);

    /** move cursor to the next key */
    ham_status_t move_next(ham_u32_t flags);

    /** move cursor to the previous key */
    ham_status_t move_previous(ham_u32_t flags);

    /** the parent cursor */
    Cursor *m_parent;

    /**
     * "coupled" or "uncoupled" states; coupled means that the
     * cursor points into a Page object, which is in
     * memory. "uncoupled" means that the cursor has a copy
     * of the key on which it points (i.e. because the coupled page was
     * flushed to disk and removed from the cache)
     */
    int m_state;

    /** the id of the duplicate key to which this cursor is coupled */
    ham_size_t m_dupe_id;

    /** cached flags and record ID of the current duplicate */
    dupe_entry_t m_dupe_cache;

    /* for coupled cursors: the page we're pointing to */
    Page *m_coupled_page;

    /* ... and the index of the key in that page */
    ham_size_t m_coupled_index;

    /* for uncoupled cursors: a copy of the key at which we're pointing */
    ham_key_t *m_uncoupled_key;
};

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
btree_uncouple_all_cursors(Page *page, ham_size_t start);

} // namespace hamsterdb

#endif /* HAM_BTREE_CURSORS_H__ */
