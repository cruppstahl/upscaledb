/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

namespace ham {

/**
 * the Cursor structure for a b+tree cursor
 */
class BtreeCursor
{
  public:
    BtreeCursor()
    : m_dupe_cache() {
        m_dupe_id=0;
        _u._coupled._page=0;
        _u._coupled._index=0;
        _u._uncoupled._key=0;
    }

    /** get the parent cursor */
    Cursor *get_parent() {
      return (m_parent);
    }

    /** set the parent cursor */
    void set_parent(Cursor *parent) {
      m_parent = parent;
    }

    /** get the flags */
    ham_u32_t get_flags();

    /** set the flags */
    void set_flags(ham_u32_t flags);

    /** get the database pointer */
    Database *get_db();

    /** get the page we're pointing to - if the cursor is coupled */
    Page *get_coupled_page() {
      return (_u._coupled._page);
    }

    /** set the page we're pointing to - if the cursor is coupled */
    void set_coupled_page(Page *page) {
      _u._coupled._page = page;
    }

    /** get the key index we're pointing to - if the cursor is coupled */
    ham_size_t get_coupled_index() {
      return (_u._coupled._index);
    }

    /** set the key index we're pointing to - if the cursor is coupled */
    void set_coupled_index(ham_size_t index) {
      _u._coupled._index = index;
    }

    /** get the duplicate key we're pointing to - if the cursor is coupled */
    ham_size_t get_dupe_id() {
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

    /** the id of the duplicate key to which this cursor is coupled */
    ham_size_t m_dupe_id;

    /** cached flags and record ID of the current duplicate */
    dupe_entry_t m_dupe_cache;

    /**
     * "coupled" or "uncoupled" states; coupled means that the
     * cursor points into a Page object, which is in
     * memory. "uncoupled" means that the cursor has a copy
     * of the key on which it points (i.e. because the coupled page was
     * flushed to disk and removed from the cache)
     */
    union btree_cursor_union_t {
        struct btree_cursor_coupled_t {
            /* the page we're pointing to */
            Page *_page;

            /* the offset of the key in the page */
            ham_size_t _index;

        } _coupled;

        struct btree_cursor_uncoupled_t {
            /* a copy of the key at which we're pointing */
            ham_key_t *_key;

        } _uncoupled;

    } _u;

  //private:
    /** the parent cursor */
    Cursor *m_parent;
};

/** cursor flag: the cursor is coupled */
#define BTREE_CURSOR_FLAG_COUPLED              1

/** cursor flag: the cursor is uncoupled */
#define BTREE_CURSOR_FLAG_UNCOUPLED            2

/** get the key we're pointing to - if the cursor is uncoupled */
#define btree_cursor_get_uncoupled_key(c)     (c)->_u._uncoupled._key

/** set the key we're pointing to - if the cursor is uncoupled */
#define btree_cursor_set_uncoupled_key(c, k)  (c)->_u._uncoupled._key=k

/** check if the cursor is coupled */
#define btree_cursor_is_coupled(c)            (c->get_flags()&BTREE_CURSOR_FLAG_COUPLED)

/** check if the cursor is uncoupled */
#define btree_cursor_is_uncoupled(c)          (c->get_flags()&BTREE_CURSOR_FLAG_UNCOUPLED)

/**
 * Create a new cursor
 */
extern void
btree_cursor_create(Database *db, Transaction *txn, ham_u32_t flags,
                BtreeCursor *cursor, Cursor *parent);

/**
 * Clone an existing cursor
 * the dest structure is already allocated
 */
extern ham_status_t
btree_cursor_clone(BtreeCursor *src, BtreeCursor *dest,
                Cursor *parent);

/**
 * Set the cursor to NIL
 */
extern ham_status_t
btree_cursor_set_to_nil(BtreeCursor *c);

/**
 * Returns true if the cursor is nil, otherwise false
 */
extern ham_bool_t
btree_cursor_is_nil(BtreeCursor *cursor);

/**
 * Couple the cursor to the same item as another (coupled!) cursor
 *
 * @remark will assert that the other cursor is coupled; will set the
 * current cursor to nil
 */
extern void
btree_cursor_couple_to_other(BtreeCursor *c, BtreeCursor *other);

/**
 * Uncouple the cursor
 *
 * @remark to uncouple a page the cursor HAS to be coupled!
 */
extern ham_status_t
btree_cursor_uncouple(BtreeCursor *c, ham_u32_t flags);

/**
 * flag for @ref btree_cursor_uncouple: uncouple from the page, but do not
 * call @ref Page::remove_cursor()
 */
#define BTREE_CURSOR_UNCOUPLE_NO_REMOVE        1

/**
 * returns true if a cursor points to this btree key, otherwise false
 */
extern bool
btree_cursor_points_to(BtreeCursor *cursor, BtreeKey *key);

/**
 * returns true if a cursor points to this external key, otherwise false
 */
extern bool
btree_cursor_points_to_key(BtreeCursor *cursor, ham_key_t *key);

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
btree_uncouple_all_cursors(Page *page, ham_size_t start);

/**
 * Inserts a key/record pair with a cursor
 */
extern ham_status_t
btree_cursor_insert(BtreeCursor *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags);

/**
 * Positions the cursor on a key and retrieves the record (if @a record
 * is a valid pointer)
 */
extern ham_status_t
btree_cursor_find(BtreeCursor *c, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags);

/**
 * Erases the key from the index; afterwards, the cursor points to NIL
 */
extern ham_status_t
btree_cursor_erase(BtreeCursor *c, ham_u32_t flags);

/**
 * Moves the cursor to the first, last, next or previous element
 */
extern ham_status_t
btree_cursor_move(BtreeCursor *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags);

/**
 * Count the number of records stored with the referenced key, i.e.
 * count the number of duplicates for the current key.
 */
extern ham_status_t
btree_cursor_get_duplicate_count(BtreeCursor *c, ham_size_t *count,
                ham_u32_t flags);

/**
 * Overwrite the record of this cursor
 */
extern ham_status_t
btree_cursor_overwrite(BtreeCursor *c, ham_record_t *record,
                ham_u32_t flags);

/**
 * retrieves the duplicate table of the current key; memory in ptable has
 * to be released by the caller.
 *
 * if key has no duplicates, *ptable is NULL.
 *
 * @warning memory has to be freed by the caller IF needs_free is true!
 */
extern ham_status_t
btree_cursor_get_duplicate_table(BtreeCursor *c, dupe_table_t **ptable,
                bool *needs_free);

/**
 * retrieves the record size of the current record
 */
extern ham_status_t
btree_cursor_get_record_size(BtreeCursor *c, ham_offset_t *size);

/**
 * Closes an existing cursor
 */
extern void
btree_cursor_close(BtreeCursor *cursor);

} // namespace ham

#endif /* HAM_BTREE_CURSORS_H__ */
