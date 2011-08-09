/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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
 */

#ifndef HAM_BTREE_CURSORS_H__
#define HAM_BTREE_CURSORS_H__

#include "internal_fwd_decl.h"

#include "blob.h"
#include "cursor.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * the cursor structure for a b+tree
 */
typedef struct btree_cursor_t btree_cursor_t;
struct btree_cursor_t
{
    /**
     * the common declarations of all cursors
     */
    CURSOR_DECLARATIONS(btree_cursor_t);

    /** the parent cursor */
    ham_cursor_t *_parent;

    /* the id of the duplicate key to which this cursor is coupled */
    ham_size_t _dupe_id;

    /* cached flags and record ID of the current duplicate */
    dupe_entry_t _dupe_cache;

    /**
     * "coupled" or "uncoupled" states; coupled means that the
     * cursor points into a ham_page_t object, which is in
     * memory. "uncoupled" means that the cursor has a copy
     * of the key on which it points
     */
    union btree_cursor_union_t {
        struct btree_cursor_coupled_t {
            /* the page we're pointing to */
            ham_page_t *_page;

            /* the offset of the key in the page */
            ham_size_t _index;

        } _coupled;

        struct btree_cursor_uncoupled_t {
            /* a copy of the key at which we're pointing */
            ham_key_t *_key;

        } _uncoupled;

    } _u;
};

/** cursor flag: the cursor is coupled */
#define BTREE_CURSOR_FLAG_COUPLED              1

/** cursor flag: the cursor is uncoupled */
#define BTREE_CURSOR_FLAG_UNCOUPLED            2

/** get the parent cursor */
#define btree_cursor_get_parent(c)            (c)->_parent

/** set the parent cursor */
#define btree_cursor_set_parent(c, p)         (c)->_parent=p

/** get the database pointer */
#define btree_cursor_get_db(c)                (c)->_db

/** set the database pointer */
#define btree_cursor_set_db(c, db)            (c)->_db=(db)

/** get the txn pointer */
#define btree_cursor_get_txn(c)               (c)->_txn

/** set the txn pointer */
#define btree_cursor_set_txn(c, txn)          (c)->_txn=(txn)

/** get the flags */
#define btree_cursor_get_flags(c)             (c)->_flags

/** set the flags */
#define btree_cursor_set_flags(c, f)          (c)->_flags=(f)

/** get the page we're pointing to - if the cursor is coupled */
#define btree_cursor_get_coupled_page(c)      (c)->_u._coupled._page

/** set the page we're pointing to - if the cursor is coupled */
#define btree_cursor_set_coupled_page(c, p)   (c)->_u._coupled._page=p

/** get the key index we're pointing to - if the cursor is coupled */
#define btree_cursor_get_coupled_index(c)     (c)->_u._coupled._index

/** set the key index we're pointing to - if the cursor is coupled */
#define btree_cursor_set_coupled_index(c, i)  (c)->_u._coupled._index=i

/** get the duplicate key we're pointing to - if the cursor is coupled */
#define btree_cursor_get_dupe_id(c)           (c)->_dupe_id

/** set the duplicate key we're pointing to - if the cursor is coupled */
#define btree_cursor_set_dupe_id(c, d)        (c)->_dupe_id=d

/** get the duplicate key's cache */
#define btree_cursor_get_dupe_cache(c)        (&(c)->_dupe_cache)

/** get the key we're pointing to - if the cursor is uncoupled */
#define btree_cursor_get_uncoupled_key(c)     (c)->_u._uncoupled._key

/** set the key we're pointing to - if the cursor is uncoupled */
#define btree_cursor_set_uncoupled_key(c, k)  (c)->_u._uncoupled._key=k

/**                                                                 
 * clone an existing cursor                                         
 * the dest structure is already allocated
 */                                                                 
extern ham_status_t
btree_cursor_clone(btree_cursor_t *src, btree_cursor_t *dest);

/*
 * set a cursor to NIL
 */
ham_status_t
btree_cursor_set_to_nil(btree_cursor_t *c);

/**
 * couple the cursor
 *
 * @remark to couple a page, it has to be uncoupled!
 */
ham_status_t
btree_cursor_couple(btree_cursor_t *c);

/**
 * couple the cursor to the same item as another (coupled!) cursor
 *
 * @remark will assert that the other cursor is coupled; will set the
 * current cursor to nil
 */
void
btree_cursor_couple_to_other(btree_cursor_t *c, btree_cursor_t *other);

/**
 * uncouple the cursor
 *
 * @remark to uncouple a page, it has to be coupled!
 */
ham_status_t
btree_cursor_uncouple(btree_cursor_t *c, ham_u32_t flags);

/**
 * flag for @ref btree_cursor_uncouple: uncouple from the page, but do not
 * call @ref page_remove_cursor()
 */
#define BTREE_CURSOR_UNCOUPLE_NO_REMOVE        1

/**
 * closes an existing cursor
 */
extern void
btree_cursor_close(btree_cursor_t *cursor);

/**
 * returns true if the cursor is nil, otherwise false
 */
extern ham_bool_t
btree_cursor_is_nil(btree_cursor_t *cursor);

/**
 * create a new cursor
 */
extern void
btree_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
                btree_cursor_t *cursor, ham_cursor_t *parent);

/**
 * returns true if a cursor points to this key, otherwise false
 */
extern ham_bool_t 
btree_cursor_points_to(btree_cursor_t *cursor, btree_key_t *key);

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
btree_uncouple_all_cursors(ham_page_t *page, ham_size_t start);

/**
 * Inserts a key/record pair with a cursor
 */
extern ham_status_t
btree_cursor_insert(btree_cursor_t *c, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * Positions the cursor on a key and retrieves the record (if @a record
 * is a valid pointer)
 */
extern ham_status_t
btree_cursor_find(btree_cursor_t *c, ham_key_t *key, ham_record_t *record, 
            ham_u32_t flags);

/**                                                                 
 * Erases the key from the index; afterwards, the cursor points to NIL
 */                                                                 
extern ham_status_t
btree_cursor_erase(btree_cursor_t *c, ham_u32_t flags);

/**                                                                    
 * Count the number of records stored with the referenced key, i.e.
 * count the number of duplicates for the current key.        
 */                                                                    
extern ham_status_t
btree_cursor_get_duplicate_count(btree_cursor_t *c, ham_size_t *count, 
                ham_u32_t flags);

/**                                                                 
 * Overwrite the record of this cursor                              
 */                                                                 
extern ham_status_t
btree_cursor_overwrite(btree_cursor_t *c, ham_record_t *record,
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
btree_cursor_get_duplicate_table(btree_cursor_t *c, dupe_table_t **ptable,
                    ham_bool_t *needs_free);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BTREE_CURSORS_H__ */
