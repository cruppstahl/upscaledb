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
 * @brief a base-"class" for cursors
 *
 */

#ifndef HAM_CURSORS_H__
#define HAM_CURSORS_H__

#include "internal_fwd_decl.h"

#include "error.h"
#include "txn_cursor.h"
#include "blob.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * A single line in the dupecache structure - can reference a btree
 * record or a txn-op 
 */
typedef struct dupecache_line_t {

    /** are we using btree or txn duplicates? */
    ham_bool_t _use_btree;

    union {
        /** the btree flags */
        ham_u32_t _btree_flags;

        /** the btree record ID */
        ham_u64_t _btree_rid;

        /** the txn op structure */
        txn_op_t *_op;
    } _u;

} dupecache_line_t;

/** Retrieves which part of the union is used */
#define dupecache_line_use_btree(dcl)           (dcl)->_use_btree

/** Specifies which part of the union is used */
#define dupecache_line_set_btree(dcl, b)        (dcl)->_use_btree=b

/** Get flags of the btree record */
#define dupecache_line_get_btree_flags(dcl)     (dcl)->_u._btree_flags

/** Set flags of the btree record */
#define dupecache_line_set_btree_flags(dcl, f)  (dcl)->_u._btree_flags=f

/** Get ID of the btree record */
#define dupecache_line_get_btree_rid(dcl)       (dcl)->_u._btree_rid

/** Set ID of the btree record */
#define dupecache_line_set_btree_rid(dcl, rid)  (dcl)->_u._btree_rid=rid

/** Get txn_op_t pointer of the txn record */
#define dupecache_line_get_txn_op(dcl)          (dcl)->_u._op

/** Set txn_op_t pointer of the txn record */
#define dupecache_line_set_txn_op(dcl, op)      (dcl)->_u._op=op


/**
 * The dupecache is a cache for duplicate keys
 */
typedef struct dupecache_t {
    /** the cursor - needed for allocator etc */
    struct ham_cursor_t *_cursor;

    /** capacity of this cache */
    ham_size_t _capacity;

    /** number of elements tracked in this cache */
    ham_size_t _count;

    /** the cached elements */
    dupecache_line_t *_elements;

} dupecache_t;

/** Set the cursor pointer */
#define dupecache_set_cursor(dc, c)         (dc)->_cursor=(c)

/** Get the cursor pointer */
#define dupecache_get_cursor(dc)            (dc)->_cursor

/** Set the capacity */
#define dupecache_set_capacity(dc, c)       (dc)->_capacity=c

/** Get the capacity */
#define dupecache_get_capacity(dc)          (dc)->_capacity

/** Set the count */
#define dupecache_set_count(dc, c)          (dc)->_count=c

/** Get the count */
#define dupecache_get_count(dc)             (dc)->_count

/** Set the pointer to the dupe_entry array */
#define dupecache_set_elements(dc, e)       (dc)->_elements=e

/** Get the pointer to the dupe_entry array */
#define dupecache_get_elements(dc)          (dc)->_elements

/**
 * creates a new dupecache structure
 */
extern ham_status_t
dupecache_create(dupecache_t *c, struct ham_cursor_t *cursor, 
                    ham_size_t capacity);

/**
 * clones two dupe-caches
 */
extern ham_status_t
dupecache_clone(dupecache_t *src, dupecache_t *dest);

/**
 * inserts a new item somewhere in the cache; resizes the cache if necessary
 */
extern ham_status_t
dupecache_insert(dupecache_t *c, ham_u32_t position, dupecache_line_t *dupe);

/**
 * appends a new item; resizes the cache if necessary
 */
extern ham_status_t
dupecache_append(dupecache_t *c, dupecache_line_t *dupe);

/**
 * erases an item 
 */
extern ham_status_t
dupecache_erase(dupecache_t *c, ham_u32_t position);

/**
 * clears the cache; frees all resources
 */
extern void
dupecache_clear(dupecache_t *c);

/**
 * empties the cache; will not free resources
 */
extern void
dupecache_reset(dupecache_t *c);


/**
 * The Cursor structure - these functions and members are "inherited"
 * by every other cursor (i.e. btree, hashdb etc).
 */
#define CURSOR_DECLARATIONS(clss)                                       \
    /**                                                                 \
     * Clone an existing cursor                                         \
     */                                                                 \
    ham_status_t (*_fun_clone)(clss *cu, clss **newit);                 \
                                                                        \
    /**                                                                 \
     * Close an existing cursor                                         \
     */                                                                 \
    void (*_fun_close)(clss *cu);                                       \
                                                                        \
    /**                                                                 \
     * Overwrite the record of this cursor                              \
     */                                                                 \
    ham_status_t (*_fun_overwrite)(clss *cu, ham_record_t *record,      \
            ham_u32_t flags);                                           \
                                                                        \
    /**                                                                 \
     * Move the cursor                                                  \
     */                                                                 \
    ham_status_t (*_fun_move)(clss *cu, ham_key_t *key,                 \
            ham_record_t *record, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * Find a key in the index and positions the cursor                 \
     * on this key                                                      \
     */                                                                 \
    ham_status_t (*_fun_find)(clss *cu, ham_key_t *key,                 \
                    ham_record_t *record, ham_u32_t flags);             \
                                                                        \
    /**                                                                 \
     * Insert (or update) a key in the index                            \
     */                                                                 \
    ham_status_t (*_fun_insert)(clss *cu, ham_key_t *key,               \
                    ham_record_t *record, ham_u32_t flags);             \
                                                                        \
    /**                                                                 \
     * Erases the key from the index and positions the cursor to the    \
     * next key                                                         \
     */                                                                 \
    ham_status_t (*_fun_erase)(clss *cu, ham_u32_t flags);              \
                                                                        \
    /**                                                                 \
     * Count the number of records stored with the referenced key.      \
     */                                                                 \
    ham_status_t (*_fun_get_duplicate_count)(ham_cursor_t *cursor,      \
            ham_size_t *count, ham_u32_t flags);                        \
                                                                        \
    /**                                                                 \
     * Returns true if cursor is nil, otherwise false                   \
     */                                                                 \
    ham_bool_t (*_fun_is_nil)(ham_cursor_t *cursor);                    \
                                                                        \
    /** Pointer to the Database object */                               \
    ham_db_t *_db;                                                      \
                                                                        \
    /** Pointer to the Transaction */                                   \
    ham_txn_t *_txn;                                                    \
                                                                        \
    /** A Cursor which can walk over Transaction trees */               \
    txn_cursor_t _txn_cursor;                                           \
                                                                        \
    /** The remote database handle */                                   \
    ham_u64_t _remote_handle;                                           \
                                                                        \
    /** Linked list of all Cursors in this Database */                  \
    clss *_next, *_previous;                                            \
                                                                        \
    /** Linked list of Cursors which point to the same page */          \
    clss *_next_in_page, *_previous_in_page;                            \
                                                                        \
    /** A cache for all duplicates of the current key. needed for       \
     * ham_cursor_move, ham_find and other functions. The cache is      \
     * used to consolidate all duplicates of btree and txn. */          \
    dupecache_t _dupecache;                                             \
                                                                        \
    /** The current position of the cursor in the cache. This is a      \
     * 1-based index. 0 means that the cache is not in use. */          \
    ham_u32_t _dupecache_index;                                         \
                                                                        \
    /** Stores the last operation (insert/find or move); needed for     \
     * ham_cursor_move. Values can be HAM_CURSOR_NEXT,                  \
     * HAM_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT */                   \
    ham_u32_t _lastop;                                                  \
                                                                        \
    /** Cursor flags */                                                 \
    ham_u32_t _flags


/**
 * a generic Cursor structure, which has the same memory layout as
 * all other backends
 */
struct ham_cursor_t
{
    CURSOR_DECLARATIONS(ham_cursor_t);
};

/** Get the Cursor flags */
#define cursor_get_flags(c)               (c)->_flags

/** Set the Cursor flags */
#define cursor_set_flags(c, f)            (c)->_flags=(f)

/** Cursor flag: cursor is coupled to the Transaction cursor (_txn_cursor) */
#define CURSOR_COUPLED_TO_TXN               0x100000

/** Get the 'next' pointer of the linked list */
#define cursor_get_next(c)                (c)->_next

/** Set the 'next' pointer of the linked list */
#define cursor_set_next(c, n)             (c)->_next=(n)

/** Get the 'previous' pointer of the linked list */
#define cursor_get_previous(c)            (c)->_previous

/** Set the 'previous' pointer of the linked list */
#define cursor_set_previous(c, p)         (c)->_previous=(p)

/** Get the 'next' pointer of the linked list */
#define cursor_get_next_in_page(c)       (c)->_next_in_page

/** Set the 'next' pointer of the linked list */
#define cursor_set_next_in_page(c, n)                                        \
    {                                                                        \
        if (n)                                                                \
            ham_assert((c)->_previous_in_page!=(n), (0));                    \
        (c)->_next_in_page=(n);                                                \
    }

/** Get the 'previous' pointer of the linked list */
#define cursor_get_previous_in_page(c)   (c)->_previous_in_page

/** Set the 'previous' pointer of the linked list */
#define cursor_set_previous_in_page(c, p)                                    \
    {                                                                        \
        if (p)                                                                \
            ham_assert((c)->_next_in_page!=(p), (0));                        \
        (c)->_previous_in_page=(p);                                            \
    }

/** Set the Database pointer */
#define cursor_set_db(c, db)            (c)->_db=db

/** Get the Database pointer */
#define cursor_get_db(c)                (c)->_db

/** Get the Transaction handle */
#define cursor_get_txn(c)               (c)->_txn

/** Set the Transaction handle */
#define cursor_set_txn(c, txn)          (c)->_txn=(txn)

/** Get a pointer to the Transaction cursor */
#define cursor_get_txn_cursor(c)        (&(c)->_txn_cursor)

/** Get the remote Database handle */
#define cursor_get_remote_handle(c)     (c)->_remote_handle

/** Set the remote Database handle */
#define cursor_set_remote_handle(c, h)  (c)->_remote_handle=(h)

/** Get a pointer to the duplicate cache */
#define cursor_get_dupecache(c)         (&(c)->_dupecache)

/** Get the current index in the dupe cache */
#define cursor_get_dupecache_index(c)   (c)->_dupecache_index

/** Set the current index in the dupe cache */
#define cursor_set_dupecache_index(c, i) (c)->_dupecache_index=i

/** Get the previous operation */
#define cursor_get_lastop(c)            (c)->_lastop

/** Store the current operation; needed for ham_cursor_move */
#define cursor_set_lastop(c, o)         (c)->_lastop=(o)

/** flag for cursor_set_lastop */
#define CURSOR_LOOKUP_INSERT            0x10000

/**
 * Updates (or builds) the dupecache for a cursor
 */
extern ham_status_t
cursor_update_dupecache(ham_cursor_t *cursor, txn_opnode_t *node);

/**
 * Couples the cursor to a duplicate in the dupe table
 * dupe_id is a 1 based index!!
 */
extern void
cursor_couple_to_dupe(ham_cursor_t *cursor, ham_u32_t dupe_id);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CURSORS_H__ */
