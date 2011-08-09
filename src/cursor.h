/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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

    /** Are we using btree or txn duplicates? */
    ham_bool_t _use_btree;

    union {
        /** The btree duplicate index (of the original btree dupe table) */
        ham_u64_t _btree_dupeidx;

        /** The txn op structure */
        txn_op_t *_op;
    } _u;

} dupecache_line_t;

/** Retrieves which part of the union is used */
#define dupecache_line_use_btree(dcl)           (dcl)->_use_btree

/** Specifies which part of the union is used */
#define dupecache_line_set_btree(dcl, b)        (dcl)->_use_btree=b

/** Get the btree duplicate index */
#define dupecache_line_get_btree_dupe_idx(dcl)  (dcl)->_u._btree_dupeidx

/** Set the btree duplicate index */
#define dupecache_line_set_btree_dupe_idx(d, i) (d)->_u._btree_dupeidx=i

/** Get txn_op_t pointer of the txn record */
#define dupecache_line_get_txn_op(dcl)          (dcl)->_u._op

/** Set txn_op_t pointer of the txn record */
#define dupecache_line_set_txn_op(dcl, op)      (dcl)->_u._op=op


/**
 * The dupecache is a cache for duplicate keys
 */
typedef struct dupecache_t {
    /** The cursor - needed for allocator etc */
    struct ham_cursor_t *_cursor;

    /** Capacity of this cache */
    ham_size_t _capacity;

    /** Number of elements tracked in this cache */
    ham_size_t _count;

    /** The cached elements */
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
 * Creates a new dupecache structure
 */
extern ham_status_t
dupecache_create(dupecache_t *c, struct ham_cursor_t *cursor, 
                    ham_size_t capacity);

/**
 * Clones two dupe-caches
 */
extern ham_status_t
dupecache_clone(dupecache_t *src, dupecache_t *dest);

/**
 * Inserts a new item somewhere in the cache; resizes the cache if necessary
 */
extern ham_status_t
dupecache_insert(dupecache_t *c, ham_u32_t position, dupecache_line_t *dupe);

/**
 * Appends a new item; resizes the cache if necessary
 */
extern ham_status_t
dupecache_append(dupecache_t *c, dupecache_line_t *dupe);

/**
 * Erases an item 
 */
extern ham_status_t
dupecache_erase(dupecache_t *c, ham_u32_t position);

/**
 * Clears the cache; frees all resources
 */
extern void
dupecache_clear(dupecache_t *c);

/**
 * Empties the cache; will not free resources
 */
extern void
dupecache_reset(dupecache_t *c);


/**
 * The Cursor structure - these functions and members are "inherited"
 * by every other cursor (i.e. btree, hashdb etc).
 */
#define CURSOR_DECLARATIONS(clss)                                       \
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
#define _CURSOR_COUPLED_TO_TXN            0x100000

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
#ifdef HAM_DEBUG
extern txn_cursor_t *
cursor_get_txn_cursor(ham_cursor_t *cursor);
#else
#   define cursor_get_txn_cursor(c)     (&(c)->_txn_cursor)
#endif

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
 * Creates a new cursor
 */
extern ham_status_t
cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_cursor_t **pcursor);

/**
 * Clones an existing cursor
 */
extern ham_status_t
cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * Returns true if a cursor is nil (Not In List - does not point to any key)
 *
 * 'what' is one of the flags below
 */
extern ham_bool_t
cursor_is_nil(ham_cursor_t *cursor, int what);

#define CURSOR_BOTH         0
#define CURSOR_BTREE        1
#define CURSOR_TXN          2

/**
 * Sets the cursor to nil
 */
extern void
cursor_set_to_nil(ham_cursor_t *cursor, int what);

/**
 * Returns true if a cursor is coupled to the btree
 */
#define cursor_is_coupled_to_btree(c)                                         \
                                 (!(cursor_get_flags(c)&_CURSOR_COUPLED_TO_TXN))

/**
 * Returns true if a cursor is coupled to a txn-op
 */
#define cursor_is_coupled_to_txnop(c)                                         \
                                    (cursor_get_flags(c)&_CURSOR_COUPLED_TO_TXN)

/**
 * Couples the cursor to a btree key
 */
#define cursor_couple_to_btree(c)                                             \
            (cursor_set_flags(c, cursor_get_flags(c)&(~_CURSOR_COUPLED_TO_TXN)))

/**
 * Couples the cursor to a txn-op
 */
#define cursor_couple_to_txnop(c)                                             \
               (cursor_set_flags(c, cursor_get_flags(c)|_CURSOR_COUPLED_TO_TXN))

/**
 * Erases the key/record pair that the cursor points to. 
 *
 * On success, the cursor is then set to nil. The Transaction is passed 
 * as a separate pointer since it might be a local/temporary Transaction 
 * that was created only for this single operation.
 */
extern ham_status_t
cursor_erase(ham_cursor_t *cursor, ham_txn_t *txn, ham_u32_t flags);

/**
 * Retrieves the number of duplicates of the current key
 *
 * The Transaction is passed as a separate pointer since it might be a 
 * local/temporary Transaction that was created only for this single operation.
 */
extern ham_status_t
cursor_get_duplicate_count(ham_cursor_t *cursor, ham_txn_t *txn, 
            ham_u32_t *pcount, ham_u32_t flags);

/**
 * Updates (or builds) the dupecache for a cursor
 *
 * The 'what' parameter specifies if the dupecache is initialized from
 * btree (CURSOR_BTREE), from txn (CURSOR_TXN) or both.
 */
extern ham_status_t
cursor_update_dupecache(ham_cursor_t *cursor, ham_u32_t what);

/**
 * Couples the cursor to a duplicate in the dupe table
 * dupe_id is a 1 based index!!
 */
extern void
cursor_couple_to_dupe(ham_cursor_t *cursor, ham_u32_t dupe_id);

/**
 * Checks if a btree cursor points to a key that was overwritten or erased
 * in the txn-cursor
 *
 * This is needed in db.c when moving the cursor backwards/forwards and 
 * consolidating the btree and the txn-tree
 */
extern ham_status_t
cursor_check_if_btree_key_is_erased_or_overwritten(ham_cursor_t *cursor);

/**
 * Synchronizes txn- and btree-cursor
 *
 * If txn-cursor is nil then try to move the txn-cursor to the same key
 * as the btree cursor.
 * If btree-cursor is nil then try to move the btree-cursor to the same key
 * as the txn cursor.
 * If both are nil, or both are valid, then nothing happens
 *
 * equal_key is set to true if the keys in both cursors are equal.
 */
extern ham_status_t
cursor_sync(ham_cursor_t *cursor, ham_u32_t flags, ham_bool_t *equal_keys);

/**
 * flag for cursor_sync: do not use approx matching if the key
 * is not available
 */
#define CURSOR_SYNC_ONLY_EQUAL_KEY            0x200000

/**
 * flag for cursor_sync: do not load the key if there's an approx.
 * match. Only positions the cursor.
 */
#define CURSOR_SYNC_DONT_LOAD_KEY             0x100000

/**
 * Returns the number of duplicates in the duplicate cache
 * The duplicate cache is updated if necessary
 */
extern ham_size_t
cursor_get_dupecache_count(ham_cursor_t *cursor);

/**
 * Closes an existing cursor
 */
extern void
cursor_close(ham_cursor_t *cursor);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CURSORS_H__ */
