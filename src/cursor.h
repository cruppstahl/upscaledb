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
 * A Cursor is an object which is used to traverse a Database. 
 *
 * A Cursor structure is separated into 3 components:
 * 1. The btree cursor
 *      This cursor can traverse btrees. It is described and implemented
 *      in btree_cursor.h.
 * 2. The txn cursor
 *      This cursor can traverse txn-trees. It is described and implemented
 *      in txn_cursor.h.
 * 3. The upper layer
 *      This layer acts as a kind of dispatcher for both cursors. If 
 *      Transactions are used, then it also uses a duplicate cache for 
 *      consolidating the duplicate keys from both cursors. This layer is
 *      described and implemented in cursor.h (this file.
 * 
 * A Cursor can have several states. It can be 
 * 1. NIL (not in list) - this is the default state, meaning that the Cursor
 *      does not point to any key. If the Cursor was initialized, then it's 
 *      "NIL". If the Cursor was erased (@ref ham_cursor_erase) then it's
 *      also "NIL".
 *
 *      relevant functions:
 *          @ref Cursor::is_nil
 *          @ref Cursor::set_to_nil
 *
 * 2. Coupled to the txn-cursor - meaning that the Cursor points to a key
 *      that is modified in a Transaction. Technically, the txn-cursor points
 *      to a @ref txn_op_t structure.
 *
 *      relevant functions:
 *          @ref Cursor::is_coupled_to_txnop
 *          @ref Cursor::couple_to_txnop
 *
 * 3. Coupled to the btree-cursor - meaning that the Cursor points to a key
 *      that is stored in a Btree. A Btree cursor itself can then be coupled
 *      (it directly points to a page in the cache) or uncoupled, meaning that
 *      the page was purged from the cache and has to be fetched from disk when
 *      the Cursor is used again. This is described in btree_cursor.h.
 *
 *      relevant functions:
 *          @ref Cursor::is_coupled_to_btree
 *          @ref Cursor::couple_to_btree
 *
 * The dupecache is used when information from the btree and the txn-tree 
 * is merged. The btree cursor has its private dupecache. Both will be merged
 * sooner or later. 
 *
 * The cursor interface is used in db.c. Many of the functions in db.c use 
 * a high-level cursor interface (i.e. @ref cursor_create, @ref cursor_clone) 
 * while some directly use the low-level interfaces of btree_cursor.h and
 * txn_cursor.h. Over time i will clean this up, trying to maintain a clear
 * separation of the 3 layers, and only accessing the top-level layer in
 * cursor.h. This is work in progress.
 *
 * In order to speed up Cursor::move() we keep track of the last compare 
 * between the two cursors. i.e. if the btree cursor is currently pointing to
 * a larger key than the txn-cursor, the 'lastcmp' field is <0 etc. 
 */

#ifndef HAM_CURSORS_H__
#define HAM_CURSORS_H__

#include <vector>

#include "internal_fwd_decl.h"

#include "error.h"
#include "txn_cursor.h"
#include "btree_cursor.h"
#include "blob.h"
#include "env.h"
#include "db.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * A single line in the dupecache structure - can reference a btree
 * record or a txn-op 
 */
class DupeCacheLine 
{
  public:
    DupeCacheLine(ham_bool_t use_btree=true, ham_u64_t btree_dupeidx=0)
    : m_use_btree(use_btree), m_btree_dupeidx(btree_dupeidx), m_op(0) {
        ham_assert(use_btree==true, (""));
    }

    DupeCacheLine(ham_bool_t use_btree, txn_op_t *op)
    : m_use_btree(use_btree), m_btree_dupeidx(0), m_op(op) {
        ham_assert(use_btree==false, (""));
    }

    /** Returns true if this cache entry is a duplicate in the btree */
    ham_bool_t use_btree(void) {
        return (m_use_btree); 
    }

    /** Returns the btree duplicate index */
    ham_offset_t get_btree_dupe_idx(void) {
        ham_assert(m_use_btree==true, (""));
        return (m_btree_dupeidx);
    }

    /** Sets the btree duplicate index */
    void set_btree_dupe_idx(ham_offset_t idx) {
        m_use_btree=true;
        m_btree_dupeidx=idx;
        m_op=0;
    }

    /** Returns the txn-op duplicate */
    txn_op_t *get_txn_op(void) {
        ham_assert(m_use_btree==false, (""));
        return (m_op);
    }

    /** Sets the txn-op duplicate */
    void set_txn_op(txn_op_t *op) {
        m_use_btree=false;
        m_op=op;
        m_btree_dupeidx=0;
    }

  private:
    /** Are we using btree or txn duplicates? */
    ham_bool_t m_use_btree;

    /** The btree duplicate index (of the original btree dupe table) */
    ham_u64_t m_btree_dupeidx;

    /** The txn op structure */
    txn_op_t *m_op;

};


/**
 * The dupecache is a cache for duplicate keys
 */
class DupeCache {
  public:
    /* default constructor - creates an empty dupecache with room for 8 
     * duplicates */
    DupeCache(void) {
        m_elements.reserve(8);
    }

    /** retrieve number of elements in the cache */
    ham_size_t get_count(void) {
        return (m_elements.size());
    }

    /** get an element from the cache */
    DupeCacheLine *get_element(unsigned idx) {
        return (&m_elements[idx]);
    }

    /** get a pointer to the first element from the cache */
    DupeCacheLine *get_first_element(void) {
        return (&m_elements[0]);
    }

    /** Clones this dupe-cache into 'other' */
    void clone(DupeCache *other) {
        other->m_elements=m_elements;
    }

    /**
     * Inserts a new item somewhere in the cache; resizes the 
     * cache if necessary
     */
    void insert(unsigned position, const DupeCacheLine &dcl) {
        m_elements.insert(m_elements.begin()+position, dcl);
    }

    /** append an element to the dupecache */
    void append(const DupeCacheLine &dcl) {
        m_elements.push_back(dcl);
    }

    /** Erases an item */
    void erase(ham_u32_t position) {
        m_elements.erase(m_elements.begin()+position);
    }

    /** Clears the cache; frees all resources */
    void clear(void) {
        m_elements.resize(0);
    }

  private:
    /** The cached elements */
    std::vector<DupeCacheLine> m_elements;

};


/**
 * a helper structure; ham_cursor_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_cursor_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_cursor_t
{
    bool _dummy;
};

/**
 * the Database Cursor
 */
class Cursor
{
  public:
    /** flags for set_to_nil, is_nil */
    static const unsigned CURSOR_BTREE = 1;
    static const unsigned CURSOR_TXN   = 2;
    static const unsigned CURSOR_BOTH  = (CURSOR_BTREE|CURSOR_TXN);

    /**
     * flag for cursor_sync: do not use approx matching if the key
     * is not available
     */
    static const unsigned CURSOR_SYNC_ONLY_EQUAL_KEY = 0x200000;

    /**
     * flag for cursor_sync: do not load the key if there's an approx.
     * match. Only positions the cursor.
     */
    static const unsigned CURSOR_SYNC_DONT_LOAD_KEY = 0x100000;

    /*
     * the flags have ranges:
     *  0 - 0x1000000-1:      btree_cursor
     *  > 0x1000000:          cursor
     */
    /** Cursor flag: cursor is coupled to the txn-cursor */
    static const unsigned _CURSOR_COUPLED_TO_TXN = 0x1000000;

    /** flag for cursor_set_lastop */
    static const unsigned CURSOR_LOOKUP_INSERT = 0x10000;

  public:
    /** Constructor; retrieves pointer to db and txn, initializes all
     * fields */
    Cursor(Database *db, ham_txn_t *txn=0, ham_u32_t flags=0);

    /** Copy constructor; used for cloning a Cursor */
    Cursor(Cursor &other);

    /**
     * Returns true if a cursor is nil (Not In List - does not point to any key)
     *
     * 'what' is one of the flags CURSOR_BOTH, CURSOR_TXN, CURSOR_BTREE
     */
    bool is_nil(int what=CURSOR_BOTH);

    /** Returns true if a cursor is coupled to the btree */
    bool is_coupled_to_btree(void) {
        return (!(get_flags()&_CURSOR_COUPLED_TO_TXN));
    }

    /** Returns true if a cursor is coupled to a txn-op */
    bool is_coupled_to_txnop(void) {
        return (get_flags()&_CURSOR_COUPLED_TO_TXN);
    }

    /** Couples the cursor to a btree key */
    void couple_to_btree(void) {
        return (set_flags(get_flags()&(~_CURSOR_COUPLED_TO_TXN)));
    }

    /** Couples the cursor to a txn-op */
    void couple_to_txnop(void) {
        return (set_flags(get_flags()|_CURSOR_COUPLED_TO_TXN));
    }

    /** Sets the cursor to nil */
    void set_to_nil(int what=CURSOR_BOTH);

    /**
     * Erases the key/record pair that the cursor points to. 
     *
     * On success, the cursor is then set to nil. The Transaction is passed 
     * as a separate pointer since it might be a local/temporary Transaction 
     * that was created only for this single operation.
     */
    ham_status_t erase(ham_txn_t *txn, ham_u32_t flags);

    /**
     * Retrieves the number of duplicates of the current key
     *
     * The Transaction is passed as a separate pointer since it might be a 
     * local/temporary Transaction that was created only for this single 
     * operation.
     */
    ham_status_t get_duplicate_count(ham_txn_t *txn, ham_u32_t *pcount, 
                ham_u32_t flags);

    /**
     * Retrieves the size of the current record
     *
     * The Transaction is passed as a separate pointer since it might be a 
     * local/temporary Transaction that was created only for this single 
     * operation.
     */
    ham_status_t get_record_size(ham_txn_t *txn, ham_offset_t *psize);

    /**
     * Overwrites the record of the current key
     *
     * The Transaction is passed as a separate pointer since it might be a 
     * local/temporary Transaction that was created only for this single 
     * operation.
     */
    ham_status_t overwrite(ham_txn_t *txn, ham_record_t *record,
            ham_u32_t flags);

    /**
     * Updates (or builds) the dupecache for a cursor
     *
     * The 'what' parameter specifies if the dupecache is initialized from
     * btree (CURSOR_BTREE), from txn (CURSOR_TXN) or both.
     */
    ham_status_t update_dupecache(ham_u32_t what);

    /** Clear the dupecache and disconnect the Cursor from any duplicate key */
    void clear_dupecache(void) {
        get_dupecache()->clear();
        set_dupecache_index(0);
    }

    /**
     * Couples the cursor to a duplicate in the dupe table
     * dupe_id is a 1 based index!!
     */
    void couple_to_dupe(ham_u32_t dupe_id);

    /**
     * Checks if a btree cursor points to a key that was overwritten or erased
     * in the txn-cursor
     *
     * This is needed in db.c when moving the cursor backwards/forwards and 
     * consolidating the btree and the txn-tree
     */
    ham_status_t check_if_btree_key_is_erased_or_overwritten(void);

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
    ham_status_t sync(ham_u32_t flags, ham_bool_t *equal_keys);

    /** Moves a Cursor */
    ham_status_t move(ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * Returns the number of duplicates in the duplicate cache
     * The duplicate cache is updated if necessary
     */
    ham_size_t get_dupecache_count(void) {
        if (!(m_db->get_rt_flags()&HAM_ENABLE_DUPLICATES))
            return (0);

        txn_cursor_t *txnc=get_txn_cursor();
        if (txn_cursor_get_coupled_op(txnc))
            update_dupecache(CURSOR_BTREE|CURSOR_TXN);
        else
            update_dupecache(CURSOR_BTREE);
        return (get_dupecache()->get_count());
    }

    /** Closes an existing cursor */
    void close(void);

    /** Get the Cursor flags */
    ham_u32_t get_flags(void) {
        return (m_flags);
    }

    /** Set the Cursor flags */
    void set_flags(ham_u32_t flags) {
        m_flags=flags;
    }

    /** Get the Database */
    Database *get_db(void) {
        return (m_db);
    }

    /** Get the 'next' Cursor in this Database */
    Cursor *get_next(void) {
        return (m_next);
    }

    /** Set the 'next' Cursor in this Database */
    void set_next(Cursor *next) {
        m_next=next;
    }

    /** Get the 'previous' Cursor in this Database */
    Cursor *get_previous(void) {
        return (m_previous);
    }

    /** Set the 'previous' Cursor in this Database */
    void set_previous(Cursor *previous) {
        m_previous=previous;
    }

    /** Get the 'next' Cursor which is attached to the same page */
    Cursor *get_next_in_page(void) {
        return (m_next_in_page);
    }

    /** Set the 'next' Cursor which is attached to the same page */
    void set_next_in_page(Cursor *next) {
        m_next_in_page=next;
    }

    /** Get the 'previous' Cursor which is attached to the same page */
    Cursor *get_previous_in_page(void) {
        return (m_previous_in_page);
    }

    /** Set the 'previous' Cursor which is attached to the same page */
    void set_previous_in_page(Cursor *previous) {
        m_previous_in_page=previous;
    }

    /** Get the Transaction handle */
    ham_txn_t *get_txn() {
        return (m_txn);
    }

    /** Sets the Transaction handle */
    void set_txn(ham_txn_t *txn) {
        m_txn=txn;
    }

    /** Get a pointer to the Transaction cursor */
    txn_cursor_t *get_txn_cursor(void) {
        return (&m_txn_cursor);
    }

    /** Get a pointer to the Btree cursor */
    btree_cursor_t *get_btree_cursor(void) {
        return (&m_btree_cursor);
    }

    /** Get the remote Cursor handle */
    ham_u64_t get_remote_handle(void) {
        return (m_remote_handle);
    }

    /** Set the remote Cursor handle */
    void set_remote_handle(ham_u64_t handle) {
        m_remote_handle=handle;
    }

    /** Get a pointer to the duplicate cache */
    DupeCache *get_dupecache(void) {
        return (&m_dupecache);
    }

    /** Get the current index in the dupe cache */
    ham_size_t get_dupecache_index(void) {
        return (m_dupecache_index);
    }

    /** Set the current index in the dupe cache */
    void set_dupecache_index(ham_size_t index) {
        m_dupecache_index=index;
    }

    /** Get the previous operation */
    ham_u32_t get_lastop(void) {
        return (m_lastop);
    }

    /** Store the current operation; needed for ham_cursor_move */
    void set_lastop(ham_u32_t lastop) {
        m_lastop=lastop;
        m_is_first_use=false;
    }

    /** Get the result of the previous compare operation:
     * db->compare_keys(btree-cursor, txn-cursor) */
    int get_lastcmp(void) {
        return (m_lastcmp);
    }

    /** Set the result of the previous compare operation:
     * db->compare_keys(btree-cursor, txn-cursor) */
    void set_lastcmp(int cmp) {
        m_lastcmp=cmp;
    }

    /** Returns true if this cursor was never used */
    bool is_first_use(void) {
        return (m_is_first_use);
    }

    /** If true then this cursor was never used (or was deleted) */
    void set_first_use(bool b) {
        m_is_first_use=b;
    }

  private:
    /** Compares btree and txn-cursor; stores result in lastcmp */
    int compare(void);

    /** Returns true if this key has duplicates */
    bool has_duplicates(void) {
        return (get_dupecache()->get_count()>0);
    }

    /** Move cursor to the first duplicate */
    ham_status_t move_first_dupe(void);

    /** Move cursor to the last duplicate */
    ham_status_t move_last_dupe(void);

    /** Move cursor to the next duplicate */
    ham_status_t move_next_dupe(void);

    /** Move cursor to the previous duplicate */
    ham_status_t move_previous_dupe(void);

    /** Move cursor to the first key */
    ham_status_t move_first_key(ham_u32_t flags);

    /** Move cursor to the last key */
    ham_status_t move_last_key(ham_u32_t flags);

    /** Move cursor to the next key */
    ham_status_t move_next_key(ham_u32_t flags);

    /** Move cursor to the previous key */
    ham_status_t move_previous_key(ham_u32_t flags);

    /** Move cursor to the first key - helper function */
    ham_status_t move_first_key_singlestep(void);

    /** Move cursor to the last key - helper function */
    ham_status_t move_last_key_singlestep(void);

    /** Move cursor to the next key - helper function */
    ham_status_t move_next_key_singlestep(void);

    /** Move cursor to the previous key - helper function */
    ham_status_t move_previous_key_singlestep(void);

    /** Pointer to the Database object */
    Database *m_db;

    /** Pointer to the Transaction */
    ham_txn_t *m_txn;

    /** A Cursor which can walk over Transaction trees */
    txn_cursor_t m_txn_cursor;

    /** A Cursor which can walk over B+trees */
    btree_cursor_t m_btree_cursor;

    /** The remote database handle */
    ham_u64_t m_remote_handle;

    /** Linked list of all Cursors in this Database */
    Cursor *m_next, *m_previous;

    /** Linked list of Cursors which point to the same page */
    Cursor *m_next_in_page, *m_previous_in_page;

    /** A cache for all duplicates of the current key. needed for
     * ham_cursor_move, ham_find and other functions. The cache is
     * used to consolidate all duplicates of btree and txn. */
    DupeCache m_dupecache;

    /** The current position of the cursor in the cache. This is a
     * 1-based index. 0 means that the cache is not in use. */
    ham_u32_t m_dupecache_index;

    /** The last operation (insert/find or move); needed for
     * ham_cursor_move. Values can be HAM_CURSOR_NEXT,
     * HAM_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT */
    ham_u32_t m_lastop;

    /** The result of the last compare operation */
    int m_lastcmp;

    /** Cursor flags */
    ham_u32_t m_flags;

    /** true if this cursor was never used */
    bool m_is_first_use;
};


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CURSORS_H__ */
