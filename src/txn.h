/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * transactions
 *
 */

#ifndef HAM_TXN_H__
#define HAM_TXN_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>

struct ham_page_t;

/**
 * a dummy transaction structure
 */
struct ham_txn_t
{
    /**
     * the id of this txn
     */
    ham_u64_t _id;

    /**
     * owner of this transaction 
     */
    ham_db_t *_db;

    /**
     * flags for this transaction
     */
    ham_u32_t _flags;

    /**
     * reference counter for cursors (how many cursors are
     * attached to this txn?)
     */
    ham_u32_t _cursor_refcount;

    /**
     * index of the log file descriptor for this transaction
     */
    int _log_desc;

    /**
     * linked list of all transactions
     */
    ham_txn_t *_next, *_previous;

    /**
     * a list of pages which are referenced by this transaction
     */
    struct ham_page_t *_pagelist;

};

/**
 * get the id
 */
#define txn_get_id(txn)                         (txn)->_id

/**
 * set the id
 */
#define txn_set_id(txn, id)                     (txn)->_id=id

/**
 * set the database pointer
 */
#define txn_set_db(txn, db)                     (txn)->_db=db

/**
 * get the database pointer
 */
#define txn_get_db(txn)                         (txn)->_db

/**
 * set the database pointer
 */
#define txn_set_db(txn, db)                     (txn)->_db=db

/**
 * get the flags
 */
#define txn_get_flags(txn)                      (txn)->_flags

/**
 * set the flags 
 */
#define txn_set_flags(txn, f)                   (txn)->_flags=f

/**
 * get the cursor refcount
 */
#define txn_get_cursor_refcount(txn)            (txn)->_cursor_refcount

/**
 * set the cursor refcount 
 */
#define txn_set_cursor_refcount(txn, cfc)       (txn)->_cursor_refcount=cfc

/**
 * get the index of the log file descriptor
 */
#define txn_get_log_desc(txn)                   (txn)->_log_desc

/**
 * set the index of the log file descriptor
 */
#define txn_set_log_desc(txn, desc)             (txn)->_log_desc=desc

/**
 * get the 'next' pointer of the linked list
 */
#define txn_get_next(txn)                       (txn)->_next

/**
 * set the 'next' pointer of the linked list
 */
#define txn_set_next(txn, n)                    (txn)->_next=n

/**
 * get the 'previous' pointer of the linked list
 */
#define txn_get_previous(txn)                   (txn)->_previous

/**
 * set the 'previous' pointer of the linked list
 */
#define txn_set_previous(txn, p)                (txn)->_previous=p

/**
 * get the page list
 */
#define txn_get_pagelist(txn)                   (txn)->_pagelist

/**
 * set the page list
 */
#define txn_set_pagelist(txn, pl)                (txn)->_pagelist=pl

/**
 * add a page to the transaction's pagelist
 */
extern ham_status_t
txn_add_page(ham_txn_t *txn, struct ham_page_t *page, 
        ham_bool_t ignore_if_inserted);

/**
 * remove a page from the transaction's pagelist
 */
extern ham_status_t
txn_remove_page(ham_txn_t *txn, struct ham_page_t *page);

/**
 * get a page from the transaction's pagelist; returns 0 if the page
 * is not in the list
 */
extern struct ham_page_t *
txn_get_page(ham_txn_t *txn, ham_offset_t address);

/**
 * mark a page in the transaction as 'deleted'
 * it will be deleted when the transaction is committed
 */
extern ham_status_t
txn_free_page(ham_txn_t *txn, struct ham_page_t *page);

/**
 * start a transaction
 *
 * @remark flags are defined below
 */
extern ham_status_t
txn_begin(ham_txn_t *txn, ham_db_t *db, ham_u32_t flags);

#define HAM_TXN_READ_ONLY       1

/**
 * commit a transaction
 */
extern ham_status_t
txn_commit(ham_txn_t *txn, ham_u32_t flags);

#define TXN_FORCE_WRITE         1

/**
 * abort a transaction
 */
extern ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TXN_H__ */
