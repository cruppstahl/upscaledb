/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
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
#include "page.h"

/**
 * a transaction structure
 */
struct ham_txn_t 
{
    /**
     * the database object
     */
    ham_db_t *_db;

    /**
     * the transaction ID
     */
    ham_u32_t _id;

    /**
     * transaction flags
     */
    ham_u32_t _flags;

    /**
     * a list of pages which are referenced by this transaction
     */
    ham_page_t *_pagelist;
};

/**
 * get the database owner
 */
#define txn_get_owner(txn)                      (txn)->_db

/**
 * set the database owner
 */
#define txn_set_owner(txn, db)                  (txn)->_db=db

/**
 * get the transaction ID
 */
#define txn_get_id(txn)                         (txn)->_id

/**
 * set the transaction ID
 */
#define txn_set_id(txn, id)                     (txn)->_id=id

/**
 * get the transaction flags
 */
#define txn_get_flags(txn)                      (txn)->_flags

/**
 * set the transaction flags
 */
#define txn_set_flags(txn, f)                   (txn)->_flags=f

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
txn_add_page(ham_txn_t *txn, ham_page_t *page);

/**
 * get a page from the transaction's pagelist
 *
 * @return 0 if the page is not in the list
 */
extern ham_page_t *
txn_get_page(ham_txn_t *txn, ham_offset_t offset);

/**
 * remove a page from the transaction's pagelist
 */
extern ham_status_t
txn_remove_page(ham_txn_t *txn, ham_page_t *page);

/**
 * start a transaction
 *
 * @remark flags are defined below
 */
extern ham_status_t
ham_txn_begin(ham_txn_t *txn, ham_db_t *db, ham_u32_t flags);

/**
 * flag for ham_txn_begin: this txn is read-only
 */
#define TXN_READ_ONLY               1

/**
 * commit a transaction
 *
 * @remark flags are undefined, set to zero
 */
extern ham_status_t
ham_txn_commit(ham_txn_t *txn, ham_u32_t flags);

/**
 * abort a transaction
 *
 * @remark flags are undefined, set to zero
 */
extern ham_status_t
ham_txn_abort(ham_txn_t *txn, ham_u32_t flags);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TXN_H__ */
