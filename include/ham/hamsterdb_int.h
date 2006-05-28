/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * include file for hamster-db
 *
 */

#ifndef HAM_HAMSTERDB_INT_H__
#define HAM_HAMSTERDB_INT_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>


/**
 * a callback function for dump - dumps a single key to stdout
 */
typedef void (*ham_dump_cb_t)(const ham_u8_t *key, ham_size_t keysize);

/** 
 * dump the whole tree to stdout
 *
 * @remark you can pass a callback function pointer, or NULL for the default
 * function (dumps the first 16 bytes of the key)
 */
extern ham_status_t
ham_dump(ham_db_t *db, ham_txn_t *txn, ham_dump_cb_t cb);

/** 
 * verify the whole tree - this is only useful when you debug hamsterdb
 */
extern ham_status_t
ham_check_integrity(ham_db_t *db, ham_txn_t *txn);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_INT_H__ */
