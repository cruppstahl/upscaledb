/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
 *
 * key handling
 *
 */

#ifndef HAM_KEY_H__
#define HAM_KEY_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include "packstart.h"

/**
 * the internal representation of a key
 */
typedef HAM_PACK_0 struct HAM_PACK_1 key_t
{
    /**
     * flags
     */
    ham_u8_t _flags; 

    /**
     * the size of this entry
     */
    ham_u16_t _keysize;

    /**
     * the pointer of this entry
     */
    ham_offset_t _ptr;

    /**
     * the key
     */
    ham_u8_t _key[1];
    
} HAM_PACK_2 key_t;

/**
 * get the pointer of an btree-entry
 */
#define key_get_ptr(bte)                (ham_db2h_offset(bte->_ptr))

/**
 * set the pointer of an btree-entry
 */
#define key_set_ptr(bte, p)             bte->_ptr=ham_h2db_offset(p)

/**
 * get the size of an btree-entry
 */
#define key_get_size(bte)               (ham_db2h16(bte->_keysize))

/**
 * set the size of an btree-entry
 */
#define key_set_size(bte, s)            (bte)->_keysize=ham_h2db16(s)

/**
 * get the real size of the btree-entry
 */
#define key_get_real_size(db, bte)      \
       (key_get_size(bte)<db_get_keysize(db) \
        ? key_get_size(bte) \
        : db_get_keysize(db))

/**
 * get the flags of a key
 */
#define key_get_flags(bte)              (bte)->_flags

/**
 * set the flags of a key
 */
#define key_set_flags(bte, f)           (bte)->_flags=f
#define KEY_BLOB_SIZE_TINY              1
#define KEY_BLOB_SIZE_SMALL             2
#define KEY_BLOB_SIZE_EMPTY             4
#define KEY_BLOB_SIZE_BIG               8

/**
 * get a pointer to the key 
 */
#define key_get_key(bte)                (bte->_key)

/**
 * set the key data 
 */
#define key_set_key(bte, ptr, len)      memcpy(bte->_key, ptr, len)

#include "packstop.h"

/**
 * compare an internal key (key_t) to a public key (ham_key_t)
 */
extern int
key_compare_int_to_pub(ham_txn_t *txn, ham_page_t *page, 
        ham_u16_t lhs, ham_key_t *rhs);

/**
 * compare a public key (ham_key_t) to an internal key (key_t)
 */
extern int
key_compare_pub_to_int(ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *lhs, ham_u16_t rhs);

/**
 * compare an internal key (key_t) to an internal key
 */
extern int
key_compare_int_to_int(ham_txn_t *txn, ham_page_t *page, 
        ham_u16_t lhs, ham_u16_t rhs);

/**
 * insert an extended key
 *
 * returns the blob-id of this key
 */
extern ham_offset_t
key_insert_extended(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_KEY_H__ */
