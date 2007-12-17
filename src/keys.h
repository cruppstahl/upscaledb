/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
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
typedef HAM_PACK_0 HAM_PACK_1 struct int_key_t
{
    /**
     * the pointer of this entry
     */
    ham_offset_t _ptr;

    /**
     * the size of this entry
     */
    ham_u16_t _keysize;

    /**
     * flags
     */
    ham_u8_t _flags;

    /**
     * the key
     */
    ham_u8_t _key[1];

} HAM_PACK_2 int_key_t;

#include "packstop.h"

/**
 * get the pointer of an btree-entry
 * 
 * !!!
 * if TINY or SMALL is set, the key is actually a char*-pointer;
 * in this case, we must not use endian-conversion!
 */
#define key_get_ptr(k)             (((key_get_flags(k)&KEY_BLOB_SIZE_TINY) ||  \
                                     (key_get_flags(k)&KEY_BLOB_SIZE_SMALL))   \
                                    ? (k)->_ptr                                \
                                    : ham_db2h_offset((k)->_ptr))

/**
 * set the pointer of an btree-entry
 *
 * !!! same problems as with key_get_ptr():
 * if TINY or SMALL is set, the key is actually a char*-pointer;
 * in this case, we must not use endian-conversion!
 */
#define key_set_ptr(k, p)          (k)->_ptr=(                                 \
                                    ((key_get_flags(k)&KEY_BLOB_SIZE_TINY) ||  \
                                     (key_get_flags(k)&KEY_BLOB_SIZE_SMALL))   \
                                    ? p                                        \
                                    : ham_h2db_offset(p))

/**
 * get the size of an btree-entry
 */
#define key_get_size(bte)               (ham_db2h16((bte)->_keysize))

/**
 * set the size of an btree-entry
 */
#define key_set_size(bte, s)            (bte)->_keysize=ham_h2db16(s)

/**
 * get the record-ID of an extended key
 */
extern ham_offset_t
key_get_extended_rid(ham_db_t *db, int_key_t *key);

/**
 * set the record-ID of an extended key
 */
extern void
key_set_extended_rid(ham_db_t *db, int_key_t *key, ham_offset_t rid);

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
#define KEY_IS_EXTENDED                 8
#define KEY_HAS_DUPLICATES             16

/**
 * get a pointer to the key 
 */
#define key_get_key(bte)                (bte->_key)

/**
 * set the key data 
 */
#define key_set_key(bte, ptr, len)      memcpy(bte->_key, ptr, len)

/**
 * compare an internal key (int_key_t) to a public key (ham_key_t)
 */
extern int
key_compare_int_to_pub(ham_page_t *page, ham_u16_t lhs, ham_key_t *rhs);

/**
 * compare a public key (ham_key_t) to an internal key (int_key_t)
 */
extern int
key_compare_pub_to_int(ham_page_t *page, ham_key_t *lhs, ham_u16_t rhs);

/**
 * compare an internal key (int_key_t) to an internal key
 */
extern int
key_compare_int_to_int(ham_page_t *page, ham_u16_t lhs, ham_u16_t rhs);

/**
 * insert an extended key
 *
 * returns the blob-id of this key
 */
extern ham_offset_t
key_insert_extended(ham_db_t *db, ham_page_t *page, ham_key_t *key);

/**
 * inserts and sets a record
 *
 * flags can be 
 * - HAM_OVERWRITE
 * - HAM_DUPLICATE_INSERT_BEFORE
 * - HAM_DUPLICATE_INSERT_AFTER
 * - HAM_DUPLICATE_INSERT_FIRST
 * - HAM_DUPLICATE_INSERT_LAST or HAM_DUPLICATE
 *
 * a previously existing blob will be deleted if necessary
 */
extern ham_status_t
key_set_record(ham_db_t *db, int_key_t *key, ham_record_t *record, 
                ham_size_t position, ham_u32_t flags,
                ham_size_t *new_position);

/*
 * deletes a record
 *
 * flag can be BLOB_FREE_ALL_DUPES (declared in blob.h)
 */
extern ham_status_t
key_erase_record(ham_db_t *db, int_key_t *key, 
                ham_size_t dupe_id, ham_u32_t flags);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_KEY_H__ */
