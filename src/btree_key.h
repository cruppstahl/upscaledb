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
 * @brief key handling
 *
 */

#ifndef HAM_KEY_H__
#define HAM_KEY_H__

#include "internal_fwd_decl.h"


#include "packstart.h"

/**
 * the internal representation of a key
 *
 * Note: the names of the fields have changed in 1.1.0 to ensure the compiler 
 * barfs on misuse of some macros, e.g. key_get_flags(): here flags are 8-bit, 
 * while ham_key_t flags are 32-bit!
 */
HAM_PACK_0 struct HAM_PACK_1 btree_key_t
{
    /** the pointer/record ID of this entry */
    ham_u64_t _ptr;

    /** the size of this entry */
    ham_u16_t _keysize;

    /** key flags (see below) */
    ham_u8_t _flags8;

    /** the key itself */
    ham_u8_t _key[1];

} HAM_PACK_2;

#include "packstop.h"

/** get the size of the internal key representation header */
#define db_get_int_key_header_size()   OFFSETOF(btree_key_t, _key)
                                       /* sizeof(btree_key_t)-1 */

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

/** same as above, but without endian conversion */
#define key_get_rawptr(k)           (k)->_ptr

/**
 * set the pointer of an btree-entry
 *
 * !!! same problems as with key_get_ptr():
 * if TINY or SMALL is set, the key is actually a char*-pointer;
 * in this case, we must not use endian-conversion
 */
#define key_set_ptr(k, p)          (k)->_ptr=(                                 \
                                    ((key_get_flags(k)&KEY_BLOB_SIZE_TINY) ||  \
                                     (key_get_flags(k)&KEY_BLOB_SIZE_SMALL))   \
                                    ? p                                        \
                                    : ham_h2db_offset(p))

/** get the size of an btree-entry */
#define key_get_size(bte)               (ham_db2h16((bte)->_keysize))

/** set the size of an btree-entry */
#define key_set_size(bte, s)            (bte)->_keysize=ham_h2db16(s)

/**
 * get the record-ID of an extended key
 */
extern ham_offset_t
key_get_extended_rid(Database *db, btree_key_t *key);

/**
 * set the record-ID of an extended key
 */
extern void
key_set_extended_rid(Database *db, btree_key_t *key, ham_offset_t rid);

/** get the (persisted) flags of a key */
#define key_get_flags(bte)         (bte)->_flags8

/**
 * set the flags of a key
 * 
 * Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must be 
 * defined such that those can peacefully co-exist with these; that's why 
 * those public flags start at the value 0x1000 (4096).
 */
#define key_set_flags(bte, f)      (bte)->_flags8=(f)

/**
 * persisted btree_key_t flags; also used with ham_key_t._flags 
 * 
 * NOTE: persisted flags must fit within a ham_u8_t (1 byte) --> mask: 
 *  0x000000FF
 */
#define KEY_BLOB_SIZE_TINY           0x01  /* size < 8; len encoded at 
                                            * byte[7] of key->ptr */
#define KEY_BLOB_SIZE_SMALL          0x02  /* size == 8; encoded in key->ptr */
#define KEY_BLOB_SIZE_EMPTY          0x04  /* size == 0; key->ptr == 0 */
#define KEY_IS_EXTENDED              0x08
#define KEY_HAS_DUPLICATES           0x10
#define KEY_IS_ALLOCATED             0x20  /* memory allocated in hamsterdb */

/** get a pointer to the key */
#define key_get_key(bte)                (bte->_key)

/** set the key data */
#define key_set_key(bte, ptr, len)      memcpy(bte->_key, ptr, len)

/* 
 * flags used with the ham_key_t INTERNAL USE field _flags.
 * 
 * Note: these flags should NOT overlap with the persisted flags for btree_key_t
 * 
 * As these flags NEVER will be persisted, they should be located outside
 * the range of a ham_u16_t, i.e. outside the mask 0x0000FFFF.
 */
#define KEY_IS_LT                      0x00010000
#define KEY_IS_GT                      0x00020000
#define KEY_IS_APPROXIMATE             (KEY_IS_LT | KEY_IS_GT)

/**
 * insert an extended key
 *
 * @return the blob-id of this key in @a rid_ref
 */
extern ham_status_t
key_insert_extended(ham_offset_t *rid_ref, Database *db, 
                ham_page_t *page, ham_key_t *key);

/**
 * inserts and sets a record
 *
 * flags can be 
 * - HAM_OVERWRITE
 * - HAM_DUPLICATE_INSERT_BEFORE
 * - HAM_DUPLICATE_INSERT_AFTER
 * - HAM_DUPLICATE_INSERT_FIRST
 * - HAM_DUPLICATE_INSERT_LAST 
 * - HAM_DUPLICATE
 *
 * a previously existing blob will be deleted if necessary
 */
extern ham_status_t
key_set_record(Database *db, btree_key_t *key, ham_record_t *record, 
                ham_size_t position, ham_u32_t flags,
                ham_size_t *new_position);

/*
 * deletes a record
 *
 * flag can be HAM_ERASE_ALL_DUPLICATES
 */
extern ham_status_t
key_erase_record(Database *db, btree_key_t *key, 
                ham_size_t dupe_id, ham_u32_t flags);


#endif /* HAM_KEY_H__ */
