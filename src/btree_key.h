/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BTREE_KEY_H__
#define HAM_BTREE_KEY_H__

#include "internal_fwd_decl.h"

namespace ham {

#include "packstart.h"

/**
 * the internal representation of a serialized key
 */
HAM_PACK_0 struct HAM_PACK_1 BtreeKey
{
  /**
   * persisted BtreeKey flags; also used in combination with ham_key_t._flags
   *
   * NOTE: persisted flags must fit within a ham_u8_t (1 byte) --> mask:
   *  0x000000FF
   */
  enum {
    /* size < 8; len encoded at byte[7] of key->ptr */
    KEY_BLOB_SIZE_TINY         = 0x01,
    /* size == 8; encoded in key->ptr */
    KEY_BLOB_SIZE_SMALL        = 0x02,
    /* size == 0; key->ptr == 0 */
    KEY_BLOB_SIZE_EMPTY        = 0x04,
    /* extended key with overflow area */
    KEY_IS_EXTENDED            = 0x08,
    /* key has duplicates */
    KEY_HAS_DUPLICATES         = 0x10,
    /* memory allocated in hamsterdb, not by caller */
    KEY_IS_ALLOCATED           = 0x20
  };

  /*
   * flags used with the ham_key_t INTERNAL USE field _flags.
   *
   * Note: these flags should NOT overlap with the persisted flags for BtreeKey
   *
   * As these flags NEVER will be persisted, they should be located outside
   * the range of a ham_u16_t, i.e. outside the mask 0x0000FFFF.
   */
  enum {
    /* actual key is lower than the requested key */
    KEY_IS_LT                    = 0x00010000,
    /* actual key is greater than the requested key */
    KEY_IS_GT                    = 0x00020000,
    /* actual key is an "approximate match" */
    KEY_IS_APPROXIMATE           = (KEY_IS_LT | KEY_IS_GT)
  };

  /**
   * get the pointer of an btree-entry
   *
   * !!!
   * if TINY or SMALL is set, the key is actually a char*-pointer;
   * in this case, we must not use endian-conversion!
   */
  ham_offset_t get_ptr() {
    return (((_flags8 & KEY_BLOB_SIZE_TINY)
                || (_flags8 & KEY_BLOB_SIZE_SMALL))
            ? _ptr
            : ham_db2h_offset(_ptr));
  }

  /** same as above, but without endian conversion */
  ham_offset_t *get_rawptr() {
    return (&_ptr);
  }

  /** same as above, but without endian conversion */
  const ham_offset_t *get_rawptr() const {
    return (&_ptr);
  }

  /**
   * set the pointer of an btree-entry
   *
   * !!! same problems as with get_ptr():
   * if TINY or SMALL is set, the key is actually a char*-pointer;
   * in this case, we must not use endian-conversion
   */
  void set_ptr(ham_offset_t ptr) {
    _ptr = (((_flags8 & KEY_BLOB_SIZE_TINY) ||
              (_flags8 & KEY_BLOB_SIZE_SMALL))
            ? ptr
            : ham_h2db_offset(ptr));
  }

  /** get the size of an btree-entry */
  ham_u16_t get_size() const {
    return (ham_db2h16(_keysize));
  }

  /** set the size of an btree-entry */
  void set_size(ham_u16_t size) {
    _keysize = ham_h2db16(size);
  }

  /** get the (persisted) flags of a key */
  ham_u8_t get_flags() const {
    return (_flags8);
  }

  /**
   * set the flags of a key
   *
   * Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must be
   * defined such that those can peacefully co-exist with these; that's why
   * those public flags start at the value 0x1000 (4096).
   */
  void set_flags(ham_u8_t flags) {
    _flags8 = flags;
  }

  /** get a pointer to the key data */
  ham_u8_t *get_key() {
    return (_key);
  }

  /** get a pointer to the key data */
  const ham_u8_t *get_key() const {
    return (_key);
  }

  /** set the key data */
  void set_key(const void *ptr, ham_size_t len) {
    memcpy(_key, ptr, len);
  }

  /** get the record address of an extended key overflow area */
  ham_offset_t get_extended_rid(Database *db);

  /** set the record address of an extended key overflow area */
  void set_extended_rid(Database *db, ham_offset_t rid);

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
  ham_status_t set_record(Database *db, Transaction *txn, ham_record_t *record,
                ham_size_t position, ham_u32_t flags, ham_size_t *new_position);

  /*
   * deletes a record
   */
  ham_status_t erase_record(Database *db, Transaction *txn, ham_size_t dupe_id,
                bool erase_all_duplicates);

  /** the size of this structure without the single byte for the _key */
  static size_t ms_sizeof_overhead;

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

} // namespace ham

#endif /* HAM_BTREE_KEY_H__ */
