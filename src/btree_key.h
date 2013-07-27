/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_BTREE_KEY_H__
#define HAM_BTREE_KEY_H__

#include <string.h>

#include <ham/hamsterdb_int.h>

#include "endianswap.h"

namespace hamsterdb {

class LocalDatabase;
class Transaction;

#include "packstart.h"

/*
 * the internal representation of a serialized key
 */
HAM_PACK_0 struct HAM_PACK_1 PBtreeKey
{
  public:
    // persisted PBtreeKey flags; also used in combination with ham_key_t._flags
    //
    // NOTE: persisted flags must fit within a ham_u8_t (1 byte) --> mask:
    // 0x000000FF
    enum {
      // record size < 8; length is encoded at byte[7] of key->ptr
      kBlobSizeTiny         = 0x01,

      // record size == 8; record is stored in key->ptr
      kBlobSizeSmall        = 0x02,

      // record size == 0; key->ptr == 0
      kBlobSizeEmpty        = 0x04,

      // key is extended with overflow area
      kExtended             = 0x08,

      // key has duplicates
      kDuplicates           = 0x10,

      // memory for a key was allocated in hamsterdb, not by caller
      kAllocated           = 0x20
    };

    // flags used with the ham_key_t::_flags (note the underscore - this
    // field is for INTERNAL USE!)
    //
    // Note: these flags should NOT overlap with the persisted flags for
    // PBtreeKey
    //
    // As these flags NEVER will be persisted, they should be located outside
    // the range of a ham_u16_t, i.e. outside the mask 0x0000FFFF.
    enum {
      // Actual key is lower than the requested key
      kLower               = 0x00010000,

      // Actual key is greater than the requested key
      kGreater             = 0x00020000,

      // Actual key is an "approximate match"
      kApproximate         = (kLower | kGreater)
    };

    // Returns the pointer of an btree-entry
    //
    // !!!
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion!
    ham_u64_t get_ptr() {
      return (((m_flags8 & kBlobSizeTiny) || (m_flags8 & kBlobSizeSmall))
              ? m_ptr
              : ham_db2h_offset(m_ptr));
    }

    // Same as above, but without endian conversion
    ham_u64_t *get_rawptr() {
      return (&m_ptr);
    }

    // Same as above, but without endian conversion
    const ham_u64_t *get_rawptr() const {
      return (&m_ptr);
    }

    // Sets the pointer of an btree-entry
    //
    // !!! same problems as with get_ptr():
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion
    void set_ptr(ham_u64_t ptr) {
      m_ptr = (((m_flags8 & kBlobSizeTiny) || (m_flags8 & kBlobSizeSmall))
              ? ptr
              : ham_h2db_offset(ptr));
    }

    // Returns the size of an btree-entry
    ham_u16_t get_size() const {
      return (ham_db2h16(m_keysize));
    }

    // Sets the size of an btree-entry
    void set_size(ham_u16_t size) {
      m_keysize = ham_h2db16(size);
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_flags() const {
      return (m_flags8);
    }

    // Sets the flags of a key
    //
    // Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must be
    // defined such that those can peacefully co-exist with these; that's why
    // those public flags start at the value 0x1000 (4096).
    void set_flags(ham_u8_t flags) {
      m_flags8 = flags;
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key() {
      return (m_key);
    }

    // Returns a pointer to the key data
    const ham_u8_t *get_key() const {
      return (m_key);
    }

    // Overwrites the key data
    void set_key(const void *ptr, ham_size_t len) {
      memcpy(m_key, ptr, len);
    }
  
    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_rid(LocalDatabase *db);

    // Sets the record address of an extended key overflow area
    void set_extended_rid(LocalDatabase *db, ham_u64_t rid);

    // Inserts and sets a record
    //
    // flags can be
    // - HAM_OVERWRITE
    // - HAM_DUPLICATE_INSERT_BEFORE
    // - HAM_DUPLICATE_INSERT_AFTER
    // - HAM_DUPLICATE_INSERT_FIRST
    // - HAM_DUPLICATE_INSERT_LAST
    // - HAM_DUPLICATE
    //
    // a previously existing blob will be deleted if necessary
    ham_status_t set_record(LocalDatabase *db, Transaction *txn,
                    ham_record_t *record, ham_size_t position, ham_u32_t flags,
                    ham_size_t *new_position);

    // Deletes a record from this key
    ham_status_t erase_record(LocalDatabase *db, Transaction *txn,
                    ham_size_t dupe_id, bool erase_all_duplicates);

    // The size of this structure without the single byte for the m_key
    static ham_size_t kSizeofOverhead;

  private:
    friend struct MiscFixture;

    // the pointer/record ID of this entry
    ham_u64_t m_ptr;

    // the size of this entry
    ham_u16_t m_keysize;

    // key flags (see above)
    ham_u8_t m_flags8;

    // the key data
    ham_u8_t m_key[1];

} HAM_PACK_2;

#include "packstop.h"

} // namespace hamsterdb

#endif /* HAM_BTREE_KEY_H__ */
