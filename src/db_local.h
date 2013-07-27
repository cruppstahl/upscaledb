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

#ifndef HAM_DB_LOCAL_H__
#define HAM_DB_LOCAL_H__

#include "btree_key.h"
#include "db.h"

namespace hamsterdb {

class BtreeIndex;
class TransactionNode;
class TransactionIndex;
class TransactionCursor;
class TransactionOperation;
class ExtKeyCache;
class LocalEnvironment;

//
// The database implementation for local file access
//
class LocalDatabase : public Database {
  public:
    // Constructor
    LocalDatabase(Environment *env, ham_u16_t name, ham_u32_t flags)
      : Database(env, name, flags), m_recno(0), m_btree_index(0),
        m_txn_index(0) , m_prefix_func(0), m_cmp_func(0), m_extkey_cache(0) {
    }

    // Returns the btree index
    BtreeIndex *get_btree_index() {
      return (m_btree_index);
    }

    // Returns the transactional index
    TransactionIndex *get_txn_index() {
      return (m_txn_index);
    }

    // Returns the LocalEnvironment instance
    LocalEnvironment *get_local_env() {
      return ((LocalEnvironment *)m_env);
    }

    // Opens an existing Database
    virtual ham_status_t open(ham_u16_t descriptor);

    // Creates a new Database
    virtual ham_status_t create(ham_u16_t descriptor, ham_u16_t keysize);

    // Erases this Database
    ham_status_t erase_me();

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_check_integrity)
    virtual ham_status_t check_integrity(Transaction *txn);

    // Returns the number of keys (ham_db_get_key_count)
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount);

    // Inserts a key/value pair (ham_db_insert)
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erase a key/value pair (ham_db_erase)
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags);

    // Lookup of a key/value pair (ham_db_find)
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Inserts a key with a cursor (ham_cursor_insert)
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erases the key of a cursor (ham_cursor_erase)
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags);

    // Positions the cursor on a key and returns the record (ham_cursor_find)
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Returns number of duplicates (ham_cursor_get_duplicate_count)
    virtual ham_status_t cursor_get_duplicate_count(Cursor *cursor,
                    ham_size_t *count, ham_u32_t flags);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Inserts a key/record pair in a txn node; if cursor is not NULL it will
    // be attached to the new txn_op structure
    // TODO this should be private
    ham_status_t insert_txn(Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags,
                TransactionCursor *cursor);

    // Erases a key/record pair from a txn; on success, cursor will be set to
    // nil
    // TODO should be private
    ham_status_t erase_txn(Transaction *txn, ham_key_t *key, ham_u32_t flags,
                TransactionCursor *cursor);

    // Sets the prefix comparison function (ham_db_set_prefix_compare_func)
    void set_prefix_compare_func(ham_prefix_compare_func_t f) {
      m_prefix_func = f;
    }

    // Returns the default comparison function
    ham_compare_func_t get_compare_func() {
      return (m_cmp_func);
    }

    // Sets the default comparison function (ham_db_set_compare_func)
    void set_compare_func(ham_compare_func_t f) {
      m_cmp_func = f;
    }

    // Returns the cache for extended keys
    ExtKeyCache *get_extkey_cache() {
      return (m_extkey_cache);
    }

    // Sets the cache for extended keys
    // TODO this should be private
    void set_extkey_cache(ExtKeyCache *c) {
      m_extkey_cache = c;
    }

    // Removes an extendex key from the cache and the blob
    ham_status_t remove_extkey(ham_u64_t blobid);

    // Returns an extended key
    // |ext_key| must have been initialized before calling this function.
    ham_status_t get_extended_key(ham_u8_t *key_data, ham_size_t key_length,
                    ham_u32_t key_flags, ham_key_t *ext_key);

    // Returns the key size of the btree
    ham_u16_t get_keysize();

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
#if __GNUC__
    __attribute__((hot))
#endif
    int compare_keys(ham_key_t *lhs, ham_key_t *rhs) {
      int cmp = HAM_PREFIX_REQUEST_FULLKEY;
      set_error(0);

      // need prefix compare? if no key is extended we can just call the
      // normal compare function
      if (!(lhs->_flags & PBtreeKey::kExtended)
              && !(rhs->_flags & PBtreeKey::kExtended)) {
        return (m_cmp_func((::ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size,
                          (ham_u8_t *)rhs->data, rhs->size));
      }

      // yes! - run prefix comparison
      if (m_prefix_func) {
        ham_size_t lhsprefixlen, rhsprefixlen;
        if (lhs->_flags & PBtreeKey::kExtended)
          lhsprefixlen = get_keysize() - sizeof(ham_u64_t);
        else
          lhsprefixlen = lhs->size;
        if (rhs->_flags & PBtreeKey::kExtended)
          rhsprefixlen = get_keysize() - sizeof(ham_u64_t);
        else
          rhsprefixlen = rhs->size;

        cmp = m_prefix_func((::ham_db_t *)this,
                    (ham_u8_t *)lhs->data, lhsprefixlen, lhs->size,
                    (ham_u8_t *)rhs->data, rhsprefixlen, rhs->size);
        if (cmp <- 1 && cmp != HAM_PREFIX_REQUEST_FULLKEY)
          return (cmp); /* unexpected error! */
      }

      if (cmp == HAM_PREFIX_REQUEST_FULLKEY) {
        // 1. load the first key, if required
        if (lhs->_flags & PBtreeKey::kExtended) {
          ham_status_t st = get_extended_key((ham_u8_t *)lhs->data,
                    lhs->size, lhs->_flags, lhs);
          if (st)
            return st;
          lhs->_flags &= ~PBtreeKey::kExtended;
        }

        // 2. load the second key, if required
        if (rhs->_flags & PBtreeKey::kExtended) {
          ham_status_t st = get_extended_key((ham_u8_t *)rhs->data,
                    rhs->size, rhs->_flags, rhs);
          if (st)
            return st;
          rhs->_flags &= ~PBtreeKey::kExtended;
        }

        // 3. run the comparison function on the full keys
        cmp = m_cmp_func((::ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size,
                        (ham_u8_t *)rhs->data, rhs->size);
      }
      return (cmp);
    }

    // Copies a key
    //
    // |dest| must have been initialized before calling this function; the
    // dest->data space will be reused when the specified size is large enough;
    // otherwise the old dest->data will be ham_mem_free()d and a new space
    // allocated.
    ham_status_t copy_key(const ham_key_t *source, ham_key_t *dest) {
      // extended key: copy the whole key
      if (source->_flags & PBtreeKey::kExtended) {
        ham_status_t st = get_extended_key((ham_u8_t *)source->data,
                    source->size, source->_flags, dest);
        if (st)
          return st;
        ham_assert(dest->data != 0);
        // dest->size is set by db->get_extended_key()
        ham_assert(dest->size == source->size);
        // the extended flag is set later, when this key is inserted
        dest->_flags = source->_flags & (~PBtreeKey::kExtended);
      }
      else if (source->size) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
          if (!dest->data || dest->size < source->size) {
            Memory::release(dest->data);
            dest->data = Memory::allocate<ham_u8_t>(source->size);
            if (!dest->data)
              return (HAM_OUT_OF_MEMORY);
          }
        }
        memcpy(dest->data, source->data, source->size);
        dest->size = source->size;
        dest->_flags = source->_flags;
      }
      else {
        // key.size is 0
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
          Memory::release(dest->data);
          dest->data = 0;
        }
        dest->size = 0;
        dest->_flags = source->_flags;
      }
      return (HAM_SUCCESS);
    }

    // Copies the ham_record_t structure from |op| into |record|
    // TODO make this private
    static ham_status_t copy_record(LocalDatabase *db, Transaction *txn,
                    TransactionOperation *op, ham_record_t *record);

    // Flushes a TransactionOperation to the btree
    ham_status_t flush_txn_operation(Transaction *txn,
                    TransactionOperation *op);

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, ham_u32_t flags);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c);

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(ham_u32_t flags);

  private:
    friend class DbFixture;
    friend class HamsterdbFixture;

    // returns the next record number
    ham_u64_t get_incremented_recno() {
      return (++m_recno);
    }

    // default comparison function for two keys, implemented with memcmp
    //
    // @return -1, 0, +1 or higher positive values are the result of a
    //    successful key comparison (0 if both keys match, -1 when LHS < RHS
    //    key, +1 when LHS > RHS key).
    // @return values less than -1 are @ref ham_status_t error codes and
    //    indicate a failed comparison execution
#if __GNUC__
    __attribute__((hot))
#endif
    static int HAM_CALLCONV default_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length);

    // compare two recno-keys
    //
    // @return -1, 0, +1 or higher positive values are the result of a
    //    successful key comparison (0 if both keys match, -1 when LHS < RHS
    //    key, +1 when LHS > RHS key).
#if __GNUC__
    __attribute__((hot))
#endif
    static int HAM_CALLCONV default_recno_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length);

    // the default prefix compare function - uses memcmp
    //
    // @return -1, 0, +1 or higher positive values are the result of a
    //    successful key comparison (0 if both keys match, -1 when LHS < RHS
    //    key, +1 when LHS > RHS key).
    //
    // @return values less than -1 are @ref ham_status_t error codes and
    //    indicate a failed comparison execution
#if __GNUC__
    __attribute__((hot))
#endif
    static int HAM_CALLCONV default_prefix_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                ham_size_t lhs_real_length,
                const ham_u8_t *rhs, ham_size_t rhs_length,
                ham_size_t rhs_real_length);


    // Checks if an insert operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_insert_conflicts(Transaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    // Checks if an erase operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_erase_conflicts(Transaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    // Increments dupe index of all cursors with a dupe index > |start|;
    // only cursor |skip| is ignored
    void increment_dupe_index(TransactionNode *node, Cursor *skip,
                    ham_u32_t start);

    // Sets all cursors attached to a TransactionNode to nil
    void nil_all_cursors_in_node(Transaction *txn, Cursor *current,
                    TransactionNode *node);

    // Sets all cursors to nil if they point to |key| in the btree index
    void nil_all_cursors_in_btree(Cursor *current, ham_key_t *key);

    // Lookup of a key/record pair in the Transaction index and in the btree,
    // if transactions are disabled/not successful; copies the
    // record into |record|. Also performs approx. matching.
    ham_status_t find_txn(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // the current record number
    ham_u64_t m_recno;

    // the btree index
    BtreeIndex *m_btree_index;

    // the transaction index
    TransactionIndex *m_txn_index;

    // the prefix-comparison function
    ham_prefix_compare_func_t m_prefix_func;

    // the comparison function
    ham_compare_func_t m_cmp_func;

    // the cache for extended keys
    ExtKeyCache *m_extkey_cache;
};

} // namespace hamsterdb

#endif /* HAM_DB_LOCAL_H__ */
