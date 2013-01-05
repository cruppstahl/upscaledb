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

/**
 * @brief internal macros and headers
 *
 */

#ifndef HAM_DB_H__
#define HAM_DB_H__

#include "internal_fwd_decl.h"

#include "statistics.h"

#include "endianswap.h"
#include "error.h"
#include "util.h"
#include "txn.h"
#include "env.h"
#include "btree.h"
#include "btree_key.h"
#include "mem.h"

/**
 * A helper structure; ham_db_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_db_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_db_t {
  int dummy;
};

namespace hamsterdb {

/**
 * a macro to cast pointers to u64 and vice versa to avoid compiler
 * warnings if the sizes of ptr and u64 are not equal
 */
#if defined(HAM_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (ham_u8_t *)(int)p
#   define PTR_TO_U64(p)  (ham_u64_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

/**
 * the maximum number of indices (if this file is an environment with
 * multiple indices)
 */
#define DB_MAX_INDICES                  16 /* 16*32 = 512 byte wasted */

/** get the (non-persisted) flags of a key */
#define ham_key_get_intflags(key)       (key)->_flags

/**
 * set the flags of a key
 *
 * Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must
 * be defined such that those can peacefully co-exist with these; that's
 * why those public flags start at the value 0x1000 (4096).
 */
#define ham_key_set_intflags(key, f)    (key)->_flags=(f)

#define PAGE_IGNORE_FREELIST          8
#define PAGE_CLEAR_WITH_ZERO         16


class BtreeIndex;

/**
 * An abstract base class for a Database; is overwritten for local and
 * remote implementations
 */
class Database
{
  public:
    Database(Environment *env, ham_u16_t name, ham_u32_t flags);

    virtual ~Database() {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn) = 0;

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount) = 0;

    /** insert a key/value pair */
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** erase a key/value pair */
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags) = 0;

    /** lookup of a key/value pair */
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** create a cursor */
    virtual Cursor *cursor_create(Transaction *txn, ham_u32_t flags) = 0;

    /** clone a cursor */
    virtual Cursor *cursor_clone(Cursor *src);

    /** insert a key with a cursor */
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** erase the key of a cursor */
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags) = 0;

    /** position the cursor on a key and return the record */
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** get number of duplicates */
    virtual ham_status_t cursor_get_duplicate_count(Cursor *cursor,
                    ham_size_t *count, ham_u32_t flags) = 0;

    /** get current record size */
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size) = 0;

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags) = 0;

    /** close a cursor */
    void cursor_close(Cursor *cursor);

    /** close the Database */
    ham_status_t close(ham_u32_t flags);

    /** get the last error code */
    ham_status_t get_error() const {
      return (m_error);
    }

    /** set the last error code */
    ham_status_t set_error(ham_status_t e) {
      return ((m_error = e));
    }

    /** get the user-provided context pointer */
    void *get_context_data() {
      return (m_context);
    }

    /** set the user-provided context pointer */
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    /** get the btree index pointer */
    // TODO move to LocalDatabase
    BtreeIndex *get_btree() {
      return (m_btree);
    }

    /** set the btree index pointer */
    // TODO move to LocalDatabase
    void set_btree(BtreeIndex *b) {
      m_btree = b;
    }

    /** get the linked list of all cursors */
    Cursor *get_cursors() {
      return (m_cursors);
    }

    /** set the linked list of all cursors */
    void set_cursors(Cursor *c) {
      m_cursors = c;
    }

    /** get the prefix comparison function */
    // TODO move to LocalDatabase
    ham_prefix_compare_func_t get_prefix_compare_func() {
      return (m_prefix_func);
    }

    /** set the prefix comparison function */
    // TODO move to LocalDatabase
    void set_prefix_compare_func(ham_prefix_compare_func_t f) {
      m_prefix_func = f;
    }

    /** get the default comparison function */
    // TODO move to LocalDatabase
    ham_compare_func_t get_compare_func() {
      return (m_cmp_func);
    }

    /** set the default comparison function */
    // TODO move to LocalDatabase
    void set_compare_func(ham_compare_func_t f) {
      m_cmp_func = f;
    }

    /**
     * get the runtime-flags - the flags are "mixed" with the flags from
     * the Environment
     */
    ham_u32_t get_rt_flags(bool raw = false) {
      if (raw)
        return (m_rt_flags);
      else
        return (m_env->get_flags() | m_rt_flags);
    }

    /** set the runtime-flags - NOT setting environment flags!  */
    void set_rt_flags(ham_u32_t flags) {
      m_rt_flags = flags;
    }

    /** get the environment pointer */
    Environment *get_env() {
      return (m_env);
    }

    /** get the cache for extended keys */
    ExtKeyCache *get_extkey_cache() {
      return (m_extkey_cache);
    }

    /** set the cache for extended keys */
    void set_extkey_cache(ExtKeyCache *c) {
      m_extkey_cache = c;
    }

    /** Get the memory buffer for the key data */
    ByteArray &get_key_arena() {
      return (m_key_arena);
    }

    /** Get the memory buffer for the record data */
    ByteArray &get_record_arena() {
      return (m_record_arena);
    }

    /** get the transaction tree */
    TransactionIndex *get_optree() {
      return (&m_optree);
    }

    /** get the database name */
    ham_u16_t get_name() {
      return (m_name);
    }

    /** set the database name */
    void set_name(ham_u16_t name) {
      m_name = name;
    }

    /** remove an extendex key from the cache and the blob */
    ham_status_t remove_extkey(ham_u64_t blobid);

    /** get the key size */
    ham_u16_t get_keysize();

    /**
     * function which compares two keys
     *
     * @return -1, 0, +1 or higher positive values are the result of a
     *         successful key comparison (0 if both keys match, -1 when
     *         LHS < RHS key, +1 when LHS > RHS key).
     */
    int compare_keys(ham_key_t *lhs, ham_key_t *rhs) {
      int cmp = HAM_PREFIX_REQUEST_FULLKEY;
      ham_compare_func_t foo = get_compare_func();
      ham_prefix_compare_func_t prefoo = get_prefix_compare_func();

      set_error(0);

      /* need prefix compare? if no key is extended we can just call the
       * normal compare function */
      if (!(lhs->_flags & BtreeKey::KEY_IS_EXTENDED)
              && !(rhs->_flags & BtreeKey::KEY_IS_EXTENDED)) {
        return (foo((::ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size,
                          (ham_u8_t *)rhs->data, rhs->size));
      }

      /* yes! - run prefix comparison */
      if (prefoo) {
        ham_size_t lhsprefixlen, rhsprefixlen;
        if (lhs->_flags & BtreeKey::KEY_IS_EXTENDED)
          lhsprefixlen = get_keysize() - sizeof(ham_u64_t);
        else
          lhsprefixlen = lhs->size;
        if (rhs->_flags & BtreeKey::KEY_IS_EXTENDED)
          rhsprefixlen = get_keysize() - sizeof(ham_u64_t);
        else
          rhsprefixlen = rhs->size;

        cmp = prefoo((::ham_db_t *)this,
                    (ham_u8_t *)lhs->data, lhsprefixlen, lhs->size,
                    (ham_u8_t *)rhs->data, rhsprefixlen, rhs->size);
        if (cmp <- 1 && cmp != HAM_PREFIX_REQUEST_FULLKEY)
          return (cmp); /* unexpected error! */
      }

      if (cmp == HAM_PREFIX_REQUEST_FULLKEY) {
        /* 1. load the first key, if needed */
        if (lhs->_flags & BtreeKey::KEY_IS_EXTENDED) {
          ham_status_t st = get_extended_key((ham_u8_t *)lhs->data,
                    lhs->size, lhs->_flags, lhs);
          if (st)
            return st;
          lhs->_flags &= ~BtreeKey::KEY_IS_EXTENDED;
        }

        /* 2. load the second key, if needed */
        if (rhs->_flags & BtreeKey::KEY_IS_EXTENDED) {
          ham_status_t st = get_extended_key((ham_u8_t *)rhs->data,
                    rhs->size, rhs->_flags, rhs);
          if (st)
            return st;
          rhs->_flags &= ~BtreeKey::KEY_IS_EXTENDED;
        }

        /* 3. run the comparison function */
        cmp=foo((::ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size,
                        (ham_u8_t *)rhs->data, rhs->size);
      }
      return (cmp);
    }

    /**
     * load an extended key
     * @a ext_key must have been initialized before calling this function.
     */
    ham_status_t get_extended_key(ham_u8_t *key_data, ham_size_t key_length,
                    ham_u32_t key_flags, ham_key_t *ext_key);

    /**
     * copy a key
     *
     * @a dest must have been initialized before calling this function; the
     * dest->data space will be reused when the specified size is large enough;
     * otherwise the old dest->data will be ham_mem_free()d and a new space
     * allocated.
     */
    ham_status_t copy_key(const ham_key_t *source, ham_key_t *dest) {
      /* extended key: copy the whole key */
      if (source->_flags & BtreeKey::KEY_IS_EXTENDED) {
        ham_status_t st = get_extended_key((ham_u8_t *)source->data,
                    source->size, source->_flags, dest);
        if (st)
          return st;
        ham_assert(dest->data != 0);
        /* dest->size is set by db->get_extended_key() */
        ham_assert(dest->size == source->size);
        /* the extended flag is set later, when this key is inserted */
        dest->_flags = source->_flags & (~BtreeKey::KEY_IS_EXTENDED);
      }
      else if (source->size) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
          if (!dest->data || dest->size < source->size) {
            if (dest->data)
              get_env()->get_allocator()->free(dest->data);
            dest->data = (ham_u8_t *)
                      get_env()->get_allocator()->alloc(source->size);
            if (!dest->data)
              return (HAM_OUT_OF_MEMORY);
          }
        }
        memcpy(dest->data, source->data, source->size);
        dest->size = source->size;
        dest->_flags = source->_flags;
      }
      else {
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
          if (dest->data)
            get_env()->get_allocator()->free(dest->data);
          dest->data = 0;
        }
        dest->size = 0;
        dest->_flags = source->_flags;
      }
      return (HAM_SUCCESS);
    }

    /**
     * default comparison function for two keys, implemented with memcmp
     *
     * @return -1, 0, +1 or higher positive values are the result of a
     *    successful key comparison (0 if both keys match, -1 when LHS < RHS
     *    key, +1 when LHS > RHS key).
     *
     * @return values less than -1 are @ref ham_status_t error codes and
     *    indicate a failed comparison execution
     */
    static int HAM_CALLCONV default_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length);

    /**
     * compare two recno-keys
     *
     * @return -1, 0, +1 or higher positive values are the result of a
     *    successful key comparison (0 if both keys match, -1 when LHS < RHS
     *    key, +1 when LHS > RHS key).
     *
     * @return values less than -1 are @ref ham_status_t error codes and
     *    indicate a failed comparison execution
     */
    static int HAM_CALLCONV default_recno_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length);

    /**
     * the default prefix compare function - uses memcmp
     *
     * @return -1, 0, +1 or higher positive values are the result of a
     *    successful key comparison (0 if both keys match, -1 when LHS < RHS
     *    key, +1 when LHS > RHS key).
     *
     * @return values less than -1 are @ref ham_status_t error codes and
     *    indicate a failed comparison execution
     */
    static int HAM_CALLCONV default_prefix_compare(ham_db_t *db,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                ham_size_t lhs_real_length,
                const ham_u8_t *rhs, ham_size_t rhs_length,
                ham_size_t rhs_real_length);

    /**
     * Fetches a page. Checks Changeset and Cache for the page; if it's not
     * found then the page is read from disk 
     * TODO move to LocalDatabase
     */
    ham_status_t fetch_page(Page **page, ham_u64_t address,
                bool only_from_cache = false);

    /**
     * Allocate a new page.
     *
     * @param flags optional allocation request flags. @a flags can be a mix
     *        of the following bits:
     *        - PAGE_IGNORE_FREELIST        ignores all freelist-operations
     *        - PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
     */
    ham_status_t alloc_page(Page **page, ham_u32_t type, ham_u32_t flags);

  protected:
    /** clone a cursor; this is the actual implementation */
    virtual Cursor *cursor_clone_impl(Cursor *src) = 0;

    /** close a cursor; this is the actual implementation */
    virtual void cursor_close_impl(Cursor *c) = 0;

    /** close a database; this is the actual implementation */
    virtual ham_status_t close_impl(ham_u32_t flags) = 0;

    /** the environment of this database - can be NULL */
    Environment *m_env;

    /** the Database name */
    ham_u16_t m_name;

    /** the last error code */
    ham_status_t m_error;

    /** the user-provided context data */
    void *m_context;

    /** the btree index */
    /* TODO move to LocalDatabase */
    BtreeIndex *m_btree;

    /** linked list of all cursors */
    Cursor *m_cursors;

    /** the prefix-comparison function */
    /* TODO move to LocalDatabase */
    ham_prefix_compare_func_t m_prefix_func;

    /** the comparison function */
    /* TODO move to LocalDatabase */
    ham_compare_func_t m_cmp_func;

    /** the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t m_rt_flags;

    /** the cache for extended keys */
    /* TODO move to LocalDatabase or Btree */
    ExtKeyCache *m_extkey_cache;

    /** the transaction tree */
    /* TODO move to LocalDatabase */
    TransactionIndex m_optree;

    /** this is where key->data points to when returning a
     * key to the user; used if Transactions are disabled */
    ByteArray m_key_arena;

    /** this is where record->data points to when returning a
     * record to the user; used if Transactions are disabled */
    ByteArray m_record_arena;
};

/**
 * The database implementation for local file access
 */
class LocalDatabase : public Database
{
  public:
    LocalDatabase(Environment *env, ham_u16_t name, ham_u32_t flags)
      : Database(env, name, flags), m_recno(0) {
    }

    /** opens an existing Database */
    virtual ham_status_t open(ham_u16_t descriptor);

    /** creates a new Database */
    virtual ham_status_t create(ham_u16_t descriptor, ham_u16_t keysize);

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount);

    /** insert a key/value pair */
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** erase a key/value pair */
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags);

    /** lookup of a key/value pair */
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** create a cursor */
    virtual Cursor *cursor_create(Transaction *txn, ham_u32_t flags);

    /** insert a key with a cursor */
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** erase the key of a cursor */
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags);

    /** position the cursor on a key and return the record */
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** get number of duplicates */
    virtual ham_status_t cursor_get_duplicate_count(Cursor *cursor,
                    ham_size_t *count, ham_u32_t flags);

    /** get current record size */
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size);

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /** returns the next record number */
    ham_u64_t get_incremented_recno() {
      return (++m_recno);
    }

    /*
     * insert a key/record pair in a txn node; if cursor is not NULL it will
     * be attached to the new txn_op structure
     * TODO this should be private
     */
    ham_status_t insert_txn(Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags,
                TransactionCursor *cursor);

    /*
     * erase a key/record pair from a txn; on success, cursor will be set to nil
     */
    ham_status_t erase_txn(Transaction *txn, ham_key_t *key, ham_u32_t flags,
                TransactionCursor *cursor);

  protected:
    /** clone a cursor; this is the actual implementation */
    virtual Cursor *cursor_clone_impl(Cursor *src);

    /** close a cursor; this is the actual implementation */
    virtual void cursor_close_impl(Cursor *c);

    /** closes a database; this is the actual implementation */
    virtual ham_status_t close_impl(ham_u32_t flags);

  private:
    /*
     * checks if an insert operation conflicts with another txn
     */
    ham_status_t check_insert_conflicts(Transaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    /*
     * checks if an erase operation conflicts with another txn
     */
    ham_status_t check_erase_conflicts(Transaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    /** the current record number */
    ham_u64_t m_recno;
};

/**
 * The database implementation for remote file access
 */
#ifdef HAM_ENABLE_REMOTE

class RemoteDatabase : public Database
{
  public:
    RemoteDatabase(Environment *env, ham_u16_t name, ham_u32_t flags)
      : Database(env, name, flags), m_remote_handle(0) {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount);

    /** insert a key/value pair */
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** erase a key/value pair */
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key, ham_u32_t flags);

    /** lookup of a key/value pair */
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** create a cursor */
    virtual Cursor *cursor_create(Transaction *txn, ham_u32_t flags);

    /** insert a key with a cursor */
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** erase the key of a cursor */
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags);

    /** position the cursor on a key and return the record */
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** get number of duplicates */
    virtual ham_status_t cursor_get_duplicate_count(Cursor *cursor,
                    ham_size_t *count, ham_u32_t flags);

    /** get current record size */
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size);

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** get the remote database handle */
    ham_u64_t get_remote_handle() {
        return (m_remote_handle);
    }

    /** set the remote database handle */
    void set_remote_handle(ham_u64_t handle) {
        m_remote_handle = handle;
    }

  protected:
    /** clone a cursor; this is the actual implementation */
    virtual Cursor *cursor_clone_impl(Cursor *src);

    /** close a cursor; this is the actual implementation */
    virtual void cursor_close_impl(Cursor *c);

    /** close a database; this is the actual implementation */
    virtual ham_status_t close_impl(ham_u32_t flags);

  private:
    /** the remote database handle */
    ham_u64_t m_remote_handle;
};
#endif // HAM_ENABLE_REMOTE

} // namespace hamsterdb

#endif /* HAM_DB_H__ */
