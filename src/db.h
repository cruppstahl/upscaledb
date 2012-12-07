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
#include "btree_key.h"
#include "btree.h"
#include "mem.h"

/**
 * A helper structure; ham_db_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_db_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_db_t {
    int dummy;
};

namespace ham {

/**
 * a macro to cast pointers to u64 and vice versa to avoid compiler
 * warnings if the sizes of ptr and u64 are not equal
 */
#if defined(HAM_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (ham_u8_t *)(int)p
#   define PTR_TO_U64(p)  (ham_offset_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

/**
 * the maximum number of indices (if this file is an environment with
 * multiple indices)
 */
#define DB_MAX_INDICES                  16 /* 16*32 = 512 byte wasted */

/* the size of an index data */
#define DB_INDEX_SIZE                   sizeof(db_indexdata_t) /* 32 */

/** get the key size */
#define db_get_keysize(db)              ((db)->get_backend()->get_keysize())

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


#include "packstart.h"

/**
 * the persistent database index header
 */
HAM_PACK_0 struct HAM_PACK_1 db_indexdata_t
{
    /** name of the DB: 1..HAM_DEFAULT_DATABASE_NAME-1 */
    ham_u16_t _dbname;

    /** maximum keys in an internal page */
    ham_u16_t _maxkeys;

    /** key size in this page */
    ham_u16_t _keysize;

    /* reserved */
    ham_u16_t _reserved1;

    /** address of this page */
    ham_offset_t _self;

    /** flags for this database */
    ham_u32_t _flags;

    /* reserved */
    ham_u64_t _reserved2;

    /* reserved */
    ham_u32_t _reserved3;

} HAM_PACK_2;

#include "packstop.h"


#define index_get_dbname(p)               ham_db2h16((p)->_dbname)
#define index_set_dbname(p, n)            (p)->_dbname=ham_h2db16(n)

#define index_get_max_keys(p)             ham_db2h16((p)->_maxkeys)
#define index_set_max_keys(p, n)          (p)->_maxkeys=ham_h2db16(n)

#define index_get_keysize(p)              ham_db2h16((p)->_keysize)
#define index_set_keysize(p, n)           (p)->_keysize=ham_h2db16(n)

#define index_get_self(p)                 ham_db2h_offset((p)->_self)
#define index_set_self(p, n)              (p)->_self=ham_h2db_offset(n)

#define index_get_flags(p)                ham_db2h32((p)->_flags)
#define index_set_flags(p, n)             (p)->_flags=ham_h2db32(n)

#define index_clear_reserved(p)           { (p)->_reserved1=0;            \
                                            (p)->_reserved2=0; }
/**
 * An abstract base class for a Database; is overwritten for local and
 * remote implementations
 */
class Database
{
  public:
    Database(Environment *env, ham_u16_t flags);

    virtual ~Database() {
    }

    /** opens a Database */
    virtual ham_status_t open() {
        return (0);
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn) = 0;

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_offset_t *keycount) = 0;

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
                    ham_offset_t *size) = 0;

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags) = 0;

    /** close a cursor */
    virtual void cursor_close(Cursor *cursor);

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags) = 0;

    /** get the last error code */
    ham_status_t get_error() {
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

    /** get the backend pointer */
    Backend *get_backend() {
        return (m_backend);
    }

    /** set the backend pointer */
    void set_backend(Backend *b) {
        m_backend = b;
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
    ham_prefix_compare_func_t get_prefix_compare_func() {
        return (m_prefix_func);
    }

    /** set the prefix comparison function */
    void set_prefix_compare_func(ham_prefix_compare_func_t f) {
        m_prefix_func = f;
    }

    /** get the default comparison function */
    ham_compare_func_t get_compare_func() {
        return (m_cmp_func);
    }

    /** set the default comparison function */
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

    /** get the next database in a linked list of databases */
    Database *get_next() {
        return (m_next);
    }

    /** set the pointer to the next database */
    void set_next(Database *next) {
        m_next = next;
    }

    /** get the cache for extended keys */
    ExtKeyCache *get_extkey_cache() {
        return (m_extkey_cache);
    }

    /** set the cache for extended keys */
    void set_extkey_cache(ExtKeyCache *c) {
        m_extkey_cache = c;
    }

    /** get the index of this database in the indexdata array */
    ham_u16_t get_indexdata_offset() {
        return (m_indexdata_offset);
    }

    /** set the index of this database in the indexdata array */
    void set_indexdata_offset(ham_u16_t offset) {
        m_indexdata_offset = offset;
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
    TransactionTree *get_optree() {
        return (&m_optree);
    }

    /** get the database name */
    ham_u16_t get_name();

    /** remove an extendex key from the cache and the blob */
    ham_status_t remove_extkey(ham_offset_t blobid);

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
                lhsprefixlen = db_get_keysize(this)-sizeof(ham_offset_t);
            else
                lhsprefixlen = lhs->size;
            if (rhs->_flags & BtreeKey::KEY_IS_EXTENDED)
                rhsprefixlen = db_get_keysize(this)-sizeof(ham_offset_t);
            else
                rhsprefixlen = rhs->size;

            cmp=prefoo((::ham_db_t *)this,
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

  protected:
    /** clone a cursor; this is the actual implementation */
    virtual Cursor *cursor_clone_impl(Cursor *src) = 0;

    /** close a cursor; this is the actual implementation */
    virtual void cursor_close_impl(Cursor *c) = 0;

    /** the environment of this database - can be NULL */
    Environment *m_env;

    /** the last error code */
    ham_status_t m_error;

    /** the user-provided context data */
    void *m_context;

    /** the backend pointer - btree, hashtable etc */
    Backend *m_backend;

    /** linked list of all cursors */
    Cursor *m_cursors;

    /** the prefix-comparison function */
    ham_prefix_compare_func_t m_prefix_func;

    /** the comparison function */
    ham_compare_func_t m_cmp_func;

    /** the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t m_rt_flags;

    /** the next database in a linked list of databases */
    Database *m_next;

    /** the cache for extended keys */
    ExtKeyCache *m_extkey_cache;

    /** the offset of this database in the environment _indexdata */
    ham_u16_t m_indexdata_offset;

    /** the transaction tree */
    TransactionTree m_optree;

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
    LocalDatabase(Environment *env, ham_u16_t flags)
        : Database(env, flags), m_recno(0) {
    }

    /** opens a Database */
    virtual ham_status_t open();

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_offset_t *keycount);

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
                    ham_offset_t *size);

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags);

    /** returns the next record number */
    ham_u64_t get_incremented_recno() {
      return (++m_recno);
    }

  protected:
    /** clone a cursor; this is the actual implementation */
    virtual Cursor *cursor_clone_impl(Cursor *src);

    /** close a cursor; this is the actual implementation */
    virtual void cursor_close_impl(Cursor *c);

  private:
    /** the current record number */
    ham_u64_t m_recno;
};

/**
 * The database implementation for remote file access
 */
#if HAM_ENABLE_REMOTE

class RemoteDatabase : public Database
{
  public:
    RemoteDatabase(Environment *env, ham_u16_t flags)
      : Database(env, flags), m_remote_handle(0) {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(Transaction *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_offset_t *keycount);

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
                    ham_offset_t *size);

    /** overwrite a cursor */
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    /** move a cursor, return key and/or record */
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags);

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

  private:
    /** the remote database handle */
    ham_u64_t m_remote_handle;
};
#endif // HAM_ENABLE_REMOTE

/**
 * compare two keys
 *
 * this function will call the prefix-compare function and the
 * default compare function whenever it's necessary.
 *
 * This is the default key compare function, which uses memcmp to compare two keys.
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate
 *         a failed comparison execution: these are listed in
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes
 */
extern int HAM_CALLCONV
db_default_compare(ham_db_t *db,
                    const ham_u8_t *lhs, ham_size_t lhs_length,
                    const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * compare two recno-keys
 *
 * this function compares two record numbers
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate
 *         a failed comparison execution: these are listed in
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes
 */
extern int HAM_CALLCONV
db_default_recno_compare(ham_db_t *db,
                    const ham_u8_t *lhs, ham_size_t lhs_length,
                    const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * the default prefix compare function - uses memcmp
 *
 * compares the prefix of two keys
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate
 *         a failed comparison execution: these are listed in
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes
 */
extern int HAM_CALLCONV
db_default_prefix_compare(ham_db_t *db,
                    const ham_u8_t *lhs, ham_size_t lhs_length,
                    ham_size_t lhs_real_length,
                    const ham_u8_t *rhs, ham_size_t rhs_length,
                    ham_size_t rhs_real_length);

/**
 * compare two records for a duplicate key
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate
 *         a failed comparison execution: these are listed in
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes
 */
extern int HAM_CALLCONV
db_default_dupe_compare(ham_db_t *db,
                    const ham_u8_t *lhs, ham_size_t lhs_length,
                    const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * fetch a page.
 *
 * @param page_ref call-by-reference variable which will be set to
 *      point to the retrieved @ref Page instance.
 * @param db the database handle - if it's not available then please
 *      use env_fetch_page()
 * @param address the storage address (a.k.a. 'RID') where the page is
 *      located in the device store (file, memory, ...).
 * @param flags An optional, bit-wise combined set of the
 *      @ref db_fetch_page_flags flag collection.
 *
 * @return the retrieved page in @a *page_ref and HAM_SUCCESS as a
 *      function return value.
 * @return a NULL value in @a *page_ref and HAM_SUCCESS when the page
 *      could not be retrieved because the set conditions were not be
 *      met (see @ref DB_ONLY_FROM_CACHE)
 * @return one of the @ref ham_status_codes error codes as an error occurred.
 */
extern ham_status_t
db_fetch_page(Page **page_ref, Database *db,
                    ham_offset_t address, ham_u32_t flags);

/*
 * this is an internal function. do not use it unless you know what you're
 * doing.
 */
extern ham_status_t
db_fetch_page_impl(Page **page_ref, Environment *env, Database *db,
                    ham_offset_t address, ham_u32_t flags);

/**
 * @defgroup db_fetch_page_flags @ref db_fetch_page Flags
 * @{
 *
 * These flags can be bitwise-OR mixed with the @ref HAM_HINTS_MASK flags,
 * i.e. the hint bits as listed in @ref ham_hinting_flags
 *
 * @sa ham_hinting_flags
 */

/**
 * Force @ref db_fetch_page to only return a valid @ref Page instance
 * reference when it is still stored in the cache, otherwise a NULL pointer
 * will be returned instead (and no error code)!
 */
#define DB_ONLY_FROM_CACHE                0x0002

/**
 * @}
 */


/**
 * Flush all pages, and clear the cache.
 *
 * @param flags Set to DB_FLUSH_NODELETE if you do NOT want the cache to
 * be cleared
 * @param cache
 */
extern ham_status_t
db_flush_all(Cache *cache, ham_u32_t flags);

#define DB_FLUSH_NODELETE       1

/**
 * Allocate a new page.
 *
 * @param page_ref call-by-reference result: will store the @ref Page
 *        instance reference.
 * @param db the database; if the database handle is not available, you
 *        can use env_alloc_page
 * @param type the page type of the new page. See @ref page_type_codes for
 *        a list of supported types.
 * @param flags optional allocation request flags. @a flags can be a mix
 *        of the following bits:
 *        - PAGE_IGNORE_FREELIST        ignores all freelist-operations
 *        - PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
 *
 * @note The page will be aligned at the current page size. Any wasted
 * space (due to the alignment) is added to the freelist.
 */
extern ham_status_t
db_alloc_page(Page **page_ref, Database *db,
                ham_u32_t type, ham_u32_t flags);

/*
 * this is an internal function. do not use it unless you know what you're
 * doing.
 */
extern ham_status_t
db_alloc_page_impl(Page **page_ref, Environment *env, Database *db,
                ham_u32_t type, ham_u32_t flags);

#define PAGE_IGNORE_FREELIST          8
#define PAGE_CLEAR_WITH_ZERO         16

/**
* @defgroup ham_database_flags
* @{
*/

/** An internal database flag - use mmap instead of read(2).  */
#define DB_USE_MMAP                  0x00000100

/** An internal database flag - env handle is remote */
#define DB_IS_REMOTE                 0x00200000

/**
 * @}
 */

/*
 * insert a key/record pair in a txn node; if cursor is not NULL it will
 * be attached to the new txn_op structure
 */
struct txn_cursor_t;
extern ham_status_t
db_insert_txn(Database *db, Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags,
                struct txn_cursor_t *cursor);

/*
 * erase a key/record pair from a txn; on success, cursor will be set to nil
 */
extern ham_status_t
db_erase_txn(Database *db, Transaction *txn, ham_key_t *key, ham_u32_t flags,
                struct txn_cursor_t *cursor);

} // namespace ham

#endif /* HAM_DB_H__ */
