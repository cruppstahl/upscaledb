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

#include <ham/hamsterdb_stats.h>

#include "endianswap.h"
#include "error.h"
#include "util.h"
#include "txn.h"
#include "env.h"
#include "btree_key.h"
#include "btree.h"
#include "mem.h"

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

/** a magic and version indicator for the remote protocol */
#define HAM_TRANSFER_MAGIC_V1   (('h'<<24)|('a'<<16)|('m'<<8)|'1')

/**
 * the maximum number of indices (if this file is an environment with 
 * multiple indices)
 */
#define DB_MAX_INDICES                  16 /* 16*32 = 512 byte wasted */

/* the size of an index data */
#define DB_INDEX_SIZE                   sizeof(db_indexdata_t) /* 32 */

/** get the key size */
#define db_get_keysize(db)              be_get_keysize((db)->get_backend())

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

    /** last used record number value */
    ham_offset_t _recno;

    /* reserved */
    ham_u32_t _reserved2;

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

#define index_get_recno(p)                ham_db2h_offset((p)->_recno)
#define index_set_recno(p, n)             (p)->_recno=ham_h2db_offset(n)

#define index_clear_reserved(p)           { (p)->_reserved1=0;            \
                                            (p)->_reserved2=0; }

/** 
 * This helper class provides the actual implementation of the 
 * database - either local file access or through remote http 
 */
class DatabaseImplementation
{
  public:
    DatabaseImplementation(Database *db) 
      : m_db(db) {
    }

    virtual ~DatabaseImplementation() {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    /** check Database integrity */
    virtual ham_status_t check_integrity(ham_txn_t *txn) = 0;

    /** get number of keys */
    virtual ham_status_t get_key_count(ham_txn_t *txn, ham_u32_t flags, 
                    ham_offset_t *keycount) = 0;

    /** insert a key/value pair */
    virtual ham_status_t insert(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** erase a key/value pair */
    virtual ham_status_t erase(ham_txn_t *txn, ham_key_t *key, 
                    ham_u32_t flags) = 0;

    /** lookup of a key/value pair */
    virtual ham_status_t find(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags) = 0;

    /** create a cursor */
    virtual Cursor *cursor_create(ham_txn_t *txn, ham_u32_t flags) = 0;

    /** clone a cursor */
    virtual Cursor *cursor_clone(Cursor *src) = 0;

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
    virtual void cursor_close(Cursor *cursor) = 0;

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags) = 0;

  protected:
    Database *m_db;
};

/** 
 * The database implementation for local file access
 */
class DatabaseImplementationLocal : public DatabaseImplementation
{
  public:
    DatabaseImplementationLocal(Database *db) 
      : DatabaseImplementation(db) {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(ham_txn_t *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(ham_txn_t *txn, ham_u32_t flags, 
                    ham_offset_t *keycount);

    /** insert a key/value pair */
    virtual ham_status_t insert(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags);

    /** erase a key/value pair */
    virtual ham_status_t erase(ham_txn_t *txn, ham_key_t *key, ham_u32_t flags);

    /** lookup of a key/value pair */
    virtual ham_status_t find(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags);

    /** create a cursor */
    virtual Cursor *cursor_create(ham_txn_t *txn, ham_u32_t flags);

    /** clone a cursor */
    virtual Cursor *cursor_clone(Cursor *src);

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

    /** close a cursor */
    virtual void cursor_close(Cursor *cursor);

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags);

};

/** 
 * The database implementation for remote file access
 */
#if HAM_ENABLE_REMOTE

class DatabaseImplementationRemote : public DatabaseImplementation
{
  public:
    DatabaseImplementationRemote(Database *db) 
      : DatabaseImplementation(db) {
    }

    /** get Database parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** check Database integrity */
    virtual ham_status_t check_integrity(ham_txn_t *txn);

    /** get number of keys */
    virtual ham_status_t get_key_count(ham_txn_t *txn, ham_u32_t flags, 
                    ham_offset_t *keycount);

    /** insert a key/value pair */
    virtual ham_status_t insert(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags);

    /** erase a key/value pair */
    virtual ham_status_t erase(ham_txn_t *txn, ham_key_t *key, ham_u32_t flags);

    /** lookup of a key/value pair */
    virtual ham_status_t find(ham_txn_t *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags);

    /** create a cursor */
    virtual Cursor *cursor_create(ham_txn_t *txn, ham_u32_t flags);

    /** clone a cursor */
    virtual Cursor *cursor_clone(Cursor *src);

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

    /** close a cursor */
    virtual void cursor_close(Cursor *cursor);

    /** close the Database */
    virtual ham_status_t close(ham_u32_t flags);
};
#endif // HAM_ENABLE_REMOTE


/**
 * A helper structure; ham_db_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_db_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_db_t {
    int dummy;
};

/**
 * The Database object
 */
class Database
{
    typedef std::pair<void *, ham_size_t> ByteArray;

  public:
    /** constructor */
    Database();

    /** destructor */
    ~Database();

    /** initialize the database for local use */
    ham_status_t initialize_local(void) {
        if (m_impl)
            delete m_impl;
        m_impl=new DatabaseImplementationLocal(this);
        return (0);
    }

    /** initialize the database for remote use (http) */
    ham_status_t initialize_remote(void) {
#if HAM_ENABLE_REMOTE
        if (m_impl)
            delete m_impl;
        m_impl=new DatabaseImplementationRemote(this);
        return (0);
#else
        return (HAM_NOT_IMPLEMENTED);
#endif
    }

    /** syntactic sugar to access the implementation object */
    DatabaseImplementation *operator()(void) {
        return (m_impl);
    }

    /** get the last error code */
    ham_status_t get_error(void) {
        return (m_error);
    }

    /** set the last error code */
    ham_status_t set_error(ham_status_t e) {
        return ((m_error=e));
    }

    /** get the user-provided context pointer */
    void *get_context_data(void) {
        return (m_context);
    }

    /** set the user-provided context pointer */
    void set_context_data(void *ctxt) {
       m_context=ctxt;
    }

    /** get the backend pointer */
    ham_backend_t *get_backend(void) {
        return (m_backend);
    }

    /** set the backend pointer */
    void set_backend(ham_backend_t *b) {
        m_backend=b;
    }

    /** get the linked list of all cursors */
    Cursor *get_cursors(void) {
        return (m_cursors);
    }

    /** set the linked list of all cursors */
    void set_cursors(Cursor *c) {
        m_cursors=c;
    }

    /** get the prefix comparison function */
    ham_prefix_compare_func_t get_prefix_compare_func() {
        return (m_prefix_func);
    }

    /** set the prefix comparison function */
    void set_prefix_compare_func(ham_prefix_compare_func_t f) {
        m_prefix_func=f;
    }

    /** get the default comparison function */
    ham_compare_func_t get_compare_func() {
        return (m_cmp_func);
    }

    /** set the default comparison function */
    void set_compare_func(ham_compare_func_t f) {
        m_cmp_func=f;
    }

    /** get the duplicate record comparison function */
    ham_compare_func_t get_duplicate_compare_func() {
        return (m_duperec_func);
    }

    /** set the duplicate record comparison function */
    void set_duplicate_compare_func(ham_compare_func_t f) {
        m_duperec_func=f;
    }

    /**
     * get the runtime-flags - the flags are "mixed" with the flags from 
     * the Environment
     */
    ham_u32_t get_rt_flags(bool raw = false) {
        if (raw)
            return (m_rt_flags);
        else
            return (m_env->get_flags()|m_rt_flags);
    }

    /** set the runtime-flags - NOT setting environment flags!  */
    void set_rt_flags(ham_u32_t flags) {
        m_rt_flags=flags;
    }

    /** get the environment pointer */
    Environment *get_env(void) {
        return (m_env);
    }

    /** set the environment pointer */
    void set_env(Environment *env) {
        m_env=env;
    }

    /** get the next database in a linked list of databases */
    Database *get_next(void) {
        return (m_next);
    }

    /** set the pointer to the next database */
    void set_next(Database *next) {
        m_next=next;
    }

    /** get the cache for extended keys */
    ExtKeyCache *get_extkey_cache(void) {
        return (m_extkey_cache);
    }

    /** set the cache for extended keys */
    void set_extkey_cache(ExtKeyCache *c) {
        m_extkey_cache=c;
    }
 
    /** get the index of this database in the indexdata array */
    ham_u16_t get_indexdata_offset(void) {
        return (m_indexdata_offset);
    }

    /** set the index of this database in the indexdata array */
    void set_indexdata_offset(ham_u16_t offset) {
        m_indexdata_offset=offset;
    }

    /** get the linked list of all record-level filters */
    ham_record_filter_t *get_record_filter(void) {
        return (m_record_filters);
    }

    /** set the linked list of all record-level filters */
    void set_record_filter(ham_record_filter_t *f) {
        m_record_filters=f;
    }

    /** get the expected data access mode for this database */
    ham_u16_t get_data_access_mode(void) {
        return (m_data_access_mode);
    }

    /** set the expected data access mode for this database */
    void set_data_access_mode(ham_u16_t dam) {
        m_data_access_mode=dam;
    }

    /** check whether this database has been opened/created */
    bool is_active(void) {
        return (m_is_active);
    }

    /**
     * set the 'active' flag of the database: a non-zero value 
     * for @a s sets the @a db to 'active', zero(0) sets the @a db 
     * to 'inactive' (closed)
     */
    void set_active(bool b) {
        m_is_active=b;
    }

    /** get a reference to the per-database statistics */
    ham_runtime_statistics_dbdata_t *get_perf_data() {
        return (&m_perf_data);
    }

    /** get the size of the last allocated data blob */
    ham_size_t get_record_allocsize(void) {
        if (!m_record_alloc.get())
            m_record_alloc.reset(new ByteArray(0, 0));
        return (m_record_alloc.get()->second);
    }

    /** set the size of the last allocated data blob */
    void set_record_allocsize(ham_size_t size) {
        if (!m_record_alloc.get())
            m_record_alloc.reset(new ByteArray(0, 0));
        m_record_alloc.get()->second=size;
    }

    /** get the pointer to the last allocated data blob */
    void *get_record_allocdata(void) {
        if (!m_record_alloc.get())
            m_record_alloc.reset(new ByteArray(0, 0));
        return (m_record_alloc.get()->first);
    }

    /** set the pointer to the last allocated data blob */
    void set_record_allocdata(void *p) {
        if (!m_record_alloc.get())
            m_record_alloc.reset(new ByteArray(0, 0));
        m_record_alloc.get()->first=p;
    }

    /** get the size of the last allocated key blob */
    ham_size_t get_key_allocsize(void) {
        if (!m_key_alloc.get())
            m_key_alloc.reset(new ByteArray(0, 0));
        return (m_key_alloc.get()->second);
    }

    /** set the size of the last allocated key blob */
    void set_key_allocsize(ham_size_t size) {
        if (!m_key_alloc.get())
            m_key_alloc.reset(new ByteArray(0, 0));
        m_key_alloc.get()->second=size;
    }

    /** get the pointer to the last allocated key blob */
    void *get_key_allocdata(void) {
        if (!m_key_alloc.get())
            m_key_alloc.reset(new ByteArray(0, 0));
        return (m_key_alloc.get()->first);
    }

    /** set the pointer to the last allocated key blob */
    void set_key_allocdata(void *p) {
        if (!m_key_alloc.get())
            m_key_alloc.reset(new ByteArray(0, 0));
        m_key_alloc.get()->first=p;
    }

    /** closes a cursor */
    void close_cursor(Cursor *c);

    /** clones a cursor into *dest */
    void clone_cursor(Cursor *src, Cursor **dest);

    /**
     * Resize the record data buffer. This buffer is an internal storage for 
     * record buffers. When a ham_record_t structure is returned to the user,
     * the record->data pointer will point to this buffer.
     *
     * Set the size to 0, and the data is freed.
     */
    ham_status_t resize_record_allocdata(ham_size_t size);

    /**
     * Resize the key data buffer. This buffer is an internal storage for 
     * key buffers. When a ham_key_t structure is returned to the user,
     * the key->data pointer will point to this buffer.
     *
     * Set the size to 0, and the data is freed.
     */
    ham_status_t resize_key_allocdata(ham_size_t size);

#if HAM_ENABLE_REMOTE
    /** get the remote database handle */
    ham_u64_t get_remote_handle(void) {
        return (m_remote_handle);
    }

    /** set the remote database handle */
    void set_remote_handle(ham_u64_t handle) {
        m_remote_handle=handle;
    }
#endif

    /** get the transaction tree */
    struct txn_optree_t *get_optree(void) {
        return (&m_optree);
    }

    /** get the database name */
    ham_u16_t get_name(void);

    /**
     * function which compares two keys
     *
     * @return -1, 0, +1 or higher positive values are the result of a 
     *         successful key comparison (0 if both keys match, -1 when 
     *         LHS < RHS key, +1 when LHS > RHS key).
     */
    int compare_keys(ham_key_t *lhs, ham_key_t *rhs) {
        int cmp=HAM_PREFIX_REQUEST_FULLKEY;
        ham_compare_func_t foo=get_compare_func();
        ham_prefix_compare_func_t prefoo=get_prefix_compare_func();
    
        set_error(0);
    
        /* need prefix compare? if no key is extended we can just call the
         * normal compare function */
        if (!(lhs->_flags&KEY_IS_EXTENDED) && !(rhs->_flags&KEY_IS_EXTENDED)) {
            return (foo((ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size, 
                            (ham_u8_t *)rhs->data, rhs->size));
        }
    
        /* yes! - run prefix comparison */
        if (prefoo) {
            ham_size_t lhsprefixlen, rhsprefixlen;
            if (lhs->_flags&KEY_IS_EXTENDED)
                lhsprefixlen=db_get_keysize(this)-sizeof(ham_offset_t);
            else
                lhsprefixlen=lhs->size;
            if (rhs->_flags&KEY_IS_EXTENDED)
                rhsprefixlen=db_get_keysize(this)-sizeof(ham_offset_t);
            else
                rhsprefixlen=rhs->size;
    
            cmp=prefoo((ham_db_t *)this, 
                        (ham_u8_t *)lhs->data, lhsprefixlen, lhs->size, 
                        (ham_u8_t *)rhs->data, rhsprefixlen, rhs->size);
            if (cmp<-1 && cmp!=HAM_PREFIX_REQUEST_FULLKEY)
                return (cmp); /* unexpected error! */
        }
    
        if (cmp==HAM_PREFIX_REQUEST_FULLKEY) {
            /* 1. load the first key, if needed */
            if (lhs->_flags&KEY_IS_EXTENDED) {
                ham_status_t st=get_extended_key((ham_u8_t *)lhs->data,
                        lhs->size, lhs->_flags, lhs);
                if (st)
                    return st;
                lhs->_flags&=~KEY_IS_EXTENDED;
            }
    
            /* 2. load the second key, if needed */
            if (rhs->_flags&KEY_IS_EXTENDED) {
                ham_status_t st=get_extended_key((ham_u8_t *)rhs->data,
                        rhs->size, rhs->_flags, rhs);
                if (st)
                    return st;
                rhs->_flags&=~KEY_IS_EXTENDED;
            }

            /* 3. run the comparison function */
            cmp=foo((ham_db_t *)this, (ham_u8_t *)lhs->data, lhs->size, 
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
        if (source->_flags&KEY_IS_EXTENDED) {
            ham_status_t st=get_extended_key((ham_u8_t *)source->data,
                        source->size, source->_flags, dest);
            if (st)
                return st;
            ham_assert(dest->data!=0, ("invalid extended key"));
            /* dest->size is set by db->get_extended_key() */
            ham_assert(dest->size == source->size, (0)); 
            /* the extended flag is set later, when this key is inserted */
            dest->_flags=source->_flags&(~KEY_IS_EXTENDED);
        }
        else if (source->size) {
            if (!(dest->flags&HAM_KEY_USER_ALLOC)) {
                if (!dest->data || dest->size<source->size) {
                    if (dest->data)
                        get_env()->get_allocator()->free(dest->data);
                    dest->data=(ham_u8_t *)
                                get_env()->get_allocator()->alloc(source->size);
                    if (!dest->data) 
                        return (HAM_OUT_OF_MEMORY);
                }
            }
            memcpy(dest->data, source->data, source->size);
            dest->size=source->size;
            dest->_flags=source->_flags;
        }
        else { 
            /* key.size is 0 */
            if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
                if (dest->data)
                    get_env()->get_allocator()->free(dest->data);
                dest->data=0;
            }
            dest->size=0;
            dest->_flags=source->_flags;
        }
        return (HAM_SUCCESS);
    }

  private:
    /** the last error code */
    ham_status_t m_error;

    /** the user-provided context data */
    void *m_context;

    /** the backend pointer - btree, hashtable etc */
    ham_backend_t *m_backend;

    /** linked list of all cursors */
    Cursor *m_cursors;

    /** the prefix-comparison function */
    ham_prefix_compare_func_t m_prefix_func;

    /** the comparison function */
    ham_compare_func_t m_cmp_func;

    /** the duplicate keys record comparison function */
    ham_compare_func_t m_duperec_func;

    /** the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t m_rt_flags;

    /** the environment of this database - can be NULL */
    Environment *m_env;

    /** the next database in a linked list of databases */
    Database *m_next;

    /** the cache for extended keys */
    ExtKeyCache *m_extkey_cache;

    /** the offset of this database in the environment _indexdata */
    ham_u16_t m_indexdata_offset;

    /** linked list of all record-level filters */
    ham_record_filter_t *m_record_filters;

    /** current data access mode (DAM) */
    ham_u16_t m_data_access_mode;

    /** non-zero after this istem has been opened/created */
    bool m_is_active;

    /** some database specific run-time data */
    ham_runtime_statistics_dbdata_t m_perf_data;

    /** TLS cached key size/alloc data */
    boost::thread_specific_ptr<ByteArray> m_key_alloc;

    /** TLS cached record size/alloc data */
    boost::thread_specific_ptr<ByteArray> m_record_alloc;

#if HAM_ENABLE_REMOTE
    /** the remote database handle */
    ham_u64_t m_remote_handle;
#endif

    /** the transaction tree */
    struct txn_optree_t m_optree;

    /** the object which does the actual work */
    DatabaseImplementation *m_impl;
};


/** check if a given data access mode / mode-set has been set */
inline bool dam_is_set(ham_u32_t coll, ham_u32_t mask) {
    return ((coll&mask)==mask);
}

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
 * flush a page
 */
extern ham_status_t
db_flush_page(Environment *env, Page *page);

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
 * Free a page.
 *
 * @remark will also remove the page from the cache and free all extended keys,
 * if there are any.
 *
 * @remark valid flag: DB_MOVE_TO_FREELIST; marks the page as 'deleted'
 * in the freelist. Ignored in in-memory databases.
 */
extern ham_status_t
db_free_page(Page *page, ham_u32_t flags);

#define DB_MOVE_TO_FREELIST         1

/**
 * Write a page, then delete the page from memory.
 *
 * @remark This function is used by the cache; it shouldn't be used
 * anywhere else.
 */
extern ham_status_t
db_write_page_and_delete(Page *page, ham_u32_t flags);

/**
* @defgroup ham_database_flags 
* @{
*/

/**
 * An internal database flag - use mmap instead of read(2).
 */
#define DB_USE_MMAP                  0x00000100

/**
 * An internal database flag - env handle is private to 
 * the @ref Database instance
 */
#define DB_ENV_IS_PRIVATE            0x00080000

/**
 * An internal database flag - env handle is remote
 */
#define DB_IS_REMOTE                 0x00200000

/**
 * An internal database flag - disable txn flushin when they're committed
 */
#define DB_DISABLE_AUTO_FLUSH        0x00400000

/**
 * @}
 */

/*
 * insert a key/record pair in a txn node; if cursor is not NULL it will
 * be attached to the new txn_op structure
 */
struct txn_cursor_t;
extern ham_status_t 
db_insert_txn(Database *db, ham_txn_t *txn, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags, 
                struct txn_cursor_t *cursor);

/*
 * erase a key/record pair from a txn; on success, cursor will be set to nil
 */
extern ham_status_t
db_erase_txn(Database *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags,
                struct txn_cursor_t *cursor);


#endif /* HAM_DB_H__ */
