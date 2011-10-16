/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief extended key cache
 *
 */

#ifndef HAM_EXTKEYS_H__
#define HAM_EXTKEYS_H__

#include <vector>

#include "internal_fwd_decl.h"
#include "hash-table.h"
#include "mem.h"
#include "env.h"


#ifdef __cplusplus
extern "C" {
#endif 

#define EXTKEY_MAX_AGE  25

/**
 * a cache for extended keys
 */
class ExtKeyCache
{
    struct extkey_t
    {
        /** the blobid of this key */
        ham_offset_t _blobid;

        /** the age of this extkey */
        ham_u64_t _age;

        /** pointer to the next key in the linked list */
        extkey_t *_next;

        /** the size of the extended key */
        ham_size_t _size;

        /** the key data */
        ham_u8_t _data[1];
    };

    class ExtKeyHelper
    {
      public:
        ExtKeyHelper(ham_env_t *env) : m_env(env) { }

        ham_offset_t key(const extkey_t *extkey) {
            return (extkey->_blobid);
        }

        unsigned hash(const extkey_t *extkey) {
            return ((unsigned)extkey->_blobid);
        }

        unsigned hash(const ham_offset_t &rid) {
            return ((unsigned)rid);
        }

        bool matches(const extkey_t *lhs, const extkey_t *rhs) {
            return (lhs->_blobid==rhs->_blobid);
        }

        bool matches(const extkey_t *lhs, ham_offset_t key) {
            return (lhs->_blobid==key);
        }

        extkey_t *next(const extkey_t *node) {
            return (node->_next);
        }

        void visit(const extkey_t *node) {
        }

        void free(const extkey_t *node) {
            allocator_free(env_get_allocator(m_env), node);
        }

        void set_next(extkey_t *node, extkey_t *other) {
            node->_next=other;
        }

        bool remove_if(const extkey_t *node) {
            if (m_removeall) {
                free(node);
                return (true);
            }
            if (env_get_txn_id(m_env)-node->_age>EXTKEY_MAX_AGE) {
                free(node);
                return (true);
            }
            return (false);
        }

        /* if true then remove_if() will remove ALL keys; otherwise only
         * those that are old enough */
        bool m_removeall;

      private:
        ham_env_t *m_env;
    };

  public:
    /** the default constructor */
    ExtKeyCache(ham_db_t *db);

    /** the destructor */
    ~ExtKeyCache();

    /**
     * insert a new extended key in the cache
     * will assert that there's no duplicate key! 
     */
    void insert(ham_offset_t blobid, ham_size_t size, const ham_u8_t *data);

    /**
     * remove an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    void remove(ham_offset_t blobid);

    /**
     * fetches an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    ham_status_t fetch(ham_offset_t blobid, ham_size_t *size, ham_u8_t **data);
    
    /**
     * removes all OLD keys from the cache
     */
    void purge(void);
    
    /**
     * removes ALL keys from the cache
     */
    void purge_all(void);

  private:
    /** the owner of the cache */
    ham_db_t *m_db;

    /** the used size, in byte */
    ham_size_t m_usedsize;

    /** helper object for the cache */
    ExtKeyHelper *m_extkeyhelper;

    /** the buckets - a list of extkey_t pointers */
    hash_table<extkey_t, ham_offset_t, ExtKeyHelper> m_hash;
};

/**
 * a combination of extkey_cache_remove and blob_free
 */
extern ham_status_t 
extkey_remove(ham_db_t *db, ham_offset_t blobid);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_EXTKEYS_H__ */
