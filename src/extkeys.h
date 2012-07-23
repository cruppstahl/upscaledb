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

#define EXTKEY_MAX_AGE  25

/**
 * a cache for extended keys
 */
class ExtKeyCache
{
    struct ExtKey {
      /** the blobid of this key */
      ham_offset_t blobid;

      /** the age of this extkey */
      ham_u64_t age;

      /** pointer to the next key in the linked list */
      ExtKey *next;

      /** the size of the extended key */
      ham_size_t size;

      /** the key data */
      ham_u8_t data[1];
    };

    /** the size of the ExtKey structure (without room for the key data) */
    static const int SIZEOF_EXTKEY_T = sizeof(ExtKey)-1;

    class ExtKeyHelper
    {
      public:
        ExtKeyHelper(Environment *env) : m_env(env) { }

        unsigned hash(const ExtKey *extkey) {
          return ((unsigned)extkey->blobid);
        }

        unsigned hash(const ham_offset_t &rid) {
          return ((unsigned)rid);
        }

        bool matches(const ExtKey *lhs, ham_offset_t key) {
          return (lhs->blobid==key);
        }

        ExtKey *next(const ExtKey *node) {
          return (node->next);
        }

        void set_next(ExtKey *node, ExtKey *other) {
          node->next=other;
        }

        bool remove_if(const ExtKey *node) {
          if (m_removeall) {
            m_env->get_allocator()->free(node);
            return (true);
          }
          if (m_env->get_txn_id()-node->age>EXTKEY_MAX_AGE) {
            m_env->get_allocator()->free(node);
            return (true);
          }
          return (false);
        }

        /* if true then remove_if() will remove ALL keys; otherwise only
         * those that are old enough */
        bool m_removeall;

      private:
        Environment *m_env;
    };

  public:
    /** the default constructor */
    ExtKeyCache(Database *db)
      : m_db(db), m_usedsize(0),
      m_extkeyhelper(new ExtKeyHelper(db->get_env())),
      m_hash(*m_extkeyhelper) {
    }

    /** the destructor */
    ~ExtKeyCache() {
      ScopedLock lock(m_mutex);
      purge_all_nolock();
      delete m_extkeyhelper;
    }

    /**
     * insert a new extended key in the cache
     * will assert that there's no duplicate key! 
     */
    void insert(ham_offset_t blobid, ham_size_t size, const ham_u8_t *data) {
      ScopedLock lock(m_mutex);
      ExtKey *e;
      Environment *env=m_db->get_env();

      /* DEBUG build: make sure that the item is not inserted twice!  */
      ham_assert(m_hash.get(blobid)==0);

      e=(ExtKey *)env->get_allocator()->alloc(SIZEOF_EXTKEY_T+size);
      e->blobid=blobid;
      /* TODO do not use txn id but lsn for age */
      e->age=env->get_txn_id();
      e->size=size;
      memcpy(e->data, data, size);

      m_hash.put(e);
    }

    /**
     * remove an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    void remove(ham_offset_t blobid) {
      ScopedLock lock(m_mutex);
      ExtKey *e=m_hash.remove(blobid);
      if (e) {
        m_usedsize-=e->size;
        m_db->get_env()->get_allocator()->free(e);
      }
    }

    /**
     * fetches an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    ham_status_t fetch(ham_offset_t blobid, ham_size_t *size, ham_u8_t **data) {
      ScopedLock lock(m_mutex);
      ExtKey *e=m_hash.get(blobid);
      if (e) {
        *size=e->size;
        *data=e->data;
        /* TODO do not use txn id but lsn for age */
        e->age=m_db->get_env()->get_txn_id();
        return (0);
      }
      else
        return (HAM_KEY_NOT_FOUND);
    }
    
    /** removes all OLD keys from the cache */
    void purge() {
      ScopedLock lock(m_mutex);
      m_extkeyhelper->m_removeall=false;
      m_hash.remove_if();
    }
    
    /** removes ALL keys from the cache */
    void purge_all() {
      ScopedLock lock(m_mutex);
      purge_all_nolock();
    }

  private:
    /** removes ALL keys from the cache (w/o mutex) */
    void purge_all_nolock() {
      m_extkeyhelper->m_removeall=true;
      m_hash.remove_if();
    }

    /** a mutex for this cache */
    Mutex m_mutex;

    /** the owner of the cache */
    Database *m_db;

    /** the used size, in byte */
    ham_size_t m_usedsize;

    /** ExtKeyHelper instance for the hash table */
    ExtKeyHelper *m_extkeyhelper;

    /** the buckets - a list of ExtKey pointers */
    hash_table<ExtKey, ham_offset_t, ExtKeyHelper> m_hash;
};

/**
 * a combination of extkey_cache_remove and blob_free
 * TODO move this to Database.cc (DatabaseImplementationLocal)
 */
inline ham_status_t 
extkey_remove(Database *db, ham_offset_t blobid)
{
  if (db->get_extkey_cache())
    db->get_extkey_cache()->remove(blobid);

  return (blob_free(db->get_env(), db, blobid, 0));
}


#endif /* HAM_EXTKEYS_H__ */
