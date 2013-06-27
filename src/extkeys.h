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

#ifndef HAM_EXTKEYS_H__
#define HAM_EXTKEYS_H__

#include <vector>

#include "hash-table.h"
#include "mem.h"
#include "env.h"
#include "db.h"

namespace hamsterdb {

/**
 * a cache for extended keys
 */
class ExtKeyCache {
  private:
    struct ExtKey {
      // the blobid of this key
      ham_u64_t blobid;

      // the age of this extkey
      ham_u64_t age;

      // pointer to the next key in the linked list
      ExtKey *next;

      // the size of the extended key
      ham_size_t size;

      // the key data
      ham_u8_t data[1];
    };

    enum {
      // when purging, keys > 25 will be removed
      kMaxAge       = 25,

      // the size of the ExtKey structure (without room for the key data)
      kSizeofExtkey = sizeof(ExtKey) - 1
    };

    class ExtKeyHelper {
      public:
        ExtKeyHelper(Environment *env)
          : m_env(env) {
        }

        unsigned hash(const ExtKey *extkey) const {
          return ((unsigned)extkey->blobid);
        }

        unsigned hash(const ham_u64_t &rid) const {
          return ((unsigned)rid);
        }

        bool matches(const ExtKey *lhs, ham_u64_t key) const {
          return (lhs->blobid == key);
        }

        ExtKey *next(const ExtKey *node) {
          return (node->next);
        }

        void set_next(ExtKey *node, ExtKey *other) {
          node->next = other;
        }

        bool remove_if(ExtKey *node) {
          if (m_removeall || (m_env->get_txn_id() - node->age > kMaxAge)) {
            Memory::release(node);
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
    // the default constructor
    ExtKeyCache(Database *db)
      : m_db(db), m_usedsize(0),
        m_extkeyhelper(new ExtKeyHelper(db->get_env())),
      m_hash(*m_extkeyhelper) {
    }

    // the destructor
    ~ExtKeyCache() {
      purge_all();
      delete m_extkeyhelper;
    }

    // insert a new extended key in the cache
    // will assert that there's no duplicate key!
    void insert(ham_u64_t blobid, ham_size_t size, const ham_u8_t *data) {
      ExtKey *e;
      Environment *env=m_db->get_env();

      // DEBUG build: make sure that the item is not inserted twice!
      ham_assert(m_hash.get(blobid) == 0);

      e = (ExtKey *)Memory::allocate<ExtKey>(kSizeofExtkey + size);
      e->blobid = blobid;
      // TODO do not use txn id but lsn for age
      e->age = env->get_txn_id();
      e->size = size;
      memcpy(e->data, data, size);

      m_hash.put(e);
    }

    // remove an extended key from the cache
    // returns HAM_KEY_NOT_FOUND if the extkey was not found
    void remove(ham_u64_t blobid) {
      ExtKey *e = m_hash.remove(blobid);
      if (e) {
        m_usedsize -= e->size;
        Memory::release(e);
      }
    }

    // fetches an extended key from the cache
    // returns HAM_KEY_NOT_FOUND if the extkey was not found
    ham_status_t fetch(ham_u64_t blobid, ham_size_t *size, ham_u8_t **data) {
      ExtKey *e = m_hash.get(blobid);
      if (e) {
        *size = e->size;
        *data = e->data;
        // TODO do not use txn id but lsn for age
        e->age = m_db->get_env()->get_txn_id();
        ms_count_hits++;
        return (0);
      }

      ms_count_misses++;
      return (HAM_KEY_NOT_FOUND);
    }

    // removes all OLD keys from the cache
    void purge() {
      m_extkeyhelper->m_removeall = false;
      m_hash.remove_if();
    }

    // removes ALL keys from the cache
    void purge_all() {
      m_extkeyhelper->m_removeall = true;
      m_hash.remove_if();
    }

    // Returns the usage metrics
    static void get_metrics(ham_env_metrics_t *metrics) {
      metrics->extkey_cache_hits = ms_count_hits;
      metrics->extkey_cache_misses = ms_count_misses;
    }

  private:
    // the owner of the cache
    Database *m_db;

    // the used size, in byte
    ham_size_t m_usedsize;

    // usage metrics - number of cache hits
    static ham_u64_t ms_count_hits;

    // usage metrics - number of cache misses
    static ham_u64_t ms_count_misses;

    // ExtKeyHelper instance for the hash table
    ExtKeyHelper *m_extkeyhelper;

    // the buckets - a list of ExtKey pointers
    hash_table<ExtKey, ham_u64_t, ExtKeyHelper> m_hash;
};

} // namespace hamsterdb

#endif /* HAM_EXTKEYS_H__ */
