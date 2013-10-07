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
#include "error.h"
#include "db_local.h"

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
      // when purging, keys older than kMaxAge will be removed
      kMaxAge       = 100,

      // the size of the ExtKey structure (without room for the key data)
      kSizeofExtkey = sizeof(ExtKey) - 1
    };

    class ExtKeyHelper {
      public:
        ExtKeyHelper(ExtKeyCache *parent)
          : m_removeall(false), m_parent(parent) {
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
          if (m_removeall || (m_parent->m_opcounter - node->age > kMaxAge)) {
            Memory::release(node);
            return (true);
          }
          return (false);
        }

        /* if true then remove_if() will remove ALL keys; otherwise only
         * those that are old enough */
        bool m_removeall;

      private:
        ExtKeyCache *m_parent;
    };

  public:
    // the default constructor
    ExtKeyCache(LocalDatabase *db)
      : m_db(db), m_usedsize(0), m_opcounter(0),
        m_extkeyhelper(new ExtKeyHelper(this)), m_hash(*m_extkeyhelper) {
    }

    // the destructor
    ~ExtKeyCache() {
      purge_all();
      delete m_extkeyhelper;
    }

    // insert a new extended key in the cache
    // will assert that there's no duplicate key!
    void insert(ham_u64_t blobid, ham_size_t size, const ham_u8_t *data) {
      // DEBUG build: make sure that the item is not inserted twice!
      ham_assert(m_hash.get(blobid) == 0);

      ExtKey *e = (ExtKey *)Memory::allocate<ExtKey>(kSizeofExtkey + size);
      e->blobid = blobid;
      e->age = m_opcounter++;
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
        e->age = m_opcounter;
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

    // Sets the opcounter; only for testing!
    void test_set_opcounter(ham_u64_t counter) {
      m_opcounter = counter;
    }

  private:
    // the owner of the cache
    LocalDatabase *m_db;

    // the used size, in byte
    ham_size_t m_usedsize;

    // counts the operations; used to calculate the "age" of an entry
    ham_u64_t m_opcounter;

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
