/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "blob.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "mem.h"


/** the size of an extkey_t, without the single data byte */
#define SIZEOF_EXTKEY_T                     (sizeof(extkey_t)-1)

/** get the blobid */
#define extkey_get_blobid(e)                (e)->_blobid

/** set the blobid */
#define extkey_set_blobid(e, id)            (e)->_blobid=(id)

/** set the age of this extkey */
#define extkey_set_age(e, age)              (e)->_age=(age)

/** get the size */
#define extkey_get_size(e)                  (e)->_size

/** set the size */
#define extkey_set_size(e, size)            (e)->_size=(size)

/** get the data pointer */
#define extkey_get_data(e)                  (e)->_data


ExtKeyCache::ExtKeyCache(Database *db)
  : m_db(db), m_usedsize(0), m_extkeyhelper(new ExtKeyHelper(db->get_env())),
    m_hash(*m_extkeyhelper)
{
}

ExtKeyCache::~ExtKeyCache()
{
    purge_all();
    delete m_extkeyhelper;
}

void
ExtKeyCache::insert(ham_offset_t blobid, ham_size_t size, const ham_u8_t *data)
{
    extkey_t *e;
    Environment *env=m_db->get_env();

    /* DEBUG build: make sure that the item is not inserted twice!  */
    ham_assert(m_hash.get(blobid)==0, ("")); 

    e=(extkey_t *)env->get_allocator()->alloc(SIZEOF_EXTKEY_T+size);
    extkey_set_blobid(e, blobid);
    /* TODO do not use txn id but lsn for age */
    extkey_set_age(e, env->get_txn_id());
    extkey_set_size(e, size);
    memcpy(extkey_get_data(e), data, size);

    m_hash.put(e);
}

void
ExtKeyCache::remove(ham_offset_t blobid)
{
    Environment *env=m_db->get_env();
    extkey_t *e=m_hash.remove(blobid);
    if (e) {
        m_usedsize-=extkey_get_size(e);
        env->get_allocator()->free(e);
    }
}

ham_status_t
ExtKeyCache::fetch(ham_offset_t blobid, ham_size_t *size, ham_u8_t **data)
{
    extkey_t *e=m_hash.get(blobid);
    if (e) {
        *size=extkey_get_size(e);
        *data=extkey_get_data(e);
        /* TODO do not use txn id but lsn for age */
        extkey_set_age(e, m_db->get_env()->get_txn_id());
        return (0);
    }
    else
        return (HAM_KEY_NOT_FOUND);
}

void
ExtKeyCache::purge()
{
    m_extkeyhelper->m_removeall=false;
    m_hash.remove_if();
}

void
ExtKeyCache::purge_all()
{
    m_extkeyhelper->m_removeall=true;
    m_hash.remove_if();
}

ham_status_t
extkey_remove(Database *db, ham_offset_t blobid)
{
    if (db->get_extkey_cache())
        db->get_extkey_cache()->remove(blobid);

    return (blob_free(db->get_env(), db, blobid, 0));
}

