/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>

#ifdef HAM_ENABLE_REMOTE
#  define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#  include <curl/curl.h>
#  include <curl/easy.h>
#  include "protocol/protocol.h"
#endif

#include "blob_manager.h"
#include "btree.h"
#include "btree_cursor.h"
#include "cache.h"
#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "serial.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "version.h"
#include "freelist.h"

using namespace hamsterdb;


/* return true if the filename is for a local file */
static bool
__filename_is_local(const char *filename)
{
  if (filename && strstr(filename, "http://") == filename)
    return (false);
  return (true);
}

ham_status_t
ham_txn_begin(ham_txn_t **htxn, ham_env_t *henv, const char *name,
        void *reserved, ham_u32_t flags)
{
  Transaction **txn = (Transaction **)htxn;

  if (!txn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *txn = 0;

  if (!henv) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = (Environment *)henv;

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  if (!(env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
    return (HAM_INV_PARAMETER);
  }

  /* initialize the txn structure */
  return (env->txn_begin(txn, name, flags));
}

HAM_EXPORT const char *
ham_txn_get_name(ham_txn_t *htxn)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn)
    return (0);

  ScopedLock lock(txn->get_env()->get_mutex());
  const std::string &name = txn->get_name();
  if (name.empty())
    return 0;
  else
    return (name.c_str());
}

ham_status_t
ham_txn_commit(ham_txn_t *htxn, ham_u32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  /* mark this transaction as committed; will also call
   * env->signal_commit() to write committed transactions
   * to disk */
  return (env->txn_commit(txn, flags));
}

ham_status_t
ham_txn_abort(ham_txn_t *htxn, ham_u32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  return (env->txn_abort(txn, flags));
}

const char * HAM_CALLCONV
ham_strerror(ham_status_t result)
{
  switch (result) {
    case HAM_SUCCESS:
      return ("Success");
    case HAM_INV_KEYSIZE:
      return ("Invalid key size");
    case HAM_INV_PAGESIZE:
      return ("Invalid page size");
    case HAM_OUT_OF_MEMORY:
      return ("Out of memory");
    case HAM_INV_PARAMETER:
      return ("Invalid parameter");
    case HAM_INV_FILE_HEADER:
      return ("Invalid database file header");
    case HAM_INV_FILE_VERSION:
      return ("Invalid database file version");
    case HAM_KEY_NOT_FOUND:
      return ("Key not found");
    case HAM_DUPLICATE_KEY:
      return ("Duplicate key");
    case HAM_INTEGRITY_VIOLATED:
      return ("Internal integrity violated");
    case HAM_INTERNAL_ERROR:
      return ("Internal error");
    case HAM_WRITE_PROTECTED:
      return ("Database opened in read-only mode");
    case HAM_BLOB_NOT_FOUND:
      return ("Data blob not found");
    case HAM_PREFIX_REQUEST_FULLKEY:
      return ("Comparator function needs more data");
    case HAM_IO_ERROR:
      return ("System I/O error");
    case HAM_CACHE_FULL:
      return ("Database cache is full");
    case HAM_NOT_IMPLEMENTED:
      return ("Operation not implemented");
    case HAM_FILE_NOT_FOUND:
      return ("File not found");
    case HAM_WOULD_BLOCK:
      return ("Operation would block");
    case HAM_NOT_READY:
      return ("Object was not initialized correctly");
    case HAM_CURSOR_STILL_OPEN:
      return ("Cursor must be closed prior to Transaction abort/commit");
    case HAM_FILTER_NOT_FOUND:
      return ("Record filter or file filter not found");
    case HAM_TXN_CONFLICT:
      return ("Operation conflicts with another Transaction");
    case HAM_TXN_STILL_OPEN:
      return ("Database cannot be closed because it is modified in a "
          "Transaction");
    case HAM_CURSOR_IS_NIL:
      return ("Cursor points to NIL");
    case HAM_DATABASE_NOT_FOUND:
      return ("Database not found");
    case HAM_DATABASE_ALREADY_EXISTS:
      return ("Database name already exists");
    case HAM_DATABASE_ALREADY_OPEN:
      return ("Database already open, or: Database handle "
          "already initialized");
    case HAM_ENVIRONMENT_ALREADY_OPEN:
      return ("Environment already open, or: Environment handle "
          "already initialized");
    case HAM_LIMITS_REACHED:
      return ("Database limits reached");
    case HAM_ALREADY_INITIALIZED:
      return ("Object was already initialized");
    case HAM_NEED_RECOVERY:
      return ("Database needs recovery");
    case HAM_LOG_INV_FILE_HEADER:
      return ("Invalid log file header");
    case HAM_NETWORK_ERROR:
      return ("Remote I/O error/Network error");
    default:
      return ("Unknown error");
  }
}

/**
 * Prepares a @ref ham_key_t structure for returning key data in.
 *
 * This function checks whether the @ref ham_key_t structure has been
 * properly initialized by the user and resets all internal used elements.
 *
 * @return true when the @a key structure has been initialized correctly
 * before.
 *
 * @return false when the @a key structure has @e not been initialized
 * correctly before.
 */
static inline bool
__prepare_key(ham_key_t *key)
{
  if (key->size && !key->data) {
    ham_trace(("key->size != 0, but key->data is NULL"));
    return (false);
  }
  if (key->flags != 0 && key->flags != HAM_KEY_USER_ALLOC) {
    ham_trace(("invalid flag in key->flags"));
    return (false);
  }
  key->_flags = 0;
  return (true);
}

/**
 * Prepares a @ref ham_record_t structure for returning record data in.
 *
 * This function checks whether the @ref ham_record_t structure has been
 * properly initialized by the user and resets all internal used elements.
 *
 * @return true when the @a record structure has been initialized
 * correctly before.
 *
 * @return false when the @a record structure has @e not been
 * initialized correctly before.
 */
static inline bool
__prepare_record(ham_record_t *record)
{
  if (record->size && !record->data) {
    ham_trace(("record->size != 0, but record->data is NULL"));
    return false;
  }
  if (record->flags & HAM_DIRECT_ACCESS)
    record->flags &= ~HAM_DIRECT_ACCESS;
  if (record->flags != 0 && record->flags != HAM_RECORD_USER_ALLOC) {
    ham_trace(("invalid flag in record->flags"));
    return (false);
  }
  record->_intflags = 0;
  record->_rid = 0;
  return (true);
}

void HAM_CALLCONV
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
        ham_u32_t *revision)
{
  if (major)
    *major = HAM_VERSION_MAJ;
  if (minor)
    *minor = HAM_VERSION_MIN;
  if (revision)
    *revision = HAM_VERSION_REV;
}

void HAM_CALLCONV
ham_get_license(const char **licensee, const char **product)
{
  if (licensee)
    *licensee = HAM_LICENSEE;
  if (product)
    *product = HAM_PRODUCT_NAME;
}

ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t **henv, const char *filename,
        ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
  ham_size_t pagesize = HAM_DEFAULT_PAGESIZE;
  ham_u64_t cachesize = 0;
  ham_u16_t maxdbs = 0;
  std::string logdir;

  if (!henv) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *henv = 0;

  /* creating a file in READ_ONLY mode? doesn't make sense */
  if (flags & HAM_READ_ONLY) {
    ham_trace(("cannot create a file in read-only mode"));
    return (HAM_INV_PARAMETER);
  }

  /* in-memory? don't allow cache limits! */
  if ((flags & HAM_IN_MEMORY) && (flags & HAM_CACHE_STRICT)) {
    ham_trace(("combination of HAM_IN_MEMORY and HAM_CACHE_STRICT "
            "not allowed"));
    return (HAM_INV_PARAMETER);
  }

  /* in-memory? recovery is not possible */
  if ((flags & HAM_IN_MEMORY) && (flags & HAM_ENABLE_RECOVERY)) {
    ham_trace(("combination of HAM_IN_MEMORY and HAM_ENABLE_RECOVERY "
            "not allowed"));
    return (HAM_INV_PARAMETER);
  }

  /* since 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_ENABLE_TRANSACTIONS)
    flags |= HAM_ENABLE_RECOVERY;

  /* flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_AUTO_RECOVERY)
    flags |= HAM_ENABLE_RECOVERY;

  /* in-memory with Transactions? disable recovery */
  if (flags & HAM_IN_MEMORY)
    flags &= ~HAM_ENABLE_RECOVERY;

  ham_u32_t mask = HAM_ENABLE_FSYNC
            | HAM_IN_MEMORY
            | HAM_DISABLE_MMAP
            | HAM_CACHE_STRICT
            | HAM_CACHE_UNLIMITED
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS;
  if (flags & ~mask) {
    ham_trace(("ham_env_create() called with invalid flag 0x%x (%d)", 
                (int)(flags & ~mask), (int)(flags & ~mask)));
    return (HAM_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case HAM_PARAM_CACHESIZE:
        cachesize = param->value;
        if (flags & HAM_IN_MEMORY && cachesize != 0) {
          ham_trace(("combination of HAM_IN_MEMORY and cachesize != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        break;
      case HAM_PARAM_PAGESIZE:
        if (param->value != 1024 && param->value % 2048 != 0) {
          ham_trace(("invalid pagesize - must be 1024 or a multiple of 2048"));
          return (HAM_INV_PAGESIZE);
        }
        pagesize = (ham_size_t)param->value;
        break;
      case HAM_PARAM_MAX_DATABASES:
        maxdbs = (ham_size_t)param->value;
        if (maxdbs == 0) {
          ham_trace(("invalid value %u for parameter HAM_PARAM_MAX_DATABASES",
                 (unsigned)param->value));
          return (HAM_INV_PARAMETER);
        }
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        logdir = (const char *)param->value;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  /* don't allow cache limits with unlimited cache */
  if (flags & HAM_CACHE_UNLIMITED) {
    if ((flags & HAM_CACHE_STRICT) || cachesize != 0) {
      ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
            "or HAM_CACHE_STRICT not allowed"));
      return (HAM_INV_PARAMETER);
    }
  }

  if (cachesize == 0)
    cachesize = HAM_DEFAULT_CACHESIZE;
  if (pagesize == 0)
    pagesize = HAM_DEFAULT_PAGESIZE;

  if (!filename && !(flags & HAM_IN_MEMORY)) {
    ham_trace(("filename is missing"));
    return (HAM_INV_PARAMETER);
  }

  /*
   * make sure that max_databases actually fit in a header
   * page!
   * leave at least 128 bytes for the freelist and the other header data
   */
  {
    ham_size_t l = pagesize - sizeof(PEnvHeader)
        - freel_get_bitmap_offset() - 128;

    l /= sizeof(PBtreeDescriptor);
    if (maxdbs > l) {
      ham_trace(("parameter HAM_PARAM_MAX_DATABASES too high for "
            "this pagesize; the maximum allowed is %u",
            (unsigned)l));
      return (HAM_INV_PARAMETER);
    }
    else if (maxdbs == 0) {
      if (DB_MAX_INDICES > l)
        maxdbs = (ham_u16_t)l;  /* small page sizes (e.g. 1K) cannot carry 
                                 * DB_MAX_INDICES databases! */
      else
        maxdbs = DB_MAX_INDICES;
    }
  }

  Environment *env;
  if (__filename_is_local(filename))
    env = new LocalEnvironment();
  else {
#ifndef HAM_ENABLE_REMOTE
    return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
    env = new RemoteEnvironment();
#endif
  }

#ifdef HAM_ENABLE_REMOTE
  atexit(curl_global_cleanup);
  atexit(Protocol::shutdown);
#endif

  if (logdir.size())
    env->set_log_directory(logdir);

  /* and finish the initialization of the Environment */
  ham_status_t st = env->create(filename, flags, mode, pagesize, cachesize,
          maxdbs);
  if (st)
    goto bail;

  /* flush the environment to make sure that the header page is written
   * to disk */
  st = env->flush(0);

bail:
  if (st) {
    delete env;
    return (st);
  }
  
  *henv = (ham_env_t *)env;
  return 0;
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *henv, ham_db_t **hdb, ham_u16_t dbname,
        ham_u32_t flags, const ham_parameter_t *param)
{
  ham_status_t st;
  Environment *env = (Environment *)henv;

  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *hdb = 0;

  if (!dbname || (dbname>HAM_DEFAULT_DATABASE_NAME
      && dbname != HAM_DUMMY_DATABASE_NAME)) {
    ham_trace(("invalid database name"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(env->get_mutex());

  /* the function handler will do the rest */
  st = env->create_db((Database **)hdb, dbname, flags, param);
  if (st)
    goto bail;

  /* flush the environment to make sure that the header page is written
   * to disk */
  st = env->flush(0);

bail:
  if (st) {
    if (*hdb)
      (void)ham_db_close((ham_db_t *)*hdb, HAM_DONT_LOCK);
    *hdb = 0;
    return (st);
  }

  return (0);
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *henv, ham_db_t **hdb, ham_u16_t dbname,
        ham_u32_t flags, const ham_parameter_t *param)
{
  ham_status_t st;
  Environment *env = (Environment *)henv;

  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *hdb = 0;

  if (!dbname) {
    ham_trace(("parameter 'dbname' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (dbname != HAM_FIRST_DATABASE_NAME
      && (dbname != HAM_DUMMY_DATABASE_NAME
        && dbname > HAM_DEFAULT_DATABASE_NAME)) {
    ham_trace(("database name must be lower than 0xf000"));
    return (HAM_INV_PARAMETER);
  }
  if (env->get_flags() & HAM_IN_MEMORY) {
    ham_trace(("cannot open a Database in an In-Memory Environment"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  /* the function handler will do the rest */
  st = env->open_db((Database **)hdb, dbname, flags, param);

  /* TODO move cleanup code to Environment::open_db() */
  if (st) {
    if (*hdb)
      (void)ham_db_close((ham_db_t *)*hdb, HAM_DONT_LOCK);
    *hdb = 0;
    return (st);
  }

  return (0);
}

ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t **henv, const char *filename, ham_u32_t flags,
        const ham_parameter_t *param)
{
  ham_u64_t cachesize = 0;
  std::string logdir;

  if (!henv) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *henv = 0;

  /* cannot open an in-memory-db */
  if (flags & HAM_IN_MEMORY) {
    ham_trace(("cannot open an in-memory database"));
    return (HAM_INV_PARAMETER);
  }

  /* HAM_ENABLE_DUPLICATES has to be specified in ham_create, not ham_open */
  if (flags & HAM_ENABLE_DUPLICATES) {
    ham_trace(("invalid flag HAM_ENABLE_DUPLICATES (only allowed when "
        "creating a database"));
    return (HAM_INV_PARAMETER);
  }

#if 0 // re-enable this after 2.1.1, when the file format becomes incompatible
  /* HAM_ENABLE_EXTENDED_KEYS has to be specified in ham_create, not ham_open */
  if (flags & HAM_ENABLE_EXTENDED_KEYS) {
    ham_trace(("invalid flag HAM_ENABLE_EXTENDED_KEYS (only allowed when "
        "creating a database"));
    return (HAM_INV_PARAMETER);
  }
#endif

  /* since 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_ENABLE_TRANSACTIONS)
    flags |= HAM_ENABLE_RECOVERY;

  /* flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_AUTO_RECOVERY)
    flags |= HAM_ENABLE_RECOVERY;

  if (!filename && !(flags & HAM_IN_MEMORY)) {
    ham_trace(("filename is missing"));
    return (HAM_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case HAM_PARAM_CACHESIZE:
        cachesize = param->value;
        if (flags & HAM_IN_MEMORY && cachesize != 0) {
          ham_trace(("combination of HAM_IN_MEMORY and cachesize != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        logdir = (const char *)param->value;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  /* don't allow cache limits with unlimited cache */
  if (flags & HAM_CACHE_UNLIMITED) {
    if ((flags & HAM_CACHE_STRICT) || cachesize != 0) {
      ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
            "or HAM_CACHE_STRICT not allowed"));
      return (HAM_INV_PARAMETER);
    }
  }

  if (cachesize == 0)
    cachesize = HAM_DEFAULT_CACHESIZE;

  Environment *env;
  if (__filename_is_local(filename))
    env = new LocalEnvironment();
  else {
#ifndef HAM_ENABLE_REMOTE
    return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
    env = new RemoteEnvironment();
#endif
  }

#ifdef HAM_ENABLE_REMOTE
  atexit(curl_global_cleanup);
  atexit(Protocol::shutdown);
#endif

  if (logdir.size())
    env->set_log_directory(logdir);

  /* and finish the initialization of the Environment */
  ham_status_t st = env->open(filename, flags, cachesize);
  if (st)
    goto bail;

bail:
  if (st) {
    delete env;
    return (st);
  }

  *henv = (ham_env_t *)env;
  return (0);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *henv, ham_u16_t oldname, ham_u16_t newname,
        ham_u32_t flags)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(env->get_mutex());

  if (!oldname) {
    ham_trace(("parameter 'oldname' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (!newname) {
    ham_trace(("parameter 'newname' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (newname >= HAM_DEFAULT_DATABASE_NAME) {
    ham_trace(("parameter 'newname' must be lower than 0xf000"));
    return (HAM_INV_PARAMETER);
  }

  /* no need to do anything if oldname==newname */
  if (oldname == newname)
    return (0);

  /* rename the database */
  return (env->rename_db(oldname, newname, flags));
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *henv, ham_u16_t name, ham_u32_t flags)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(env->get_mutex());

  if (!name) {
    ham_trace(("parameter 'name' must not be 0"));
    return (HAM_INV_PARAMETER);
  }

  /* erase the database */
  return (env->erase_db(name, flags));
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *henv, ham_u16_t *names, ham_size_t *count)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(env->get_mutex());

  if (!names) {
    ham_trace(("parameter 'names' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!count) {
    ham_trace(("parameter 'count' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  /* get all database names */
  return (env->get_database_names(names, count));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *henv, ham_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(env->get_mutex());

  if (!param) {
    ham_trace(("parameter 'param' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  /* get the parameters */
  return (env->get_parameters(param));
}

ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *henv, ham_u32_t flags)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (flags) {
    ham_trace(("parameter 'flags' is unused, set to 0"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock = ScopedLock(env->get_mutex());

  /* flush the Environment */
  return (env->flush(flags));
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *henv, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = (Environment *)henv;

  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock = ScopedLock(env->get_mutex());

  /* make sure that the changeset is empty */
  ham_assert(env->get_changeset().is_empty());

  /* auto-abort (or commit) all pending transactions */
  if (env && env->get_newest_txn()) {
    Transaction *n, *t = env->get_newest_txn();
    while (t) {
      n = t->get_older();
      if (t->is_aborted() || t->is_committed())
        ; /* nop */
      else {
        if (flags & HAM_TXN_AUTO_COMMIT) {
          if ((st = env->txn_commit(t, 0)))
            return (st);
        }
        else { /* if (flags&HAM_TXN_AUTO_ABORT) */
          if ((st = env->txn_abort(t, 0)))
            return (st);
        }
      }
      t = n;
    }
  }

  /* flush all committed transactions */
  st = env->flush_committed_txns();
  if (st)
    return (st);

  /* close the environment */
  st = env->close(flags);
  if (st)
    return (st);

  lock.unlock();

  delete env;
  return (0);
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_parameters(ham_db_t *hdb, ham_parameter_t *param)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->get_mutex());

  if (!param) {
    ham_trace(("parameter 'param' must not be NULL"));
    return HAM_INV_PARAMETER;
  }

  /* get the parameters */
  return (db->get_parameters(param));
}

ham_status_t HAM_CALLCONV
ham_db_get_error(ham_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (0);
  }

  ScopedLock lock;
  if (db->get_env())
    lock = ScopedLock(db->get_env()->get_mutex());

  return (db->get_error());
}

ham_status_t HAM_CALLCONV
ham_db_set_prefix_compare_func(ham_db_t *hdb, ham_prefix_compare_func_t foo)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->get_mutex());

  db->set_error(0);
  db->set_prefix_compare_func(foo);
  return (db->set_error(0));
}

ham_status_t HAM_CALLCONV
ham_db_set_compare_func(ham_db_t *hdb, ham_compare_func_t foo)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock;
  if (db->get_env())
    lock = ScopedLock(db->get_env()->get_mutex());

  /* set the compare functions */
  db->set_compare_func(foo);
  return (db->set_error(HAM_SUCCESS));
}

ham_status_t HAM_CALLCONV
ham_db_find(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  env = db->get_env();
  if (!env) {
    ham_trace(("parameter 'db' must be linked to a valid (implicit "
           "or explicit) environment"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  ScopedLock lock(env->get_mutex());

  if (!key) {
    ham_trace(("parameter 'key' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!record) {
    ham_trace(("parameter 'record' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_PREPEND) {
    ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_APPEND) {
    ham_trace(("flag HAM_HINT_APPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DIRECT_ACCESS)
      && !(env->get_flags() & HAM_IN_MEMORY)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
          "In-Memory Databases"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DIRECT_ACCESS)
      && (env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  /* record number: make sure that we have a valid key structure */
  if (db->get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != sizeof(ham_u64_t) || !key->data) {
      ham_trace(("key->size must be 8, key->data must not be NULL"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
  }

  if (!__prepare_key(key) || !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->find(txn, key, record, flags)));
}

int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
  if (key && (ham_key_get_intflags(key) & PBtreeKey::KEY_IS_APPROXIMATE)) {
    int rv = (ham_key_get_intflags(key) & PBtreeKey::KEY_IS_LT) ? -1 : +1;
    return (rv);
  }

  return (0);
}

ham_status_t HAM_CALLCONV
ham_db_insert(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return HAM_INV_PARAMETER;
  }
  env = db->get_env();
  if (!env) {
    ham_trace(("parameter 'db' must be linked to a valid (implicit or "
           "explicit) environment"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  if (!key) {
    ham_trace(("parameter 'key' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!record) {
    ham_trace(("parameter 'record' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_APPEND) {
    ham_trace(("flags HAM_HINT_APPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_PREPEND) {
    ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (db->get_rt_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot insert in a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }
  if ((db->get_rt_flags() & HAM_DISABLE_VAR_KEYLEN) &&
      (key->size > db->get_keysize())) {
    ham_trace(("database does not support variable length keys"));
    return (db->set_error(HAM_INV_KEYSIZE));
  }
  if ((flags & HAM_OVERWRITE) && (flags & HAM_DUPLICATE)) {
    ham_trace(("cannot combine HAM_OVERWRITE and HAM_DUPLICATE"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (record->size <= sizeof(ham_u64_t))) {
    ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
          "<= 8"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DUPLICATE)
      && !(db->get_rt_flags() & HAM_ENABLE_DUPLICATES)) {
    ham_trace(("database does not support duplicate keys "
          "(see HAM_ENABLE_DUPLICATES)"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DUPLICATE_INSERT_AFTER)
      || (flags & HAM_DUPLICATE_INSERT_BEFORE)
      || (flags & HAM_DUPLICATE_INSERT_LAST)
      || (flags & HAM_DUPLICATE_INSERT_FIRST)) {
    ham_trace(("function does not support flags HAM_DUPLICATE_INSERT_*; "
          "see ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size)) {
    ham_trace(("partial offset+size is greater than the total "
          "record size"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (record->size <= sizeof(ham_u64_t))) {
    ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
          "<= 8"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (!__prepare_key(key) || !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  /* allocate temp. storage for a recno key */
  if (db->get_rt_flags() & HAM_RECORD_NUMBER) {
    if (flags & HAM_OVERWRITE) {
      if (key->size != sizeof(ham_u64_t) || !key->data) {
        ham_trace(("key->size must be 8, key->data must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
      }
    }
    else {
      if (key->flags & HAM_KEY_USER_ALLOC) {
        if (!key->data || key->size != sizeof(ham_u64_t)) {
          ham_trace(("key->size must be 8, key->data must not "
                "be NULL"));
          return (db->set_error(HAM_INV_PARAMETER));
        }
      }
      else {
        if (key->data || key->size) {
          ham_trace(("key->size must be 0, key->data must be NULL"));
          return (db->set_error(HAM_INV_PARAMETER));
        }
      }
    }
  }

  return (db->set_error(db->insert(txn, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_db_erase(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key, ham_u32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  env = db->get_env();
  if (!env) {
    ham_trace(("parameter 'db' must be linked to a valid (implicit "
           "or explicit) environment"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  if (!key) {
    ham_trace(("parameter 'key' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_PREPEND) {
    ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_APPEND) {
    ham_trace(("flags HAM_HINT_APPEND is only allowed in "
          "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (!__prepare_key(key))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->erase(txn, key, flags)));
}

ham_status_t HAM_CALLCONV
ham_db_check_integrity(ham_db_t *hdb, ham_txn_t *htxn)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->get_mutex());

  return (db->set_error(db->check_integrity(txn)));
}

/*
 * always shut down entirely, even when a page flush or other
 * 'non-essential' element of the process fails.
 */
ham_status_t HAM_CALLCONV
ham_db_close(ham_db_t *hdb, ham_u32_t flags)
{
  Database *db = (Database *)hdb;
  ham_status_t st = HAM_SUCCESS;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if ((flags & HAM_TXN_AUTO_ABORT) && (flags & HAM_TXN_AUTO_COMMIT)) {
    ham_trace(("invalid combination of flags: HAM_TXN_AUTO_ABORT + "
          "HAM_TXN_AUTO_COMMIT"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  Environment *env = db->get_env();

  /* it's ok to close an uninitialized Database */
  if (!env) {
    delete db;
    return (0);
  }

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  /* the function pointer will do the actual implementation */
  st = db->close(flags);
  if (st)
    return (db->set_error(st));

  delete db;
  return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_create(ham_cursor_t **hcursor, ham_db_t *hdb, ham_txn_t *htxn,
        ham_u32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;
  Cursor **cursor = 0;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  cursor = (Cursor **)hcursor;

  env = db->get_env();
  if (!env) {
    ham_trace(("parameter 'db' must be linked to a valid (implicit or "
           "explicit) environment"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  *cursor = db->cursor_create(txn, flags);

  /* fix the linked list of cursors */
  // TODO move this to db->cursor_create()
  (*cursor)->set_next(db->get_cursors());
  if (db->get_cursors())
    db->get_cursors()->set_previous(*cursor);
  db->set_cursors(*cursor);

  if (txn) {
    txn->set_cursor_refcount(txn->get_cursor_refcount() + 1);
    (*cursor)->set_txn(txn);
  }

  return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_clone(ham_cursor_t *hsrc, ham_cursor_t **hdest)
{
  Database *db;

  if (!hsrc) {
    ham_trace(("parameter 'src' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!hdest) {
    ham_trace(("parameter 'dest' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *src, **dest;
  src = (Cursor *)hsrc;
  dest = (Cursor **)hdest;

  db = src->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  *dest = db->cursor_clone(src);

  return (db->set_error(0));
}

ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *hcursor, ham_record_t *record,
        ham_u32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  if (flags) {
    ham_trace(("function does not support a non-zero flags value; "
          "see ham_cursor_insert for an alternative then"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!record) {
    ham_trace(("parameter 'record' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));
  if (db->get_rt_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot overwrite in a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }

  return (db->set_error(db->cursor_overwrite(cursor, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *hcursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Database *db;
  Environment *env;
  ham_status_t st;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  if ((flags & HAM_ONLY_DUPLICATES) && (flags & HAM_SKIP_DUPLICATES)) {
    ham_trace(("combination of HAM_ONLY_DUPLICATES and "
          "HAM_SKIP_DUPLICATES not allowed"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  env = db->get_env();

  if ((flags & HAM_DIRECT_ACCESS)
      && !(env->get_flags() & HAM_IN_MEMORY)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DIRECT_ACCESS)
      && (env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (key && !__prepare_key(key))
    return (db->set_error(HAM_INV_PARAMETER));
  if (record && !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  st = db->cursor_move(cursor, key, record, flags);

  /* make sure that the changeset is empty */
  ham_assert(env->get_changeset().is_empty());

  return (db->set_error(st));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *hcursor, ham_key_t *key, ham_record_t *record,
        ham_u32_t flags)
{
  Database *db;
  Environment *env;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();
  env = db->get_env();

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->get_mutex());

  if (!key) {
    ham_trace(("parameter 'key' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DIRECT_ACCESS)
      && !(env->get_flags() & HAM_IN_MEMORY)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_FIND_NEAR_MATCH)
      && (env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("approx. matching is not allowed if Transactions "
           "are enabled"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DIRECT_ACCESS)
      && (env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_PREPEND) {
    ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
           "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_APPEND) {
    ham_trace(("flag HAM_HINT_APPEND is only allowed in "
           "ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (key && !__prepare_key(key))
    return (db->set_error(HAM_INV_PARAMETER));
  if (record && !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->cursor_find(cursor, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *hcursor, ham_key_t *key, ham_record_t *record,
        ham_u32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();
  ScopedLock lock(db->get_env()->get_mutex());

  if (!key) {
    ham_trace(("parameter 'key' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!record) {
    ham_trace(("parameter 'record' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags&HAM_HINT_APPEND) && (flags&HAM_HINT_PREPEND)) {
    ham_trace(("flags HAM_HINT_APPEND and HAM_HINT_PREPEND "
           "are mutually exclusive"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (!__prepare_key(key) || !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  if (db->get_rt_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot insert to a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }
  if ((db->get_rt_flags() & HAM_DISABLE_VAR_KEYLEN) &&
      (key->size > db->get_keysize())) {
    ham_trace(("database does not support variable length keys"));
    return (db->set_error(HAM_INV_KEYSIZE));
  }
  if ((flags & HAM_DUPLICATE) && (flags & HAM_OVERWRITE)) {
    ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DUPLICATE)
      && !(db->get_rt_flags() & HAM_ENABLE_DUPLICATES)) {
    ham_trace(("database does not support duplicate keys "
          "(see HAM_ENABLE_DUPLICATES)"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags&HAM_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size)) {
    ham_trace(("partial offset+size is greater than the total "
          "record size"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (record->size <= sizeof(ham_u64_t))) {
    ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
          "<= 8"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  /*
   * set flag HAM_DUPLICATE if one of DUPLICATE_INSERT* is set, but do
   * not allow these flags if duplicate sorting is enabled
   */
  if (flags & (HAM_DUPLICATE_INSERT_AFTER
        | HAM_DUPLICATE_INSERT_BEFORE
        | HAM_DUPLICATE_INSERT_LAST
        | HAM_DUPLICATE_INSERT_FIRST)) {
    flags |= HAM_DUPLICATE;
  }

  /* allocate temp. storage for a recno key */
  if (db->get_rt_flags() & HAM_RECORD_NUMBER) {
    if (flags & HAM_OVERWRITE) {
      if (key->size != sizeof(ham_u64_t) || !key->data) {
        ham_trace(("key->size must be 8, key->data must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
      }
    }
    else {
      if (key->flags & HAM_KEY_USER_ALLOC) {
        if (!key->data || key->size != sizeof(ham_u64_t)) {
          ham_trace(("key->size must be 8, key->data must not be NULL"));
          return (db->set_error(HAM_INV_PARAMETER));
        }
      }
      else {
        if (key->data || key->size) {
          ham_trace(("key->size must be 0, key->data must be NULL"));
          return (db->set_error(HAM_INV_PARAMETER));
        }
      }
    }
  }

  return (db->set_error(db->cursor_insert(cursor, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *hcursor, ham_u32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  if (db->get_rt_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot erase from a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }
  if (flags & HAM_HINT_PREPEND) {
    ham_trace(("flags HAM_HINT_PREPEND is only allowed in ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if (flags & HAM_HINT_APPEND) {
    ham_trace(("flags HAM_HINT_APPEND is only allowed in ham_cursor_insert"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  return (db->set_error(db->cursor_erase(cursor, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *hcursor,
        ham_size_t *count, ham_u32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  if (!count) {
    ham_trace(("parameter 'count' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  *count = 0;

  return (db->set_error(db->cursor_get_duplicate_count(cursor, count, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_record_size(ham_cursor_t *hcursor, ham_u64_t *size)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  if (!size) {
    ham_trace(("parameter 'size' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  *size = 0;

  return (db->set_error(db->cursor_get_record_size(cursor, size)));
}

ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *hcursor)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->get_mutex());

  db->cursor_close(cursor);

  return (0);
}

void HAM_CALLCONV
ham_set_context_data(ham_db_t *hdb, void *data)
{
  Database *db = (Database *)hdb;

  if (!db)
    return;

  ScopedLock lock(db->get_env()->get_mutex());
  db->set_context_data(data);
}

void * HAM_CALLCONV
ham_get_context_data(ham_db_t *hdb, ham_bool_t dont_lock)
{
  Database *db = (Database *)hdb;
  if (!db)
    return (0);

  if (dont_lock)
    return (db->get_context_data());

  ScopedLock lock(db->get_env()->get_mutex());
  return (db->get_context_data());
}

void HAM_CALLCONV
ham_env_set_context_data(ham_env_t *henv, void *data)
{
  Environment *env = (Environment *)henv;
  if (!env)
    return;

  ScopedLock lock(env->get_mutex());
  env->set_context_data(data);
}

void * HAM_CALLCONV
ham_env_get_context_data(ham_env_t *henv)
{
  Environment *env = (Environment *)henv;
  if (!env)
    return (0);

  ScopedLock lock(env->get_mutex());
  return (env->get_context_data());
}


ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *hcursor)
{
  if (hcursor) {
    Cursor *cursor = (Cursor *)hcursor;
    return ((ham_db_t *)cursor->get_db());
  }
  else
    return (0);
}

ham_env_t * HAM_CALLCONV
ham_db_get_env(ham_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (!db)
    return (0);

  return ((ham_env_t *)db->get_env());
}

ham_status_t HAM_CALLCONV
ham_db_get_key_count(ham_db_t *hdb, ham_txn_t *htxn, ham_u32_t flags,
      ham_u64_t *keycount)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (!keycount) {
    ham_trace(("parameter 'keycount' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  *keycount = 0;

  ScopedLock lock(db->get_env()->get_mutex());

  return (db->set_error(db->get_key_count(txn, flags, keycount)));
}

void HAM_CALLCONV
ham_set_errhandler(ham_errhandler_fun f)
{
  if (f)
    hamsterdb::g_handler = f;
  else
    hamsterdb::g_handler = hamsterdb::default_errhandler;
}

ham_status_t HAM_CALLCONV
ham_env_get_metrics(ham_env_t *henv, ham_env_metrics_t *metrics)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!metrics) {
    ham_trace(("parameter 'metrics' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  memset(metrics, 0, sizeof(ham_env_metrics_t));
  metrics->version = HAM_METRICS_VERSION;

  ScopedLock lock(env->get_mutex());
  // fill in memory metrics
  Memory::get_global_metrics(metrics);
  // ... and everything else
  env->get_metrics(metrics);

  return (0);
}

