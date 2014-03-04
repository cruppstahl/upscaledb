/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#  include "protocol/protocol.h"
#endif

#include "blob_manager.h"
#include "btree_index.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "env_header.h"
#include "env_local.h"
#include "env_remote.h"
#include "error.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "serial.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "version.h"

using namespace hamsterdb;


/* return true if the filename is for a local file */
static bool
__filename_is_local(const char *filename)
{
  if (filename && strstr(filename, "ham://") == filename)
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    if (!(env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
      ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
      return (HAM_INV_PARAMETER);
    }

    /* initialize the txn structure */
    *txn = env->txn_begin(name, flags);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

HAM_EXPORT const char *
ham_txn_get_name(ham_txn_t *htxn)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn)
    return (0);

  try {
    ScopedLock lock(txn->get_env()->get_mutex());
    const std::string &name = txn->get_name();
    if (name.empty())
      return 0;
    else
      return (name.c_str());
  }
  catch (Exception &) {
    return (0);
  }
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    txn->commit(flags);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    txn->abort(flags);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

const char * HAM_CALLCONV
ham_strerror(ham_status_t result)
{
  switch (result) {
    case HAM_SUCCESS:
      return ("Success");
    case HAM_INV_KEY_SIZE:
      return ("Invalid key size");
    case HAM_INV_RECORD_SIZE:
      return ("Invalid record size");
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
    case HAM_IO_ERROR:
      return ("System I/O error");
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
  ham_u32_t page_size = HAM_DEFAULT_PAGESIZE;
  ham_u64_t cache_size = 0;
  ham_u16_t max_databases = 0;
  ham_u32_t timeout = 0;
  std::string logdir;
  ham_u8_t *encryption_key = 0;

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
            | HAM_CACHE_UNLIMITED
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS
            | HAM_DISABLE_RECLAIM_INTERNAL;
  if (flags & ~mask) {
    ham_trace(("ham_env_create() called with invalid flag 0x%x (%d)", 
                (int)(flags & ~mask), (int)(flags & ~mask)));
    return (HAM_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case HAM_PARAM_CACHESIZE:
        cache_size = param->value;
        if (flags & HAM_IN_MEMORY && cache_size != 0) {
          ham_trace(("combination of HAM_IN_MEMORY and cache size != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        break;
      case HAM_PARAM_PAGESIZE:
        if (param->value != 1024 && param->value % 2048 != 0) {
          ham_trace(("invalid page size - must be 1024 or a multiple of 2048"));
          return (HAM_INV_PAGESIZE);
        }
        page_size = (ham_u32_t)param->value;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        logdir = (const char *)param->value;
        break;
      case HAM_PARAM_NETWORK_TIMEOUT_SEC:
        timeout = (ham_u32_t)param->value;
        break;
      case HAM_PARAM_ENCRYPTION_KEY:
#ifdef HAM_ENABLE_ENCRYPTION
        /* in-memory? encryption is not possible */
        if (flags & HAM_IN_MEMORY) {
          ham_trace(("aes encryption not allowed in combination with "
                  "HAM_IN_MEMORY"));
          return (HAM_INV_PARAMETER);
        }
        encryption_key = (ham_u8_t *)param->value;
        flags |= HAM_DISABLE_MMAP;
#else
        ham_trace(("aes encrpytion was disabled at compile time"));
        return (HAM_NOT_IMPLEMENTED);
#endif
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  /* don't allow cache limits with unlimited cache */
  if (flags & HAM_CACHE_UNLIMITED && cache_size != 0) {
    ham_trace(("combination of HAM_CACHE_UNLIMITED and cache size != 0 "
          "not allowed"));
    return (HAM_INV_PARAMETER);
  }

  if (cache_size == 0)
    cache_size = HAM_DEFAULT_CACHESIZE;
  if (page_size == 0)
    page_size = HAM_DEFAULT_PAGESIZE;

  if (!filename && !(flags & HAM_IN_MEMORY)) {
    ham_trace(("filename is missing"));
    return (HAM_INV_PARAMETER);
  }

  /*
   * make sure that max_databases actually fit in a header
   * page!
   * leave at least 128 bytes for other header data
   */
  max_databases = page_size - sizeof(PEnvironmentHeader) - 128;
  max_databases /= sizeof(PBtreeHeader);

  ham_status_t st = 0;
  Environment *env = 0;
  try {
    if (__filename_is_local(filename)) {
      LocalEnvironment *lenv = new LocalEnvironment();
      env = lenv;
      if (logdir.size())
        lenv->set_log_directory(logdir);
      if (encryption_key)
        lenv->enable_encryption(encryption_key);
    }
    else {
#ifndef HAM_ENABLE_REMOTE
      return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
      RemoteEnvironment *renv = new RemoteEnvironment();
      if (timeout)
        renv->set_timeout(timeout);
      env = renv;
#endif
    }

#ifdef HAM_ENABLE_REMOTE
    atexit(Protocol::shutdown);
#endif

    /* and finish the initialization of the Environment */
    st = env->create(filename, flags, mode, page_size,
                    cache_size, max_databases);

    /* flush the environment to make sure that the header page is written
     * to disk */
    if (st == 0)
      st = env->flush(0);
  }
  catch (Exception &ex) {
    st = ex.code;
  }

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

  if (!dbname || (dbname >= 0xf000)) {
    ham_trace(("invalid database name"));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(env->get_mutex());

    /* the function handler will do the rest */
    st = env->create_db((Database **)hdb, dbname, flags, param);

    /* flush the environment to make sure that the header page is written
     * to disk */
    if (st == 0)
      st = env->flush(0);
  }
  catch (Exception &ex) {
    st = ex.code;
  }

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
  if (dbname >= 0xf000) {
    ham_trace(("database name must be lower than 0xf000"));
    return (HAM_INV_PARAMETER);
  }
  if (env->get_flags() & HAM_IN_MEMORY) {
    ham_trace(("cannot open a Database in an In-Memory Environment"));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    /* the function handler will do the rest */
    st = env->open_db((Database **)hdb, dbname, flags, param);
  }
  catch (Exception &ex) {
    st = ex.code;
  }

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
  ham_u64_t cache_size = 0;
  ham_u32_t timeout = 0;
  std::string logdir;
  ham_u8_t *encryption_key = 0;

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

  /* HAM_ENABLE_DUPLICATE_KEYS has to be specified in ham_create, not ham_open */
  if (flags & HAM_ENABLE_DUPLICATE_KEYS) {
    ham_trace(("invalid flag HAM_ENABLE_DUPLICATE_KEYS (only allowed when "
        "creating a database"));
    return (HAM_INV_PARAMETER);
  }

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
        cache_size = param->value;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        logdir = (const char *)param->value;
        break;
      case HAM_PARAM_NETWORK_TIMEOUT_SEC:
        timeout = (ham_u32_t)param->value;
        break;
      case HAM_PARAM_ENCRYPTION_KEY:
#ifdef HAM_ENABLE_ENCRYPTION
        encryption_key = (ham_u8_t *)param->value;
        flags |= HAM_DISABLE_MMAP;
#else
        ham_trace(("aes encryption was disabled at compile time"));
        return (HAM_NOT_IMPLEMENTED);
#endif
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  /* don't allow cache limits with unlimited cache */
  if (flags & HAM_CACHE_UNLIMITED && cache_size != 0) {
    ham_trace(("combination of HAM_CACHE_UNLIMITED and cache size != 0 "
          "not allowed"));
    return (HAM_INV_PARAMETER);
  }

  if (cache_size == 0)
    cache_size = HAM_DEFAULT_CACHESIZE;

  ham_status_t st = 0;
  Environment *env = 0;

  try {
    if (__filename_is_local(filename)) {
      LocalEnvironment *lenv = new LocalEnvironment();
      env = lenv;
      if (logdir.size())
        lenv->set_log_directory(logdir);
      if (encryption_key)
        lenv->enable_encryption(encryption_key);
    }
    else {
#ifndef HAM_ENABLE_REMOTE
      return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
      RemoteEnvironment *renv = new RemoteEnvironment();
      if (timeout)
        renv->set_timeout(timeout);
      env = renv;
#endif
    }

#ifdef HAM_ENABLE_REMOTE
    atexit(Protocol::shutdown);
#endif

    /* and finish the initialization of the Environment */
    st = env->open(filename, flags, cache_size);
  }
  catch (Exception &ex) {
    st = ex.code;
  }

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

  if (!oldname) {
    ham_trace(("parameter 'oldname' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (!newname) {
    ham_trace(("parameter 'newname' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (newname >= 0xf000) {
    ham_trace(("parameter 'newname' must be lower than 0xf000"));
    return (HAM_INV_PARAMETER);
  }

  /* no need to do anything if oldname==newname */
  if (oldname == newname)
    return (0);

  try {
    ScopedLock lock(env->get_mutex());

    /* rename the database */
    return (env->rename_db(oldname, newname, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *henv, ham_u16_t name, ham_u32_t flags)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (!name) {
    ham_trace(("parameter 'name' must not be 0"));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(env->get_mutex());

    /* erase the database */
    return (env->erase_db(name, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *henv, ham_u16_t *names, ham_u32_t *count)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (!names) {
    ham_trace(("parameter 'names' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!count) {
    ham_trace(("parameter 'count' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(env->get_mutex());

    /* get all database names */
    return (env->get_database_names(names, count));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *henv, ham_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (!param) {
    ham_trace(("parameter 'param' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(env->get_mutex());

    /* get the parameters */
    return (env->get_parameters(param));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock = ScopedLock(env->get_mutex());

    /* flush the Environment */
    return (env->flush(flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock = ScopedLock(env->get_mutex());

#ifdef HAM_DEBUG
    /* make sure that the changeset is empty */
    LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(env);
    if (lenv)
      ham_assert(lenv->get_changeset().is_empty());
#endif

    /* auto-abort (or commit) all pending transactions */
    if (env && env->get_oldest_txn()) {
      Transaction *n, *t = env->get_oldest_txn();
      while (t) {
        n = t->get_next();
        if (t->is_aborted() || t->is_committed())
          ; /* nop */
        else {
          if (flags & HAM_TXN_AUTO_COMMIT)
            t->commit(0);
          else /* if (flags&HAM_TXN_AUTO_ABORT) */
            t->abort(0);
        }
        t = n;
      }
    }

    /* close the environment */
    st = env->close(flags);
    if (st)
      return (st);

    lock.unlock();

    delete env;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_parameters(ham_db_t *hdb, ham_parameter_t *param)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (!param) {
    ham_trace(("parameter 'param' must not be NULL"));
    return HAM_INV_PARAMETER;
  }

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    /* get the parameters */
    return (db->get_parameters(param));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_db_get_error(ham_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (0);
  }

  try {
    ScopedLock lock;
    if (db->get_env())
      lock = ScopedLock(db->get_env()->get_mutex());

    return (db->get_error());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_db_set_compare_func(ham_db_t *hdb, ham_compare_func_t foo)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!foo) {
    ham_trace(("function pointer must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  LocalDatabase *ldb = dynamic_cast<LocalDatabase *>(db);
  if (!ldb) {
    ham_trace(("operation not possible for remote databases"));
    return (HAM_INV_PARAMETER); 
  }

  try {
    ScopedLock lock;
    if (ldb->get_env())
      lock = ScopedLock(ldb->get_env()->get_mutex());

    /* set the compare functions */
    return (ldb->set_error(ldb->set_compare_func(foo)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
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
    if ((flags & HAM_PARTIAL)
        && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
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
  catch (Exception &ex) {
    return (ex.code);
  }
}

int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
  if (key && (ham_key_get_intflags(key) & BtreeKey::kApproximate)) {
    int rv = (ham_key_get_intflags(key) & BtreeKey::kLower) ? -1 : +1;
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

  try {
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
    if ((flags & HAM_OVERWRITE) && (flags & HAM_DUPLICATE)) {
      ham_trace(("cannot combine HAM_OVERWRITE and HAM_DUPLICATE"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_PARTIAL)
        && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
      ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
            "transactions"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_PARTIAL) && (record->size <= sizeof(ham_u64_t))) {
      ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
            "<= 8"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_PARTIAL)
        && (record->partial_size + record->partial_offset > record->size)) {
      ham_trace(("partial offset+size is greater than the total "
            "record size"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_DUPLICATE)
        && !(db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS)) {
      ham_trace(("database does not support duplicate keys "
            "(see HAM_ENABLE_DUPLICATE_KEYS)"));
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
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    if (!key) {
      ham_trace(("parameter 'key' must not be NULL"));
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
    if (db->get_rt_flags() & HAM_READ_ONLY) {
      ham_trace(("cannot erase from a read-only database"));
      return (HAM_WRITE_PROTECTED);
    }

    if (!__prepare_key(key))
      return (db->set_error(HAM_INV_PARAMETER));

    return (db->set_error(db->erase(txn, key, flags)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_db_check_integrity(ham_db_t *hdb, ham_u32_t flags)
{
  Database *db = (Database *)hdb;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (flags && flags != HAM_PRINT_GRAPH) {
    ham_trace(("unknown flag 0x%u", flags));
    return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    return (db->set_error(db->check_integrity(flags)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    /* the function pointer will do the actual implementation */
    st = db->close(flags);
    if (st)
      return (db->set_error(st));

    ham_u16_t dbname = db->get_name();
    delete db;

    /* in-memory database: make sure that a database with the same name
     * can be re-created */
    if (env->get_flags() & HAM_IN_MEMORY)
      (void)env->erase_db(dbname, 0);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock;
    if (!(flags & HAM_DONT_LOCK))
      lock = ScopedLock(env->get_mutex());

    *cursor = db->cursor_create(txn, flags);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    *dest = db->cursor_clone(src);

    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
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
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *hcursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Database *db;
  Environment *env;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  try {
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
    if ((flags & HAM_PARTIAL)
        && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
      ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
            "transactions"));
      return (db->set_error(HAM_INV_PARAMETER));
    }

    if (key && !__prepare_key(key))
      return (db->set_error(HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
      return (db->set_error(HAM_INV_PARAMETER));

    return (db->set_error(db->cursor_move(cursor, key, record, flags)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
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
    if ((flags & HAM_PARTIAL)
        && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
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
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
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
    if ((flags & HAM_DUPLICATE) && (flags & HAM_OVERWRITE)) {
      ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_DUPLICATE)
        && !(db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS)) {
      ham_trace(("database does not support duplicate keys "
            "(see HAM_ENABLE_DUPLICATE_KEYS)"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags & HAM_PARTIAL)
        && (db->get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
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
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_rt_flags() & HAM_READ_ONLY) {
      ham_trace(("cannot erase from a read-only database"));
      return (db->set_error(HAM_WRITE_PROTECTED));
    }
    if (flags & HAM_HINT_PREPEND) {
      ham_trace(("flags HAM_HINT_PREPEND only allowed in ham_cursor_insert"));
      return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags & HAM_HINT_APPEND) {
      ham_trace(("flags HAM_HINT_APPEND only allowed in ham_cursor_insert"));
      return (db->set_error(HAM_INV_PARAMETER));
    }

    return (db->set_error(db->cursor_erase(cursor, flags)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *hcursor, ham_u32_t *count,
                ham_u32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    if (!count) {
      ham_trace(("parameter 'count' must not be NULL"));
      return (db->set_error(HAM_INV_PARAMETER));
    }

    *count = 0;

    return (db->set_error(db->cursor_get_record_count(cursor,
                                    count, flags)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    if (!size) {
      ham_trace(("parameter 'size' must not be NULL"));
      return (db->set_error(HAM_INV_PARAMETER));
    }

    *size = 0;

    return (db->set_error(db->cursor_get_record_size(cursor, size)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    db->cursor_close(cursor);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    return (db->set_error(db->get_key_count(txn, flags, keycount)));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
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

ham_bool_t HAM_CALLCONV
ham_is_debug_build()
{
#ifdef HAM_DEBUG
  return ((ham_bool_t)1);
#else
  return ((ham_bool_t)0);
#endif
}
