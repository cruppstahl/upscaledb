/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "0root/root.h"

#include <stdlib.h>
#include <string.h>

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "1mem/mem.h"
#include "2config/db_config.h"
#include "2config/env_config.h"
#include "2page/page.h"
#ifdef HAM_ENABLE_REMOTE
#  include "2protobuf/protocol.h"
#endif
#include "2device/device.h"
#include "3btree/btree_stats.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "4cursor/cursor.h"
#include "4db/db.h"
#include "4env/env.h"
#include "4env/env_header.h"
#include "4env/env_local.h"
#include "4env/env_remote.h"
#include "4txn/txn.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

/* return true if the filename is for a local file */
static bool
filename_is_local(const char *filename)
{
  return (!filename || strstr(filename, "ham://") != filename);
}

ham_status_t
ham_txn_begin(ham_txn_t **htxn, ham_env_t *henv, const char *name,
                void *, uint32_t flags)
{
  Transaction **ptxn = (Transaction **)htxn;

  if (!ptxn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *ptxn = 0;

  if (!henv) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = (Environment *)henv;

  return (env->txn_begin(ptxn, name, flags));
}

HAM_EXPORT const char *
ham_txn_get_name(ham_txn_t *htxn)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn)
    return (0);

  const std::string &name = txn->get_env()->txn_get_name(txn);
  return (name.empty() ? 0 : name.c_str());
}

ham_status_t
ham_txn_commit(ham_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  return (env->txn_commit(txn, flags));
}

ham_status_t
ham_txn_abort(ham_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ham_trace(("parameter 'txn' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  return (env->txn_abort(txn, flags));
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
  if (unlikely(key->size && !key->data)) {
    ham_trace(("key->size != 0, but key->data is NULL"));
    return (false);
  }
  if (unlikely(key->flags != 0 && key->flags != HAM_KEY_USER_ALLOC)) {
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
  if (unlikely(record->size && !record->data)) {
    ham_trace(("record->size != 0, but record->data is NULL"));
    return false;
  }
  if (unlikely(record->flags & HAM_DIRECT_ACCESS))
    record->flags &= ~HAM_DIRECT_ACCESS;
  if (unlikely(record->flags != 0 && record->flags != HAM_RECORD_USER_ALLOC)) {
    ham_trace(("invalid flag in record->flags"));
    return (false);
  }
  return (true);
}

void HAM_CALLCONV
ham_get_version(uint32_t *major, uint32_t *minor, uint32_t *revision)
{
  if (major)
    *major = HAM_VERSION_MAJ;
  if (minor)
    *minor = HAM_VERSION_MIN;
  if (revision)
    *revision = HAM_VERSION_REV;
}

ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t **henv, const char *filename,
                uint32_t flags, uint32_t mode, const ham_parameter_t *param)
{
  EnvironmentConfiguration config;
  config.filename = filename ? filename : "";
  config.file_mode = mode;

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

  if (flags & HAM_ENABLE_CRC32) {
    ham_trace(("Crc32 is only available in hamsterdb pro"));
    return (HAM_NOT_IMPLEMENTED);
  }

  /* HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY, unless explicitly
   * disabled */
  if ((flags & HAM_ENABLE_TRANSACTIONS) && !(flags & HAM_DISABLE_RECOVERY))
    flags |= HAM_ENABLE_RECOVERY;

  /* flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_AUTO_RECOVERY)
    flags |= HAM_ENABLE_RECOVERY;

  /* in-memory with Transactions? disable recovery */
  if (flags & HAM_IN_MEMORY)
    flags &= ~HAM_ENABLE_RECOVERY;

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case HAM_PARAM_JOURNAL_COMPRESSION:
        ham_trace(("Journal compression is only available in hamsterdb pro"));
        return (HAM_NOT_IMPLEMENTED);
      case HAM_PARAM_CACHE_SIZE:
        if (flags & HAM_IN_MEMORY && param->value != 0) {
          ham_trace(("combination of HAM_IN_MEMORY and cache size != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        /* don't allow cache limits with unlimited cache */
        if (flags & HAM_CACHE_UNLIMITED && param->value != 0) {
          ham_trace(("combination of HAM_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        if (param->value > 0)
          config.cache_size_bytes = (size_t)param->value;
        break;
      case HAM_PARAM_PAGE_SIZE:
        if (param->value != 1024 && param->value % 2048 != 0) {
          ham_trace(("invalid page size - must be 1024 or a multiple of 2048"));
          return (HAM_INV_PAGESIZE);
        }
        if (param->value > 0)
          config.page_size_bytes = (uint32_t)param->value;
        break;
      case HAM_PARAM_FILE_SIZE_LIMIT:
        if (param->value > 0)
          config.file_size_limit_bytes = (size_t)param->value;
        break;
      case HAM_PARAM_JOURNAL_SWITCH_THRESHOLD:
        config.journal_switch_threshold = (uint32_t)param->value;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        config.log_filename = (const char *)param->value;
        break;
      case HAM_PARAM_NETWORK_TIMEOUT_SEC:
        config.remote_timeout_sec = (uint32_t)param->value;
        break;
      case HAM_PARAM_ENCRYPTION_KEY:
        ham_trace(("Encryption is only available in hamsterdb pro"));
        return (HAM_NOT_IMPLEMENTED);
      case HAM_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  if (config.filename.empty() && !(flags & HAM_IN_MEMORY)) {
    ham_trace(("filename is missing"));
    return (HAM_INV_PARAMETER);
  }

  config.flags = flags;

  /*
   * make sure that max_databases actually fit in a header
   * page!
   * leave at least 128 bytes for other header data
   */
  config.max_databases = config.page_size_bytes
          - sizeof(PEnvironmentHeader) - 128;
  config.max_databases /= sizeof(PBtreeHeader);

  ham_status_t st = 0;
  Environment *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnvironment(config);
  }
  else {
#ifndef HAM_ENABLE_REMOTE
    return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
    env = new RemoteEnvironment(config);
#endif
  }

#ifdef HAM_ENABLE_REMOTE
  atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  st = env->create();

  /* flush the environment to make sure that the header page is written
   * to disk TODO required?? */
  if (st == 0)
    st = env->flush(0);

  if (st) {
    env->close(HAM_AUTO_CLEANUP);
    delete env;
    return (st);
  }
 
  *henv = (ham_env_t *)env;
  return (0);
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *henv, ham_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ham_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  DatabaseConfiguration config;

  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *hdb = 0;

  if (!db_name || (db_name >= 0xf000)) {
    ham_trace(("invalid database name"));
    return (HAM_INV_PARAMETER);
  }

  config.db_name = db_name;
  config.flags = flags;

  return (env->create_db((Database **)hdb, config, param));
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *henv, ham_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ham_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  DatabaseConfiguration config;

  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  *hdb = 0;

  if (!db_name) {
    ham_trace(("parameter 'db_name' must not be 0"));
    return (HAM_INV_PARAMETER);
  }
  if (db_name >= 0xf000) {
    ham_trace(("database name must be lower than 0xf000"));
    return (HAM_INV_PARAMETER);
  }
  if (env->get_flags() & HAM_IN_MEMORY) {
    ham_trace(("cannot open a Database in an In-Memory Environment"));
    return (HAM_INV_PARAMETER);
  }

  config.flags = flags;
  config.db_name = db_name;

  return (env->open_db((Database **)hdb, config, param));
}

ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t **henv, const char *filename, uint32_t flags,
                const ham_parameter_t *param)
{
  EnvironmentConfiguration config;
  config.filename = filename ? filename : "";

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

  /* HAM_ENABLE_DUPLICATE_KEYS has to be specified in ham_env_create_db,
   * not ham_env_open */
  if (flags & HAM_ENABLE_DUPLICATE_KEYS) {
    ham_trace(("invalid flag HAM_ENABLE_DUPLICATE_KEYS (only allowed when "
        "creating a database"));
    return (HAM_INV_PARAMETER);
  }

  if (flags & HAM_ENABLE_CRC32) {
    ham_trace(("Crc32 is only available in hamsterdb pro"));
    return (HAM_NOT_IMPLEMENTED);
  }

  /* HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY, unless explicitly
   * disabled */
  if ((flags & HAM_ENABLE_TRANSACTIONS) && !(flags & HAM_DISABLE_RECOVERY))
    flags |= HAM_ENABLE_RECOVERY;

  /* flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY */
  if (flags & HAM_AUTO_RECOVERY)
    flags |= HAM_ENABLE_RECOVERY;

  if (config.filename.empty() && !(flags & HAM_IN_MEMORY)) {
    ham_trace(("filename is missing"));
    return (HAM_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case HAM_PARAM_JOURNAL_COMPRESSION:
        ham_trace(("Journal compression is only available in hamsterdb pro"));
        return (HAM_NOT_IMPLEMENTED);
      case HAM_PARAM_CACHE_SIZE:
        /* don't allow cache limits with unlimited cache */
        if (flags & HAM_CACHE_UNLIMITED && param->value != 0) {
          ham_trace(("combination of HAM_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return (HAM_INV_PARAMETER);
        }
        if (param->value > 0)
          config.cache_size_bytes = param->value;
        break;
      case HAM_PARAM_FILE_SIZE_LIMIT:
        if (param->value > 0)
          config.file_size_limit_bytes = (size_t)param->value;
        break;
      case HAM_PARAM_JOURNAL_SWITCH_THRESHOLD:
        config.journal_switch_threshold = (uint32_t)param->value;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        config.log_filename = (const char *)param->value;
        break;
      case HAM_PARAM_NETWORK_TIMEOUT_SEC:
        config.remote_timeout_sec = (uint32_t)param->value;
        break;
      case HAM_PARAM_ENCRYPTION_KEY:
        ham_trace(("Encryption is only available in hamsterdb pro"));
        return (HAM_NOT_IMPLEMENTED);
      case HAM_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)param->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  config.flags = flags;

  ham_status_t st = 0;
  Environment *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnvironment(config);
  }
  else {
#ifndef HAM_ENABLE_REMOTE
    return (HAM_NOT_IMPLEMENTED);
#else // HAM_ENABLE_REMOTE
    env = new RemoteEnvironment(config);
#endif
  }

#ifdef HAM_ENABLE_REMOTE
  atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  st = env->open();

  if (st) {
    (void)env->close(HAM_AUTO_CLEANUP);
    delete env;
    return (st);
  }

  *henv = (ham_env_t *)env;
  return (0);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *henv, uint16_t oldname, uint16_t newname,
                uint32_t flags)
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

  /* rename the database */
  return (env->rename_db(oldname, newname, flags));
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *henv, uint16_t name, uint32_t flags)
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

  /* erase the database */
  return (env->erase_db(name, flags));
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *henv, uint16_t *names, uint32_t *count)
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

  if (!param) {
    ham_trace(("parameter 'param' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  /* get the parameters */
  return (env->get_parameters(param));
}

ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *henv, uint32_t flags)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  if (flags && flags != HAM_FLUSH_COMMITTED_TRANSACTIONS) {
    ham_trace(("parameter 'flags' is unused, set to 0"));
    return (HAM_INV_PARAMETER);
  }

  /* flush the Environment */
  return (env->flush(flags));
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *henv, uint32_t flags)
{
  ham_status_t st;
  Environment *env = (Environment *)henv;

  if (!env) {
    ham_trace(("parameter 'env' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  try {
    /* close the environment */
    st = env->close(flags);
    if (st)
      return (st);

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

  ScopedLock lock(db->get_env()->mutex());

  /* get the parameters */
  return (db->set_error(db->get_parameters(param)));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_error(ham_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (0);
  }

  ScopedLock lock;
  if (db->get_env())
    lock = ScopedLock(db->get_env()->mutex());

  return (db->get_error());
}

HAM_EXPORT ham_status_t HAM_CALLCONV
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

  ScopedLock lock(ldb->get_env()->mutex());

  /* set the compare functions */
  return (ldb->set_error(ldb->set_compare_func(foo)));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_find(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  env = db->get_env();

  ScopedLock lock(env->mutex());

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
      && (db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  /* record number: make sure that we have a valid key structure */
  if ((db->get_flags() & HAM_RECORD_NUMBER32) && !key->data) {
    ham_trace(("key->data must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((db->get_flags() & HAM_RECORD_NUMBER64) && !key->data) {
    ham_trace(("key->data must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (!__prepare_key(key) || !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->find(0, txn, key, record, flags)));
}

HAM_EXPORT int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
  if (key && (ham_key_get_intflags(key) & BtreeKey::kApproximate)) {
    int rv = (ham_key_get_intflags(key) & BtreeKey::kLower) ? -1 : +1;
    return (rv);
  }

  return (0);
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_insert(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return HAM_INV_PARAMETER;
  }
  env = db->get_env();

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->mutex());

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
  if (db->get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot insert in a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }
  if ((flags & HAM_OVERWRITE) && (flags & HAM_DUPLICATE)) {
    ham_trace(("cannot combine HAM_OVERWRITE and HAM_DUPLICATE"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL)
      && (db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL) && (record->size <= sizeof(uint64_t))) {
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
      && !(db->get_flags() & HAM_ENABLE_DUPLICATE_KEYS)) {
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
  if ((db->get_flags() & HAM_RECORD_NUMBER32)
      || (db->get_flags() & HAM_RECORD_NUMBER64)) {
    if (flags & HAM_OVERWRITE) {
      if (!key->data) {
        ham_trace(("key->data must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
      }
    }
    else {
      if (key->flags & HAM_KEY_USER_ALLOC) {
        if (!key->data) {
          ham_trace(("key->data must not be NULL"));
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

  return (db->set_error(db->insert(0, txn, key, record, flags)));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_erase(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  env = db->get_env();

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->mutex());

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
  if (db->get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot erase from a read-only database"));
    return (HAM_WRITE_PROTECTED);
  }

  if (!__prepare_key(key))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->erase(0, txn, key, flags)));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_check_integrity(ham_db_t *hdb, uint32_t flags)
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

  ScopedLock lock(db->get_env()->mutex());

  return (db->set_error(db->check_integrity(flags)));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_close(ham_db_t *hdb, uint32_t flags)
{
  Database *db = (Database *)hdb;

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

  return (env->close_db(db, flags));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_create(ham_cursor_t **hcursor, ham_db_t *hdb, ham_txn_t *htxn,
                uint32_t flags)
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

  ScopedLock lock;
  if (!(flags & HAM_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  return (db->set_error(db->cursor_create(cursor, txn, flags)));
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

  ScopedLock lock(db->get_env()->mutex());

  return (db->set_error(db->cursor_clone(dest, src)));
}

ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *hcursor, ham_record_t *record,
                uint32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

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
  if (db->get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot overwrite in a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }

  return (db->set_error(db->cursor_overwrite(cursor, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *hcursor, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  Database *db;
  Environment *env;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

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
      && (db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
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

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *hcursor, ham_key_t *key, ham_record_t *record,
                uint32_t flags)
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
    lock = ScopedLock(env->mutex());

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
      && (db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
          "transactions"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  if (key && !__prepare_key(key))
    return (db->set_error(HAM_INV_PARAMETER));
  if (record && !__prepare_record(record))
    return (db->set_error(HAM_INV_PARAMETER));

  return (db->set_error(db->find(cursor, cursor->get_txn(),
                                    key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *hcursor, ham_key_t *key, ham_record_t *record,
                uint32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

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

  if (db->get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot insert to a read-only database"));
    return (db->set_error(HAM_WRITE_PROTECTED));
  }
  if ((flags & HAM_DUPLICATE) && (flags & HAM_OVERWRITE)) {
    ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_DUPLICATE)
      && !(db->get_flags() & HAM_ENABLE_DUPLICATE_KEYS)) {
    ham_trace(("database does not support duplicate keys "
          "(see HAM_ENABLE_DUPLICATE_KEYS)"));
    return (db->set_error(HAM_INV_PARAMETER));
  }
  if ((flags & HAM_PARTIAL)
      && (db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
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
  if ((flags & HAM_PARTIAL) && (record->size <= sizeof(uint64_t))) {
    ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
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
  if ((db->get_flags() & HAM_RECORD_NUMBER32)
      || (db->get_flags() & HAM_RECORD_NUMBER64)) {
    if (flags & HAM_OVERWRITE) {
      if (!key->data) {
        ham_trace(("key->data must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
      }
    }
    else {
      if (key->flags & HAM_KEY_USER_ALLOC) {
        if (!key->data) {
          ham_trace(("key->data must not be NULL"));
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

  return (db->set_error(db->insert(cursor, cursor->get_txn(), key,
                                  record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *hcursor, uint32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

  if (db->get_flags() & HAM_READ_ONLY) {
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

  return (db->set_error(db->erase(cursor, cursor->get_txn(), 0, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *hcursor, uint32_t *count,
                uint32_t flags)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

  if (!count) {
    ham_trace(("parameter 'count' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  return (db->set_error(db->cursor_get_record_count(cursor, flags, count)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_position(ham_cursor_t *hcursor, uint32_t *position)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

  if (!position) {
    ham_trace(("parameter 'position' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  return (db->set_error(db->cursor_get_duplicate_position(cursor, position)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_record_size(ham_cursor_t *hcursor, uint64_t *size)
{
  Database *db;

  if (!hcursor) {
    ham_trace(("parameter 'cursor' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  db = cursor->get_db();

  ScopedLock lock(db->get_env()->mutex());

  if (!size) {
    ham_trace(("parameter 'size' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

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

  ScopedLock lock(db->get_env()->mutex());

  return (db->set_error(db->cursor_close(cursor)));
}

void HAM_CALLCONV
ham_set_context_data(ham_db_t *hdb, void *data)
{
  Database *db = (Database *)hdb;

  if (!db)
    return;

  ScopedLock lock(db->get_env()->mutex());
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

  ScopedLock lock(db->get_env()->mutex());
  return (db->get_context_data());
}

ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *hcursor)
{
  if (hcursor) {
    Cursor *cursor = (Cursor *)hcursor;
    return ((ham_db_t *)cursor->get_db());
  }
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
ham_db_get_key_count(ham_db_t *hdb, ham_txn_t *htxn, uint32_t flags,
                uint64_t *keycount)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (flags & ~(HAM_SKIP_DUPLICATES)) {
    ham_trace(("parameter 'flag' contains unsupported flag bits: %08x",
          flags & (~HAM_SKIP_DUPLICATES)));
    return (HAM_INV_PARAMETER);
  }
  if (!keycount) {
    ham_trace(("parameter 'keycount' must not be NULL"));
    return (db->set_error(HAM_INV_PARAMETER));
  }

  ScopedLock lock(db->get_env()->mutex());

  return (db->set_error(db->count(txn, (flags & HAM_SKIP_DUPLICATES) != 0,
                  keycount)));
}

void HAM_CALLCONV
ham_set_errhandler(ham_errhandler_fun f)
{
  if (f)
    hamsterdb::Globals::ms_error_handler = f;
  else
    hamsterdb::Globals::ms_error_handler = hamsterdb::default_errhandler;
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

  // fill in memory metrics
  Memory::get_global_metrics(metrics);
  // ... and everything else
  return (env->fill_metrics(metrics));
}

ham_bool_t HAM_CALLCONV
ham_is_debug()
{
#ifdef HAM_DEBUG
  return (HAM_TRUE);
#else
  return (HAM_FALSE);
#endif
}

ham_bool_t HAM_CALLCONV
ham_is_pro()
{
  return (HAM_FALSE);
}

uint32_t HAM_CALLCONV
ham_is_pro_evaluation()
{
  return (0);
}
