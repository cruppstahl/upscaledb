/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

#include <stdlib.h>
#include <string.h>

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "1globals/callbacks.h"
#include "1mem/mem.h"
#include "2config/db_config.h"
#include "2config/env_config.h"
#include "2page/page.h"
#ifdef UPS_ENABLE_REMOTE
#  include "2protobuf/protocol.h"
#endif
#include "2compressor/compressor_factory.h"
#include "2device/device.h"
#include "3btree/btree_stats.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "4cursor/cursor.h"
#include "4db/db_local.h"
#include "4env/env_header.h"
#include "4env/env_local.h"
#include "4env/env_remote.h"
#include "4txn/txn.h"
#include "4uqi/plugins.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

static bool
filename_is_local(const char *filename)
{
  return !filename || ::strstr(filename, "ups://") != filename;
}

static inline bool
prepare_key(ups_key_t *key)
{
  if (unlikely(key->size && !key->data)) {
    ups_trace(("key->size != 0, but key->data is NULL"));
    return false;
  }
  if (unlikely(key->flags != 0 && key->flags != UPS_KEY_USER_ALLOC)) {
    ups_trace(("invalid flag in key->flags"));
    return false;
  }
  key->_flags = 0;
  return true;
}

static inline bool
prepare_record(ups_record_t *record)
{
  if (unlikely(record->size && !record->data)) {
    ups_trace(("record->size != 0, but record->data is NULL"));
    return false;
  }
  if (unlikely(record->flags != 0 && record->flags != UPS_RECORD_USER_ALLOC)) {
    ups_trace(("invalid flag in record->flags"));
    return false;
  }
  return true;
}

static inline ups_status_t
check_recno_key(ups_key_t *key, uint32_t flags)
{
  if (ISSET(flags, UPS_OVERWRITE)) {
    if (unlikely(!key->data)) {
      ups_trace(("key->data must not be NULL"));
      return UPS_INV_PARAMETER;
    }
  }
  else {
    if (ISSET(key->flags, UPS_KEY_USER_ALLOC)) {
      if (unlikely(!key->data)) {
        ups_trace(("key->data must not be NULL"));
        return UPS_INV_PARAMETER;
      }
    }
    else {
      if (unlikely(key->data || key->size)) {
        ups_trace(("key->size must be 0, key->data must be NULL"));
        return UPS_INV_PARAMETER;
      }
    }
  }

  return 0;
}

ups_status_t
ups_txn_begin(ups_txn_t **htxn, ups_env_t *henv, const char *name,
                void *, uint32_t flags)
{
  Txn **ptxn = (Txn **)htxn;

  if (unlikely(!ptxn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!henv)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Env *env = (Env *)henv;

  try {
    ScopedLock lock;
    if (NOTSET(flags, UPS_DONT_LOCK))
      lock = ScopedLock(env->mutex);

    if (unlikely(NOTSET(env->config.flags, UPS_ENABLE_TRANSACTIONS))) {
      ups_trace(("transactions are disabled (see UPS_ENABLE_TRANSACTIONS)"));
      return UPS_INV_PARAMETER;
    }

    *ptxn = env->txn_begin(name, flags);
    return 0;
  }
  catch (Exception &ex) {
    *ptxn = 0;
    return ex.code;
  }
}

UPS_EXPORT const char *
ups_txn_get_name(ups_txn_t *htxn)
{
  Txn *txn = (Txn *)htxn;
  if (unlikely(!txn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return 0;
  }

  return txn->name.empty() ? 0 : txn->name.c_str();
}

ups_status_t
ups_txn_commit(ups_txn_t *htxn, uint32_t flags)
{
  Txn *txn = (Txn *)htxn;
  if (unlikely(!txn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Env *env = txn->env;

  try {
    ScopedLock lock(env->mutex);
    return env->txn_commit(txn, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t
ups_txn_abort(ups_txn_t *htxn, uint32_t flags)
{
  if (unlikely(!htxn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Txn *txn = (Txn *)htxn;
  Env *env = txn->env;
  try {
    ScopedLock lock(env->mutex);
    return env->txn_abort(txn, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

const char * UPS_CALLCONV
ups_strerror(ups_status_t result)
{
  switch (result) {
    case UPS_SUCCESS:
      return "Success";
    case UPS_INV_KEY_SIZE:
      return "Invalid key size";
    case UPS_INV_RECORD_SIZE:
      return "Invalid record size";
    case UPS_INV_PAGESIZE:
      return "Invalid page size";
    case UPS_OUT_OF_MEMORY:
      return "Out of memory";
    case UPS_INV_PARAMETER:
      return "Invalid parameter";
    case UPS_INV_FILE_HEADER:
      return "Invalid database file header";
    case UPS_INV_FILE_VERSION:
      return "Invalid database file version";
    case UPS_KEY_NOT_FOUND:
      return "Key not found";
    case UPS_DUPLICATE_KEY:
      return "Duplicate key";
    case UPS_INTEGRITY_VIOLATED:
      return "Internal integrity violated";
    case UPS_INTERNAL_ERROR:
      return "Internal error";
    case UPS_WRITE_PROTECTED:
      return "Database opened in read-only mode";
    case UPS_BLOB_NOT_FOUND:
      return "Data blob not found";
    case UPS_IO_ERROR:
      return "System I/O error";
    case UPS_NOT_IMPLEMENTED:
      return "Operation not implemented";
    case UPS_FILE_NOT_FOUND:
      return "File not found";
    case UPS_WOULD_BLOCK:
      return "Operation would block";
    case UPS_NOT_READY:
      return "Object was not initialized correctly";
    case UPS_CURSOR_STILL_OPEN:
      return "Cursor must be closed prior to Transaction abort/commit";
    case UPS_FILTER_NOT_FOUND:
      return "Record filter or file filter not found";
    case UPS_TXN_CONFLICT:
      return "Operation conflicts with another Transaction";
    case UPS_TXN_STILL_OPEN:
      return "Database cannot be closed because it is modified in a Transaction";
    case UPS_CURSOR_IS_NIL:
      return "Cursor points to NIL";
    case UPS_DATABASE_NOT_FOUND:
      return "Database not found";
    case UPS_DATABASE_ALREADY_EXISTS:
      return "Database name already exists";
    case UPS_DATABASE_ALREADY_OPEN:
      return "Database already open, or: Database handle "
          "already initialized";
    case UPS_ENVIRONMENT_ALREADY_OPEN:
      return "Environment already open, or: Environment handle "
          "already initialized";
    case UPS_LIMITS_REACHED:
      return "Database limits reached";
    case UPS_ALREADY_INITIALIZED:
      return "Object was already initialized";
    case UPS_NEED_RECOVERY:
      return "Database needs recovery";
    case UPS_LOG_INV_FILE_HEADER:
      return "Invalid log file header";
    case UPS_NETWORK_ERROR:
      return "Remote I/O error/Network error";
    default:
      return "Unknown error";
  }
}

void UPS_CALLCONV
ups_get_version(uint32_t *major, uint32_t *minor, uint32_t *revision)
{
  if (likely(major != 0))
    *major = UPS_VERSION_MAJ;
  if (likely(minor != 0))
    *minor = UPS_VERSION_MIN;
  if (likely(revision != 0))
    *revision = UPS_VERSION_REV;
}

ups_status_t UPS_CALLCONV
ups_env_create(ups_env_t **henv, const char *filename,
                uint32_t flags, uint32_t mode, const ups_parameter_t *param)
{
  EnvConfig config;
  config.filename = filename ? filename : "";
  config.file_mode = mode;

  if (unlikely(!henv)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  *henv = 0;

  /* creating a file in READ_ONLY mode? doesn't make sense */
  if (unlikely(ISSET(flags, UPS_READ_ONLY))) {
    ups_trace(("cannot create a file in read-only mode"));
    return UPS_INV_PARAMETER;
  }

  /* in-memory? crc32 is not possible */
  if (unlikely(ISSET(flags, UPS_IN_MEMORY) && ISSET(flags, UPS_ENABLE_CRC32))) {
    ups_trace(("combination of UPS_IN_MEMORY and UPS_ENABLE_CRC32 "
            "not allowed"));
    return UPS_INV_PARAMETER;
  }

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_TRANSACTIONS */
  if (ISSET(flags, UPS_AUTO_RECOVERY))
    flags |= UPS_ENABLE_TRANSACTIONS;

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case UPS_PARAM_JOURNAL_COMPRESSION:
        if (!CompressorFactory::is_available((int)param->value)) {
          ups_trace(("unknown algorithm for journal compression"));
          return UPS_INV_PARAMETER;
        }
        config.journal_compressor = (int)param->value;
        break;
      case UPS_PARAM_CACHESIZE:
        if (ISSET(flags, UPS_IN_MEMORY) && param->value != 0) {
          ups_trace(("combination of UPS_IN_MEMORY and cache size != 0 "
                "not allowed"));
          return UPS_INV_PARAMETER;
        }
        /* don't allow cache limits with unlimited cache */
        if (ISSET(flags, UPS_CACHE_UNLIMITED) && param->value != 0) {
          ups_trace(("combination of UPS_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return UPS_INV_PARAMETER;
        }
        if (param->value > 0)
          config.cache_size_bytes = (size_t)param->value;
        break;
      case UPS_PARAM_PAGE_SIZE:
        if (param->value != 1024 && param->value % 2048 != 0) {
          ups_trace(("invalid page size - must be 1024 or a multiple of 2048"));
          return UPS_INV_PAGESIZE;
        }
        if (param->value > 0)
          config.page_size_bytes = (uint32_t)param->value;
        break;
      case UPS_PARAM_FILE_SIZE_LIMIT:
        if (param->value > 0)
          config.file_size_limit_bytes = (size_t)param->value;
        break;
      case UPS_PARAM_JOURNAL_SWITCH_THRESHOLD:
        config.journal_switch_threshold = (uint32_t)param->value;
        break;
      case UPS_PARAM_LOG_DIRECTORY:
        config.log_filename = (const char *)param->value;
        break;
      case UPS_PARAM_NETWORK_TIMEOUT_SEC:
        config.remote_timeout_sec = (uint32_t)param->value;
        break;
      case UPS_PARAM_ENCRYPTION_KEY:
#ifdef UPS_ENABLE_ENCRYPTION
        /* in-memory? encryption is not possible */
        if (ISSET(flags, UPS_IN_MEMORY)) {
          ups_trace(("aes encryption not allowed in combination with "
                  "UPS_IN_MEMORY"));
          return UPS_INV_PARAMETER;
        }
        ::memcpy(config.encryption_key, (uint8_t *)param->value, 16);
        config.is_encryption_enabled = true;
        flags |= UPS_DISABLE_MMAP;
        break;
#else
        ups_trace(("aes encryption was disabled at compile time"));
        return UPS_NOT_IMPLEMENTED;
#endif
      case UPS_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)param->name));
        return UPS_INV_PARAMETER;
      }
    }
  }

  if (config.filename.empty() && NOTSET(flags, UPS_IN_MEMORY)) {
    ups_trace(("filename is missing"));
    return UPS_INV_PARAMETER;
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

  Env *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnv(config);
  }
  else {
#ifndef UPS_ENABLE_REMOTE
    return UPS_NOT_IMPLEMENTED;
#else // UPS_ENABLE_REMOTE
    env = new RemoteEnv(config);
#endif
  }

  ::atexit(ups_at_exit);
  ups_status_t st = 0;

  try {
    /* and finish the initialization of the Environment */
    st = env->create();

    /* flush the environment to make sure that the header page is written
     * to disk */
    if (likely(st == 0))
      st = env->flush(0);
  }
  catch (Exception &ex) {
    st = ex.code;
  }

  if (unlikely(st)) {
    try {
      env->close(UPS_AUTO_CLEANUP);
      delete env;
    }
    catch (Exception &) {
    }
    return st;
  }
 
  *henv = (ups_env_t *)env;
  return 0;
}

ups_status_t UPS_CALLCONV
ups_env_open(ups_env_t **henv, const char *filename, uint32_t flags,
                const ups_parameter_t *param)
{
  EnvConfig config;
  config.filename = filename ? filename : "";

  if (unlikely(!henv)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *henv = 0;

  /* cannot open an in-memory-db */
  if (unlikely(ISSET(flags, UPS_IN_MEMORY))) {
    ups_trace(("cannot open an in-memory database"));
    return UPS_INV_PARAMETER;
  }

  /* UPS_ENABLE_DUPLICATE_KEYS has to be specified in ups_env_create_db,
   * not ups_env_open */
  if (unlikely(ISSET(flags, UPS_ENABLE_DUPLICATE_KEYS))) {
    ups_trace(("invalid flag UPS_ENABLE_DUPLICATE_KEYS (only allowed when "
        "creating a database"));
    return UPS_INV_PARAMETER;
  }

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_TRANSACTIONS */
  if (ISSET(flags, UPS_AUTO_RECOVERY))
    flags |= UPS_ENABLE_TRANSACTIONS;

  if (unlikely(config.filename.empty() && NOTSET(flags, UPS_IN_MEMORY))) {
    ups_trace(("filename is missing"));
    return UPS_INV_PARAMETER;
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case UPS_PARAM_JOURNAL_COMPRESSION:
        ups_trace(("Journal compression parameters are only allowed in "
                    "ups_env_create"));
        return UPS_INV_PARAMETER;
      case UPS_PARAM_CACHE_SIZE:
        /* don't allow cache limits with unlimited cache */
        if (ISSET(flags, UPS_CACHE_UNLIMITED) && param->value != 0) {
          ups_trace(("combination of UPS_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return UPS_INV_PARAMETER;
        }
        if (param->value > 0)
          config.cache_size_bytes = param->value;
        break;
      case UPS_PARAM_FILE_SIZE_LIMIT:
        if (param->value > 0)
          config.file_size_limit_bytes = (size_t)param->value;
        break;
      case UPS_PARAM_JOURNAL_SWITCH_THRESHOLD:
        config.journal_switch_threshold = (uint32_t)param->value;
        break;
      case UPS_PARAM_LOG_DIRECTORY:
        config.log_filename = (const char *)param->value;
        break;
      case UPS_PARAM_NETWORK_TIMEOUT_SEC:
        config.remote_timeout_sec = (uint32_t)param->value;
        break;
      case UPS_PARAM_ENCRYPTION_KEY:
#ifdef UPS_ENABLE_ENCRYPTION
        ::memcpy(config.encryption_key, (uint8_t *)param->value, 16);
        config.is_encryption_enabled = true;
        flags |= UPS_DISABLE_MMAP;
        break;
#else
        ups_trace(("aes encryption was disabled at compile time"));
        return UPS_NOT_IMPLEMENTED;
#endif
      case UPS_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)param->name));
        return UPS_INV_PARAMETER;
      }
    }
  }

  config.flags = flags;

  Env *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnv(config);
  }
  else {
#ifndef UPS_ENABLE_REMOTE
    return UPS_NOT_IMPLEMENTED;
#else // UPS_ENABLE_REMOTE
    env = new RemoteEnv(config);
#endif
  }

  ::atexit(ups_at_exit);
  ups_status_t st = 0;

  try {
    /* and finish the initialization of the Environment */
    st = env->open();
  }
  catch (Exception &ex) {
    st = ex.code;
  }

  if (unlikely(st)) {
    try {
      (void)env->close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
      delete env;
    }
    catch (Exception &) {
    }
    return st;
  }

  *henv = (ups_env_t *)env;
  return 0;
}

ups_status_t UPS_CALLCONV
ups_env_create_db(ups_env_t *henv, ups_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ups_parameter_t *param)
{
  Env *env = (Env *)henv;
  DbConfig config;

  if (unlikely(!hdb)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  *hdb = 0;

  if (unlikely((db_name == 0) || (db_name >= 0xf000))) {
    ups_trace(("invalid database name"));
    return UPS_INV_PARAMETER;
  }

  config.db_name = db_name;
  config.flags = flags;

  try {
    ScopedLock lock(env->mutex);

    if (unlikely(ISSET(env->flags(), UPS_READ_ONLY))) {
      ups_trace(("cannot create database in a read-only environment"));
      return UPS_WRITE_PROTECTED;
    }

    *(Db **)hdb = env->create_db(config, param);
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_open_db(ups_env_t *henv, ups_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ups_parameter_t *param)
{
  Env *env = (Env *)henv;
  DbConfig config;

  if (unlikely(!hdb)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  *hdb = 0;

  if (unlikely(!db_name)) {
    ups_trace(("parameter 'db_name' must not be 0"));
    return UPS_INV_PARAMETER;
  }

  config.flags = flags;
  config.db_name = db_name;

  try {
    ScopedLock lock(env->mutex);

    if (unlikely(ISSET(env->flags(), UPS_IN_MEMORY))) {
      ups_trace(("cannot open a Database in an In-Memory Environment"));
      return UPS_INV_PARAMETER;
    }

    *(Db **)hdb = env->open_db(config, param);
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_rename_db(ups_env_t *henv, uint16_t oldname, uint16_t newname,
                uint32_t flags)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  if (unlikely(!oldname)) {
    ups_trace(("parameter 'oldname' must not be 0"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!newname)) {
    ups_trace(("parameter 'newname' must not be 0"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(newname >= 0xf000)) {
    ups_trace(("parameter 'newname' must be lower than 0xf000"));
    return UPS_INV_PARAMETER;
  }

  /* no need to do anything if oldname == newname */
  if (unlikely(oldname == newname))
    return 0;

  /* rename the database */
  try {
    ScopedLock lock(env->mutex);
    return env->rename_db(oldname, newname, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_erase_db(ups_env_t *henv, uint16_t name, uint32_t flags)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!name)) {
    ups_trace(("parameter 'name' must not be 0"));
    return UPS_INV_PARAMETER;
  }

  /* erase the database */
  try {
    ScopedLock lock(env->mutex);
    return env->erase_db(name, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_get_database_names(ups_env_t *henv, uint16_t *names, uint32_t *length)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!names)) {
    ups_trace(("parameter 'names' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!length)) {
    ups_trace(("parameter 'length' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  /* get all database names */
  try {
    ScopedLock lock(env->mutex);

    std::vector<uint16_t> vec = env->get_database_names();
    if (unlikely(vec.size() > *length)) {
      *length = vec.size();
      return UPS_LIMITS_REACHED;
    }
    for (std::vector<uint16_t>::iterator it = vec.begin();
            it != vec.end();
            it++, names++) {
      *names = *it;
    }
    *length = vec.size();
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_get_parameters(ups_env_t *henv, ups_parameter_t *param)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!param)) {
    ups_trace(("parameter 'param' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  /* get the parameters */
  try {
    ScopedLock lock(env->mutex);
    return env->get_parameters(param);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_flush(ups_env_t *henv, uint32_t flags)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(flags && flags != UPS_FLUSH_COMMITTED_TRANSACTIONS)) {
    ups_trace(("parameter 'flags' is unused, set to 0"));
    return UPS_INV_PARAMETER;
  }

  try {
    ScopedLock lock(env->mutex);
    return env->flush(flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_env_close(ups_env_t *henv, uint32_t flags)
{
  Env *env = (Env *)henv;

  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  try {
    /* close the environment */
    ups_status_t st = env->close(flags);
    if (st)
      return st;

    delete env;
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_get_parameters(ups_db_t *hdb, ups_parameter_t *param)
{
  Db *db = (Db *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!param)) {
    ups_trace(("parameter 'param' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  /* get the parameters */
  try {
    ScopedLock lock(db->env->mutex);
    return db->get_parameters(param);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_register_compare(const char *name, ups_compare_func_t func)
{
  CallbackManager::add(name, func);
  return 0;
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_set_compare_func(ups_db_t *hdb, ups_compare_func_t foo)
{
  Db *db = (Db *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!foo)) {
    ups_trace(("function pointer must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  LocalDb *ldb = dynamic_cast<LocalDb *>(db);
  if (unlikely(!ldb)) {
    ups_trace(("operation not possible for remote databases"));
    return UPS_INV_PARAMETER; 
  }

  ScopedLock lock(ldb->env->mutex);

  if (unlikely(db->config.key_type != UPS_TYPE_CUSTOM)) {
    ups_trace(("ups_set_compare_func only allowed for UPS_TYPE_CUSTOM "
                    "databases!"));
    return UPS_INV_PARAMETER;
  }

  /* set the compare functions */
  ldb->compare_function = foo;
  return 0;
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_find(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Db *db = (Db *)hdb;
  Txn *txn = (Txn *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return UPS_INV_PARAMETER;

  Env *env = db->env;

  try {
    ScopedLock lock(env->mutex);
  
    if (unlikely(ISSETANY(db->flags(),
                            UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)
          && !key->data)) {
      ups_trace(("key->data must not be NULL"));
      return UPS_INV_PARAMETER;
    }

    return db->find(0, txn, key, record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT int UPS_CALLCONV
ups_key_get_approximate_match_type(ups_key_t *key)
{
  if (key && (ups_key_get_intflags(key) & BtreeKey::kApproximate))
    return (ups_key_get_intflags(key) & BtreeKey::kLower) ? -1 : +1;
  return 0;
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_insert(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Db *db = (Db *)hdb;
  Txn *txn = (Txn *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(ISSET(flags, UPS_OVERWRITE) && ISSET(flags, UPS_DUPLICATE))) {
    ups_trace(("cannot combine UPS_OVERWRITE and UPS_DUPLICATE"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(ISSETANY(flags, UPS_DUPLICATE_INSERT_AFTER
                                | UPS_DUPLICATE_INSERT_BEFORE
                                | UPS_DUPLICATE_INSERT_LAST
                                | UPS_DUPLICATE_INSERT_FIRST))) {
    ups_trace(("function does not support flags UPS_DUPLICATE_INSERT_*; "
          "see ups_cursor_insert"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return UPS_INV_PARAMETER;

  Env *env = db->env;

  try {
    ScopedLock lock;
    if (likely(NOTSET(flags, UPS_DONT_LOCK)))
      lock = ScopedLock(env->mutex);

    if (unlikely(ISSET(db->flags(), UPS_READ_ONLY))) {
      ups_trace(("cannot insert in a read-only database"));
      return UPS_WRITE_PROTECTED;
    }
    if (unlikely(ISSET(flags, UPS_DUPLICATE)
        && NOTSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))) {
      ups_trace(("database does not support duplicate keys "
            "(see UPS_ENABLE_DUPLICATE_KEYS)"));
      return UPS_INV_PARAMETER;
    }
    if (ISSETANY(db->flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
      ups_status_t st = check_recno_key(key, flags);
      if (unlikely(st))
        return st;
    }

    flags &= ~UPS_DONT_LOCK;

    return db->insert(0, txn, key, record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_erase(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key, uint32_t flags)
{
  Db *db = (Db *)hdb;
  Txn *txn = (Txn *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_key(key)))
    return UPS_INV_PARAMETER;

  Env *env = db->env;

  try {
    ScopedLock lock;
    if (likely(NOTSET(flags, UPS_DONT_LOCK)))
      lock = ScopedLock(env->mutex);

    if (unlikely(ISSET(db->flags(), UPS_READ_ONLY))) {
      ups_trace(("cannot erase from a read-only database"));
      return UPS_WRITE_PROTECTED;
    }

    flags &= ~UPS_DONT_LOCK;

    return db->erase(0, txn, key, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_check_integrity(ups_db_t *hdb, uint32_t flags)
{
  Db *db = (Db *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (flags && NOTSET(flags, UPS_PRINT_GRAPH)) {
    ups_trace(("unknown flag 0x%u", flags));
    return UPS_INV_PARAMETER;
  }

  try {
    ScopedLock lock(db->env->mutex);
    return db->check_integrity(flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_close(ups_db_t *hdb, uint32_t flags)
{
  Db *db = (Db *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(ISSET(flags, UPS_TXN_AUTO_ABORT)
      && ISSET(flags, UPS_TXN_AUTO_COMMIT))) {
    ups_trace(("invalid combination of flags: UPS_TXN_AUTO_ABORT + "
          "UPS_TXN_AUTO_COMMIT"));
    return UPS_INV_PARAMETER;
  }

  Env *env = db->env;

  /* it's ok to close an uninitialized Database */
  if (unlikely(!env)) {
    delete db;
    return 0;
  }

  try {
    ScopedLock lock;
    if (likely(NOTSET(flags, UPS_DONT_LOCK)))
      lock = ScopedLock(env->mutex);

    // auto-cleanup cursors?
    if (ISSET(flags, UPS_AUTO_CLEANUP)) {
      Cursor *cursor;
      while ((cursor = db->cursor_list)) {
        cursor->close();
        if (cursor->txn)
          cursor->txn->release();
        db->remove_cursor(cursor);
        delete cursor;
      }
    }
    else if (unlikely(db->cursor_list != 0)) {
      ups_trace(("cannot close Database if Cursors are still open"));
      return UPS_CURSOR_STILL_OPEN;
    }

    return env->close_db(db, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_create(ups_cursor_t **hcursor, ups_db_t *hdb, ups_txn_t *htxn,
                uint32_t flags)
{
  Db *db = (Db *)hdb;
  Txn *txn = (Txn *)htxn;
  Cursor **cursor = (Cursor **)hcursor;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!hcursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Env *env = db->env;

  try {
    ScopedLock lock;
    if (likely(NOTSET(flags, UPS_DONT_LOCK)))
      lock = ScopedLock(env->mutex);

    *cursor = db->cursor_create(txn, flags);
    db->add_cursor(*cursor);
    if (txn)
      txn->add_ref();
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_clone(ups_cursor_t *hsrc, ups_cursor_t **hdest)
{
  Cursor *src = (Cursor *)hsrc;
  Cursor **dest = (Cursor **)hdest;

  if (unlikely(!src)) {
    ups_trace(("parameter 'src' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!dest)) {
    ups_trace(("parameter 'dest' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Db *db = src->db;

  try {
    ScopedLock lock(db->env->mutex);

    *dest = db->cursor_clone(src);
    (*dest)->previous = 0;
    db->add_cursor(*dest);
    if (src->txn)
      src->txn->add_ref();
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_overwrite(ups_cursor_t *hcursor, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(flags)) {
    ups_trace(("function does not support a non-zero flags value; "
          "see ups_cursor_insert for an alternative then"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_record(record)))
    return UPS_INV_PARAMETER;

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);

    if (unlikely(ISSET(db->flags(), UPS_READ_ONLY))) {
      ups_trace(("cannot overwrite in a read-only database"));
      return UPS_WRITE_PROTECTED;
    }

    return cursor->overwrite(record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_move(ups_cursor_t *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(ISSET(flags, UPS_ONLY_DUPLICATES)
      && ISSET(flags, UPS_SKIP_DUPLICATES))) {
    ups_trace(("combination of UPS_ONLY_DUPLICATES and "
          "UPS_SKIP_DUPLICATES not allowed"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(key && unlikely(!prepare_key(key))))
    return UPS_INV_PARAMETER;
  if (unlikely(record && unlikely(!prepare_record(record))))
    return UPS_INV_PARAMETER;

  Db *db = cursor->db;
  Env *env = db->env;

  try {
    ScopedLock lock(env->mutex);
    return db->cursor_move(cursor, key, record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_find(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_key(key)))
    return UPS_INV_PARAMETER;
  if (unlikely(record && unlikely(!prepare_record(record))))
    return UPS_INV_PARAMETER;

  Db *db = cursor->db;
  Env *env = db->env;

  try {
    ScopedLock lock;
    if (likely(NOTSET(flags, UPS_DONT_LOCK)))
      lock = ScopedLock(env->mutex);

    flags &= ~UPS_DONT_LOCK;

    return db->find(cursor, cursor->txn, key, record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_insert(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(ISSET(flags, UPS_DUPLICATE | UPS_OVERWRITE))) {
    ups_trace(("cannot combine UPS_DUPLICATE and UPS_OVERWRITE"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return UPS_INV_PARAMETER;

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);

    if (unlikely(ISSET(db->flags(), UPS_READ_ONLY))) {
      ups_trace(("cannot insert to a read-only database"));
      return UPS_WRITE_PROTECTED;
    }
    if (unlikely(ISSET(flags, UPS_DUPLICATE)
        && NOTSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))) {
      ups_trace(("database does not support duplicate keys "
            "(see UPS_ENABLE_DUPLICATE_KEYS)"));
      return UPS_INV_PARAMETER;
    }

    // set flag UPS_DUPLICATE if one of DUPLICATE_INSERT* is set
    if (ISSETANY(flags, UPS_DUPLICATE_INSERT_AFTER
                              | UPS_DUPLICATE_INSERT_BEFORE
                              | UPS_DUPLICATE_INSERT_LAST
                              | UPS_DUPLICATE_INSERT_FIRST))
      flags |= UPS_DUPLICATE;

    if (ISSETANY(db->flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
      ups_status_t st = check_recno_key(key, flags);
      if (unlikely(st))
        return st;
    }

    flags &= ~UPS_DONT_LOCK;

    return db->insert(cursor, cursor->txn, key, record, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_erase(ups_cursor_t *hcursor, uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);

    if (ISSET(db->flags(), UPS_READ_ONLY)) {
      ups_trace(("cannot erase from a read-only database"));
      return UPS_WRITE_PROTECTED;
    }

    return db->erase(cursor, cursor->txn, 0, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_count(ups_cursor_t *hcursor, uint32_t *count,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!count)) {
    ups_trace(("parameter 'count' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);
    *count = cursor->get_duplicate_count(flags);
    return 0;
  }
  catch (Exception &ex) {
    *count = 0;
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_position(ups_cursor_t *hcursor, uint32_t *position)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!position)) {
    ups_trace(("parameter 'position' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);
    *position = cursor->get_duplicate_position();
    return 0;
  }
  catch (Exception &ex) {
    *position = 0;
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_get_record_size(ups_cursor_t *hcursor, uint32_t *size)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!size)) {
    ups_trace(("parameter 'size' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);
    *size = cursor->get_record_size();
    return 0;
  }
  catch (Exception &ex) {
    *size = 0;
    return ex.code;
  }
}

ups_status_t UPS_CALLCONV
ups_cursor_close(ups_cursor_t *hcursor)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  Db *db = cursor->db;

  try {
    ScopedLock lock(db->env->mutex);
    cursor->close();
    if (cursor->txn)
      cursor->txn->release();
    db->remove_cursor(cursor);
    delete cursor;
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

void UPS_CALLCONV
ups_set_context_data(ups_db_t *hdb, void *data)
{
  Db *db = (Db *)hdb;
  if (unlikely(!db))
    return;

  ScopedLock lock(db->env->mutex);
  db->context = data;
}

void * UPS_CALLCONV
ups_get_context_data(ups_db_t *hdb, ups_bool_t dont_lock)
{
  Db *db = (Db *)hdb;
  if (unlikely(!db))
    return 0;

  if (dont_lock)
    return db->context;

  ScopedLock lock(db->env->mutex);
  return db->context;
}

ups_db_t * UPS_CALLCONV
ups_cursor_get_database(ups_cursor_t *hcursor)
{
  Cursor *cursor = (Cursor *)hcursor;
  if (unlikely(!cursor))
    return 0;

  return (ups_db_t *)cursor->db;
}

ups_env_t * UPS_CALLCONV
ups_db_get_env(ups_db_t *hdb)
{
  Db *db = (Db *)hdb;
  if (unlikely(!db))
    return 0;

  return (ups_env_t *)db->env;
}

uint16_t UPS_CALLCONV
ups_db_get_name(ups_db_t *hdb)
{
  Db *db = (Db *)hdb;
  if (unlikely(!db))
    return 0;

  return db->config.db_name;
}

UPS_EXPORT uint32_t UPS_CALLCONV
ups_db_get_flags(ups_db_t *hdb)
{
  Db *db = (Db *)hdb;
  if (unlikely(!db))
    return 0;

  return db->config.flags;
}

ups_status_t UPS_CALLCONV
ups_db_count(ups_db_t *hdb, ups_txn_t *htxn, uint32_t flags,
                uint64_t *count)
{
  Db *db = (Db *)hdb;
  Txn *txn = (Txn *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!count)) {
    ups_trace(("parameter 'count' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  try {
    ScopedLock lock(db->env->mutex);

    *count = db->count(txn, ISSET(flags, UPS_SKIP_DUPLICATES));
    return 0;
  }
  catch (Exception &ex) {
    *count = 0;
    return ex.code;
  }
}

void UPS_CALLCONV
ups_set_error_handler(ups_error_handler_fun f)
{
  if (f)
    upscaledb::Globals::ms_error_handler = f;
  else
    upscaledb::Globals::ms_error_handler = upscaledb::default_errhandler;
}

ups_status_t UPS_CALLCONV
ups_env_get_metrics(ups_env_t *henv, ups_env_metrics_t *metrics)
{
  Env *env = (Env *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!metrics)) {
    ups_trace(("parameter 'metrics' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  ::memset(metrics, 0, sizeof(ups_env_metrics_t));
  metrics->version = UPS_METRICS_VERSION;

  try {
    // fill in memory metrics
    Memory::get_global_metrics(metrics);
    // ... and everything else
    env->fill_metrics(metrics);
    return 0;
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

ups_bool_t UPS_CALLCONV
ups_is_debug()
{
#ifndef NDEBUG
  return UPS_TRUE;
#else
  return UPS_FALSE;
#endif
}

UPS_EXPORT uint32_t UPS_CALLCONV
ups_calc_compare_name_hash(const char *zname)
{
  return CallbackManager::hash(zname);
}

UPS_EXPORT uint32_t UPS_CALLCONV
ups_db_get_compare_name_hash(ups_db_t *hdb)
{
  Db *db = (Db *)hdb;
  LocalDb *ldb = dynamic_cast<LocalDb *>(db);
  if (unlikely(!ldb)) {
    ups_trace(("operation not possible for remote databases"));
    return 0; 
  }
  return ldb->btree_index->compare_hash();
}

UPS_EXPORT void UPS_CALLCONV
ups_set_committed_flush_threshold(int threshold)
{
  Globals::ms_flush_threshold = threshold;
}

UPS_EXPORT ups_db_t *UPS_CALLCONV
ups_env_get_open_database(ups_env_t *henv, uint16_t name)
{
  Env *env = (Env *)henv;
  Env::DatabaseMap::iterator it = env->_database_map.find(name);
  if (likely(it != env->_database_map.end()))
    return (ups_db_t *)it->second;
  return 0;
}

UPS_EXPORT void UPS_CALLCONV
ups_at_exit()
{
#ifdef UPS_ENABLE_REMOTE
  Protocol::shutdown();
  upscaledb::PluginManager::cleanup();
#endif
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_bulk_operations(ups_db_t *hdb, ups_txn_t *txn,
                    ups_operation_t *operations, size_t operations_length,
                    uint32_t flags)
{
  if (unlikely(hdb == 0)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(operations == 0)) {
    ups_trace(("parameter 'operations' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(flags != 0)) {
    ups_trace(("parameter 'flags' must be 0"));
    return UPS_INV_PARAMETER;
  }

  Db *db = (Db *)hdb;
  try {
    ScopedLock lock = ScopedLock(db->env->mutex);
    return db->bulk_operations((Txn *)txn, operations,
                    operations_length, flags);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}
