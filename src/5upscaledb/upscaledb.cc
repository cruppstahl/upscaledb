/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

static bool
filename_is_local(const char *filename)
{
  return (!filename || ::strstr(filename, "ups://") != filename);
}

static inline bool
prepare_key(ups_key_t *key)
{
  if (unlikely(key->size && !key->data)) {
    ups_trace(("key->size != 0, but key->data is NULL"));
    return (false);
  }
  if (unlikely(key->flags != 0 && key->flags != UPS_KEY_USER_ALLOC)) {
    ups_trace(("invalid flag in key->flags"));
    return (false);
  }
  key->_flags = 0;
  return (true);
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
    return (false);
  }
  return (true);
}

static inline ups_status_t
check_recno_key(ups_key_t *key, uint32_t flags)
{
  if (isset(flags, UPS_OVERWRITE)) {
    if (!key->data) {
      ups_trace(("key->data must not be NULL"));
      return (UPS_INV_PARAMETER);
    }
  }
  else {
    if (isset(key->flags, UPS_KEY_USER_ALLOC)) {
      if (!key->data) {
        ups_trace(("key->data must not be NULL"));
        return (UPS_INV_PARAMETER);
      }
    }
    else {
      if (key->data || key->size) {
        ups_trace(("key->size must be 0, key->data must be NULL"));
        return (UPS_INV_PARAMETER);
      }
    }
  }
  return (0);
}

ups_status_t
ups_txn_begin(ups_txn_t **htxn, ups_env_t *henv, const char *name,
                void *, uint32_t flags)
{
  Transaction **ptxn = (Transaction **)htxn;

  if (unlikely(!ptxn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *ptxn = 0;

  if (unlikely(!henv)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = (Environment *)henv;

  return (env->txn_begin(ptxn, name, flags));
}

UPS_EXPORT const char *
ups_txn_get_name(ups_txn_t *htxn)
{
  Transaction *txn = (Transaction *)htxn;
  if (unlikely(!txn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (0);
  }

  const std::string &name = txn->get_name();
  return (name.empty() ? 0 : name.c_str());
}

ups_status_t
ups_txn_commit(ups_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (unlikely(!txn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  return (env->txn_commit(txn, flags));
}

ups_status_t
ups_txn_abort(ups_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (unlikely(!txn)) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  return (env->txn_abort(txn, flags));
}

const char * UPS_CALLCONV
ups_strerror(ups_status_t result)
{
  switch (result) {
    case UPS_SUCCESS:
      return ("Success");
    case UPS_INV_KEY_SIZE:
      return ("Invalid key size");
    case UPS_INV_RECORD_SIZE:
      return ("Invalid record size");
    case UPS_INV_PAGESIZE:
      return ("Invalid page size");
    case UPS_OUT_OF_MEMORY:
      return ("Out of memory");
    case UPS_INV_PARAMETER:
      return ("Invalid parameter");
    case UPS_INV_FILE_HEADER:
      return ("Invalid database file header");
    case UPS_INV_FILE_VERSION:
      return ("Invalid database file version");
    case UPS_KEY_NOT_FOUND:
      return ("Key not found");
    case UPS_DUPLICATE_KEY:
      return ("Duplicate key");
    case UPS_INTEGRITY_VIOLATED:
      return ("Internal integrity violated");
    case UPS_INTERNAL_ERROR:
      return ("Internal error");
    case UPS_WRITE_PROTECTED:
      return ("Database opened in read-only mode");
    case UPS_BLOB_NOT_FOUND:
      return ("Data blob not found");
    case UPS_IO_ERROR:
      return ("System I/O error");
    case UPS_NOT_IMPLEMENTED:
      return ("Operation not implemented");
    case UPS_FILE_NOT_FOUND:
      return ("File not found");
    case UPS_WOULD_BLOCK:
      return ("Operation would block");
    case UPS_NOT_READY:
      return ("Object was not initialized correctly");
    case UPS_CURSOR_STILL_OPEN:
      return ("Cursor must be closed prior to Transaction abort/commit");
    case UPS_FILTER_NOT_FOUND:
      return ("Record filter or file filter not found");
    case UPS_TXN_CONFLICT:
      return ("Operation conflicts with another Transaction");
    case UPS_TXN_STILL_OPEN:
      return ("Database cannot be closed because it is modified in a "
          "Transaction");
    case UPS_CURSOR_IS_NIL:
      return ("Cursor points to NIL");
    case UPS_DATABASE_NOT_FOUND:
      return ("Database not found");
    case UPS_DATABASE_ALREADY_EXISTS:
      return ("Database name already exists");
    case UPS_DATABASE_ALREADY_OPEN:
      return ("Database already open, or: Database handle "
          "already initialized");
    case UPS_ENVIRONMENT_ALREADY_OPEN:
      return ("Environment already open, or: Environment handle "
          "already initialized");
    case UPS_LIMITS_REACHED:
      return ("Database limits reached");
    case UPS_ALREADY_INITIALIZED:
      return ("Object was already initialized");
    case UPS_NEED_RECOVERY:
      return ("Database needs recovery");
    case UPS_LOG_INV_FILE_HEADER:
      return ("Invalid log file header");
    case UPS_NETWORK_ERROR:
      return ("Remote I/O error/Network error");
    default:
      return ("Unknown error");
  }
}

void UPS_CALLCONV
ups_get_version(uint32_t *major, uint32_t *minor, uint32_t *revision)
{
  if (major)
    *major = UPS_VERSION_MAJ;
  if (minor)
    *minor = UPS_VERSION_MIN;
  if (revision)
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
    return (UPS_INV_PARAMETER);
  }

  *henv = 0;

  /* creating a file in READ_ONLY mode? doesn't make sense */
  if (unlikely(isset(flags, UPS_READ_ONLY))) {
    ups_trace(("cannot create a file in read-only mode"));
    return (UPS_INV_PARAMETER);
  }

  /* in-memory? crc32 is not possible */
  if (unlikely(isset(flags, UPS_IN_MEMORY) && isset(flags, UPS_ENABLE_CRC32))) {
    ups_trace(("combination of UPS_IN_MEMORY and UPS_ENABLE_CRC32 "
            "not allowed"));
    return (UPS_INV_PARAMETER);
  }

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_TRANSACTIONS */
  if (isset(flags, UPS_AUTO_RECOVERY))
    flags |= UPS_ENABLE_TRANSACTIONS;

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case UPS_PARAM_JOURNAL_COMPRESSION:
        if (!CompressorFactory::is_available(param->value)) {
          ups_trace(("unknown algorithm for journal compression"));
          return (UPS_INV_PARAMETER);
        }
        config.journal_compressor = (int)param->value;
        break;
      case UPS_PARAM_CACHESIZE:
        if (isset(flags, UPS_IN_MEMORY) && param->value != 0) {
          ups_trace(("combination of UPS_IN_MEMORY and cache size != 0 "
                "not allowed"));
          return (UPS_INV_PARAMETER);
        }
        /* don't allow cache limits with unlimited cache */
        if (isset(flags, UPS_CACHE_UNLIMITED) && param->value != 0) {
          ups_trace(("combination of UPS_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return (UPS_INV_PARAMETER);
        }
        if (param->value > 0)
          config.cache_size_bytes = (size_t)param->value;
        break;
      case UPS_PARAM_PAGE_SIZE:
        if (param->value != 1024 && param->value % 2048 != 0) {
          ups_trace(("invalid page size - must be 1024 or a multiple of 2048"));
          return (UPS_INV_PAGESIZE);
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
        if (isset(flags, UPS_IN_MEMORY)) {
          ups_trace(("aes encryption not allowed in combination with "
                  "UPS_IN_MEMORY"));
          return (UPS_INV_PARAMETER);
        }
        ::memcpy(config.encryption_key, (uint8_t *)param->value, 16);
        config.is_encryption_enabled = true;
        flags |= UPS_DISABLE_MMAP;
        break;
#else
        ups_trace(("aes encryption was disabled at compile time"));
        return (UPS_NOT_IMPLEMENTED);
#endif
      case UPS_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)param->name));
        return (UPS_INV_PARAMETER);
      }
    }
  }

  if (config.filename.empty() && notset(flags, UPS_IN_MEMORY)) {
    ups_trace(("filename is missing"));
    return (UPS_INV_PARAMETER);
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

  Environment *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnvironment(config);
  }
  else {
#ifndef UPS_ENABLE_REMOTE
    return (UPS_NOT_IMPLEMENTED);
#else // UPS_ENABLE_REMOTE
    env = new RemoteEnvironment(config);
#endif
  }

#ifdef UPS_ENABLE_REMOTE
  ::atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  ups_status_t st = env->create();

  /* flush the environment to make sure that the header page is written
   * to disk */
  if (st == 0)
    st = env->flush(0);

  if (st) {
    env->close(UPS_AUTO_CLEANUP);
    delete env;
    return (st);
  }
 
  *henv = (ups_env_t *)env;
  return (0);
}

ups_status_t UPS_CALLCONV
ups_env_create_db(ups_env_t *henv, ups_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ups_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  DbConfig config;

  if (unlikely(!hdb)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *hdb = 0;

  if (unlikely((db_name == 0) || (db_name >= 0xf000))) {
    ups_trace(("invalid database name"));
    return (UPS_INV_PARAMETER);
  }

  config.db_name = db_name;
  config.flags = flags;

  return (env->create_db((Database **)hdb, config, param));
}

ups_status_t UPS_CALLCONV
ups_env_open_db(ups_env_t *henv, ups_db_t **hdb, uint16_t db_name,
                uint32_t flags, const ups_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  DbConfig config;

  if (unlikely(!hdb)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *hdb = 0;

  if (unlikely(!db_name)) {
    ups_trace(("parameter 'db_name' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(env->get_flags(), UPS_IN_MEMORY))) {
    ups_trace(("cannot open a Database in an In-Memory Environment"));
    return (UPS_INV_PARAMETER);
  }

  config.flags = flags;
  config.db_name = db_name;

  return (env->open_db((Database **)hdb, config, param));
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
  if (unlikely(isset(flags, UPS_IN_MEMORY))) {
    ups_trace(("cannot open an in-memory database"));
    return (UPS_INV_PARAMETER);
  }

  /* UPS_ENABLE_DUPLICATE_KEYS has to be specified in ups_env_create_db,
   * not ups_env_open */
  if (unlikely(isset(flags, UPS_ENABLE_DUPLICATE_KEYS))) {
    ups_trace(("invalid flag UPS_ENABLE_DUPLICATE_KEYS (only allowed when "
        "creating a database"));
    return (UPS_INV_PARAMETER);
  }

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_TRANSACTIONS */
  if (isset(flags, UPS_AUTO_RECOVERY))
    flags |= UPS_ENABLE_TRANSACTIONS;

  if (config.filename.empty() && notset(flags, UPS_IN_MEMORY)) {
    ups_trace(("filename is missing"));
    return (UPS_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
      case UPS_PARAM_JOURNAL_COMPRESSION:
        ups_trace(("Journal compression parameters are only allowed in "
                    "ups_env_create"));
        return (UPS_INV_PARAMETER);
      case UPS_PARAM_CACHE_SIZE:
        /* don't allow cache limits with unlimited cache */
        if (isset(flags, UPS_CACHE_UNLIMITED) && param->value != 0) {
          ups_trace(("combination of UPS_CACHE_UNLIMITED and cache size != 0 "
                "not allowed"));
          return (UPS_INV_PARAMETER);
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
        return (UPS_NOT_IMPLEMENTED);
#endif
      case UPS_PARAM_POSIX_FADVISE:
        config.posix_advice = (int)param->value;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)param->name));
        return (UPS_INV_PARAMETER);
      }
    }
  }

  config.flags = flags;

  Environment *env = 0;

  if (filename_is_local(config.filename.c_str())) {
    env = new LocalEnvironment(config);
  }
  else {
#ifndef UPS_ENABLE_REMOTE
    return (UPS_NOT_IMPLEMENTED);
#else // UPS_ENABLE_REMOTE
    env = new RemoteEnvironment(config);
#endif
  }

#ifdef UPS_ENABLE_REMOTE
  ::atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  ups_status_t st = env->open();

  if (st) {
    (void)env->close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    delete env;
    return (st);
  }

  *henv = (ups_env_t *)env;
  return (0);
}

ups_status_t UPS_CALLCONV
ups_env_rename_db(ups_env_t *henv, uint16_t oldname, uint16_t newname,
                uint32_t flags)
{
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (unlikely(!oldname)) {
    ups_trace(("parameter 'oldname' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!newname)) {
    ups_trace(("parameter 'newname' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(newname >= 0xf000)) {
    ups_trace(("parameter 'newname' must be lower than 0xf000"));
    return (UPS_INV_PARAMETER);
  }

  /* no need to do anything if oldname==newname */
  if (oldname == newname)
    return (0);

  /* rename the database */
  return (env->rename_db(oldname, newname, flags));
}

ups_status_t UPS_CALLCONV
ups_env_erase_db(ups_env_t *henv, uint16_t name, uint32_t flags)
{
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (unlikely(!name)) {
    ups_trace(("parameter 'name' must not be 0"));
    return (UPS_INV_PARAMETER);
  }

  /* erase the database */
  return (env->erase_db(name, flags));
}

ups_status_t UPS_CALLCONV
ups_env_get_database_names(ups_env_t *henv, uint16_t *names, uint32_t *count)
{
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (unlikely(!names)) {
    ups_trace(("parameter 'names' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!count)) {
    ups_trace(("parameter 'count' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  /* get all database names */
  return (env->get_database_names(names, count));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_get_parameters(ups_env_t *henv, ups_parameter_t *param)
{
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!param)) {
    ups_trace(("parameter 'param' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  /* get the parameters */
  return (env->get_parameters(param));
}

ups_status_t UPS_CALLCONV
ups_env_flush(ups_env_t *henv, uint32_t flags)
{
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (flags && flags != UPS_FLUSH_COMMITTED_TRANSACTIONS) {
    ups_trace(("parameter 'flags' is unused, set to 0"));
    return (UPS_INV_PARAMETER);
  }

  return (env->flush(flags));
}

ups_status_t UPS_CALLCONV
ups_env_close(ups_env_t *henv, uint32_t flags)
{
  Environment *env = (Environment *)henv;

  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  try {
    /* close the environment */
    ups_status_t st = env->close(flags);
    if (st)
      return (st);

    delete env;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }

  return (0);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_get_parameters(ups_db_t *hdb, ups_parameter_t *param)
{
  Database *db = (Database *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!param)) {
    ups_trace(("parameter 'param' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  ScopedLock lock(db->get_env()->mutex());

  /* get the parameters */
  return (db->get_parameters(param));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_register_compare(const char *name, ups_compare_func_t func)
{
  CallbackManager::add(name, func);
  return (0);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_set_compare_func(ups_db_t *hdb, ups_compare_func_t foo)
{
  Database *db = (Database *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!foo)) {
    ups_trace(("function pointer must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  LocalDatabase *ldb = dynamic_cast<LocalDatabase *>(db);
  if (unlikely(!ldb)) {
    ups_trace(("operation not possible for remote databases"));
    return (UPS_INV_PARAMETER); 
  }

  ScopedLock lock(ldb->get_env()->mutex());

  /* set the compare functions */
  return (ldb->set_compare_func(foo));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_find(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Environment *env = db->get_env();
  ScopedLock lock(env->mutex());

  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && notset(env->get_flags(), UPS_IN_MEMORY))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
          "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS))) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(issetany(db->get_flags(),
        (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)))
      && !key->data) {
    ups_trace(("key->data must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  return (db->find(0, txn, key, record, flags));
}

UPS_EXPORT int UPS_CALLCONV
ups_key_get_approximate_match_type(ups_key_t *key)
{
  if (key && (ups_key_get_intflags(key) & BtreeKey::kApproximate)) {
    int rv = (ups_key_get_intflags(key) & BtreeKey::kLower) ? -1 : +1;
    return (rv);
  }

  return (0);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_insert(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_HINT_APPEND))) {
    ups_trace(("flags UPS_HINT_APPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_HINT_PREPEND))) {
    ups_trace(("flags UPS_HINT_PREPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_OVERWRITE) && isset(flags, UPS_DUPLICATE))) {
    ups_trace(("cannot combine UPS_OVERWRITE and UPS_DUPLICATE"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(issetany(flags, UPS_DUPLICATE_INSERT_AFTER
                                | UPS_DUPLICATE_INSERT_BEFORE
                                | UPS_DUPLICATE_INSERT_LAST
                                | UPS_DUPLICATE_INSERT_FIRST))) {
    ups_trace(("function does not support flags UPS_DUPLICATE_INSERT_*; "
          "see ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Environment *env = db->get_env();
  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (unlikely(isset(db->get_flags(), UPS_READ_ONLY))) {
    ups_trace(("cannot insert in a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && (isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS)))) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL) && (record->size <= 8))) {
    ups_trace(("flag UPS_PARTIAL is not allowed if record->size <= 8"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size))) {
    ups_trace(("partial offset+size is greater than the total "
          "record size"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_DUPLICATE)
      && notset(db->get_flags(), UPS_ENABLE_DUPLICATE_KEYS))) {
    ups_trace(("database does not support duplicate keys "
          "(see UPS_ENABLE_DUPLICATE_KEYS)"));
    return (UPS_INV_PARAMETER);
  }

  if (issetany(db->get_flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    ups_status_t st = check_recno_key(key, flags);
    if (st)
      return (st);
  }

  return (db->insert(0, txn, key, record, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_erase(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_key(key)))
    return (UPS_INV_PARAMETER);

  Environment *env = db->get_env();
  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (unlikely(isset(db->get_flags(), UPS_READ_ONLY))) {
    ups_trace(("cannot erase from a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }

  return (db->erase(0, txn, key, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_check_integrity(ups_db_t *hdb, uint32_t flags)
{
  Database *db = (Database *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (flags && notset(flags, UPS_PRINT_GRAPH)) {
    ups_trace(("unknown flag 0x%u", flags));
    return (UPS_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  return (db->check_integrity(flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_close(ups_db_t *hdb, uint32_t flags)
{
  Database *db = (Database *)hdb;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_TXN_AUTO_ABORT)
      && isset(flags, UPS_TXN_AUTO_COMMIT))) {
    ups_trace(("invalid combination of flags: UPS_TXN_AUTO_ABORT + "
          "UPS_TXN_AUTO_COMMIT"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = db->get_env();

  /* it's ok to close an uninitialized Database */
  if (!env) {
    delete db;
    return (0);
  }

  return (env->close_db(db, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_create(ups_cursor_t **hcursor, ups_db_t *hdb, ups_txn_t *htxn,
                uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Cursor **cursor = (Cursor **)hcursor;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!hcursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = db->get_env();
  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  return (db->cursor_create(cursor, txn, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_clone(ups_cursor_t *hsrc, ups_cursor_t **hdest)
{
  Cursor *src = (Cursor *)hsrc;
  Cursor **dest = (Cursor **)hdest;

  if (unlikely(!src)) {
    ups_trace(("parameter 'src' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!dest)) {
    ups_trace(("parameter 'dest' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = src->db();
  ScopedLock lock(db->get_env()->mutex());
  return (db->cursor_clone(dest, src));
}

ups_status_t UPS_CALLCONV
ups_cursor_overwrite(ups_cursor_t *hcursor, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(flags)) {
    ups_trace(("function does not support a non-zero flags value; "
          "see ups_cursor_insert for an alternative then"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (unlikely(isset(db->get_flags(), UPS_READ_ONLY))) {
    ups_trace(("cannot overwrite in a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }

  return (cursor->overwrite(record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_move(ups_cursor_t *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_ONLY_DUPLICATES)
      && isset(flags, UPS_SKIP_DUPLICATES))) {
    ups_trace(("combination of UPS_ONLY_DUPLICATES and "
          "UPS_SKIP_DUPLICATES not allowed"));
    return (UPS_INV_PARAMETER);
  }
  if (key && unlikely(!prepare_key(key)))
    return (UPS_INV_PARAMETER);
  if (record && unlikely(!prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Database *db = cursor->db();
  Environment *env = db->get_env();
  ScopedLock lock(env->mutex());

  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && notset(env->get_flags(), UPS_IN_MEMORY))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS))) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }

  return (db->cursor_move(cursor, key, record, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_find(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_key(key)))
    return (UPS_INV_PARAMETER);
  if (record && unlikely(!prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Database *db = cursor->db();
  Environment *env = db->get_env();
  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && notset(env->get_flags(), UPS_IN_MEMORY))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_DIRECT_ACCESS)
      && isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS))) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && (isset(env->get_flags(), UPS_ENABLE_TRANSACTIONS)))) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }

  return (db->find(cursor, cursor->get_txn(), key, record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_insert(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!key)) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!record)) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_HINT_APPEND | UPS_HINT_PREPEND))) {
    ups_trace(("flags UPS_HINT_APPEND and UPS_HINT_PREPEND "
           "are mutually exclusive"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_DUPLICATE | UPS_OVERWRITE))) {
    ups_trace(("cannot combine UPS_DUPLICATE and UPS_OVERWRITE"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!prepare_key(key) || !prepare_record(record)))
    return (UPS_INV_PARAMETER);

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (unlikely(isset(db->get_flags(), UPS_READ_ONLY))) {
    ups_trace(("cannot insert to a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }
  if (unlikely(isset(flags, UPS_DUPLICATE)
      && notset(db->get_flags(), UPS_ENABLE_DUPLICATE_KEYS))) {
    ups_trace(("database does not support duplicate keys "
          "(see UPS_ENABLE_DUPLICATE_KEYS)"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && (isset(db->get_env()->get_flags(), UPS_ENABLE_TRANSACTIONS)))) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL) && (record->size <= 8))) {
    ups_trace(("flag UPS_PARTIAL is not allowed if record->size "
          "<= 8"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(isset(flags, UPS_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size))) {
    ups_trace(("partial offset+size is greater than the total "
          "record size"));
    return (UPS_INV_PARAMETER);
  }

  /*
   * set flag UPS_DUPLICATE if one of DUPLICATE_INSERT* is set, but do
   * not allow these flags if duplicate sorting is enabled
   */
  if (issetany(flags, UPS_DUPLICATE_INSERT_AFTER
                            | UPS_DUPLICATE_INSERT_BEFORE
                            | UPS_DUPLICATE_INSERT_LAST
                            | UPS_DUPLICATE_INSERT_FIRST))
    flags |= UPS_DUPLICATE;

  if (issetany(db->get_flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    ups_status_t st = check_recno_key(key, flags);
    if (st)
      return (st);
  }

  return (db->insert(cursor, cursor->get_txn(), key, record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_erase(ups_cursor_t *hcursor, uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (isset(db->get_flags(), UPS_READ_ONLY)) {
    ups_trace(("cannot erase from a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }

  return (db->erase(cursor, cursor->get_txn(), 0, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_count(ups_cursor_t *hcursor, uint32_t *count,
                uint32_t flags)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!count)) {
    ups_trace(("parameter 'count' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  return (cursor->get_duplicate_count(flags, count));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_position(ups_cursor_t *hcursor, uint32_t *position)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!position)) {
    ups_trace(("parameter 'position' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  return (cursor->get_duplicate_position(position));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_record_size(ups_cursor_t *hcursor, uint64_t *size)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!size)) {
    ups_trace(("parameter 'size' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  return (cursor->get_record_size(size));
}

ups_status_t UPS_CALLCONV
ups_cursor_close(ups_cursor_t *hcursor)
{
  Cursor *cursor = (Cursor *)hcursor;

  if (unlikely(!cursor)) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  return (db->cursor_close(cursor));
}

void UPS_CALLCONV
ups_set_context_data(ups_db_t *hdb, void *data)
{
  Database *db = (Database *)hdb;
  if (unlikely(!db))
    return;

  ScopedLock lock(db->get_env()->mutex());
  db->set_context_data(data);
}

void * UPS_CALLCONV
ups_get_context_data(ups_db_t *hdb, ups_bool_t dont_lock)
{
  Database *db = (Database *)hdb;
  if (unlikely(!db))
    return (0);

  if (dont_lock)
    return (db->get_context_data());

  ScopedLock lock(db->get_env()->mutex());
  return (db->get_context_data());
}

ups_db_t * UPS_CALLCONV
ups_cursor_get_database(ups_cursor_t *hcursor)
{
  Cursor *cursor = (Cursor *)hcursor;
  if (unlikely(!cursor))
    return (0);

  return ((ups_db_t *)cursor->db());
}

ups_env_t * UPS_CALLCONV
ups_db_get_env(ups_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (unlikely(!db))
    return (0);

  return ((ups_env_t *)db->get_env());
}

ups_status_t UPS_CALLCONV
ups_db_count(ups_db_t *hdb, ups_txn_t *htxn, uint32_t flags,
                uint64_t *count)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (unlikely(!db)) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!count)) {
    ups_trace(("parameter 'count' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());

  return (db->count(txn, (flags & UPS_SKIP_DUPLICATES) != 0, count));
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
  Environment *env = (Environment *)henv;
  if (unlikely(!env)) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (unlikely(!metrics)) {
    ups_trace(("parameter 'metrics' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  ::memset(metrics, 0, sizeof(ups_env_metrics_t));
  metrics->version = UPS_METRICS_VERSION;

  // fill in memory metrics
  Memory::get_global_metrics(metrics);
  // ... and everything else
  return (env->fill_metrics(metrics));
}

ups_bool_t UPS_CALLCONV
ups_is_debug()
{
#ifdef UPS_DEBUG
  return (UPS_TRUE);
#else
  return (UPS_FALSE);
#endif
}

UPS_EXPORT uint32_t UPS_CALLCONV
ups_calc_compare_name_hash(const char *zname)
{
  return (CallbackManager::hash(zname));
}

UPS_EXPORT uint32_t UPS_CALLCONV
ups_db_get_compare_name_hash(ups_db_t *hdb)
{
  Database *db = (Database *)hdb;
  LocalDatabase *ldb = dynamic_cast<LocalDatabase *>(db);
  if (!ldb) {
    ups_trace(("operation not possible for remote databases"));
    return (0); 
  }
  return (ldb->btree_index()->compare_hash());
}

