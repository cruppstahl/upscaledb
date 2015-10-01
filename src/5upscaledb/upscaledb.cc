/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "1eventlog/eventlog.h"
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

/* return true if the filename is for a local file */
static bool
filename_is_local(const char *filename)
{
  return (!filename || strstr(filename, "ups://") != filename);
}

ups_status_t
ups_txn_begin(ups_txn_t **htxn, ups_env_t *henv, const char *name,
                void *, uint32_t flags)
{
  Transaction **ptxn = (Transaction **)htxn;

  if (!ptxn) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *ptxn = 0;

  if (!henv) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = (Environment *)henv;

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.txn_begin", "\"%s\", 0x%x", name ? name : "", flags));

  return (env->txn_begin(ptxn, name, flags));
}

UPS_EXPORT const char *
ups_txn_get_name(ups_txn_t *htxn)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn)
    return (0);

  const std::string &name = txn->get_name();
  return (name.empty() ? 0 : name.c_str());
}

ups_status_t
ups_txn_commit(ups_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.txn_commit", "%u", (uint32_t)txn->get_id()));

  return (env->txn_commit(txn, flags));
}

ups_status_t
ups_txn_abort(ups_txn_t *htxn, uint32_t flags)
{
  Transaction *txn = (Transaction *)htxn;
  if (!txn) {
    ups_trace(("parameter 'txn' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = txn->get_env();

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.txn_abort", "%u", (uint32_t)txn->get_id()));

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

/**
 * Prepares a @ref ups_key_t structure for returning key data in.
 *
 * This function checks whether the @ref ups_key_t structure has been
 * properly initialized by the user and resets all internal used elements.
 *
 * @return true when the @a key structure has been initialized correctly
 * before.
 *
 * @return false when the @a key structure has @e not been initialized
 * correctly before.
 */
static inline bool
__prepare_key(ups_key_t *key)
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

/**
 * Prepares a @ref ups_record_t structure for returning record data in.
 *
 * This function checks whether the @ref ups_record_t structure has been
 * properly initialized by the user and resets all internal used elements.
 *
 * @return true when the @a record structure has been initialized
 * correctly before.
 *
 * @return false when the @a record structure has @e not been
 * initialized correctly before.
 */
static inline bool
__prepare_record(ups_record_t *record)
{
  if (unlikely(record->size && !record->data)) {
    ups_trace(("record->size != 0, but record->data is NULL"));
    return false;
  }
  if (unlikely(record->flags & UPS_DIRECT_ACCESS))
    record->flags &= ~UPS_DIRECT_ACCESS;
  if (unlikely(record->flags != 0 && record->flags != UPS_RECORD_USER_ALLOC)) {
    ups_trace(("invalid flag in record->flags"));
    return (false);
  }
  return (true);
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
  EnvironmentConfiguration config;
  config.filename = filename ? filename : "";
  config.file_mode = mode;

  EVENTLOG_CREATE(filename);
  EVENTLOG_APPEND((filename, "f.env_create", "0x%x, %u", flags, mode));

  if (!henv) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  *henv = 0;

  /* creating a file in READ_ONLY mode? doesn't make sense */
  if (flags & UPS_READ_ONLY) {
    ups_trace(("cannot create a file in read-only mode"));
    return (UPS_INV_PARAMETER);
  }

  /* in-memory? recovery is not possible */
  if ((flags & UPS_IN_MEMORY) && (flags & UPS_ENABLE_RECOVERY)) {
    ups_trace(("combination of UPS_IN_MEMORY and UPS_ENABLE_RECOVERY "
            "not allowed"));
    return (UPS_INV_PARAMETER);
  }

  /* in-memory? crc32 is not possible */
  if ((flags & UPS_IN_MEMORY) && (flags & UPS_ENABLE_CRC32)) {
    ups_trace(("combination of UPS_IN_MEMORY and UPS_ENABLE_CRC32 "
            "not allowed"));
    return (UPS_INV_PARAMETER);
  }

  /* UPS_ENABLE_TRANSACTIONS implies UPS_ENABLE_RECOVERY, unless explicitly
   * disabled */
  if ((flags & UPS_ENABLE_TRANSACTIONS) && !(flags & UPS_DISABLE_RECOVERY))
    flags |= UPS_ENABLE_RECOVERY;

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_RECOVERY */
  if (flags & UPS_AUTO_RECOVERY)
    flags |= UPS_ENABLE_RECOVERY;

  /* in-memory with Transactions? disable recovery */
  if (flags & UPS_IN_MEMORY)
    flags &= ~UPS_ENABLE_RECOVERY;

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
        if (flags & UPS_IN_MEMORY && param->value != 0) {
          ups_trace(("combination of UPS_IN_MEMORY and cache size != 0 "
                "not allowed"));
          return (UPS_INV_PARAMETER);
        }
        /* don't allow cache limits with unlimited cache */
        if (flags & UPS_CACHE_UNLIMITED && param->value != 0) {
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
        if (flags & UPS_IN_MEMORY) {
          ups_trace(("aes encryption not allowed in combination with "
                  "UPS_IN_MEMORY"));
          return (UPS_INV_PARAMETER);
        }
        memcpy(config.encryption_key, (uint8_t *)param->value, 16);
        config.is_encryption_enabled = true;
        flags |= UPS_DISABLE_MMAP;
        break;
#else
        ups_trace(("aes encrpytion was disabled at compile time"));
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

  if (config.filename.empty() && !(flags & UPS_IN_MEMORY)) {
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

  ups_status_t st = 0;
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
  atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  st = env->create();

  /* flush the environment to make sure that the header page is written
   * to disk TODO required?? */
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
  DatabaseConfiguration config;

  if (!hdb) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.env_create_db", "%u, 0x%x", (uint32_t)db_name, flags));

  *hdb = 0;

  if (!db_name || (db_name >= 0xf000)) {
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
  DatabaseConfiguration config;

  if (!hdb) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.env_open_db", "%u, 0x%x", (uint32_t)db_name, flags));

  *hdb = 0;

  if (!db_name) {
    ups_trace(("parameter 'db_name' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (db_name >= 0xf000) {
    ups_trace(("database name must be lower than 0xf000"));
    return (UPS_INV_PARAMETER);
  }
  if (env->get_flags() & UPS_IN_MEMORY) {
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
  EnvironmentConfiguration config;
  config.filename = filename ? filename : "";

  if (!henv) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_OPEN(filename);
  EVENTLOG_APPEND((filename, "f.env_open", "0x%x", flags));

  *henv = 0;

  /* cannot open an in-memory-db */
  if (flags & UPS_IN_MEMORY) {
    ups_trace(("cannot open an in-memory database"));
    return (UPS_INV_PARAMETER);
  }

  /* UPS_ENABLE_DUPLICATE_KEYS has to be specified in ups_env_create_db,
   * not ups_env_open */
  if (flags & UPS_ENABLE_DUPLICATE_KEYS) {
    ups_trace(("invalid flag UPS_ENABLE_DUPLICATE_KEYS (only allowed when "
        "creating a database"));
    return (UPS_INV_PARAMETER);
  }

  /* UPS_ENABLE_TRANSACTIONS implies UPS_ENABLE_RECOVERY, unless explicitly
   * disabled */
  if ((flags & UPS_ENABLE_TRANSACTIONS) && !(flags & UPS_DISABLE_RECOVERY))
    flags |= UPS_ENABLE_RECOVERY;

  /* flag UPS_AUTO_RECOVERY implies UPS_ENABLE_RECOVERY */
  if (flags & UPS_AUTO_RECOVERY)
    flags |= UPS_ENABLE_RECOVERY;

  if (config.filename.empty() && !(flags & UPS_IN_MEMORY)) {
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
        if (flags & UPS_CACHE_UNLIMITED && param->value != 0) {
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
        memcpy(config.encryption_key, (uint8_t *)param->value, 16);
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

  ups_status_t st = 0;
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
  atexit(Protocol::shutdown);
#endif

  /* and finish the initialization of the Environment */
  st = env->open();

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
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!oldname) {
    ups_trace(("parameter 'oldname' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (!newname) {
    ups_trace(("parameter 'newname' must not be 0"));
    return (UPS_INV_PARAMETER);
  }
  if (newname >= 0xf000) {
    ups_trace(("parameter 'newname' must be lower than 0xf000"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.env_rename_db", "%u, %u", (uint32_t)oldname,
              (uint32_t)newname));

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
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!name) {
    ups_trace(("parameter 'name' must not be 0"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.env_erase_db", "%u", (uint32_t)name));

  /* erase the database */
  return (env->erase_db(name, flags));
}

ups_status_t UPS_CALLCONV
ups_env_get_database_names(ups_env_t *henv, uint16_t *names, uint32_t *count)
{
  Environment *env = (Environment *)henv;
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!names) {
    ups_trace(("parameter 'names' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!count) {
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
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!param) {
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
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.env_flush", "0x%x", flags));

  if (flags && flags != UPS_FLUSH_COMMITTED_TRANSACTIONS) {
    ups_trace(("parameter 'flags' is unused, set to 0"));
    return (UPS_INV_PARAMETER);
  }

  /* flush the Environment */
  return (env->flush(flags));
}

ups_status_t UPS_CALLCONV
ups_env_close(ups_env_t *henv, uint32_t flags)
{
  ups_status_t st;
  Environment *env = (Environment *)henv;

  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

#ifdef UPS_ENABLE_EVENT_LOGGING
  std::string filename = env->config().filename;
  EVENTLOG_APPEND((filename.c_str(), "f.env_close", "0x%x", flags));
#endif

  try {
    /* close the environment */
    st = env->close(flags);
    if (st)
      return (st);

    delete env;
#ifdef UPS_ENABLE_EVENT_LOGGING
    EVENTLOG_CLOSE(filename.c_str());
#endif
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
  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!param) {
    ups_trace(("parameter 'param' must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  ScopedLock lock(db->get_env()->mutex());

  /* get the parameters */
  return (db->get_parameters(param));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_set_compare_func(ups_db_t *hdb, ups_compare_func_t foo)
{
  Database *db = (Database *)hdb;
  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!foo) {
    ups_trace(("function pointer must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  LocalDatabase *ldb = dynamic_cast<LocalDatabase *>(db);
  if (!ldb) {
    ups_trace(("operation not possible for remote databases"));
    return (UPS_INV_PARAMETER); 
  }

  EVENTLOG_APPEND((ldb->get_env()->config().filename.c_str(),
              "f.db_set_compare_func", "%u", (uint32_t)ldb->name()));

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
  Environment *env;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  env = db->get_env();

  ScopedLock lock(env->mutex());

  if (!key) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!record) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_PREPEND) {
    ups_trace(("flag UPS_HINT_PREPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_APPEND) {
    ups_trace(("flag UPS_HINT_APPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DIRECT_ACCESS)
      && !(env->get_flags() & UPS_IN_MEMORY)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
          "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DIRECT_ACCESS)
      && (env->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (db->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }

  /* record number: make sure that we have a valid key structure */
  if ((db->get_flags() & UPS_RECORD_NUMBER32) && !key->data) {
    ups_trace(("key->data must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if ((db->get_flags() & UPS_RECORD_NUMBER64) && !key->data) {
    ups_trace(("key->data must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (!__prepare_key(key) || !__prepare_record(record))
    return (UPS_INV_PARAMETER);

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.db_find", "%u, %u, %s, 0x%x", (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0,
              key ? EventLog::escape(key->data, key->size) : "",
              flags));

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
  Environment *env;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return UPS_INV_PARAMETER;
  }
  env = db->get_env();

  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (!key) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!record) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_APPEND) {
    ups_trace(("flags UPS_HINT_APPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_PREPEND) {
    ups_trace(("flags UPS_HINT_PREPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (db->get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot insert in a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }
  if ((flags & UPS_OVERWRITE) && (flags & UPS_DUPLICATE)) {
    ups_trace(("cannot combine UPS_OVERWRITE and UPS_DUPLICATE"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (db->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL) && (record->size <= sizeof(uint64_t))) {
    ups_trace(("flag UPS_PARTIAL is not allowed if record->size "
          "<= 8"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size)) {
    ups_trace(("partial offset+size is greater than the total "
          "record size"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DUPLICATE)
      && !(db->get_flags() & UPS_ENABLE_DUPLICATE_KEYS)) {
    ups_trace(("database does not support duplicate keys "
          "(see UPS_ENABLE_DUPLICATE_KEYS)"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DUPLICATE_INSERT_AFTER)
      || (flags & UPS_DUPLICATE_INSERT_BEFORE)
      || (flags & UPS_DUPLICATE_INSERT_LAST)
      || (flags & UPS_DUPLICATE_INSERT_FIRST)) {
    ups_trace(("function does not support flags UPS_DUPLICATE_INSERT_*; "
          "see ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }

  if (!__prepare_key(key) || !__prepare_record(record))
    return (UPS_INV_PARAMETER);

  /* allocate temp. storage for a recno key */
  if ((db->get_flags() & UPS_RECORD_NUMBER32)
      || (db->get_flags() & UPS_RECORD_NUMBER64)) {
    if (flags & UPS_OVERWRITE) {
      if (!key->data) {
        ups_trace(("key->data must not be NULL"));
        return (UPS_INV_PARAMETER);
      }
    }
    else {
      if (key->flags & UPS_KEY_USER_ALLOC) {
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
  }

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.db_insert", "%u, %u, %s, %u, 0x%x", (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0,
              key ? EventLog::escape(key->data, key->size) : "",
              (uint32_t)record->size, flags));

  return (db->insert(0, txn, key, record, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_erase(ups_db_t *hdb, ups_txn_t *htxn, ups_key_t *key, uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  env = db->get_env();

  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (!key) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_PREPEND) {
    ups_trace(("flag UPS_HINT_PREPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_APPEND) {
    ups_trace(("flag UPS_HINT_APPEND is only allowed in "
          "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (db->get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot erase from a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }

  if (!__prepare_key(key))
    return (UPS_INV_PARAMETER);

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.db_erase", "%u, %u, %s, 0x%x", (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0,
              key ? EventLog::escape(key->data, key->size) : "",
              flags));

  return (db->erase(0, txn, key, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_check_integrity(ups_db_t *hdb, uint32_t flags)
{
  Database *db = (Database *)hdb;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if (flags && flags != UPS_PRINT_GRAPH) {
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

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  if ((flags & UPS_TXN_AUTO_ABORT) && (flags & UPS_TXN_AUTO_COMMIT)) {
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

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.db_close", "%u, 0x%x", (uint32_t)db->name(), flags));

  return (env->close_db(db, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_create(ups_cursor_t **hcursor, ups_db_t *hdb, ups_txn_t *htxn,
                uint32_t flags)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;
  Environment *env;
  Cursor **cursor = 0;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  cursor = (Cursor **)hcursor;
  env = db->get_env();

  EVENTLOG_APPEND((env->config().filename.c_str(),
              "f.cursor_create", "%u, %u, 0x%x", (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0, flags));

  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  return (db->cursor_create(cursor, txn, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_clone(ups_cursor_t *hsrc, ups_cursor_t **hdest)
{
  if (!hsrc) {
    ups_trace(("parameter 'src' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!hdest) {
    ups_trace(("parameter 'dest' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *src, **dest;
  src = (Cursor *)hsrc;
  dest = (Cursor **)hdest;

  Database *db = src->db();
  ScopedLock lock(db->get_env()->mutex());

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_clone", ""));

  return (db->cursor_clone(dest, src));
}

ups_status_t UPS_CALLCONV
ups_cursor_overwrite(ups_cursor_t *hcursor, ups_record_t *record,
                uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (flags) {
    ups_trace(("function does not support a non-zero flags value; "
          "see ups_cursor_insert for an alternative then"));
    return (UPS_INV_PARAMETER);
  }
  if (!record) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!__prepare_record(record))
    return (UPS_INV_PARAMETER);
  if (db->get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot overwrite in a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_overwrite", "%u, %u, 0x%x", (uint32_t)db->name(),
              (uint32_t)record->size, flags));

  return (cursor->overwrite(record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_move(ups_cursor_t *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if ((flags & UPS_ONLY_DUPLICATES) && (flags & UPS_SKIP_DUPLICATES)) {
    ups_trace(("combination of UPS_ONLY_DUPLICATES and "
          "UPS_SKIP_DUPLICATES not allowed"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = db->get_env();

  if ((flags & UPS_DIRECT_ACCESS)
      && !(env->get_flags() & UPS_IN_MEMORY)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DIRECT_ACCESS)
      && (env->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (db->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }

  if (key && !__prepare_key(key))
    return (UPS_INV_PARAMETER);
  if (record && !__prepare_record(record))
    return (UPS_INV_PARAMETER);

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_move", "%u, 0x%x", (uint32_t)db->name(), flags));

  return (db->cursor_move(cursor, key, record, flags));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_find(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  Environment *env = db->get_env();

  ScopedLock lock;
  if (!(flags & UPS_DONT_LOCK))
    lock = ScopedLock(env->mutex());

  if (!key) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DIRECT_ACCESS)
      && !(env->get_flags() & UPS_IN_MEMORY)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is only allowed in "
           "In-Memory Databases"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DIRECT_ACCESS)
      && (env->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_DIRECT_ACCESS is not allowed in "
          "combination with Transactions"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_PREPEND) {
    ups_trace(("flag UPS_HINT_PREPEND is only allowed in "
           "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_APPEND) {
    ups_trace(("flag UPS_HINT_APPEND is only allowed in "
           "ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (db->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }

  if (key && !__prepare_key(key))
    return (UPS_INV_PARAMETER);
  if (record && !__prepare_record(record))
    return (UPS_INV_PARAMETER);

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_find", "%u, %s, 0x%x", (uint32_t)db->name(),
              key ? EventLog::escape(key->data, key->size) : "",
              flags));

  return (db->find(cursor, cursor->get_txn(), key, record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_insert(ups_cursor_t *hcursor, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;

  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (!key) {
    ups_trace(("parameter 'key' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!record) {
    ups_trace(("parameter 'record' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags&UPS_HINT_APPEND) && (flags&UPS_HINT_PREPEND)) {
    ups_trace(("flags UPS_HINT_APPEND and UPS_HINT_PREPEND "
           "are mutually exclusive"));
    return (UPS_INV_PARAMETER);
  }
  if (!__prepare_key(key) || !__prepare_record(record))
    return (UPS_INV_PARAMETER);

  if (db->get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot insert to a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }
  if ((flags & UPS_DUPLICATE) && (flags & UPS_OVERWRITE)) {
    ups_trace(("cannot combine UPS_DUPLICATE and UPS_OVERWRITE"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_DUPLICATE)
      && !(db->get_flags() & UPS_ENABLE_DUPLICATE_KEYS)) {
    ups_trace(("database does not support duplicate keys "
          "(see UPS_ENABLE_DUPLICATE_KEYS)"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL)
      && (db->get_flags() & UPS_ENABLE_TRANSACTIONS)) {
    ups_trace(("flag UPS_PARTIAL is not allowed in combination with "
          "transactions"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags&UPS_PARTIAL)
      && (record->partial_size + record->partial_offset > record->size)) {
    ups_trace(("partial offset+size is greater than the total "
          "record size"));
    return (UPS_INV_PARAMETER);
  }
  if ((flags & UPS_PARTIAL) && (record->size <= sizeof(uint64_t))) {
    ups_trace(("flag UPS_PARTIAL is not allowed if record->size <= 8"));
    return (UPS_INV_PARAMETER);
  }

  /*
   * set flag UPS_DUPLICATE if one of DUPLICATE_INSERT* is set, but do
   * not allow these flags if duplicate sorting is enabled
   */
  if (flags & (UPS_DUPLICATE_INSERT_AFTER
        | UPS_DUPLICATE_INSERT_BEFORE
        | UPS_DUPLICATE_INSERT_LAST
        | UPS_DUPLICATE_INSERT_FIRST)) {
    flags |= UPS_DUPLICATE;
  }

  /* allocate temp. storage for a recno key */
  if ((db->get_flags() & UPS_RECORD_NUMBER32)
      || (db->get_flags() & UPS_RECORD_NUMBER64)) {
    if (flags & UPS_OVERWRITE) {
      if (!key->data) {
        ups_trace(("key->data must not be NULL"));
        return (UPS_INV_PARAMETER);
      }
    }
    else {
      if (key->flags & UPS_KEY_USER_ALLOC) {
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
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_insert", "%u, %s, %u, 0x%x", (uint32_t)db->name(),
              key ? EventLog::escape(key->data, key->size) : "",
              record->size, flags));

  return (db->insert(cursor, cursor->get_txn(), key, record, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_erase(ups_cursor_t *hcursor, uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (db->get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot erase from a read-only database"));
    return (UPS_WRITE_PROTECTED);
  }
  if (flags & UPS_HINT_PREPEND) {
    ups_trace(("flags UPS_HINT_PREPEND only allowed in ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & UPS_HINT_APPEND) {
    ups_trace(("flags UPS_HINT_APPEND only allowed in ups_cursor_insert"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_erase", "%u, 0x%x", (uint32_t)db->name(), flags));

  return (db->erase(cursor, cursor->get_txn(), 0, flags));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_count(ups_cursor_t *hcursor, uint32_t *count,
                uint32_t flags)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (!count) {
    ups_trace(("parameter 'count' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_get_duplicate_count", "%u, 0x%x",
              (uint32_t)db->name(), flags));

  return (cursor->get_duplicate_count(flags, count));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_position(ups_cursor_t *hcursor, uint32_t *position)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (!position) {
    ups_trace(("parameter 'position' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_get_duplicate_position", "%u",
              (uint32_t)db->name()));

  return (cursor->get_duplicate_position(position));
}

ups_status_t UPS_CALLCONV
ups_cursor_get_record_size(ups_cursor_t *hcursor, uint64_t *size)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  if (!size) {
    ups_trace(("parameter 'size' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_get_record_size", "%u", (uint32_t)db->name()));

  return (cursor->get_record_size(size));
}

ups_status_t UPS_CALLCONV
ups_cursor_close(ups_cursor_t *hcursor)
{
  if (!hcursor) {
    ups_trace(("parameter 'cursor' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  Cursor *cursor = (Cursor *)hcursor;
  Database *db = cursor->db();
  ScopedLock lock(db->get_env()->mutex());

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.cursor_close", "%u", (uint32_t)db->name()));

  return (db->cursor_close(cursor));
}

void UPS_CALLCONV
ups_set_context_data(ups_db_t *hdb, void *data)
{
  Database *db = (Database *)hdb;

  if (!db)
    return;

  ScopedLock lock(db->get_env()->mutex());
  db->set_context_data(data);
}

void * UPS_CALLCONV
ups_get_context_data(ups_db_t *hdb, ups_bool_t dont_lock)
{
  Database *db = (Database *)hdb;
  if (!db)
    return (0);

  if (dont_lock)
    return (db->get_context_data());

  ScopedLock lock(db->get_env()->mutex());
  return (db->get_context_data());
}

ups_db_t * UPS_CALLCONV
ups_cursor_get_database(ups_cursor_t *hcursor)
{
  if (hcursor) {
    Cursor *cursor = (Cursor *)hcursor;
    return ((ups_db_t *)cursor->db());
  }
  return (0);
}

ups_env_t * UPS_CALLCONV
ups_db_get_env(ups_db_t *hdb)
{
  Database *db = (Database *)hdb;
  if (!db)
    return (0);

  return ((ups_env_t *)db->get_env());
}

ups_status_t UPS_CALLCONV
ups_db_count(ups_db_t *hdb, ups_txn_t *htxn, uint32_t flags,
                uint64_t *count)
{
  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  if (!db) {
    ups_trace(("parameter 'db' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (flags & ~(UPS_SKIP_DUPLICATES)) {
    ups_trace(("parameter 'flag' contains unsupported flag bits: %08x",
          flags & (~UPS_SKIP_DUPLICATES)));
    return (UPS_INV_PARAMETER);
  }
  if (!count) {
    ups_trace(("parameter 'count' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());

  EVENTLOG_APPEND((db->get_env()->config().filename.c_str(),
              "f.db_count", "%u, 0x%x", (uint32_t)db->name(),
              flags));

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
  if (!env) {
    ups_trace(("parameter 'env' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }
  if (!metrics) {
    ups_trace(("parameter 'metrics' must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  memset(metrics, 0, sizeof(ups_env_metrics_t));
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
