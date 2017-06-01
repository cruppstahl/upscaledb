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

#include <iostream>
#include <boost/filesystem.hpp>

#include <ups/upscaledb_int.h>

#include "metrics.h"
#include "configuration.h"
#include "misc.h"
#include "upscaledb.h"
#include "1globals/globals.h"

ups_env_t *UpscaleDatabase::ms_env = 0;
#ifdef UPS_ENABLE_REMOTE
ups_env_t *UpscaleDatabase::ms_remote_env = 0;
ups_srv_t *UpscaleDatabase::ms_srv = 0;
#endif
Mutex      UpscaleDatabase::ms_mutex;
int        UpscaleDatabase::ms_refcount;

static int 
compare_keys(ups_db_t *db,
      const uint8_t *lhs_data, uint32_t lhs_size, 
      const uint8_t *rhs_data, uint32_t rhs_size)
{
  (void)db;

  if (lhs_size < rhs_size) {
    int m = ::memcmp(lhs_data, rhs_data, lhs_size);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (-1);
  }
  else if (rhs_size < lhs_size) {
    int m = ::memcmp(lhs_data, rhs_data, rhs_size);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (+1);
  }
  else {
    int m = memcmp(lhs_data, rhs_data, lhs_size);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (0);
  }
}

ups_status_t
UpscaleDatabase::do_create_env()
{
  ups_status_t st = 0;
  uint32_t flags = 0;
  ups_parameter_t params[6] = {{0, 0}};

  ScopedLock lock(ms_mutex);

  ms_refcount++;

  upscaledb::Globals::ms_extended_threshold = m_config->extkey_threshold;
  upscaledb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;

  int p = 0;
  if (ms_env == 0) {
    params[p].name = UPS_PARAM_CACHE_SIZE;
    params[p].value = m_config->cachesize;
    p++;
    params[p].name = UPS_PARAM_PAGE_SIZE;
    params[p].value = m_config->pagesize;
    p++;
    params[p].name = UPS_PARAM_POSIX_FADVISE;
    params[p].value = m_config->posix_fadvice;
    p++;
    if (m_config->use_encryption) {
      params[p].name = UPS_PARAM_ENCRYPTION_KEY;
      params[p].value = (uint64_t)"1234567890123456";
      p++;
    }
    if (m_config->journal_compression) {
      params[p].name = UPS_PARAM_JOURNAL_COMPRESSION;
      params[p].value = m_config->journal_compression;
      p++;
    }

    flags |= m_config->inmemory ? UPS_IN_MEMORY : 0; 
    flags |= m_config->no_mmap ? UPS_DISABLE_MMAP : 0; 
    flags |= m_config->cacheunlimited ? UPS_CACHE_UNLIMITED : 0;
    flags |= m_config->use_transactions ? UPS_ENABLE_TRANSACTIONS : 0;
    flags |= m_config->flush_txn_immediately ? UPS_FLUSH_TRANSACTIONS_IMMEDIATELY : 0;
    flags |= m_config->use_fsync ? UPS_ENABLE_FSYNC : 0;
    flags |= m_config->disable_recovery ? UPS_DISABLE_RECOVERY : 0;
    flags |= m_config->enable_crc32 ? UPS_ENABLE_CRC32 : 0;

    boost::filesystem::remove("test-ham.db");

    st = ups_env_create(&ms_env, "test-ham.db", flags, 0664,
                  &params[0]);
    if (st) {
      LOG_ERROR(("ups_env_create failed with error %d (%s)\n",
                              st, ups_strerror(st)));
      return (st);
    }
  }

  // remote client/server? start the server, attach the environment and then
  // open the remote environment
#ifdef UPS_ENABLE_REMOTE
  if (m_config->use_remote) {
    ms_remote_env = ms_env;
    ms_env = 0;

    if (ms_srv == 0) {
      ups_srv_config_t cfg;
      memset(&cfg, 0, sizeof(cfg));
      cfg.port = 10123;
      ups_srv_init(&cfg, &ms_srv);
      ups_srv_add_env(ms_srv, ms_remote_env, "/env1.db");
    }

    uint32_t flags = 0;
    // flags |= m_config->duplicate ? UPS_ENABLE_DUPLICATES : 0;
    st = ups_env_open(&m_env, "ups://localhost:10123/env1.db", flags, 0);
    if (st)
      LOG_ERROR(("ups_env_open failed with error %d (%s)\n",
                              st, ups_strerror(st)));
  }
#endif

  return (st);
}

ups_status_t
UpscaleDatabase::do_open_env()
{
  ups_status_t st = 0;
  uint32_t flags = 0;
  ups_parameter_t params[6] = {{0, 0}};

  ScopedLock lock(ms_mutex);

  ms_refcount++;

  upscaledb::Globals::ms_extended_threshold = m_config->extkey_threshold;
  upscaledb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;

  // check if another thread was faster
  if (ms_env == 0) {
    int p = 0;
    params[p].name = UPS_PARAM_CACHE_SIZE;
    params[p].value = m_config->cachesize;
    p++;
    params[p].name = UPS_PARAM_POSIX_FADVISE;
    params[p].value = m_config->posix_fadvice;
    p++;
    if (m_config->use_encryption) {
      params[p].name = UPS_PARAM_ENCRYPTION_KEY;
      params[p].value = (uint64_t)"1234567890123456";
      p++;
    }

    flags |= m_config->no_mmap ? UPS_DISABLE_MMAP : 0; 
    flags |= m_config->cacheunlimited ? UPS_CACHE_UNLIMITED : 0;
    flags |= m_config->use_transactions
                ? (UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY)
                : 0;
    flags |= m_config->flush_txn_immediately ? UPS_FLUSH_TRANSACTIONS_IMMEDIATELY : 0;
    flags |= m_config->use_fsync ? UPS_ENABLE_FSYNC : 0;
    flags |= m_config->disable_recovery ? UPS_DISABLE_RECOVERY : 0;
    flags |= m_config->read_only ? UPS_READ_ONLY : 0;
    flags |= m_config->enable_crc32 ? UPS_ENABLE_CRC32 : 0;

    st = ups_env_open(&ms_env, "test-ham.db", flags, &params[0]);
    if (st) {
      LOG_ERROR(("ups_env_open failed with error %d (%s)\n",
                              st, ups_strerror(st)));
      return (st);
    }
  }

  // remote client/server? start the server, attach the environment and then
  // open the remote environment
#ifdef UPS_ENABLE_REMOTE
  if (m_config->use_remote) {
    ms_remote_env = ms_env;
    ms_env = 0;

    if (ms_srv == 0) {
      ups_srv_config_t cfg;
      memset(&cfg, 0, sizeof(cfg));
      cfg.port = 10123;
      ups_srv_init(&cfg, &ms_srv);
      ups_srv_add_env(ms_srv, ms_remote_env, "/env1.db");
    }

    uint32_t flags = 0;
    // flags |= m_config->duplicate ? UPS_ENABLE_DUPLICATES : 0;
    st = ups_env_open(&m_env, "ups://localhost:10123/env1.db", flags, 0);
    if (st)
      LOG_ERROR(("ups_env_open failed with error %d (%s)\n", st, ups_strerror(st)));
  }
#endif

  return (st);
}

ups_status_t
UpscaleDatabase::do_close_env()
{
  ScopedLock lock(ms_mutex);

  if (m_env)
    ups_env_get_metrics(m_env, &m_upscaledb_metrics);

  if (ms_refcount == 0) {
    assert(m_env == 0);
    assert(ms_env == 0);
    return (0);
  }

  if (--ms_refcount > 0)
    return (0);

  if (m_env) {
    ups_env_close(m_env, 0);
    m_env = 0;
  }
  if (ms_env) {
    ups_env_get_metrics(ms_env, &m_upscaledb_metrics);
    ups_env_close(ms_env, 0);
    ms_env = 0;
  }
#ifdef UPS_ENABLE_REMOTE
  if (ms_remote_env) {
    ups_env_close(ms_remote_env, 0);
    ms_remote_env = 0;
  }
  if (ms_srv) {
    ups_srv_close(ms_srv);
    ms_srv = 0;
  }
#endif
  return (0);
}

ups_status_t
UpscaleDatabase::do_create_db(int id)
{
  ups_status_t st;
  ups_parameter_t params[8] = {{0, 0}};

  int n = 0;
  params[n].name = UPS_PARAM_KEY_SIZE;
  params[n].value = 0;
  n++;
  switch (m_config->key_type) {
    case Configuration::kKeyCustom:
      params[0].value = m_config->key_is_fixed_size
                            ? m_config->key_size
                            : UPS_KEY_SIZE_UNLIMITED;
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_CUSTOM;
      n++;
      break;
    case Configuration::kKeyBinary:
    case Configuration::kKeyString:
      params[0].value = m_config->key_is_fixed_size
                            ? m_config->key_size
                            : UPS_KEY_SIZE_UNLIMITED;
      break;
    case Configuration::kKeyUint8:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_UINT8;
      n++;
      break;
    case Configuration::kKeyUint16:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_UINT16;
      n++;
      break;
    case Configuration::kKeyUint32:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_UINT32;
      n++;
      break;
    case Configuration::kKeyUint64:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_UINT64;
      n++;
      break;
    case Configuration::kKeyReal32:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_REAL32;
      n++;
      break;
    case Configuration::kKeyReal64:
      params[n].name = UPS_PARAM_KEY_TYPE;
      params[n].value = UPS_TYPE_REAL64;
      n++;
      break;
    default:
      assert(!"shouldn't be here");
  }
  switch (m_config->record_type) {
    case Configuration::kKeyBinary:
    case Configuration::kKeyString:
      break;
    case Configuration::kKeyUint8:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_UINT8;
      n++;
      break;
    case Configuration::kKeyUint16:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_UINT16;
      n++;
      break;
    case Configuration::kKeyUint32:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_UINT32;
      n++;
      break;
    case Configuration::kKeyUint64:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_UINT64;
      n++;
      break;
    case Configuration::kKeyReal32:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_REAL32;
      n++;
      break;
    case Configuration::kKeyReal64:
      params[n].name = UPS_PARAM_RECORD_TYPE;
      params[n].value = UPS_TYPE_REAL64;
      n++;
      break;
    default:
      assert(!"shouldn't be here");
  }

  params[n].name = UPS_PARAM_RECORD_SIZE;
  params[n].value = m_config->rec_size_fixed;
  n++;
  if (m_config->record_compression) {
    params[n].name = UPS_PARAM_RECORD_COMPRESSION;
    params[n].value = m_config->record_compression;
    n++;
  }
  if (m_config->key_compression) {
    params[n].name = UPS_PARAM_KEY_COMPRESSION;
    params[n].value = m_config->key_compression;
    n++;
  }
  if (m_config->key_type == Configuration::kKeyCustom) {
    ups_register_compare("cmp", compare_keys);
    params[n].name = UPS_PARAM_CUSTOM_COMPARE_NAME;
    params[n].value = (uint64_t)"cmp";
    n++;
  }

  uint32_t flags = 0;

  flags |= m_config->duplicate ? UPS_ENABLE_DUPLICATES : 0;
  flags |= m_config->record_number32 ? UPS_RECORD_NUMBER32 : 0;
  flags |= m_config->record_number64 ? UPS_RECORD_NUMBER64 : 0;
  if (m_config->force_records_inline)
    flags |= UPS_FORCE_RECORDS_INLINE;

  st = ups_env_create_db(m_env ? m_env : ms_env, &m_db, 1 + id,
                  flags, &params[0]);
  if (st) {
    LOG_ERROR(("ups_env_create_db failed with error %d (%s)\n", st,
                            ups_strerror(st)));
    exit(-1);
  }

  return (0);
}

ups_status_t
UpscaleDatabase::do_open_db(int id)
{
  ups_parameter_t params[6] = {{0, 0}};
  ups_register_compare("cmp", compare_keys);

  ups_status_t st = ups_env_open_db(m_env ? m_env : ms_env,
                        &m_db, 1 + id, 0, &params[0]);
  if (st) {
    LOG_ERROR(("ups_env_open_db failed with error %d (%s)\n", st,
                            ups_strerror(st)));
    exit(-1);
  }

  return (st);
}

ups_status_t
UpscaleDatabase::do_close_db()
{
  if (m_db)
    ups_db_close(m_db, UPS_AUTO_CLEANUP);
  m_db = 0;
  return (0);
}

ups_status_t
UpscaleDatabase::do_flush()
{
  return (ups_env_flush(m_env ? m_env : ms_env, 0));
}

ups_status_t
UpscaleDatabase::do_insert(Txn *txn, ups_key_t *key,
                ups_record_t *record)
{
  uint32_t flags = 0;

  if (m_config->overwrite)
    flags |= UPS_OVERWRITE;
  else if (m_config->duplicate)
    flags |= UPS_DUPLICATE;

  ups_key_t recno_key = {0};
  if (m_config->record_number32 || m_config->record_number64)
    key = &recno_key;

  ups_status_t st = ups_db_insert(m_db, (ups_txn_t *)txn, key, record, flags);
  if (st)
    LOG_VERBOSE(("insert: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_erase(Txn *txn, ups_key_t *key)
{
  ups_status_t st = ups_db_erase(m_db, (ups_txn_t *)txn, key, 0);
  if (st)
    LOG_VERBOSE(("erase: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_find(Txn *txn, ups_key_t *key, ups_record_t *record)
{
  uint32_t flags = 0;

#if 0
  if (!m_txn) {
    record->flags = UPS_RECORD_USER_ALLOC;
    record->data = m_useralloc;
  }
#endif

  ups_status_t st = ups_db_find(m_db, (ups_txn_t *)txn, key, record, flags);
  if (st)
     LOG_VERBOSE(("find: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_check_integrity()
{
  return (ups_db_check_integrity(m_db, 0));
}

Database::Txn *
UpscaleDatabase::do_txn_begin()
{
  ups_status_t st = ups_txn_begin(&m_txn, m_env ? m_env : ms_env, 0, 0, 0);
  if (st) {
    LOG_ERROR(("ups_txn_begin failed with error %d (%s)\n",
                st, ups_strerror(st)));
    return (0);
  }
  return ((Database::Txn *)m_txn);
}

ups_status_t
UpscaleDatabase::do_txn_commit(Txn *txn)
{
  assert((ups_txn_t *)txn == m_txn);

  ups_status_t st = ups_txn_commit((ups_txn_t *)txn, 0);
  if (st)
    LOG_ERROR(("ups_txn_commit failed with error %d (%s)\n",
                st, ups_strerror(st)));
  m_txn = 0;
  return (st);
}

ups_status_t
UpscaleDatabase::do_txn_abort(Txn *txn)
{
  assert((ups_txn_t *)txn == m_txn);

  ups_status_t st = ups_txn_abort((ups_txn_t *)txn, 0);
  if (st)
    LOG_ERROR(("ups_txn_abort failed with error %d (%s)\n",
                st, ups_strerror(st)));
  m_txn = 0;
  return (st);
}

Database::Cursor *
UpscaleDatabase::do_cursor_create()
{
  ups_cursor_t *cursor;

  ups_status_t st = ups_cursor_create(&cursor, m_db, m_txn, 0);
  if (st) {
    LOG_ERROR(("ups_cursor_create failed with error %d (%s)\n", st,
                            ups_strerror(st)));
    exit(-1);
  }

  return ((Database::Cursor *)cursor);
}

ups_status_t
UpscaleDatabase::do_cursor_insert(Cursor *cursor, ups_key_t *key,
                ups_record_t *record)
{
  uint32_t flags = 0;

  if (m_config->overwrite)
    flags |= UPS_OVERWRITE;
  if (m_config->duplicate == Configuration::kDuplicateFirst)
    flags |= UPS_DUPLICATE | UPS_DUPLICATE_INSERT_FIRST;
  else if (m_config->duplicate == Configuration::kDuplicateLast)
    flags |= UPS_DUPLICATE | UPS_DUPLICATE_INSERT_LAST;

  ups_status_t st = ups_cursor_insert((ups_cursor_t *)cursor, key,
                  record, flags);
  if (st)
     LOG_VERBOSE(("cursor_insert: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_cursor_erase(Cursor *cursor, ups_key_t *key)
{
  ups_status_t st = ups_cursor_find((ups_cursor_t *)cursor, key, 0, 0);
  if (st) {
    LOG_VERBOSE(("cursor_find: failed w/ %d (%s)\n", st, ups_strerror(st)));
    return (st);
  }
  st = ups_cursor_erase((ups_cursor_t *)cursor, 0);
  if (st)
    LOG_VERBOSE(("cursor_erase: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_cursor_find(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record)
{
  ups_status_t st = ups_cursor_find((ups_cursor_t *)cursor, key, record, 0);
  if (st)
    LOG_VERBOSE(("cursor_erase: failed w/ %d (%s)\n", st, ups_strerror(st)));
  return (st);
}

ups_status_t
UpscaleDatabase::do_cursor_get_previous(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  uint32_t flags = 0;

  if (skip_duplicates)
    flags |= UPS_SKIP_DUPLICATES;

  return (ups_cursor_move((ups_cursor_t *)cursor, key, record,
                          UPS_CURSOR_PREVIOUS | flags));
}

ups_status_t
UpscaleDatabase::do_cursor_get_next(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  uint32_t flags = 0;

  if (skip_duplicates)
    flags |= UPS_SKIP_DUPLICATES;

  return (ups_cursor_move((ups_cursor_t *)cursor, key, record,
                          UPS_CURSOR_NEXT | flags));
}

ups_status_t
UpscaleDatabase::do_cursor_close(Cursor *cursor)
{
  return (ups_cursor_close((ups_cursor_t *)cursor));
}

