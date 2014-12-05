/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include <iostream>
#include <boost/filesystem.hpp>

#include <ham/hamsterdb_int.h>

#include "metrics.h"
#include "configuration.h"
#include "misc.h"
#include "hamsterdb.h"
#include "1globals/globals.h"

ham_env_t *HamsterDatabase::ms_env = 0;
#ifdef HAM_ENABLE_REMOTE
ham_env_t *HamsterDatabase::ms_remote_env = 0;
ham_srv_t *HamsterDatabase::ms_srv = 0;
#endif
Mutex      HamsterDatabase::ms_mutex;
int        HamsterDatabase::ms_refcount;

static int 
compare_keys(ham_db_t *db,
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

ham_status_t
HamsterDatabase::do_create_env()
{
  ham_status_t st = 0;
  uint32_t flags = 0;
  ham_parameter_t params[6] = {{0, 0}};

  ScopedLock lock(ms_mutex);

  ms_refcount++;

  hamsterdb::Globals::ms_extended_threshold = m_config->extkey_threshold;
  hamsterdb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;

  int p = 0;
  if (ms_env == 0) {
    params[p].name = HAM_PARAM_CACHE_SIZE;
    params[p].value = m_config->cachesize;
    p++;
    params[p].name = HAM_PARAM_PAGE_SIZE;
    params[p].value = m_config->pagesize;
    p++;
    params[p].name = HAM_PARAM_POSIX_FADVISE;
    params[p].value = m_config->posix_fadvice;
    p++;
    if (m_config->use_encryption) {
      params[p].name = HAM_PARAM_ENCRYPTION_KEY;
      params[p].value = (uint64_t)"1234567890123456";
      p++;
    }
    if (m_config->journal_compression) {
      params[p].name = HAM_PARAM_JOURNAL_COMPRESSION;
      params[p].value = m_config->journal_compression;
      p++;
    }

    flags |= m_config->inmemory ? HAM_IN_MEMORY : 0; 
    flags |= m_config->no_mmap ? HAM_DISABLE_MMAP : 0; 
    flags |= m_config->use_recovery ? HAM_ENABLE_RECOVERY : 0;
    flags |= m_config->cacheunlimited ? HAM_CACHE_UNLIMITED : 0;
    flags |= m_config->use_transactions ? HAM_ENABLE_TRANSACTIONS : 0;
    flags |= m_config->use_fsync ? HAM_ENABLE_FSYNC : 0;
    flags |= m_config->flush_txn_immediately ? HAM_FLUSH_WHEN_COMMITTED : 0;
    flags |= m_config->disable_recovery ? HAM_DISABLE_RECOVERY : 0;
    flags |= m_config->enable_crc32 ? HAM_ENABLE_CRC32 : 0;

    boost::filesystem::remove("test-ham.db");

    st = ham_env_create(&ms_env, "test-ham.db", flags, 0664,
                  &params[0]);
    if (st) {
      LOG_ERROR(("ham_env_create failed with error %d (%s)\n",
                              st, ham_strerror(st)));
      return (st);
    }
  }

  // remote client/server? start the server, attach the environment and then
  // open the remote environment
#ifdef HAM_ENABLE_REMOTE
  if (m_config->use_remote) {
    ms_remote_env = ms_env;
    ms_env = 0;

    if (ms_srv == 0) {
      ham_srv_config_t cfg;
      memset(&cfg, 0, sizeof(cfg));
      cfg.port = 10123;
      ham_srv_init(&cfg, &ms_srv);
      ham_srv_add_env(ms_srv, ms_remote_env, "/env1.db");
    }

    uint32_t flags = 0;
    // flags |= m_config->duplicate ? HAM_ENABLE_DUPLICATES : 0;
    st = ham_env_open(&m_env, "ham://localhost:10123/env1.db", flags, 0);
    if (st)
      LOG_ERROR(("ham_env_open failed with error %d (%s)\n",
                              st, ham_strerror(st)));
  }
#endif

  return (st);
}

ham_status_t
HamsterDatabase::do_open_env()
{
  ham_status_t st = 0;
  uint32_t flags = 0;
  ham_parameter_t params[6] = {{0, 0}};

  ScopedLock lock(ms_mutex);

  ms_refcount++;

  hamsterdb::Globals::ms_extended_threshold = m_config->extkey_threshold;
  hamsterdb::Globals::ms_duplicate_threshold = m_config->duptable_threshold;

  // check if another thread was faster
  if (ms_env == 0) {
    int p = 0;
    params[p].name = HAM_PARAM_CACHE_SIZE;
    params[p].value = m_config->cachesize;
    p++;
    params[p].name = HAM_PARAM_POSIX_FADVISE;
    params[p].value = m_config->posix_fadvice;
    p++;
    if (m_config->use_encryption) {
      params[p].name = HAM_PARAM_ENCRYPTION_KEY;
      params[p].value = (uint64_t)"1234567890123456";
      p++;
    }

    flags |= m_config->no_mmap ? HAM_DISABLE_MMAP : 0; 
    flags |= m_config->cacheunlimited ? HAM_CACHE_UNLIMITED : 0;
    flags |= m_config->use_transactions ? HAM_ENABLE_TRANSACTIONS : 0;
    flags |= m_config->use_fsync ? HAM_ENABLE_FSYNC : 0;
    flags |= m_config->flush_txn_immediately ? HAM_FLUSH_WHEN_COMMITTED : 0;
    flags |= m_config->disable_recovery ? HAM_DISABLE_RECOVERY : 0;
    flags |= m_config->read_only ? HAM_READ_ONLY : 0;
    flags |= m_config->enable_crc32 ? HAM_ENABLE_CRC32 : 0;
    flags |= m_config->use_recovery ? HAM_AUTO_RECOVERY : 0;

    st = ham_env_open(&ms_env, "test-ham.db", flags, &params[0]);
    if (st) {
      LOG_ERROR(("ham_env_open failed with error %d (%s)\n",
                              st, ham_strerror(st)));
      return (st);
    }
  }

  // remote client/server? start the server, attach the environment and then
  // open the remote environment
#ifdef HAM_ENABLE_REMOTE
  if (m_config->use_remote) {
    ms_remote_env = ms_env;
    ms_env = 0;

    if (ms_srv == 0) {
      ham_srv_config_t cfg;
      memset(&cfg, 0, sizeof(cfg));
      cfg.port = 10123;
      ham_srv_init(&cfg, &ms_srv);
      ham_srv_add_env(ms_srv, ms_remote_env, "/env1.db");
    }

    uint32_t flags = 0;
    // flags |= m_config->duplicate ? HAM_ENABLE_DUPLICATES : 0;
    st = ham_env_open(&m_env, "ham://localhost:10123/env1.db", flags, 0);
    if (st)
      LOG_ERROR(("ham_env_open failed with error %d (%s)\n", st, ham_strerror(st)));
  }
#endif

  return (st);
}

ham_status_t
HamsterDatabase::do_close_env()
{
  ScopedLock lock(ms_mutex);

  if (m_env)
    ham_env_get_metrics(m_env, &m_hamster_metrics);

  if (ms_refcount == 0) {
    assert(m_env == 0);
    assert(ms_env == 0);
    return (0);
  }

  if (--ms_refcount > 0)
    return (0);

  if (m_env) {
    ham_env_close(m_env, 0);
    m_env = 0;
  }
  if (ms_env) {
    ham_env_get_metrics(ms_env, &m_hamster_metrics);
    ham_env_close(ms_env, 0);
    ms_env = 0;
  }
#ifdef HAM_ENABLE_REMOTE
  if (ms_remote_env) {
    ham_env_close(ms_remote_env, 0);
    ms_remote_env = 0;
  }
  if (ms_srv) {
    ham_srv_close(ms_srv);
    ms_srv = 0;
  }
#endif
  return (0);
}

ham_status_t
HamsterDatabase::do_create_db(int id)
{
  ham_status_t st;
  ham_parameter_t params[8] = {{0, 0}};

  int n = 0;
  params[n].name = HAM_PARAM_KEY_SIZE;
  params[n].value = 0;
  n++;
  switch (m_config->key_type) {
    case Configuration::kKeyCustom:
      params[0].value = m_config->key_is_fixed_size
                            ? m_config->key_size
                            : HAM_KEY_SIZE_UNLIMITED;
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_CUSTOM;
      n++;
      break;
    case Configuration::kKeyBinary:
    case Configuration::kKeyString:
      params[0].value = m_config->key_is_fixed_size
                            ? m_config->key_size
                            : HAM_KEY_SIZE_UNLIMITED;
      break;
    case Configuration::kKeyUint8:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_UINT8;
      n++;
      break;
    case Configuration::kKeyUint16:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_UINT16;
      n++;
      break;
    case Configuration::kKeyUint32:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_UINT32;
      n++;
      break;
    case Configuration::kKeyUint64:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_UINT64;
      n++;
      break;
    case Configuration::kKeyReal32:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_REAL32;
      n++;
      break;
    case Configuration::kKeyReal64:
      params[n].name = HAM_PARAM_KEY_TYPE;
      params[n].value = HAM_TYPE_REAL64;
      n++;
      break;
    default:
      assert(!"shouldn't be here");
  }
  params[n].name = HAM_PARAM_RECORD_SIZE;
  params[n].value = m_config->rec_size_fixed;
  n++;
  if (m_config->record_compression) {
    params[n].name = HAM_PARAM_RECORD_COMPRESSION;
    params[n].value = m_config->record_compression;
    n++;
  }
  if (m_config->key_compression) {
    params[n].name = HAM_PARAM_KEY_COMPRESSION;
    params[n].value = m_config->key_compression;
    n++;
  }

  uint32_t flags = 0;

  flags |= m_config->duplicate ? HAM_ENABLE_DUPLICATES : 0;
  flags |= m_config->record_number ? HAM_RECORD_NUMBER : 0;
  if (m_config->force_records_inline)
    flags |= HAM_FORCE_RECORDS_INLINE;

  st = ham_env_create_db(m_env ? m_env : ms_env, &m_db, 1 + id,
                  flags, &params[0]);
  if (st) {
    LOG_ERROR(("ham_env_create_db failed with error %d (%s)\n", st,
                            ham_strerror(st)));
    exit(-1);
  }

  if (m_config->key_type == Configuration::kKeyCustom) {
    st = ham_db_set_compare_func(m_db, compare_keys);
    if (st) {
      LOG_ERROR(("ham_db_set_compare_func failed with error %d (%s)\n", st,
                              ham_strerror(st)));
      exit(-1);
    }
  }

  return (0);
}

ham_status_t
HamsterDatabase::do_open_db(int id)
{
  ham_status_t st;

  ham_parameter_t params[6] = {{0, 0}};

  st = ham_env_open_db(m_env ? m_env : ms_env, &m_db, 1 + id, 0, &params[0]);
  if (st) {
    LOG_ERROR(("ham_env_open_db failed with error %d (%s)\n", st,
                            ham_strerror(st)));
    exit(-1);
  }

  if (m_config->key_type == Configuration::kKeyCustom) {
    st = ham_db_set_compare_func(m_db, compare_keys);
    if (st) {
      LOG_ERROR(("ham_db_set_compare_func failed with error %d (%s)\n", st,
                              ham_strerror(st)));
      exit(-1);
    }
  }
 
  return (st);
}

ham_status_t
HamsterDatabase::do_close_db()
{
  if (m_db)
    ham_db_close(m_db, HAM_AUTO_CLEANUP);
  m_db = 0;
  return (0);
}

ham_status_t
HamsterDatabase::do_flush()
{
  return (ham_env_flush(m_env ? m_env : ms_env, 0));
}

ham_status_t
HamsterDatabase::do_insert(Transaction *txn, ham_key_t *key,
                ham_record_t *record)
{
  uint32_t flags = m_config->hints;

  if (m_config->overwrite)
    flags |= HAM_OVERWRITE;
  else if (m_config->duplicate)
    flags |= HAM_DUPLICATE;

  ham_key_t recno_key = {0};
  if (m_config->record_number)
    key = &recno_key;

  ham_status_t st = ham_db_insert(m_db, (ham_txn_t *)txn, key, record, flags);
  if (st)
    LOG_VERBOSE(("insert: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_erase(Transaction *txn, ham_key_t *key)
{
  ham_status_t st = ham_db_erase(m_db, (ham_txn_t *)txn, key, 0);
  if (st)
    LOG_VERBOSE(("erase: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_find(Transaction *txn, ham_key_t *key, ham_record_t *record)
{
  uint32_t flags = 0;

  if (m_config->direct_access && m_config->inmemory)
    flags |= HAM_DIRECT_ACCESS;

#if 0
  if (!m_txn) {
    record->flags = HAM_RECORD_USER_ALLOC;
    record->data = m_useralloc;
  }
#endif

  ham_status_t st = ham_db_find(m_db, (ham_txn_t *)txn, key, record, flags);
  if (st)
     LOG_VERBOSE(("find: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_check_integrity()
{
  return (ham_db_check_integrity(m_db, 0));
}

Database::Transaction *
HamsterDatabase::do_txn_begin()
{
  ham_status_t st = ham_txn_begin(&m_txn, m_env ? m_env : ms_env, 0, 0, 0);
  if (st) {
    LOG_ERROR(("ham_txn_begin failed with error %d (%s)\n",
                st, ham_strerror(st)));
    return (0);
  }
  return ((Database::Transaction *)m_txn);
}

ham_status_t
HamsterDatabase::do_txn_commit(Transaction *txn)
{
  assert((ham_txn_t *)txn == m_txn);

  ham_status_t st = ham_txn_commit((ham_txn_t *)txn, 0);
  if (st)
    LOG_ERROR(("ham_txn_commit failed with error %d (%s)\n",
                st, ham_strerror(st)));
  m_txn = 0;
  return (st);
}

ham_status_t
HamsterDatabase::do_txn_abort(Transaction *txn)
{
  assert((ham_txn_t *)txn == m_txn);

  ham_status_t st = ham_txn_abort((ham_txn_t *)txn, 0);
  if (st)
    LOG_ERROR(("ham_txn_abort failed with error %d (%s)\n",
                st, ham_strerror(st)));
  m_txn = 0;
  return (st);
}

Database::Cursor *
HamsterDatabase::do_cursor_create()
{
  ham_cursor_t *cursor;

  ham_status_t st = ham_cursor_create(&cursor, m_db, m_txn, 0);
  if (st) {
    LOG_ERROR(("ham_cursor_create failed with error %d (%s)\n", st,
                            ham_strerror(st)));
    exit(-1);
  }

  return ((Database::Cursor *)cursor);
}

ham_status_t
HamsterDatabase::do_cursor_insert(Cursor *cursor, ham_key_t *key,
                ham_record_t *record)
{
  uint32_t flags = m_config->hints;

  if (m_config->overwrite)
    flags |= HAM_OVERWRITE;
  if (m_config->duplicate == Configuration::kDuplicateFirst)
    flags |= HAM_DUPLICATE | HAM_DUPLICATE_INSERT_FIRST;
  else if (m_config->duplicate == Configuration::kDuplicateLast)
    flags |= HAM_DUPLICATE | HAM_DUPLICATE_INSERT_LAST;

  ham_status_t st = ham_cursor_insert((ham_cursor_t *)cursor, key,
                  record, flags);
  if (st)
     LOG_VERBOSE(("cursor_insert: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_cursor_erase(Cursor *cursor, ham_key_t *key)
{
  ham_status_t st = ham_cursor_find((ham_cursor_t *)cursor, key, 0, 0);
  if (st) {
    LOG_VERBOSE(("cursor_find: failed w/ %d (%s)\n", st, ham_strerror(st)));
    return (st);
  }
  st = ham_cursor_erase((ham_cursor_t *)cursor, 0);
  if (st)
    LOG_VERBOSE(("cursor_erase: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record)
{
  ham_status_t st = ham_cursor_find((ham_cursor_t *)cursor, key, record, 0);
  if (st)
    LOG_VERBOSE(("cursor_erase: failed w/ %d (%s)\n", st, ham_strerror(st)));
  return (st);
}

ham_status_t
HamsterDatabase::do_cursor_get_previous(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
{
  uint32_t flags = 0;

  if (m_config->direct_access && m_config->inmemory)
    flags |= HAM_DIRECT_ACCESS;

  if (skip_duplicates)
    flags |= HAM_SKIP_DUPLICATES;

  return (ham_cursor_move((ham_cursor_t *)cursor, key, record,
                          HAM_CURSOR_PREVIOUS | flags));
}

ham_status_t
HamsterDatabase::do_cursor_get_next(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
{
  uint32_t flags = 0;

  if (m_config->direct_access && m_config->inmemory)
    flags |= HAM_DIRECT_ACCESS;
  if (skip_duplicates)
    flags |= HAM_SKIP_DUPLICATES;

  return (ham_cursor_move((ham_cursor_t *)cursor, key, record,
                          HAM_CURSOR_NEXT | flags));
}

ham_status_t
HamsterDatabase::do_cursor_close(Cursor *cursor)
{
  return (ham_cursor_close((ham_cursor_t *)cursor));
}

