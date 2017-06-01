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

#ifdef UPS_WITH_BERKELEYDB

#include <iostream>
#include <boost/filesystem.hpp>

#include "metrics.h"
#include "configuration.h"
#include "misc.h"
#include "berkeleydb.h"

#if DB_VERSION_MAJOR == 5
#  define COMP(name)  static int name(DB *db, const DBT *dbt1, const DBT *dbt2)
#else
#  define COMP(name)  static int name(DB *db, const DBT *dbt1, const DBT *dbt2, size_t *)
#endif


COMP(compare_uint8) {
  uint8_t l = *(uint8_t *)dbt1->data;
  uint8_t r = *(uint8_t *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

COMP(compare_uint16) {
  uint16_t l = *(uint16_t *)dbt1->data;
  uint16_t r = *(uint16_t *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

COMP(compare_uint32) {
  uint32_t l = *(uint32_t *)dbt1->data;
  uint32_t r = *(uint32_t *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

COMP(compare_uint64) {
  uint64_t l = *(uint64_t *)dbt1->data;
  uint64_t r = *(uint64_t *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

COMP(compare_real32) {
  float l = *(float *)dbt1->data;
  float r = *(float *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

COMP(compare_real64) {
  double l = *(double *)dbt1->data;
  double r = *(double *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

void
BerkeleyDatabase::get_metrics(Metrics *metrics, bool live)
{
}

ups_status_t
BerkeleyDatabase::do_create_env()
{
  boost::filesystem::remove("test-berk.db");

  return (do_open_env());
}

ups_status_t
BerkeleyDatabase::do_open_env()
{
  int ret = db_create(&m_db, 0, 0);
  if (ret) {
    LOG_ERROR(("db_create failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  // use same cachesize as upscaledb
  int cachesize = m_config->cachesize;
  if (cachesize == 0)
    cachesize = 1024 * 1024 * 2;

  ret = m_db->set_cachesize(m_db, 0, m_config->cachesize, 1);
  if (ret) {
    LOG_ERROR(("db->set_cachesize failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  if (m_config->pagesize) {
    // berkeleydb max. pagesize is 64k and must be a power of two
    int pagesize = m_config->pagesize;
    if (pagesize > 64 * 1024) {
      pagesize = 64 * 1024;
      printf("[info] berkeleydb pagesize reduced to 64kb\n");
    }
    if ((pagesize - 1) & pagesize) {
      pagesize = 0;
      printf("[info] berkeleydb pagesize ignored, must be pow(2)\n");
    }

    if (pagesize) {
      ret = m_db->set_pagesize(m_db, pagesize);
      if (ret) {
        LOG_ERROR(("db->set_pagesize failed w/ status %d\n", ret));
        return (db2ham(ret));
      }
    }
  }

  return (0);
}

ups_status_t
BerkeleyDatabase::do_close_env()
{
  int ret;

  if (m_db) {
    ret = m_db->close(m_db, 0);
    if (ret) {
      LOG_ERROR(("db->close() failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
    m_db = 0;
  }

  return (0);
}

ups_status_t
BerkeleyDatabase::do_create_db(int id)
{
  int ret = 0;

  switch (m_config->key_type) {
    case Configuration::kKeyUint8:
      ret = m_db->set_bt_compare(m_db, compare_uint8);
      break;
    case Configuration::kKeyUint16:
      ret = m_db->set_bt_compare(m_db, compare_uint16);
      break;
    case Configuration::kKeyUint32:
      ret = m_db->set_bt_compare(m_db, compare_uint32);
      break;
    case Configuration::kKeyUint64:
      ret = m_db->set_bt_compare(m_db, compare_uint64);
      break;
    case Configuration::kKeyReal32:
      ret = m_db->set_bt_compare(m_db, compare_real32);
      break;
    case Configuration::kKeyReal64:
      ret = m_db->set_bt_compare(m_db, compare_real64);
      break;
  }
  if (ret) {
    LOG_ERROR(("set_bt_compare failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  if (m_config->duplicate) {
    ret = m_db->set_flags(m_db, DB_DUP);
    if (ret) {
      LOG_ERROR(("db->set_flags(DB_DUP) failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
  }

  /* don't change dupe sorting - they're records and therefore never numeric!
   * if (m_config->sort_dupes && m_config->numeric) {
   *  ret = m_db->set_dup_compare(m_db, compare_db);
   *  if (ret)
   *    return (db2ham(ret));
   * }
   */

  ret = m_db->open(m_db, 0, m_config->inmemory ? 0 : "test-berk.db",
          0, DB_BTREE, DB_CREATE, 0644);
  if (ret) {
    LOG_ERROR(("db->open() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  ret = m_db->cursor(m_db, 0, &m_cursor, 0);
  if (ret) {
    LOG_ERROR(("db->cursor() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  return (0);
}

ups_status_t
BerkeleyDatabase::do_open_db(int id)
{
  int ret = 0;

  switch (m_config->key_type) {
    case Configuration::kKeyUint8:
      ret = m_db->set_bt_compare(m_db, compare_uint8);
      break;
    case Configuration::kKeyUint16:
      ret = m_db->set_bt_compare(m_db, compare_uint16);
      break;
    case Configuration::kKeyUint32:
      ret = m_db->set_bt_compare(m_db, compare_uint32);
      break;
    case Configuration::kKeyUint64:
      ret = m_db->set_bt_compare(m_db, compare_uint64);
      break;
    case Configuration::kKeyReal32:
      ret = m_db->set_bt_compare(m_db, compare_real32);
      break;
    case Configuration::kKeyReal64:
      ret = m_db->set_bt_compare(m_db, compare_real64);
      break;
  }

  if (ret) {
    LOG_ERROR(("set_bt_compare failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  ret = m_db->open(m_db, 0, "test-berk.db", 0, DB_BTREE, 0, 0);
  if (ret) {
    LOG_ERROR(("db->open() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  ret = m_db->cursor(m_db, 0, &m_cursor, 0);
  if (ret) {
    LOG_ERROR(("db->cursor() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  return (0);
}

ups_status_t
BerkeleyDatabase::do_close_db()
{
  int ret;

  if (m_cursor) {
    ret = m_cursor->c_close(m_cursor);
    if (ret) {
      LOG_ERROR(("cursor->c_close() failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
    m_cursor = 0;
  }

  return (0);
}

ups_status_t
BerkeleyDatabase::do_flush()
{
  int ret = m_db->sync(m_db, 0);
  if (ret) {
    LOG_ERROR(("db->sync() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }
  return (0);
}

ups_status_t
BerkeleyDatabase::do_insert(Txn *txn, ups_key_t *key,
                ups_record_t *record)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));

  k.data = key->data;
  k.size = key->size;
  r.data = record->data;
  r.size = record->size;

  int flags = 0;
  if (!m_config->overwrite && !m_config->duplicate)
    flags |= DB_NOOVERWRITE;

  int ret = m_db->put(m_db, 0, &k, &r, flags);
  return (db2ham(ret));
}

ups_status_t
BerkeleyDatabase::do_erase(Txn *txn, ups_key_t *key)
{
  int ret;
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));

  k.data = key->data;
  k.size = key->size;

  ret = m_db->del(m_db, 0, &k, 0);

  return (db2ham(ret));
}

ups_status_t
BerkeleyDatabase::do_find(Txn *txn, ups_key_t *key,
        ups_record_t *record)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(k));

  k.data = key->data;
  k.size = key->size;

  int ret = m_db->get(m_db, 0, &k, &r, 0);
  if (ret)
    return (db2ham(ret));

  record->data = r.data;
  record->size = r.size;
  return (0);
}

ups_status_t
BerkeleyDatabase::do_check_integrity()
{
  return (0);
}

Database::Txn *
BerkeleyDatabase::do_txn_begin()
{
  static Database::Txn t;
  return (&t);
}

ups_status_t
BerkeleyDatabase::do_txn_commit(Txn *txn)
{
  return (0);
}

ups_status_t
BerkeleyDatabase::do_txn_abort(Txn *txn)
{
  return (0);
}

Database::Cursor *
BerkeleyDatabase::do_cursor_create()
{
  DBC *cursor;

  if (!m_db)
    return (0);

  int ret = m_db->cursor(m_db, 0, &cursor, 0);
  if (ret) {
    LOG_ERROR(("db->cursor() failed w/ status %d\n", ret));
    exit(-1);
  }

  return ((Database::Cursor *)cursor);
}

ups_status_t
BerkeleyDatabase::do_cursor_insert(Cursor *cursor, ups_key_t *key,
                ups_record_t *record)
{
  int ret;
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));

  k.data = key->data;
  k.size = key->size;
  r.data = record->data;
  r.size = record->size;

  int flags = 0;
  if (!m_config->overwrite && !m_config->duplicate)
    flags |= DB_NOOVERWRITE;

  if (m_config->duplicate == Configuration::kDuplicateFirst) {
    ret = m_cursor->c_put(m_cursor, &k, &r, DB_KEYFIRST);
  }
  else {
    // do not use cursors - they fail mysteriously
    // ret = m_cursor->c_put(m_cursor, &k, &r, flags);
    ret = m_db->put(m_db, 0, &k, &r, flags);
  }

  return (db2ham(ret));
}

ups_status_t
BerkeleyDatabase::do_cursor_erase(Cursor *cursor, ups_key_t *key)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));

  k.data = key->data;
  k.size = key->size;

  int ret = m_cursor->c_get(m_cursor, &k, &r, DB_SET);
  if (ret)
    return (db2ham(ret));
  ret = m_cursor->c_del(m_cursor, 0);

  return (db2ham(ret));
}

ups_status_t
BerkeleyDatabase::do_cursor_find(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(k));

  k.data = key->data;
  k.size = key->size;

  int ret = m_cursor->c_get(m_cursor, &k, &r, DB_SET);
  if (ret)
    return (db2ham(ret));

  record->data = r.data;
  record->size = r.size;
  return (0);
}

ups_status_t
BerkeleyDatabase::do_cursor_get_previous(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));
  DBC *c = (DBC *)cursor;

  int flags = 0;
  if (skip_duplicates)
    flags = DB_PREV_NODUP;
  else
    flags = DB_PREV;

  int ret = c->c_get(c, &k, &r, flags);
  if (ret)
    return (db2ham(ret));
  key->data = k.data;
  key->size = k.size;
  record->data = r.data;
  record->size = r.size;
  return (0);
}

ups_status_t
BerkeleyDatabase::do_cursor_get_next(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  DBT k, r;
  memset(&k, 0, sizeof(k));
  memset(&r, 0, sizeof(r));
  DBC *c = (DBC *)cursor;

  int flags = 0;
  if (skip_duplicates)
    flags = DB_NEXT_NODUP;
  else
    flags = DB_NEXT;

  int ret = c->c_get(c, &k, &r, flags);
  if (ret)
    return (db2ham(ret));
  key->data = k.data;
  key->size = k.size;
  record->data = r.data;
  record->size = r.size;
  return (0);
}

ups_status_t
BerkeleyDatabase::do_cursor_close(Cursor *cursor)
{
  DBC *c = (DBC *)cursor;

  int ret = c->c_close(c);
  if (ret) {
    LOG_ERROR(("cursor->close() failed w/ status %d\n", ret));
    exit(-1);
  }

  return (0);
}

ups_status_t 
BerkeleyDatabase::db2ham(int ret)
{
  switch (ret) {
    case 0: return (UPS_SUCCESS);
    case DB_KEYEXIST: return (UPS_DUPLICATE_KEY);
    case DB_NOTFOUND: return (UPS_KEY_NOT_FOUND);
  }

  LOG_TRACE(("unknown berkeley return code %d\n", ret));
  return ((ups_status_t)ret);
}

#endif // UPS_WITH_BERKELEYDB
