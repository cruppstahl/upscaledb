/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifdef HAM_WITH_BERKELEYDB

#include <iostream>
#include <boost/filesystem.hpp>

#include "metrics.h"
#include "configuration.h"
#include "misc.h"
#include "berkeleydb.h"

static int
#if DB_VERSION_MAJOR == 5
compare_db8(DB *db, const DBT *dbt1, const DBT *dbt2)
#else
compare_db8(DB *db, const DBT *dbt1, const DBT *dbt2, size_t *)
#endif
{
  uint8_t l = *(int *)dbt1->data;
  uint8_t r = *(int *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

static int
#if DB_VERSION_MAJOR == 5
compare_db16(DB *db, const DBT *dbt1, const DBT *dbt2)
#else
compare_db16(DB *db, const DBT *dbt1, const DBT *dbt2, size_t *)
#endif
{
  uint16_t l = *(int *)dbt1->data;
  uint16_t r = *(int *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

static int
#if DB_VERSION_MAJOR == 5
compare_db32(DB *db, const DBT *dbt1, const DBT *dbt2)
#else
compare_db32(DB *db, const DBT *dbt1, const DBT *dbt2, size_t *)
#endif
{
  uint32_t l = *(int *)dbt1->data;
  uint32_t r = *(int *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

static int
#if DB_VERSION_MAJOR == 5
compare_db64(DB *db, const DBT *dbt1, const DBT *dbt2)
#else
compare_db64(DB *db, const DBT *dbt1, const DBT *dbt2, size_t *)
#endif
{
  uint64_t l = *(int *)dbt1->data;
  uint64_t r = *(int *)dbt2->data;
  if (l < r) return (-1);
  if (r < l) return (+1);
  return (0);
}

void
BerkeleyDatabase::get_metrics(Metrics *metrics, bool live)
{
}

ham_status_t
BerkeleyDatabase::do_create_env()
{
  boost::filesystem::remove("test-berk.db");

  return (do_open_env());
}

ham_status_t
BerkeleyDatabase::do_open_env()
{
  int ret = db_create(&m_db, 0, 0);
  if (ret) {
    ERROR(("db_create failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  // use same cachesize as hamsterdb
  int cachesize = m_config->cachesize;
  if (cachesize == 0)
    cachesize = 1024 * 1024 * 2;

  ret = m_db->set_cachesize(m_db, 0, m_config->cachesize, 1);
  if (ret) {
    ERROR(("db->set_cachesize failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  if (m_config->pagesize) {
    ret = m_db->set_pagesize(m_db, m_config->pagesize);
    if (ret) {
      ERROR(("db->set_pagesize failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
  }

  return (0);
}

ham_status_t
BerkeleyDatabase::do_close_env()
{
  int ret;

  if (m_db) {
    ret = m_db->close(m_db, 0);
    if (ret) {
      ERROR(("db->close() failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
    m_db = 0;
  }

  return (0);
}

ham_status_t
BerkeleyDatabase::do_create_db(int id)
{
  int ret = 0;

  switch (m_config->key_type) {
    case Configuration::kKeyUint8:
      ret = m_db->set_bt_compare(m_db, compare_db8);
      break;
    case Configuration::kKeyUint16:
      ret = m_db->set_bt_compare(m_db, compare_db16);
      break;
    case Configuration::kKeyUint32:
      ret = m_db->set_bt_compare(m_db, compare_db32);
      break;
    case Configuration::kKeyUint64:
      ret = m_db->set_bt_compare(m_db, compare_db64);
      break;
  }
  if (ret) {
    ERROR(("set_bt_compare failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  if (m_config->duplicate) {
    ret = m_db->set_flags(m_db, DB_DUP);
    if (ret) {
      ERROR(("db->set_flags(DB_DUP) failed w/ status %d\n", ret));
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
    ERROR(("db->open() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  ret = m_db->cursor(m_db, 0, &m_cursor, 0);
  if (ret) {
    ERROR(("db->cursor() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  return (0);
}

ham_status_t
BerkeleyDatabase::do_open_db(int id)
{
  int ret = m_db->open(m_db, 0, "test-berk.db", 0, DB_BTREE, 0, 0);
  if (ret) {
    ERROR(("db->open() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  ret = m_db->cursor(m_db, 0, &m_cursor, 0);
  if (ret) {
    ERROR(("db->cursor() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }

  return (0);
}

ham_status_t
BerkeleyDatabase::do_close_db()
{
  int ret;

  if (m_cursor) {
    ret = m_cursor->c_close(m_cursor);
    if (ret) {
      ERROR(("cursor->c_close() failed w/ status %d\n", ret));
      return (db2ham(ret));
    }
    m_cursor = 0;
  }

  return (0);
}

ham_status_t
BerkeleyDatabase::do_flush()
{
  int ret = m_db->sync(m_db, 0);
  if (ret) {
    ERROR(("db->sync() failed w/ status %d\n", ret));
    return (db2ham(ret));
  }
  return (0);
}

ham_status_t
BerkeleyDatabase::do_insert(Transaction *txn, ham_key_t *key,
                ham_record_t *record)
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

ham_status_t
BerkeleyDatabase::do_erase(Transaction *txn, ham_key_t *key)
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

ham_status_t
BerkeleyDatabase::do_find(Transaction *txn, ham_key_t *key,
        ham_record_t *record)
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

ham_status_t
BerkeleyDatabase::do_check_integrity(Transaction *txn)
{
  return (0);
}

Database::Transaction *
BerkeleyDatabase::do_txn_begin()
{
  static Database::Transaction t;
  return (&t);
}

ham_status_t
BerkeleyDatabase::do_txn_commit(Transaction *txn)
{
  return (0);
}

ham_status_t
BerkeleyDatabase::do_txn_abort(Transaction *txn)
{
  return (0);
}

Database::Cursor *
BerkeleyDatabase::do_cursor_create(Transaction *txn)
{
  DBC *cursor;

  int ret = m_db->cursor(m_db, 0, &cursor, 0);
  if (ret) {
    ERROR(("db->cursor() failed w/ status %d\n", ret));
    exit(-1);
  }

  return ((Database::Cursor *)cursor);
}

ham_status_t
BerkeleyDatabase::do_cursor_insert(Cursor *cursor, ham_key_t *key,
                ham_record_t *record)
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

ham_status_t
BerkeleyDatabase::do_cursor_erase(Cursor *cursor, ham_key_t *key)
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

ham_status_t
BerkeleyDatabase::do_cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record)
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

ham_status_t
BerkeleyDatabase::do_cursor_get_previous(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
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

ham_status_t
BerkeleyDatabase::do_cursor_get_next(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
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

ham_status_t
BerkeleyDatabase::do_cursor_close(Cursor *cursor)
{
  DBC *c = (DBC *)cursor;

  int ret = c->c_close(c);
  if (ret) {
    ERROR(("cursor->close() failed w/ status %d\n", ret));
    exit(-1);
  }

  return (0);
}

ham_status_t 
BerkeleyDatabase::db2ham(int ret)
{
  switch (ret) {
    case 0: return (HAM_SUCCESS);
    case DB_KEYEXIST: return (HAM_DUPLICATE_KEY);
    case DB_NOTFOUND: return (HAM_KEY_NOT_FOUND);
  }

  TRACE(("unknown berkeley return code %d\n", ret));
  return ((ham_status_t)ret);
}

#endif // HAM_WITH_BERKELEYDB
