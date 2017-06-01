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

#include "3rdparty/catch/catch.hpp"

#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "4env/env_local.h"
#include "4cursor/cursor_local.h"
#include "4context/context.h"

#include "fixture.hpp"

using namespace upscaledb;

struct DupeCursorFixture : BaseFixture {
  ups_cursor_t *cursor;
  ups_txn_t *m_txn;
  ScopedPtr<Context> context;

  DupeCursorFixture() {
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr,
            UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    REQUIRE(0 == ups_txn_begin(&m_txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, m_txn, 0));
    context.reset(new Context(lenv(), (LocalTxn *)m_txn, ldb()));
  }

  ~DupeCursorFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    if (cursor) {
      REQUIRE(0 == ups_cursor_close(cursor));
      cursor = 0;
    }
    if (m_txn) {
      REQUIRE(0 == ups_txn_commit(m_txn, 0));
      m_txn = 0;
    }
    close();
  }

  ups_status_t insertBtree(const char *key, const char *rec,
                  uint32_t flags = 0) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));
    ups_record_t r = ups_make_record((void *)rec,
                    rec ? (uint32_t)(::strlen(rec) + 1) : 0);

    ups_status_t st = btree_index()->insert(context.get(), 0, &k, &r, flags);
    context->changeset.clear(); // unlock pages
    return st;
  }

  ups_status_t eraseTxn(const char *key) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));

    return ups_db_erase(db, m_txn, &k, 0);
  }

  ups_status_t move(const char *key, const char *rec, uint32_t flags,
                  ups_cursor_t *c = 0) {
    ups_key_t k = {0};
    ups_record_t r = {0};

    if (!c)
      c = cursor;

    ups_status_t st = ups_cursor_move(c, &k, &r, flags);
    if (st)
      return st;
    if (::strcmp(key, (char *)k.data))
      return UPS_INTERNAL_ERROR;
    if (rec)
      if (::strcmp(rec, (char *)r.data))
        return UPS_INTERNAL_ERROR;

    // now verify again, but with flags=0
    if (flags == 0)
      return 0;
    st = ups_cursor_move(c, &k, &r, 0);
    if (st)
      return st;
    if (::strcmp(key, (char *)k.data))
      return UPS_INTERNAL_ERROR;
    if (rec)
      if (::strcmp(rec, (char *)r.data))
        return UPS_INTERNAL_ERROR;
    return 0;
  }

  ups_status_t find(const char *key, const char *rec) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));
    ups_record_t r = {0};
    ups_status_t st = ups_db_find(db, m_txn, &k, &r, 0);
    if (st)
      return st;
    if (rec && ::strcmp(rec, (char *)r.data))
      return UPS_INTERNAL_ERROR;
    return 0;
  }

  ups_status_t insertTxn(const char *key, const char *rec,
                  uint32_t flags = 0) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));
    ups_record_t r = ups_make_record((void *)rec,
                    rec ? (uint32_t)(::strlen(rec) + 1) : 0);

    return ups_cursor_insert(cursor, &k, &r, flags);
  }

  void simpleBtreeTest() {
    REQUIRE(0 == insertBtree("33333", "aaaaa"));
    REQUIRE(0 == insertBtree("33333", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaac", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaad", UPS_DUPLICATE));

    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_NEXT));
    REQUIRE(4u ==
          ((LocalCursor *)cursor)->duplicate_cache_count(context.get()));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void multipleBtreeTest() {
    REQUIRE(0 == insertBtree("33333", "aaaaa"));
    REQUIRE(0 == insertBtree("33333", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaac", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11111", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("11111", "aaaac", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("44444", "aaaaa"));
    REQUIRE(0 == insertBtree("44444", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("44444", "aaaac", UPS_DUPLICATE));

    REQUIRE(0 == move     ("11111", "aaaaa", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("11111", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("11111", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaac", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("44444", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("44444", "aaaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("44444", "aaaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void simpleTxnInsertLastTest() {
    REQUIRE(0 == insertTxn  ("33333", "aaaaa"));
    REQUIRE(0 == insertTxn  ("33333", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("33333", "aaaac", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_DUPLICATE));

    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void simpleTxnInsertFirstTest() {
    REQUIRE(0 == insertTxn  ("33333", "aaaaa"));
    REQUIRE(0 == insertTxn  ("33333", "aaaab",
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_FIRST));
    REQUIRE(0 == insertTxn  ("33333", "aaaac",
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_FIRST));
    REQUIRE(0 == insertTxn  ("33333", "aaaad",
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_FIRST));

    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void multipleTxnTest() {
    REQUIRE(0 == insertTxn("33333", "3aaaa"));
    REQUIRE(0 == insertTxn("33333", "3aaab", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn("33333", "3aaac", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn("11111", "1aaaa"));
    REQUIRE(0 == insertTxn("11111", "1aaab", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn("11111", "1aaac", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn("44444", "4aaaa"));
    REQUIRE(0 == insertTxn("44444", "4aaab", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn("44444", "4aaac", UPS_DUPLICATE));

    REQUIRE(0 == move   ("11111", "1aaaa", UPS_CURSOR_FIRST));
    REQUIRE(0 == move   ("11111", "1aaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("11111", "1aaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaac", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaac", UPS_CURSOR_LAST));
    REQUIRE(0 == move   ("44444", "4aaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("44444", "4aaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("44444", "4aaaa", UPS_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaac", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaab", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaaa", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void mixedTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k2", "r2.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k2", "r2.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.4", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.4", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.6", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.4", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k8", "r8.1", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k8", "r8.1", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k7", "r7.4", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.6", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.5", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.4", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.4", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k2", "r2.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k2", "r2.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void findInDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 3;

    key.data = (void *)"k1";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r1.1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2.1"));

    key.data = (void *)"k3";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r3.1"));

    key.data = (void *)"k4";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r4.1"));

    key.data = (void *)"k5";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r5.1"));

    key.data = (void *)"k6";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r6.1"));

    key.data = (void *)"k7";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r7.1"));

    key.data = (void *)"k8";
    REQUIRE(0 == ups_db_find(db, m_txn, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r8.1"));
  }

  void cursorFindInDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 3;

    key.data = (void *)"k1";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r1.1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2.1"));

    key.data = (void *)"k3";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r3.1"));

    key.data = (void *)"k4";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r4.1"));

    key.data = (void *)"k5";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r5.1"));

    key.data = (void *)"k6";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r6.1"));

    key.data = (void *)"k7";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r7.1"));

    key.data = (void *)"k8";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r8.1"));
  }

  void skipDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == move     ("k1", "r1.1",
          UPS_CURSOR_FIRST|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k2", "r2.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k3", "r3.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k4", "r4.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k5", "r5.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k6", "r6.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k7", "r7.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k8", "r8.1",
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0,
          UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k8", "r8.1",
          UPS_CURSOR_LAST|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k7", "r7.4",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k6", "r6.6",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k5", "r5.4",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k4", "r4.3",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k3", "r3.3",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k2", "r2.2",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k1", "r1.1",
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0,
          UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES));
  }

  void txnInsertConflictTest() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = {};
    key.data = (void *)"hello";
    key.size = 5;
    ups_record_t rec = {};

    ups_cursor_t *c;

    /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn2, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(UPS_TXN_CONFLICT == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnEraseConflictTest() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = {};
    key.data = (void *)"hello";
    key.size = 5;
    ups_record_t rec = {};

    ups_cursor_t *c;

    /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn2, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, UPS_DUPLICATE));
    REQUIRE(UPS_TXN_CONFLICT == ups_db_erase(db, 0, &key, 0));
    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void eraseDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == eraseTxn   ("k1"));
    REQUIRE(0 == eraseTxn   ("k2"));
    REQUIRE(0 == eraseTxn   ("k3"));
    REQUIRE(0 == eraseTxn   ("k4"));
    REQUIRE(0 == eraseTxn   ("k5"));
    REQUIRE(0 == eraseTxn   ("k6"));
    REQUIRE(0 == eraseTxn   ("k7"));
    REQUIRE(0 == eraseTxn   ("k8"));

    REQUIRE(UPS_KEY_NOT_FOUND == find("k1", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k2", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k3", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k4", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k5", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k6", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k7", 0));
    REQUIRE(UPS_KEY_NOT_FOUND == find("k8", 0));
  }

  void cloneDuplicateCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.3", UPS_DUPLICATE));

    REQUIRE(0 == move("k1", "r2.2", UPS_CURSOR_FIRST));

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_clone(cursor, &c));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    REQUIRE(0 == ups_cursor_move(c, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2.2"));
    REQUIRE(0 == ::strcmp((char *)key.data, "k1"));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertCursorCouplesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r2.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.3", UPS_DUPLICATE));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r3.3"));
    REQUIRE(0 == ::strcmp((char *)key.data, "k1"));
  }

  void insertFirstTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));

    /* now insert a key at the beginning */
    ups_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.2";
    REQUIRE(0 == ups_cursor_insert(c[0], &key, &rec,
          UPS_DUPLICATE | UPS_DUPLICATE_INSERT_FIRST));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void insertLastTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));

    /* now insert a key at the beginning */
    ups_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.2";
    REQUIRE(0 == ups_cursor_insert(c[0], &key, &rec,
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_LAST));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_LAST));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void insertAfterTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = ups_make_key((void *)"k1", 3);

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 == ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));

    REQUIRE(0 == ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_move(c[1], &key, 0, UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_cursor_move(c[2], &key, 0, UPS_CURSOR_LAST));
    REQUIRE(0 == ups_cursor_move(c[2], &key, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 == ups_cursor_move(c[3], &key, 0, UPS_CURSOR_LAST));

    /* now insert keys in-between */
    ups_record_t rec = ups_make_record((void *)"r1.2", 5);

    ups_cursor_t *clone;
    REQUIRE(0 == ups_cursor_clone(c[0], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE | UPS_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.4";
    REQUIRE(0 == ups_cursor_clone(c[1], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE | UPS_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.6";
    REQUIRE(0 == ups_cursor_clone(c[2], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE | UPS_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.8";
    REQUIRE(0 == ups_cursor_clone(c[3], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE | UPS_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ups_cursor_close(clone));

    /* now verify that the original 4 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.6", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.8", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void insertBeforeTest() {
    const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));

    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_NEXT));

    REQUIRE(0 ==
          ups_cursor_move(c[2], &key, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_move(c[2], &key, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ups_cursor_move(c[3], &key, 0, UPS_CURSOR_LAST));

    /* now insert keys in-between */
    ups_record_t rec = {0};
    rec.size = 5;
    ups_cursor_t *clone;
    rec.data = (void *)"r1.0";
    REQUIRE(0 == ups_cursor_clone(c[0], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.2";
    REQUIRE(0 == ups_cursor_clone(c[1], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.4";
    REQUIRE(0 == ups_cursor_clone(c[2], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ups_cursor_close(clone));

    rec.data = (void *)"r1.6";
    REQUIRE(0 == ups_cursor_clone(c[3], &clone));
    REQUIRE(0 == ups_cursor_insert(clone, &key, &rec,
          UPS_DUPLICATE|UPS_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ups_cursor_close(clone));

    /* now verify that the original 4 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.0", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.6", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void extendDupeCacheTest() {
    const int MAX = 512;
    int i = 0;

    for (; i < MAX / 2; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == insertBtree("k1", buf, UPS_DUPLICATE));
    }

    for (; i < MAX; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == insertTxn  ("k1", buf, UPS_DUPLICATE));
    }

    for (i = 0; i < MAX; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == move("k1", buf,
          i == 0 ? UPS_CURSOR_FIRST : UPS_CURSOR_NEXT));
    }
  }

  void overwriteTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    ups_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    rec.data = (void *)"r2.2";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    rec.data = (void *)"r2.3";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(0 == move     ("k1", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r2.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r2.3", UPS_CURSOR_NEXT));
  }

  void overwriteBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    ups_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));

    rec.data = (void *)"r2.2";
    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));

    rec.data = (void *)"r2.3";
    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(0 == move     ("k1", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r2.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r2.3", UPS_CURSOR_NEXT));
  }

  void eraseFirstTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseSecondTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseThirdTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 == ups_cursor_erase(cursor, 0));
    }

    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousTxnTest() {
    REQUIRE(0 == insertTxn  ("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 == ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastTxnTest() {
    REQUIRE(0 == insertTxn  ("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseFirstBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseSecondBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseThirdBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousBtreeTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastBtreeTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseFirstMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseSecondMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseSecondMixedDupeTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseThirdMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseThirdMixedDupeTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_LAST));
  }

  void eraseAllDuplicatesMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMoveNextMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMoveNextMixedTest3() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest2() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest3() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest3() {
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest2() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest3() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));
  }

  void eraseFirstTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));

    /* now erase the first key */
    REQUIRE(0 == ups_cursor_erase(c[0], 0));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void eraseLastTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_LAST));

    /* now erase the key */
    REQUIRE(0 == ups_cursor_erase(c[0], 0));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void eraseAfterTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));

    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_move(c[1], &key, 0, UPS_CURSOR_NEXT));

    REQUIRE(0 ==
          ups_cursor_move(c[2], &key, 0, UPS_CURSOR_LAST));
    REQUIRE(0 ==
          ups_cursor_move(c[2], &key, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ups_cursor_move(c[3], &key, 0, UPS_CURSOR_LAST));

    /* now erase the second key */
    REQUIRE(0 == ups_cursor_erase(c[1], 0));

    /* now verify that the other 3 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(UPS_CURSOR_IS_NIL == move("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.5", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void eraseBeforeTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ups_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", UPS_DUPLICATE));

    ups_key_t key = ups_make_key((void *)"k1", 3);

    // each cursor is positioned on a different duplicate
    REQUIRE(0 == ups_cursor_move(c[0], &key, 0, UPS_CURSOR_FIRST));

    REQUIRE(0 == ups_cursor_move(c[1], &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_move(c[1], &key, 0, UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_cursor_move(c[2], &key, 0, UPS_CURSOR_LAST));
    REQUIRE(0 == ups_cursor_move(c[2], &key, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 == ups_cursor_move(c[3], &key, 0, UPS_CURSOR_LAST));

    // erase the 3rd key
    REQUIRE(0 == ups_cursor_erase(c[2], 0));

    // now verify that the other 3 cursors are still coupled to the
    // same duplicate
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(UPS_CURSOR_IS_NIL == move("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    // now verify that the keys were inserted in the correct order
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", UPS_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_PREVIOUS));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
  }

  void eraseWithCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_erase(cursor, 0));

    /* now verify that the last duplicate was erased */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void overwriteWithCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));

    ups_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.4";
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));

    /* now verify that the last duplicate was overwritten */
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  uint32_t count(const char *key, ups_status_t st = 0) {
    ups_key_t k = ups_make_key((void *)key, (uint16_t)(::strlen(key) + 1));
    REQUIRE(st == ups_cursor_find(cursor, &k, 0, 0));
    if (st)
      return 0;

    uint32_t c = 0;
    REQUIRE(0 == ups_cursor_get_duplicate_count(cursor, &c, 0));
    return c;
  }

  void negativeCountTest() {
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
  }

  void countTxnTest() {
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(3u == count("k1"));
  }

  void countBtreeTest() {
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(3u == count("k1"));
  }

  void countMixedTest() {
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(3u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.4", UPS_DUPLICATE));
    REQUIRE(4u == count("k1"));
  }

  void countMixedOverwriteTest() {
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(3u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.4", UPS_DUPLICATE));
    REQUIRE(4u == count("k1"));

    ups_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(4u == count("k1"));

    rec.data = (void *)"r2.2";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(4u == count("k1"));

    rec.data = (void *)"r2.3";
    REQUIRE(0 ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 ==
          ups_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(4u == count("k1"));
  }

  void countMixedErasedTest() {
    REQUIRE(0u == count("k0", UPS_KEY_NOT_FOUND));
    REQUIRE(0u == count("k1", UPS_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(3u == count("k1"));

    for (int i = 0; i < 3; i++) {
      ups_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 ==
          ups_cursor_erase(cursor, 0));
      REQUIRE((unsigned)(2 - i) == count("k1", i == 2 ? UPS_KEY_NOT_FOUND : 0));
    }
  }

  void negativeWithoutDupesTest() {
    teardown();

    REQUIRE(0 == ups_env_create(&env, "test.db",
                            UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 13, 0, 0));
    REQUIRE(0 == ups_txn_begin(&m_txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.1"));
    REQUIRE(1u == count("k1"));

    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    uint32_t c;
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_get_duplicate_count(cursor, &c, 0));
  }

  void nullDupesTest() {
    REQUIRE(0 == insertBtree("k0", 0, UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", 0, UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", 0, UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", 0, UPS_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", 0, UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void tinyDupesTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", UPS_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "r0.1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void smallDupesTest() {
    REQUIRE(0 == insertBtree("k0", "0000000", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "1111111", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "2222222", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3333333", UPS_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "0000000", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "1111111", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "2222222", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "3333333", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void bigDupesTest() {
    REQUIRE(0 == insertBtree("k0", "0000000000", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "1111111111", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "2222222222", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3333333333", UPS_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "0000000000", UPS_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "1111111111", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "2222222222", UPS_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "3333333333", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void conflictFirstTest() {
    REQUIRE(0 == insertTxn  ("k1", "1"));
    REQUIRE(0 == insertTxn  ("k2", "2"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(UPS_TXN_CONFLICT ==
          move("k1", "1", UPS_CURSOR_FIRST, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void conflictFirstTest2() {
    REQUIRE(0 == insertTxn  ("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(UPS_TXN_CONFLICT == move(0, 0, UPS_CURSOR_FIRST, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void conflictLastTest() {
    REQUIRE(0 == insertTxn  ("k0", "0"));
    REQUIRE(0 == insertTxn  ("k1", "1"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(UPS_TXN_CONFLICT ==
          move("k1", "1", UPS_CURSOR_LAST, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void conflictLastTest2() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertTxn  ("k1", "1"));
    REQUIRE(0 == insertBtree("k2", "0"));
    REQUIRE(0 == insertTxn  ("k3", "1"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(UPS_TXN_CONFLICT ==
          move("k3", "1", UPS_CURSOR_LAST, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void conflictNextTest() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(0 == move("k0", "0", UPS_CURSOR_FIRST, c));
    REQUIRE(0 == move("k3", "3", UPS_CURSOR_NEXT, c));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        move(0, 0, UPS_CURSOR_NEXT, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void conflictPreviousTest() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));
    REQUIRE(0 == move("k3", "3", UPS_CURSOR_LAST, c));
    REQUIRE(0 == move("k0", "0", UPS_CURSOR_PREVIOUS, c));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        move(0, 0, UPS_CURSOR_PREVIOUS, c));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ups_txn_t *txn2;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(UPS_TXN_CONFLICT ==
          ups_db_insert(db, txn2, &key, &rec, 0));

    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void eraseDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ups_txn_t *txn2;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(UPS_TXN_CONFLICT ==
          ups_db_erase(db, txn2, &key, 0));

    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void findDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ups_txn_t *txn2;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(UPS_TXN_CONFLICT ==
          ups_db_find(db, txn2, &key, &rec, 0));

    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void cursorInsertDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ups_txn_t *txn2;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn2, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(UPS_TXN_CONFLICT ==
          ups_cursor_insert(c, &key, &rec, 0));

    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void cursorFindDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ups_txn_t *txn2;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn2, 0));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(UPS_TXN_CONFLICT ==
          ups_cursor_find(c, &key, 0, 0));

    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void flushErasedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "3", UPS_DUPLICATE));

    /* erase k1/2 */
    REQUIRE(0 == move("k1", "1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move("k1", "2", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));

    /* flush the transaction to disk */
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(m_txn, 0));

    REQUIRE(0 == ups_txn_begin(&m_txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, m_txn, 0));

    /* verify that the duplicate was erased */
    REQUIRE(0 == move("k1", "1", UPS_CURSOR_FIRST));
    REQUIRE(0 == move("k1", "3", UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND == move(0, 0, UPS_CURSOR_NEXT));
  }

  void duplicatePositionBtreeTest() {
    teardown();

    REQUIRE(0 == ups_env_create(&env, "test.db", 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 13, UPS_ENABLE_DUPLICATES, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    uint32_t position = 0;
    REQUIRE(0 == insertBtree("33333", "aaaaa"));
    REQUIRE(0 == insertBtree("33333", "aaaab", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaac", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaad", UPS_DUPLICATE));

    REQUIRE(0 == move     ("33333", "aaaaa", UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(0 == position);
    REQUIRE(0 == move     ("33333", "aaaab", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(1 == position);
    REQUIRE(0 == move     ("33333", "aaaac", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(2 == position);
    REQUIRE(0 == move     ("33333", "aaaad", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(3 == position);
  }

  void duplicatePositionTxnTest() {
    uint32_t position = 0;
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertTxn  ("k1", "2", UPS_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "3", UPS_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "4", UPS_DUPLICATE));

    REQUIRE(0 == move("k1", "1", UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(0 == position);

    REQUIRE(0 == move("k1", "2", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(1 == position);

    REQUIRE(0 == move("k1", "3", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(2 == position);

    REQUIRE(0 == move("k1", "4", UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_get_duplicate_position(cursor, &position));
    REQUIRE(3 == position);

    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_get_duplicate_position(cursor,
                            &position));
  }
};

TEST_CASE("Cursor/dupes/simpleBtreeTest", "")
{
  DupeCursorFixture f;
  f.simpleBtreeTest();
}

TEST_CASE("Cursor/dupes/multipleBtreeTest", "")
{
  DupeCursorFixture f;
  f.multipleBtreeTest();
}

TEST_CASE("Cursor/dupes/simpleTxnInsertLastTest", "")
{
  DupeCursorFixture f;
  f.simpleTxnInsertLastTest();
}

TEST_CASE("Cursor/dupes/simpleTxnInsertFirstTest", "")
{
  DupeCursorFixture f;
  f.simpleTxnInsertFirstTest();
}

TEST_CASE("Cursor/dupes/multipleTxnTest", "")
{
  DupeCursorFixture f;
  f.multipleTxnTest();
}

TEST_CASE("Cursor/dupes/mixedTest", "")
{
  DupeCursorFixture f;
  f.mixedTest();
}

TEST_CASE("Cursor/dupes/findInDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.findInDuplicatesTest();
}

TEST_CASE("Cursor/dupes/cursorFindInDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.cursorFindInDuplicatesTest();
}

TEST_CASE("Cursor/dupes/skipDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.skipDuplicatesTest();
}

TEST_CASE("Cursor/dupes/txnInsertConflictTest", "")
{
  DupeCursorFixture f;
  f.txnInsertConflictTest();
}

TEST_CASE("Cursor/dupes/txnEraseConflictTest", "")
{
  DupeCursorFixture f;
  f.txnEraseConflictTest();
}

TEST_CASE("Cursor/dupes/eraseDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.eraseDuplicatesTest();
}

TEST_CASE("Cursor/dupes/cloneDuplicateCursorTest", "")
{
  DupeCursorFixture f;
  f.cloneDuplicateCursorTest();
}

TEST_CASE("Cursor/dupes/insertCursorCouplesTest", "")
{
  DupeCursorFixture f;
  f.insertCursorCouplesTest();
}

TEST_CASE("Cursor/dupes/insertFirstTest", "")
{
  DupeCursorFixture f;
  f.insertFirstTest();
}

TEST_CASE("Cursor/dupes/insertLastTest", "")
{
  DupeCursorFixture f;
  f.insertLastTest();
}

TEST_CASE("Cursor/dupes/insertAfterTest", "")
{
  DupeCursorFixture f;
  f.insertAfterTest();
}

TEST_CASE("Cursor/dupes/insertBeforeTest", "")
{
  DupeCursorFixture f;
  f.insertBeforeTest();
}

TEST_CASE("Cursor/dupes/extendDupeCacheTest", "")
{
  DupeCursorFixture f;
  f.extendDupeCacheTest();
}

TEST_CASE("Cursor/dupes/overwriteTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.overwriteTxnDupeTest();
}

TEST_CASE("Cursor/dupes/overwriteBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.overwriteBtreeDupeTest();
}

TEST_CASE("Cursor/dupes/eraseFirstTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstTxnDupeTest();
}

TEST_CASE("Cursor/dupes/eraseSecondTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondTxnDupeTest();
}

TEST_CASE("Cursor/dupes/eraseThirdTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdTxnDupeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesTxnTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMoveNextTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextTxnTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMovePreviousTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousTxnTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindFirstTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstTxnTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindLastTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastTxnTest();
}

TEST_CASE("Cursor/dupes/eraseFirstBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstBtreeDupeTest();
}

TEST_CASE("Cursor/dupes/eraseSecondBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondBtreeDupeTest();
}

TEST_CASE("Cursor/dupes/eraseThirdBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdBtreeDupeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesBtreeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMoveNextBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextBtreeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMovePreviousBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousBtreeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindFirstBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstBtreeTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindLastBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastBtreeTest();
}

TEST_CASE("Cursor/dupes/eraseFirstMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstMixedDupeTest();
}

TEST_CASE("Cursor/dupes/eraseSecondMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondMixedDupeTest();
}

TEST_CASE("Cursor/dupes/eraseSecondMixedDupeTest2", "")
{
  DupeCursorFixture f;
  f.eraseSecondMixedDupeTest2();
}

TEST_CASE("Cursor/dupes/eraseThirdMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdMixedDupeTest();
}

TEST_CASE("Cursor/dupes/eraseThirdMixedDupeTest2", "")
{
  DupeCursorFixture f;
  f.eraseThirdMixedDupeTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMixedTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMixedTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMoveNextMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMoveNextMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMoveNextMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest3();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMovePreviousMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMovePreviousMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesMovePreviousMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest3();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindFirstMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindFirstMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindFirstMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest3();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindLastMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindLastMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest2();
}

TEST_CASE("Cursor/dupes/eraseAllDuplicatesFindLastMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest3();
}

TEST_CASE("Cursor/dupes/eraseFirstTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstTest();
}

TEST_CASE("Cursor/dupes/eraseLastTest", "")
{
  DupeCursorFixture f;
  f.eraseLastTest();
}

TEST_CASE("Cursor/dupes/eraseAfterTest", "")
{
  DupeCursorFixture f;
  f.eraseAfterTest();
}

TEST_CASE("Cursor/dupes/eraseBeforeTest", "")
{
  DupeCursorFixture f;
  f.eraseBeforeTest();
}

TEST_CASE("Cursor/dupes/eraseWithCursorTest", "")
{
  DupeCursorFixture f;
  f.eraseWithCursorTest();
}

TEST_CASE("Cursor/dupes/overwriteWithCursorTest", "")
{
  DupeCursorFixture f;
  f.overwriteWithCursorTest();
}

TEST_CASE("Cursor/dupes/negativeCountTest", "")
{
  DupeCursorFixture f;
  f.negativeCountTest();
}

TEST_CASE("Cursor/dupes/countTxnTest", "")
{
  DupeCursorFixture f;
  f.countTxnTest();
}

TEST_CASE("Cursor/dupes/countBtreeTest", "")
{
  DupeCursorFixture f;
  f.countBtreeTest();
}

TEST_CASE("Cursor/dupes/countMixedTest", "")
{
  DupeCursorFixture f;
  f.countMixedTest();
}

TEST_CASE("Cursor/dupes/countMixedOverwriteTest", "")
{
  DupeCursorFixture f;
  f.countMixedOverwriteTest();
}

TEST_CASE("Cursor/dupes/countMixedErasedTest", "")
{
  DupeCursorFixture f;
  f.countMixedErasedTest();
}

TEST_CASE("Cursor/dupes/negativeWithoutDupesTest", "")
{
  DupeCursorFixture f;
  f.negativeWithoutDupesTest();
}

TEST_CASE("Cursor/dupes/nullDupesTest", "")
{
  DupeCursorFixture f;
  f.nullDupesTest();
}

TEST_CASE("Cursor/dupes/tinyDupesTest", "")
{
  DupeCursorFixture f;
  f.tinyDupesTest();
}

TEST_CASE("Cursor/dupes/smallDupesTest", "")
{
  DupeCursorFixture f;
  f.smallDupesTest();
}

TEST_CASE("Cursor/dupes/bigDupesTest", "")
{
  DupeCursorFixture f;
  f.bigDupesTest();
}

TEST_CASE("Cursor/dupes/conflictFirstTest", "")
{
  DupeCursorFixture f;
  f.conflictFirstTest();
}

TEST_CASE("Cursor/dupes/conflictFirstTest2", "")
{
  DupeCursorFixture f;
  f.conflictFirstTest2();
}

TEST_CASE("Cursor/dupes/conflictLastTest", "")
{
  DupeCursorFixture f;
  f.conflictLastTest();
}

TEST_CASE("Cursor/dupes/conflictLastTest2", "")
{
  DupeCursorFixture f;
  f.conflictLastTest2();
}

TEST_CASE("Cursor/dupes/conflictNextTest", "")
{
  DupeCursorFixture f;
  f.conflictNextTest();
}

TEST_CASE("Cursor/dupes/conflictPreviousTest", "")
{
  DupeCursorFixture f;
  f.conflictPreviousTest();
}

TEST_CASE("Cursor/dupes/insertDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.insertDupeConflictsTest();
}

TEST_CASE("Cursor/dupes/eraseDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.eraseDupeConflictsTest();
}

TEST_CASE("Cursor/dupes/findDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.findDupeConflictsTest();
}

TEST_CASE("Cursor/dupes/cursorInsertDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.cursorInsertDupeConflictsTest();
}

TEST_CASE("Cursor/dupes/cursorFindDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.cursorFindDupeConflictsTest();
}

TEST_CASE("Cursor/dupes/flushErasedDupeTest", "")
{
  DupeCursorFixture f;
  f.flushErasedDupeTest();
}

TEST_CASE("Cursor/dupes/duplicatePositionBtreeTest", "")
{
  DupeCursorFixture f;
  f.duplicatePositionBtreeTest();
}

TEST_CASE("Cursor/dupes/duplicatePositionTxnTest", "")
{
  DupeCursorFixture f;
  f.duplicatePositionBtreeTest();
}

TEST_CASE("Cursor/issue41", "")
{
  ups_env_t *env;
  ups_db_t *db;
  ups_txn_t *txn;
  ups_cursor_t *cw; // writing cursor
  ups_cursor_t *cr; // reading cursor

  REQUIRE(0 == ups_env_create(&env, "test.db",
                UPS_ENABLE_TRANSACTIONS, 0664, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 13, 0, 0));

  for (uint64_t i = 1; i <= 6; i++) {
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cw, db, txn, 0));
    if (i > 1) {
      ups_key_t k = {0};
      ups_record_t r = {0};
      REQUIRE(0 == ups_cursor_create(&cr, db, 0, 0));
      REQUIRE(0 == ups_cursor_move(cr, &k, &r, UPS_CURSOR_LAST));
      REQUIRE(*(uint64_t *)k.data == i - 1);
      REQUIRE(*(uint64_t *)r.data == i - 1);
      REQUIRE(0 == ups_cursor_close(cr));
    }
    ups_key_t key = {0};
    key.data = &i;
    key.size = sizeof(i);
    ups_record_t record = {0};
    record.data = &i;
    record.size = sizeof(i);
    REQUIRE(0 == ups_cursor_insert(cw, &key, &record, 0));
    REQUIRE(0 == ups_cursor_close(cw));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  REQUIRE(0 == ups_cursor_create(&cr, db, 0, 0));

  // 6,6
  ups_key_t k = {0};
  ups_record_t r = {0};
  REQUIRE(0 == ups_cursor_move(cr, &k, &r, UPS_CURSOR_LAST));
  REQUIRE(*(uint64_t *)k.data == 6);
  REQUIRE(*(uint64_t *)r.data == 6);

  // Now the read cursor is asked to find(key,record,UPS_FIND_LT_MATCH)
  // with key = 6. The result is key=5 and record=5 (ok)
  REQUIRE(0 == ups_cursor_find(cr, &k, &r, UPS_FIND_LT_MATCH));
  REQUIRE(*(uint64_t *)k.data == 5);
  REQUIRE(*(uint64_t *)r.data == 5);

  // Now repeat the step backward in time: find(key,record,UPS_FIND_LT_MATCH)
  // with key=5. The result is key=4 and record = 4 (ok)
  REQUIRE(0 == ups_cursor_find(cr, &k, &r, UPS_FIND_LT_MATCH));
  REQUIRE(*(uint64_t *)k.data == 4);
  REQUIRE(*(uint64_t *)r.data == 4);

  // Now ask for the step forward in time: find(key,record,UPS_FIND_GT_MATCH)
  // with key=4. The result is key=4 and record=6 (?????)
  REQUIRE(0 == ups_cursor_find(cr, &k, &r, UPS_FIND_GT_MATCH));
  REQUIRE(*(uint64_t *)k.data == 5);
  REQUIRE(*(uint64_t *)r.data == 5);

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

// this was a failing test from the erlang package
TEST_CASE("Cursor/erlangTest", "")
{
  ups_env_t *env;
  ups_db_t *db;
  ups_cursor_t *cursor;

  REQUIRE(0 == ups_env_create(&env, "test.db",
                UPS_ENABLE_TRANSACTIONS, 0664, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 13, 0, 0));
  REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

  ups_key_t key = ups_make_key((void *)"foo1", 4);
  ups_record_t record = ups_make_record((void *)"value1", 6);
  REQUIRE(0 == ups_cursor_insert(cursor, &key, &record, 0));
  key.data = (void *)"foo2";
  record.data = (void *)"value2";
  REQUIRE(0 == ups_cursor_insert(cursor, &key, &record, 0));
  key.data = (void *)"foo3";
  record.data = (void *)"value3";
  REQUIRE(0 == ups_cursor_insert(cursor, &key, &record, 0));
  key.data = (void *)"foo4";
  record.data = (void *)"value4";
  REQUIRE(0 == ups_cursor_insert(cursor, &key, &record, 0));
  key.data = (void *)"foo5";
  record.data = (void *)"value5";
  REQUIRE(0 == ups_cursor_insert(cursor, &key, &record, 0));

  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
  REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));

  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));
  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));
  REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));

  REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
  uint32_t size = 0;
  REQUIRE(0 == ups_cursor_get_record_size(cursor, &size));
  REQUIRE(size == 6ull);
  uint32_t count = 0;
  REQUIRE(0 == ups_cursor_get_duplicate_count(cursor, &count, 0));
  REQUIRE(count == 1u);

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}
