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

#include "ups/upscaledb.hpp"

static int
my_compare_func(ups_db_t *db,
                const uint8_t *lhs, uint32_t lhs_length,
                const uint8_t *rhs, uint32_t rhs_length) {
  (void)db;
  (void)lhs;
  (void)rhs;
  (void)lhs_length;
  (void)rhs_length;
  return 0;
}

TEST_CASE("CppApi/keyTest", "")
{
  void *p = (void *)"123";
  void *q = (void *)"234";
  upscaledb::key k1, k2(p, 4, UPS_KEY_USER_ALLOC);

  REQUIRE((void *)0 == k1.get_data());
  REQUIRE((uint16_t)0 == k1.get_size());
  REQUIRE((uint32_t)0 == k1.get_flags());

  REQUIRE(p == k2.get_data());
  REQUIRE((uint16_t)4 == k2.get_size());
  REQUIRE((uint32_t)UPS_KEY_USER_ALLOC == k2.get_flags());

  k1 = k2;
  REQUIRE(p == k1.get_data());
  REQUIRE((uint16_t)4 == k1.get_size());
  REQUIRE((uint32_t)UPS_KEY_USER_ALLOC == k1.get_flags());

  upscaledb::key k3(k1);
  REQUIRE(p == k3.get_data());
  REQUIRE((uint16_t)4 == k3.get_size());
  REQUIRE((uint32_t)UPS_KEY_USER_ALLOC == k3.get_flags());

  int i = 3;
  upscaledb::key k4;
  k4.set<int>(i);
  REQUIRE((void *)&i == k4.get_data());
  REQUIRE(sizeof(int) == (size_t)k4.get_size());

  k1.set_data(q);
  k1.set_size(2);
  k1.set_flags(0);
  REQUIRE(q == k1.get_data());
  REQUIRE((uint16_t)2 == k1.get_size());
  REQUIRE((uint32_t)0 == k1.get_flags());
}

TEST_CASE("CppApi/recordTest", "")
{
  void *p = (void *)"123";
  void *q = (void *)"234";
  upscaledb::record r1, r2(p, 4, UPS_RECORD_USER_ALLOC);

  REQUIRE((void *)0 == r1.get_data());
  REQUIRE((uint32_t)0 == r1.get_size());
  REQUIRE((uint32_t)0 == r1.get_flags());

  REQUIRE(p == r2.get_data());
  REQUIRE((uint32_t)4 == r2.get_size());
  REQUIRE((uint32_t)UPS_RECORD_USER_ALLOC == r2.get_flags());

  r1=r2;
  REQUIRE(p == r1.get_data());
  REQUIRE((uint32_t)4 == r1.get_size());
  REQUIRE((uint32_t)UPS_RECORD_USER_ALLOC == r1.get_flags());

  upscaledb::record r3(r1);
  REQUIRE(p == r3.get_data());
  REQUIRE((uint32_t)4 == r3.get_size());
  REQUIRE((uint32_t)UPS_RECORD_USER_ALLOC == r3.get_flags());

  r1.set_data(q);
  r1.set_size(2);
  r1.set_flags(0);
  REQUIRE(q == r1.get_data());
  REQUIRE((uint32_t)2 == r1.get_size());
  REQUIRE((uint32_t)0 == r1.get_flags());
}

TEST_CASE("CppApi/staticFunctionsTest", "")
{
  upscaledb::db db;
  // check for obvious errors

  db.get_version(0, 0, 0);
  REQUIRE(".get_version() did not throw while receiving NULL arguments");
}

TEST_CASE("CppApi/compareTest", "")
{
  ups_parameter_t p[] = {
      { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
      { 0, 0 }
  };

  upscaledb::env env;
  env.create("test.db");
  upscaledb::db db = env.create_db(1, 0, &p[0]);
  db.set_compare_func(my_compare_func);
  env.close(UPS_AUTO_CLEANUP);
}

TEST_CASE("CppApi/createOpenCloseDbTest", "")
{
  upscaledb::env env;

  try {
    env.create("data/");
  }
  catch (upscaledb::error &) {
  }

  env.create("test.db");
  env.close();

  try {
    env.open("xxxxxx");
  }
  catch (upscaledb::error &) {
  }

  env.open("test.db");
  env = env;
  env.close();
}

TEST_CASE("CppApi/insertFindEraseTest", "")
{
  upscaledb::env env;
  upscaledb::db db;
  upscaledb::key k;
  upscaledb::record r, out;

  k.set_data((void *)"12345");
  k.set_size(6);
  r.set_data((void *)"12345");
  r.set_size(6);

  env.create("test.db");
  db = env.create_db(1);

  try {
    db.insert(0, &r);
  }
  catch (upscaledb::error &) {
  }

  try {
    db.insert(&k, 0);
  }
  catch (upscaledb::error &) {
  }

  db.insert(&k, &r);
  try {
    db.insert(&k, &r);  // already exists
  }
  catch (upscaledb::error &) {
  }

  out = db.find(&k);
  REQUIRE(r.get_size() == out.get_size());
  REQUIRE(0 == ::memcmp(r.get_data(), out.get_data(), out.get_size()));
  db.erase(&k);

  try {
    db.erase(0);
  }
  catch(upscaledb::error &) {
  }

  try {
    db.erase(&k);
  }
  catch (upscaledb::error &) {
  }

  try {
    out = db.find(&k);
  }
  catch (upscaledb::error &e) {
    REQUIRE(UPS_KEY_NOT_FOUND == e.get_errno());
    REQUIRE(0 == strcmp("Key not found", e.get_string()));
  }

  try {
    out = db.find(0);
  }
  catch (upscaledb::error &) {
  }

  db.close();
  env.close();
  db.close();
  env.close();
  env.close();
  env.open("test.db");
}

TEST_CASE("CppApi/cursorTest", "")
{
  upscaledb::env env;
  upscaledb::db db;

  try {
    upscaledb::cursor cerr(&db);
  }
  catch (upscaledb::error &) {
  }

  upscaledb::key k((void *)"12345", 5), k2;
  upscaledb::record r((void *)"12345", 5), r2;

  env.create("test.db");
  db = env.create_db(1);
  upscaledb::cursor c(&db);
  c.create(&db); // overwrite

  c.insert(&k, &r);
  try {
    c.insert(&k, 0);
  }
  catch (upscaledb::error &) {
  }
  try {
    c.insert(0, &r);
  }
  catch (upscaledb::error &) {
  }
  try {
    c.insert(&k, &r);  // already exists
  }
  catch (upscaledb::error &) {
  }
  try {
    c.overwrite(0);
  }
  catch (upscaledb::error &) {
  }
  c.overwrite(&r);
  upscaledb::cursor clone = c.clone();

  c.move_first(&k2, &r2);
  REQUIRE(k.get_size() == k2.get_size());
  REQUIRE(r.get_size() == r2.get_size());

  c.move_last(&k2, &r2);
  REQUIRE(k.get_size() == k2.get_size());
  REQUIRE(r.get_size() == r2.get_size());

  try {
    c.move_next();
  }
  catch (upscaledb::error &e) {
    REQUIRE(e.get_errno() == UPS_KEY_NOT_FOUND);
  }

  try {
    c.move_previous();
  }
  catch (upscaledb::error &e) {
    REQUIRE(e.get_errno() == UPS_KEY_NOT_FOUND);
  }

  c.find(&k);
  REQUIRE((uint32_t)1 == c.get_duplicate_count());

  c.erase();
  try {
    c.erase();
  }
  catch (upscaledb::error &) {
  }

  try {
    c.find(&k);
  }
  catch (upscaledb::error &) {
  }

  upscaledb::cursor temp;
  temp.close();
}

TEST_CASE("CppApi/envTest", "")
{
  upscaledb::env env;

  env.create("test.db");
  env.flush();
  env.close();
  env.close();
  env.close();
  env.open("test.db");

  upscaledb::db db1 = env.create_db(1);
  db1.close();
  db1 = env.open_db(1);
  env.rename_db(1, 2);

  try {
    env.erase_db(2);
  }
  catch (upscaledb::error &e) {
    REQUIRE(UPS_DATABASE_ALREADY_OPEN == e.get_errno());
  }
  db1.close();
  env.erase_db(2);
}

TEST_CASE("CppApi/envDestructorTest", "")
{
  upscaledb::env env;
  upscaledb::db db1;

  env.create("test.db");
  db1 = env.create_db(1);

  // let the objects go out of scope
}

TEST_CASE("CppApi/envGetDatabaseNamesTest", "")
{
  upscaledb::env env;
  std::vector<uint16_t> v;

  env.create("test.db");

  v = env.get_database_names();
  REQUIRE((uint32_t)0 == (uint32_t)v.size());

  upscaledb::db db1 = env.create_db(1);
  v = env.get_database_names();
  REQUIRE((uint32_t)1 == (uint32_t)v.size());
  REQUIRE((uint16_t)1 == v[0]);
  env.close();
}

TEST_CASE("CppApi/beginAbortTest", "")
{
  upscaledb::env env;
  upscaledb::db db;
  upscaledb::key k;
  upscaledb::record r, out;
  upscaledb::txn txn;

  k.set_data((void *)"12345");
  k.set_size(6);
  r.set_data((void *)"12345");
  r.set_size(6);

  env.create("test.db", UPS_ENABLE_TRANSACTIONS);
  db = env.create_db(1);
  txn = env.begin();
  db.insert(&txn, &k, &r);
  txn.abort();
  try {
    out = db.find(&k);
  }
  catch (upscaledb::error &e) {
    REQUIRE(UPS_KEY_NOT_FOUND == e.get_errno());
  }
}

TEST_CASE("CppApi/beginCommitTest", "")
{
  upscaledb::env env;
  upscaledb::db db;
  upscaledb::key k;
  upscaledb::record r, out;
  upscaledb::txn txn;

  k.set_data((void *)"12345");
  k.set_size(6);
  r.set_data((void *)"12345");
  r.set_size(6);

  env.create("test.db", UPS_ENABLE_TRANSACTIONS);
  db = env.create_db(1);
  txn = env.begin("name");
  db.insert(&txn, &k, &r);
  std::string n = txn.get_name();
  REQUIRE(0 == strcmp("name", n.c_str()));
  txn.commit();
  out = db.find(&k);
}

TEST_CASE("CppApi/beginCursorAbortTest", "")
{
  upscaledb::env env;
  upscaledb::db db;
  upscaledb::key k;
  upscaledb::record r, out;
  upscaledb::txn txn;

  k.set_data((void *)"12345");
  k.set_size(6);
  r.set_data((void *)"12345");
  r.set_size(6);

  env.create("test.db", UPS_ENABLE_TRANSACTIONS);
  db = env.create_db(1);
  txn = env.begin();
  upscaledb::cursor c(&db, &txn);
  c.insert(&k, &r);
  REQUIRE(r.get_size() == c.get_record_size());
  c.close();
  txn.abort();
  try {
    out = db.find(&k);
  }
  catch (upscaledb::error &e) {
    REQUIRE(UPS_KEY_NOT_FOUND == e.get_errno());
  }
}

TEST_CASE("CppApi/beginCursorCommitTest", "")
{
  upscaledb::env env;
  upscaledb::db db;
  upscaledb::key k;
  upscaledb::record r, out;
  upscaledb::txn txn;

  k.set_data((void *)"12345");
  k.set_size(6);
  r.set_data((void *)"12345");
  r.set_size(6);

  env.create("test.db", UPS_ENABLE_TRANSACTIONS);
  db = env.create_db(1);
  txn = env.begin();
  upscaledb::cursor c(&db, &txn);
  c.insert(&k, &r);
  c.close();
  txn.commit();
  out = db.find(&k);
}

