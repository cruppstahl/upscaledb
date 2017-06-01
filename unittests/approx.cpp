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

#include <vector>
#include <string>
#include <algorithm>

#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

static int
slot_key_cmp(ups_db_t *db, const uint8_t *lhs, uint32_t lsz,
                const uint8_t *rhs, uint32_t rsz) {
  uint32_t i;

  for (i = 0; i < lsz; ++i) {
    if (lhs[i] != rhs[i]) {
      return lhs[i] > rhs[i] ? 1 : -1;
    }
  }
  return 0;
}

struct ApproxFixture : BaseFixture {
  ups_txn_t *txn;

  ApproxFixture() {
    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
  }

  ~ApproxFixture() {
    close();
  }

  ups_status_t insertBtree(const char *s) {
    ups_key_t k = ups_make_key((void *)s, (uint16_t)(::strlen(s) + 1));
    ups_record_t r = ups_make_record(k.data, k.size);

    Context context(lenv(), 0, 0);
    return btree_index()->insert(&context, 0, &k, &r, 0);
  }

  ups_status_t insertTxn(const char *s, uint32_t flags = 0) {
    ups_key_t k = ups_make_key((void *)s, (uint16_t)(::strlen(s) + 1));
    ups_record_t r = ups_make_record(k.data, k.size);

    return ups_db_insert(db, txn, &k, &r, flags);
  }

  ups_status_t eraseTxn(const char *s) {
    ups_key_t k = ups_make_key((void *)s, (uint16_t)(::strlen(s) + 1));

    return ups_db_erase(db, txn, &k, 0);
  }

  ups_status_t find(uint32_t flags, const char *search, const char *expected) {
    ups_key_t k = ups_make_key((void *)search,
                    (uint16_t)(::strlen(search) + 1));
    ups_record_t r = {0};

    ups_status_t st = ups_db_find(db, txn, &k, &r, flags);
    if (st)
      return st;
    if (::strcmp(expected, (const char *)k.data))
      REQUIRE((ups_key_get_intflags(&k) & BtreeKey::kApproximate));
    return ::strcmp(expected, (const char *)r.data);
  }

  void lessThanTest1() {
    // btree < nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "2", "1"));
  }

  void lessThanTest2() {
    // txn < nil
    REQUIRE(0 == insertTxn("2"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "3", "2"));
  }

  void lessThanTest3() {
    // btree < txn
    REQUIRE(0 == insertBtree("10"));
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "11", "10"));
  }

  void lessThanTest4() {
    // txn < btree
    REQUIRE(0 == insertTxn("20"));
    REQUIRE(0 == insertBtree("21"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "21", "20"));
  }

  void lessThanTest5() {
    // btree < btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "31", "30"));
  }

  void lessThanTest6() {
    // txn < txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "41", "40"));
  }

  void lessThanTest7() {
    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == insertTxn("52"));
    REQUIRE(0 == eraseTxn("51"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "52", "50"));
  }

  void lessThanTest8() {
    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertTxn("61"));
    REQUIRE(0 == insertTxn("62"));
    REQUIRE(0 == eraseTxn("61"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "62", "60"));
  }

  void lessThanTest9() {
    // skip erased btree
    REQUIRE(0 == insertBtree("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == insertTxn("70"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "71", "70"));
  }

  void lessThanTest10() {
    // skip 2 erased btree keys
    REQUIRE(0 == insertBtree("80"));
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "83", "80"));
  }

  void lessThanTest11() {
    // overwrite btree
    REQUIRE(0 == insertBtree("92"));
    REQUIRE(0 == insertTxn("92", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("93"));
    REQUIRE(0 == insertTxn("93", UPS_OVERWRITE));
    REQUIRE(0 == find(UPS_FIND_LT_MATCH, "93", "92"));
  }

  void lessOrEqualTest1() {
    // btree < nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "2", "1"));
  }

  void lessOrEqualTest2() {
    // btree = nil
    REQUIRE(0 == insertBtree("2"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "2", "2"));
  }

  void lessOrEqualTest3() {
    // txn < nil
    REQUIRE(0 == insertTxn("3"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "4", "3"));
  }

  void lessOrEqualTest4() {
    // txn = nil
    REQUIRE(0 == insertTxn("4"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "5", "4"));
  }

  void lessOrEqualTest5() {
    // btree < txn
    REQUIRE(0 == insertBtree("10"));
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "11", "11"));
  }

  void lessOrEqualTest6() {
    // txn < btree
    REQUIRE(0 == insertTxn("20"));
    REQUIRE(0 == insertBtree("21"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "21", "21"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "22", "21"));
  }

  void lessOrEqualTest7() {
    // btree < btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "31", "31"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "32", "31"));
  }

  void lessOrEqualTest8() {
    // txn < txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "41", "41"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "42", "41"));
  }

  void lessOrEqualTest9() {
    // txn =
    REQUIRE(0 == insertBtree("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "51", "51"));
  }

  void lessOrEqualTest10() {
    // btree =
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertBtree("61"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "61", "61"));
  }

  void lessOrEqualTest11() {
    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("70"));
    REQUIRE(0 == insertTxn("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "71", "70"));
  }

  void lessOrEqualTest12() {
    // skip 3 erased btree keys
    REQUIRE(0 == insertBtree("80"));
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == eraseTxn("83"));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "83", "80"));
  }

  void lessOrEqualTest13() {
    // overwrite btree
    REQUIRE(0 == insertBtree("92"));
    REQUIRE(0 == insertTxn("92", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("93"));
    REQUIRE(0 == insertTxn("93", UPS_OVERWRITE));
    REQUIRE(0 == find(UPS_FIND_LEQ_MATCH, "93", "93"));
  }

  void greaterThanTest1() {
    // btree > nil
    REQUIRE(0 == insertBtree("2"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "1", "2"));
  }

  void greaterThanTest2() {
    // txn > nil
    REQUIRE(0 == insertTxn("4"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "3", "4"));
  }

  void greaterThanTest3() {
    // btree > txn
    REQUIRE(0 == insertTxn("10"));
    REQUIRE(0 == insertBtree("11"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "10", "11"));
  }

  void greaterThanTest4() {
    // txn > btree
    REQUIRE(0 == insertBtree("20"));
    REQUIRE(0 == insertTxn("21"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "20", "21"));
  }

  void greaterThanTest5() {
    // btree > btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "30", "31"));
  }

  void greaterThanTest6() {
    // txn > txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "40", "41"));
  }

  void greaterThanTest7() {
    // txn > txn w/ empty node
    REQUIRE(0 == insertTxn("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == eraseTxn("51"));
    REQUIRE(0 == insertTxn("52"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "50", "52"));
  }

  void greaterThanTest8() {
    // skip 2 erased btree keys
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(UPS_FIND_GT_MATCH, "80", "83"));
  }

  void greaterThanTest9() {
    close();

    ups_parameter_t param[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_BINARY},
        {UPS_PARAM_KEY_SIZE, 32},
        {0, 0}
    };

    require_create(0, 0, 0, param);

    std::vector<uint8_t> data(32);
    std::vector<uint8_t> expected_key(32);
    std::vector<uint8_t> rec(32);

    DbProxy dbp(db);
    dbp.require_insert(data, rec);
    data[31] = 1;
    dbp.require_find_approx(data, expected_key, rec, UPS_FIND_LT_MATCH);
  }

  void greaterOrEqualTest1() {
    // btree > nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "0", "1"));
  }

  void greaterOrEqualTest2() {
    // btree = nil
    REQUIRE(0 == insertBtree("3"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "3", "3"));
  }

  void greaterOrEqualTest3() {
    // txn > nil
    REQUIRE(0 == insertTxn("5"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "4", "5"));
  }

  void greaterOrEqualTest4() {
    // txn = nil
    REQUIRE(0 == insertTxn("7"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "7", "7"));
  }

  void greaterOrEqualTest5() {
    // btree > txn
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == insertBtree("12"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "11", "11"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "10", "11"));
  }

  void greaterOrEqualTest6() {
    // txn > btree
    REQUIRE(0 == insertBtree("20"));
    REQUIRE(0 == insertTxn("21"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "19", "20"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "20", "20"));
  }

  void greaterOrEqualTest7() {
    // btree > btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "31", "31"));
  }

  void greaterOrEqualTest8() {
    // txn > txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "41", "41"));
  }

  void greaterOrEqualTest9() {
    // txn =
    REQUIRE(0 == insertBtree("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "51", "51"));
  }

  void greaterOrEqualTest10() {
    // btree =
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertBtree("61"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "61", "61"));
  }

  void greaterOrEqualTest11() {
    // txn > txn w/ empty node
    REQUIRE(0 == insertTxn("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == insertTxn("72"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "71", "72"));
  }

  void greaterOrEqualTest12() {
    // skip erased btree keys
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(UPS_FIND_GEQ_MATCH, "81", "83"));
  }

  void issue44Test() {
    ups_parameter_t param[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM},
        {UPS_PARAM_KEY_SIZE, 41},
        {0, 0}
    };

    close();
    require_create(0, 0, 0, param);
    REQUIRE(0 == ups_db_set_compare_func(db, slot_key_cmp));

    const char *values[] = { "11", "22", "33", "44", "55" };
    for (int i = 0; i < 5; i++) {
      char keydata[41];
      ::memcpy(&keydata[0], values[i], 3);
      ups_key_t key = ups_make_key(&keydata[0], sizeof(keydata));
      ups_record_t rec = ups_make_record((void *)values[i], 3);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    char keydata[41];
    ::memcpy(&keydata[0], "10", 3);
    ups_key_t key = ups_make_key((void *)keydata, sizeof(keydata));
    ups_record_t rec = {0};
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_GEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "11"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "11"));
  }

  void issue46Test() {
    REQUIRE(0 == insertBtree("aa"));
    REQUIRE(0 == eraseTxn("aa"));

    ups_key_t key = ups_make_key((void *)"aa", 3);
    ups_record_t rec = {0};

    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, UPS_FIND_GEQ_MATCH));
  }

  void issue52Test() {
    uint8_t buffer[525933] = {0};

    ups_parameter_t param[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT64},
        {0, 0}
    };

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, 0,
                    UPS_ENABLE_DUPLICATE_KEYS, &param[0]);

    uint64_t k1 = 1;
    uint64_t k2 = 2;
    uint64_t k3 = 3;
    ups_key_t key1 = ups_make_key(&k1, sizeof(k1));
    ups_key_t key2 = ups_make_key(&k2, sizeof(k2));
    ups_key_t key3 = ups_make_key(&k3, sizeof(k3));

    ups_record_t rec1 = ups_make_record(&buffer[0], 46228);
    ups_record_t rec11 = ups_make_record(&buffer[0], 446380);
    ups_record_t rec12 = ups_make_record(&buffer[0], 525933);
    ups_record_t rec21 = ups_make_record(&buffer[0], 334157);
    ups_record_t rec22 = ups_make_record(&buffer[0], 120392);

    REQUIRE(0 == ups_db_insert(db, 0, &key1, &rec1, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_insert(db, 0, &key2, &rec11, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_insert(db, 0, &key2, &rec12, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_insert(db, 0, &key3, &rec21, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_insert(db, 0, &key3, &rec22, UPS_DUPLICATE));

    ups_txn_t *txn;
    ups_cursor_t *c;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};

    REQUIRE(0 == ups_cursor_find(c, &key1, &rec, UPS_FIND_GEQ_MATCH));
    REQUIRE(1u == *(unsigned long long *)key1.data);
    REQUIRE(rec1.size == rec.size);

    REQUIRE(0 == ups_cursor_move(c, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(2u == *(unsigned long long *)key.data);
    REQUIRE(rec11.size == rec.size);

    REQUIRE(0 == ups_cursor_move(c, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(2u == *(unsigned long long *)key.data);
    REQUIRE(rec12.size == rec.size);

    REQUIRE(0 == ups_cursor_move(c, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(3u == *(unsigned long long *)key.data);
    REQUIRE(rec21.size == rec.size);

    REQUIRE(0 == ups_cursor_move(c, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(3u == *(unsigned long long *)key.data);
    REQUIRE(rec22.size == rec.size);

    REQUIRE(0 == ups_cursor_close(c));
    // cleanup is in teardown()
  }

  void greaterThanTest() {
    ups_parameter_t param[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_BINARY},
        {UPS_PARAM_KEY_SIZE, 32},
        {0, 0}
    };

    close();
    require_create(0, 0, 0, param);

    std::vector<uint8_t> data(32);
    std::vector<uint8_t> expected_key(32);
    std::vector<uint8_t> rec;

    DbProxy dbp(db);
    dbp.require_insert(data, rec);

    data[31] = 1;
    dbp.require_find_approx(data, expected_key, rec, UPS_FIND_LT_MATCH);
  }

  template<typename Generator>
  void btreeLessThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(0, envparam, UPS_FORCE_RECORDS_INLINE, &dbparam[0]);

    ups_key_t key = {0};
    ups_record_t record = {0};
    std::vector<uint8_t> rec(32);
    DbProxy dbp(db);

    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    gen.generate(0, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));

    for (int i = 1; i < 5000; i++) {
      ups_key_t key = {0};
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void btreeLessEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(0, envparam, UPS_FORCE_RECORDS_INLINE, &dbparam[0]);

    ups_key_t key = {0};
    ups_record_t record = {0};
    std::vector<uint8_t> rec(32);
    DbProxy dbp(db);

    for (int i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void btreeGreaterThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(0, envparam, UPS_FORCE_RECORDS_INLINE, &dbparam[0]);

    ups_key_t key = {0};
    ups_record_t record = {0};
    std::vector<uint8_t> rec(32);
    DbProxy dbp(db);

    for (int i = 0; i <= 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
  }

  template<typename Generator>
  void btreeGreaterEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(0, envparam, UPS_FORCE_RECORDS_INLINE, &dbparam[0]);

    ups_key_t key = {0};
    ups_record_t record = {0};
    std::vector<uint8_t> rec(32);
    DbProxy dbp(db);

    for (int i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void txnLessThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    ups_key_t key = {0};
    std::vector<uint8_t> rec(32);
    DbProxy dbp(db);

    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(txn, &key, rec);
    }

    gen.generate(0, &key);
    ups_record_t record = {0};
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, txn, &key, &record, UPS_FIND_LT_MATCH));

    for (int i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ups_db_find(db, txn, &key, &record, UPS_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void txnLessEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(txn, &key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, txn, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void txnGreaterThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 1; i <= 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(txn, &key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, txn, &key, &record, UPS_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, txn, &key, &record, UPS_FIND_GT_MATCH));
  }

  template<typename Generator>
  void txnGreaterEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(txn, &key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, txn, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ups_db_find(db, txn, &key, &record, UPS_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void mixedLessThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    ups_record_t record = {0};
    gen.generate(0, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));

    for (int i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixedLessEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixedGreaterThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 1; i <= 5000; i++) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
  }

  template<typename Generator>
  void mixedGreaterEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);
    }

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void mixed2LessThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 5000; i += 4) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);

      gen.generate(i + 1, &key);
      dbp.require_insert(&key, rec);

      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
      gen.generate(i + 2, &key);
      dbp.require_insert(txn, &key, rec);

      gen.generate(i + 3, &key);
      dbp.require_insert(txn, &key, rec);
      REQUIRE(0 == ups_txn_commit(txn, 0));
    }

    txn = 0;

    ups_record_t record = {0};
    gen.generate(0, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));

    for (int i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixed2GreaterThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 1; i <= 5000; i += 4) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);

      gen.generate(i + 1, &key);
      dbp.require_insert(&key, rec);

      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
      gen.generate(i + 2, &key);
      dbp.require_insert(txn, &key, rec);

      gen.generate(i + 3, &key);
      dbp.require_insert(txn, &key, rec);
      REQUIRE(0 == ups_txn_commit(txn, 0));
    }
    txn = 0;

    ups_record_t record = {0};
    for (int i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ups_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(UPS_KEY_NOT_FOUND
                    == ups_db_find(db, 0, &key, &record, UPS_FIND_GT_MATCH));
  }

  template<typename Generator>
  void mixed2LessEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 10000; i += 5) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);

      gen.generate(i + 1, &key);
      dbp.require_insert(&key, rec);

      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
      gen.generate(i + 2, &key);
      dbp.require_insert(txn, &key, rec);

      gen.generate(i + 3, &key);
      dbp.require_insert(txn, &key, rec);
      REQUIRE(0 == ups_txn_commit(txn, 0));

      // skip i + 4
    }
    txn = 0;

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i += 5) {
      ups_key_t key2 = {0};

      gen.generate(i, &key);
      gen2.generate(i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 1, &key);
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 2, &key);
      gen2.generate(i + 2, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 3, &key);
      gen2.generate(i + 3, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 4, &key);
      gen2.generate(i + 3, &key2); // !!
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixed2GreaterEqualThanTest() {
    Generator gen, gen2;

    ups_parameter_t envparam[] = {
        {UPS_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ups_parameter_t dbparam[] = {
        {UPS_PARAM_KEY_TYPE, gen.get_key_type()},
        {UPS_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = UPS_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, envparam,
                    UPS_ENABLE_DUPLICATE_KEYS, dbparam);
    std::vector<uint8_t> rec(32);
    ups_key_t key = {0};
    DbProxy dbp(db);

    for (int i = 0; i < 10000; i += 5) {
      gen.generate(i, &key);
      dbp.require_insert(&key, rec);

      gen.generate(i + 1, &key);
      dbp.require_insert(&key, rec);

      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
      gen.generate(i + 2, &key);
      dbp.require_insert(txn, &key, rec);

      gen.generate(i + 3, &key);
      dbp.require_insert(txn, &key, rec);
      REQUIRE(0 == ups_txn_commit(txn, 0));

      // skip i + 4
    }
    txn = 0;

    ups_record_t record = {0};
    for (int i = 0; i < 10000; i += 5) {
      ups_key_t key2 = {0};

      gen.generate(i, &key);
      gen2.generate(i, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 1, &key);
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 2, &key);
      gen2.generate(i + 2, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 3, &key);
      gen2.generate(i + 3, &key2);
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      if (i + 5 < 10000) {
        gen.generate(i + 4, &key);
        gen2.generate(i + 5, &key2); // !!
        REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
        REQUIRE(key2.size == key.size);
        REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
      }
    }
  }
};

TEST_CASE("Approx/lessThanTest1", "") {
  ApproxFixture f;
  f.lessThanTest1();
}

TEST_CASE("Approx/lessThanTest2", "") {
  ApproxFixture f;
  f.lessThanTest2();
}

TEST_CASE("Approx/lessThanTest3", "") {
  ApproxFixture f;
  f.lessThanTest3();
}

TEST_CASE("Approx/lessThanTest4", "") {
  ApproxFixture f;
  f.lessThanTest4();
}

TEST_CASE("Approx/lessThanTest5", "") {
  ApproxFixture f;
  f.lessThanTest5();
}

TEST_CASE("Approx/lessThanTest6", "") {
  ApproxFixture f;
  f.lessThanTest6();
}

TEST_CASE("Approx/lessThanTest7", "") {
  ApproxFixture f;
  f.lessThanTest7();
}

TEST_CASE("Approx/lessThanTest8", "") {
  ApproxFixture f;
  f.lessThanTest8();
}

TEST_CASE("Approx/lessThanTest9", "") {
  ApproxFixture f;
  f.lessThanTest9();
}

TEST_CASE("Approx/lessThanTest10", "") {
  ApproxFixture f;
  f.lessThanTest10();
}

TEST_CASE("Approx/lessThanTest11", "") {
  ApproxFixture f;
  f.lessThanTest11();
}

TEST_CASE("Approx/lessOrEqualTest1", "") {
  ApproxFixture f;
  f.lessOrEqualTest1();
}

TEST_CASE("Approx/lessOrEqualTest2", "") {
  ApproxFixture f;
  f.lessOrEqualTest2();
}

TEST_CASE("Approx/lessOrEqualTest3", "") {
  ApproxFixture f;
  f.lessOrEqualTest3();
}

TEST_CASE("Approx/lessOrEqualTest4", "") {
  ApproxFixture f;
  f.lessOrEqualTest4();
}

TEST_CASE("Approx/lessOrEqualTest5", "") {
  ApproxFixture f;
  f.lessOrEqualTest5();
}

TEST_CASE("Approx/lessOrEqualTest6", "") {
  ApproxFixture f;
  f.lessOrEqualTest6();
}

TEST_CASE("Approx/lessOrEqualTest7", "") {
  ApproxFixture f;
  f.lessOrEqualTest7();
}

TEST_CASE("Approx/lessOrEqualTest8", "") {
  ApproxFixture f;
  f.lessOrEqualTest8();
}

TEST_CASE("Approx/lessOrEqualTest9", "") {
  ApproxFixture f;
  f.lessOrEqualTest9();
}

TEST_CASE("Approx/lessOrEqualTest10", "") {
  ApproxFixture f;
  f.lessOrEqualTest10();
}

TEST_CASE("Approx/lessOrEqualTest11", "") {
  ApproxFixture f;
  f.lessOrEqualTest11();
}

TEST_CASE("Approx/lessOrEqualTest12", "") {
  ApproxFixture f;
  f.lessOrEqualTest12();
}

TEST_CASE("Approx/lessOrEqualTest13", "") {
  ApproxFixture f;
  f.lessOrEqualTest13();
}

TEST_CASE("Approx/greaterThanTest", "") {
  ApproxFixture f;
  f.greaterThanTest();
}

TEST_CASE("Approx/greaterThanTest1", "") {
  ApproxFixture f;
  f.greaterThanTest1();
}

TEST_CASE("Approx/greaterThanTest2", "") {
  ApproxFixture f;
  f.greaterThanTest2();
}

TEST_CASE("Approx/greaterThanTest3", "") {
  ApproxFixture f;
  f.greaterThanTest3();
}

TEST_CASE("Approx/greaterThanTest4", "") {
  ApproxFixture f;
  f.greaterThanTest4();
}

TEST_CASE("Approx/greaterThanTest5", "") {
  ApproxFixture f;
  f.greaterThanTest5();
}

TEST_CASE("Approx/greaterThanTest6", "") {
  ApproxFixture f;
  f.greaterThanTest6();
}

TEST_CASE("Approx/greaterThanTest7", "") {
  ApproxFixture f;
  f.greaterThanTest7();
}

TEST_CASE("Approx/greaterThanTest8", "") {
  ApproxFixture f;
  f.greaterThanTest8();
}

TEST_CASE("Approx/greaterThanTest9", "") {
  ApproxFixture f;
  f.greaterThanTest9();
}

TEST_CASE("Approx/greaterOrEqualTest1", "") {
  ApproxFixture f;
  f.greaterOrEqualTest1();
}

TEST_CASE("Approx/greaterOrEqualTest2", "") {
  ApproxFixture f;
  f.greaterOrEqualTest2();
}

TEST_CASE("Approx/greaterOrEqualTest3", "") {
  ApproxFixture f;
  f.greaterOrEqualTest3();
}

TEST_CASE("Approx/greaterOrEqualTest4", "") {
  ApproxFixture f;
  f.greaterOrEqualTest4();
}

TEST_CASE("Approx/greaterOrEqualTest5", "") {
  ApproxFixture f;
  f.greaterOrEqualTest5();
}

TEST_CASE("Approx/greaterOrEqualTest6", "") {
  ApproxFixture f;
  f.greaterOrEqualTest6();
}

TEST_CASE("Approx/greaterOrEqualTest7", "") {
  ApproxFixture f;
  f.greaterOrEqualTest7();
}

TEST_CASE("Approx/greaterOrEqualTest8", "") {
  ApproxFixture f;
  f.greaterOrEqualTest8();
}

TEST_CASE("Approx/greaterOrEqualTest9", "") {
  ApproxFixture f;
  f.greaterOrEqualTest9();
}

TEST_CASE("Approx/greaterOrEqualTest10", "") {
  ApproxFixture f;
  f.greaterOrEqualTest10();
}

TEST_CASE("Approx/greaterOrEqualTest11", "") {
  ApproxFixture f;
  f.greaterOrEqualTest11();
}

TEST_CASE("Approx/greaterOrEqualTest12", "") {
  ApproxFixture f;
  f.greaterOrEqualTest12();
}

TEST_CASE("Approx/issue44Test", "") {
  ApproxFixture f;
  f.issue44Test();
}

TEST_CASE("Approx/issue46Test", "") {
  ApproxFixture f;
  f.issue46Test();
}

TEST_CASE("Approx/issue52Test", "") {
  ApproxFixture f;
  f.issue52Test();
}

template<uint16_t Length>
struct BinaryGenerator {
  BinaryGenerator() {
    ::memset(buffer, 0, Length);
  }

  void generate(int i, ups_key_t *key) {
    ::sprintf(buffer, "%05d", i);
    key->data = buffer;
    key->size = Length;
  }

  uint16_t get_key_size() const {
    return (Length);
  }

  uint64_t get_key_type() const {
    return (UPS_TYPE_BINARY);
  }

  char buffer[Length];
};

struct BinaryVarLenGenerator : public BinaryGenerator<32> {
  uint16_t get_key_size() const {
    return (UPS_KEY_SIZE_UNLIMITED);
  }
};

template<uint64_t type, typename T>
struct PodGenerator {
  PodGenerator() : t(0) {
  }

  void generate(int i, ups_key_t *key) {
    t = (T)i;
    key->data = &t;
    key->size = sizeof(t);
  }

  uint16_t get_key_size() const {
    return (0);
  }

  uint64_t get_key_type() const {
    return (type);
  }

  T t;
};

// Btree tests for UPS_FIND_LT_MATCH

TEST_CASE("Approx/btreeLessThanBinary8Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/btreeLessThanBinary32Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/btreeLessThanBinary48Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/btreeLessThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.btreeLessThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/btreeLessThanUint16Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeLessThanUint32Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeLessThanUint64Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeLessThanReal32Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeLessThanReal64Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Btree tests for UPS_FIND_GT_MATCH

TEST_CASE("Approx/btreeGreaterThanBinary8Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/btreeGreaterThanBinary32Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/btreeGreaterThanBinary48Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/btreeGreaterThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/btreeGreaterThanUint16Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Btree tests for UPS_FIND_LEQ_MATCH

TEST_CASE("Approx/btreeLessEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/btreeLessEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/btreeLessEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/btreeLessEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/btreeLessEqualThanUint16Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Btree tests for UPS_FIND_GEQ_MATCH

TEST_CASE("Approx/btreeGreaterEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/btreeGreaterEqualThanUint16Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Txn tests for UPS_FIND_LT_MATCH

TEST_CASE("Approx/txnLessThanBinary8Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/txnLessThanBinary32Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/txnLessThanBinary48Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/txnLessThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.txnLessThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/txnLessThanUint16Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnLessThanUint32Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnLessThanUint64Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnLessThanReal32Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnLessThanReal64Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Txn tests for UPS_FIND_GT_MATCH

TEST_CASE("Approx/txnGreaterThanBinary8Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/txnGreaterThanBinary32Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/txnGreaterThanBinary48Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/txnGreaterThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/txnGreaterThanUint16Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Txn tests for UPS_FIND_LEQ_MATCH

TEST_CASE("Approx/txnLessEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/txnLessEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/txnLessEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/txnLessEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/txnLessEqualThanUint16Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Txn tests for UPS_FIND_GEQ_MATCH

TEST_CASE("Approx/txnGreaterEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/txnGreaterEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/txnGreaterEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/txnGreaterEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/txnGreaterEqualThanUint16Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_LT_MATCH

TEST_CASE("Approx/mixedLessThanBinary8Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixedLessThanBinary32Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixedLessThanBinary48Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixedLessThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixedLessThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixedLessThanUint16Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedLessThanUint32Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedLessThanUint64Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedLessThanReal32Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedLessThanReal64Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_GT_MATCH

TEST_CASE("Approx/mixedGreaterThanBinary8Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixedGreaterThanBinary32Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixedGreaterThanBinary48Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixedGreaterThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixedGreaterThanUint16Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_LEQ_MATCH

TEST_CASE("Approx/mixedLessEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixedLessEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixedLessEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixedLessEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixedLessEqualThanUint16Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_GEQ_MATCH

TEST_CASE("Approx/mixedGreaterEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixedGreaterEqualThanUint16Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_LT_MATCH

TEST_CASE("Approx/mixed2LessThanBinary8Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixed2LessThanBinary32Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixed2LessThanBinary48Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixed2LessThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixed2LessThanUint16Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2LessThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2LessThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2LessThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2LessThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_GT_MATCH

TEST_CASE("Approx/mixed2GreaterThanBinary8Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixed2GreaterThanBinary32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixed2GreaterThanBinary48Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixed2GreaterThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixed2GreaterThanUint16Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2GreaterThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_LEQ_MATCH

TEST_CASE("Approx/mixed2LessEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixed2LessEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixed2LessEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixed2LessEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixed2LessEqualThanUint16Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2LessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

// Mixed tests (Txn + Btree) for UPS_FIND_GEQ_MATCH

TEST_CASE("Approx/mixed2GreaterEqualThanBinary8Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<BinaryGenerator<8> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanBinary32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<BinaryGenerator<32> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanBinary48Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<BinaryGenerator<48> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanBinaryVarlenTest", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<BinaryVarLenGenerator>();
}

TEST_CASE("Approx/mixed2GreaterEqualThanUint16Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<UPS_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<UPS_TYPE_REAL64, double> >();
}

