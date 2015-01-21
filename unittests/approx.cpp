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

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include <vector>
#include <string>
#include <algorithm>

#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "4db/db.h"
#include "4cursor/cursor.h"
#include "4env/env_local.h"

using namespace hamsterdb;

static int
slot_key_cmp(ham_db_t *db, const uint8_t *lhs, uint32_t lsz,
        const uint8_t *rhs, uint32_t rsz)
{
  uint32_t i;

  for (i = 0; i < lsz; ++i) {
    if (lhs[i] != rhs[i]) {
      return lhs[i] > rhs[i] ? 1 : -1;
    }
  }
  return 0;
}

struct ApproxFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_txn_t *m_txn;

  ApproxFixture() {
    (void)os::unlink(Utils::opath(".test"));

    REQUIRE(0 ==
          ham_env_create(&m_env, Utils::opath(".test"),
              HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
  }

  ~ApproxFixture() {
    teardown();
  }

  void teardown() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    lenv->get_changeset()->clear();

    if (m_txn) {
      REQUIRE(0 == ham_txn_abort(m_txn, 0));
      m_txn = 0;
    }
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  ham_status_t insertBtree(const char *s) {
    ham_key_t k = {};
    k.data = (void *)s;
    k.size = strlen(s) + 1;
    ham_record_t r = {};
    r.data = k.data;
    r.size = k.size;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    return (be->insert(0, &k, &r, 0));
  }

  ham_status_t insertTxn(const char *s, uint32_t flags = 0) {
    ham_key_t k = {};
    k.data = (void *)s;
    k.size = strlen(s) + 1;
    ham_record_t r = {};
    r.data = k.data;
    r.size = k.size;

    return (ham_db_insert(m_db, m_txn, &k, &r, flags));
  }

  ham_status_t eraseTxn(const char *s) {
    ham_key_t k = {};
    k.data = (void *)s;
    k.size = strlen(s)+1;

    return (ham_db_erase(m_db, m_txn, &k, 0));
  }

  ham_status_t find(uint32_t flags, const char *search, const char *expected) {
    ham_key_t k = {};
    k.data = (void *)search;
    k.size = strlen(search) + 1;
    ham_record_t r = {};

    ham_status_t st = ham_db_find(m_db, m_txn, &k, &r, flags);
    if (st)
      return (st);
    if (strcmp(expected, (const char *)k.data))
      REQUIRE((ham_key_get_intflags(&k) & BtreeKey::kApproximate));
    return (::strcmp(expected, (const char *)r.data));
  }

  void lessThanTest() {
    // btree < nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "2", "1"));

    // txn < nil
    REQUIRE(0 == insertTxn("2"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "3", "2"));

    // btree < txn
    REQUIRE(0 == insertBtree("10"));
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "11", "10"));

    // txn < btree
    REQUIRE(0 == insertTxn("20"));
    REQUIRE(0 == insertBtree("21"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "21", "20"));

    // btree < btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "31", "30"));

    // txn < txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "41", "40"));

    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == insertTxn("52"));
    REQUIRE(0 == eraseTxn("51"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "52", "50"));

    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertTxn("61"));
    REQUIRE(0 == insertTxn("62"));
    REQUIRE(0 == eraseTxn("61"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "62", "60"));

    // skip erased btree
    REQUIRE(0 == insertBtree("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == insertTxn("70"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "71", "70"));

    // skip 2 erased btree keys
    REQUIRE(0 == insertBtree("80"));
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "83", "80"));

    // overwrite btree
    REQUIRE(0 == insertBtree("92"));
    REQUIRE(0 == insertTxn("92", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("93"));
    REQUIRE(0 == insertTxn("93", HAM_OVERWRITE));
    REQUIRE(0 == find(HAM_FIND_LT_MATCH, "93", "92"));
  }

  void lessOrEqualTest() {
    // btree < nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "2", "1"));

    // btree = nil
    REQUIRE(0 == insertBtree("2"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "2", "2"));

    // txn < nil
    REQUIRE(0 == insertTxn("3"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "4", "3"));

    // txn = nil
    REQUIRE(0 == insertTxn("4"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "5", "4"));

    // btree < txn
    REQUIRE(0 == insertBtree("10"));
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "11", "11"));

    // txn < btree
    REQUIRE(0 == insertTxn("20"));
    REQUIRE(0 == insertBtree("21"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "21", "21"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "22", "21"));

    // btree < btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "31", "31"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "32", "31"));

    // txn < txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "41", "41"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "42", "41"));

    // txn =
    REQUIRE(0 == insertBtree("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "51", "51"));

    // btree =
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertBtree("61"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "61", "61"));

    // txn < txn w/ empty node
    REQUIRE(0 == insertTxn("70"));
    REQUIRE(0 == insertTxn("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "71", "70"));

    // skip 3 erased btree keys
    REQUIRE(0 == insertBtree("80"));
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == eraseTxn("83"));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "83", "80"));

    // overwrite btree
    REQUIRE(0 == insertBtree("92"));
    REQUIRE(0 == insertTxn("92", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("93"));
    REQUIRE(0 == insertTxn("93", HAM_OVERWRITE));
    REQUIRE(0 == find(HAM_FIND_LEQ_MATCH, "93", "93"));
  }

  void greaterThanTest() {
    // btree > nil
    REQUIRE(0 == insertBtree("2"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "1", "2"));

    // txn > nil
    REQUIRE(0 == insertTxn("4"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "3", "4"));

    // btree > txn
    REQUIRE(0 == insertTxn("10"));
    REQUIRE(0 == insertBtree("11"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "10", "11"));

    // txn > btree
    REQUIRE(0 == insertBtree("20"));
    REQUIRE(0 == insertTxn("21"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "20", "21"));

    // btree > btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "30", "31"));

    // txn > txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "40", "41"));

    // txn > txn w/ empty node
    REQUIRE(0 == insertTxn("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == eraseTxn("51"));
    REQUIRE(0 == insertTxn("52"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "50", "52"));

    // skip 2 erased btree keys
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(HAM_FIND_GT_MATCH, "80", "83"));
  }

  void greaterOrEqualTest() {
    // btree > nil
    REQUIRE(0 == insertBtree("1"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "0", "1"));

    // btree = nil
    REQUIRE(0 == insertBtree("3"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "3", "3"));

    // txn > nil
    REQUIRE(0 == insertTxn("5"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "4", "5"));

    // txn = nil
    REQUIRE(0 == insertTxn("7"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "7", "7"));

    // btree > txn
    REQUIRE(0 == insertTxn("11"));
    REQUIRE(0 == insertBtree("12"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "11", "11"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "10", "11"));

    // txn > btree
    REQUIRE(0 == insertBtree("20"));
    REQUIRE(0 == insertTxn("21"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "19", "20"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "20", "20"));

    // btree > btree
    REQUIRE(0 == insertBtree("30"));
    REQUIRE(0 == insertBtree("31"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "31", "31"));

    // txn > txn
    REQUIRE(0 == insertTxn("40"));
    REQUIRE(0 == insertTxn("41"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "41", "41"));

    // txn =
    REQUIRE(0 == insertBtree("50"));
    REQUIRE(0 == insertTxn("51"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "51", "51"));

    // btree =
    REQUIRE(0 == insertTxn("60"));
    REQUIRE(0 == insertBtree("61"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "61", "61"));

    // txn > txn w/ empty node
    REQUIRE(0 == insertTxn("71"));
    REQUIRE(0 == eraseTxn("71"));
    REQUIRE(0 == insertTxn("72"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "71", "72"));

    // skip erased btree keys
    REQUIRE(0 == insertBtree("81"));
    REQUIRE(0 == eraseTxn("81"));
    REQUIRE(0 == insertBtree("82"));
    REQUIRE(0 == eraseTxn("82"));
    REQUIRE(0 == insertTxn("83"));
    REQUIRE(0 == find(HAM_FIND_GEQ_MATCH, "81", "83"));
  }

  void issue44Test() {
    teardown();

    ham_parameter_t param[] = {
        {HAM_PARAM_KEY_TYPE, HAM_TYPE_CUSTOM},
        {HAM_PARAM_KEY_SIZE, 41},
        {0, 0}
    };

    REQUIRE(0 == ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, &param[0]));
    REQUIRE(0 == ham_db_set_compare_func(m_db, slot_key_cmp));

    const char *values[] = { "11", "22", "33", "44", "55" };
    for (int i = 0; i < 5; i++) {
      char keydata[41];
      ::memcpy(&keydata[0], values[i], 3);
      ham_key_t key = ham_make_key(&keydata[0], sizeof(keydata));
      ham_record_t rec = ham_make_record((void *)values[i], 3);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    char keydata[41];
    ::memcpy(&keydata[0], "10", 3);
    ham_key_t key = ham_make_key((void *)keydata, sizeof(keydata));
    ham_record_t rec = {0};
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "11"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "11"));
  }

  void issue46Test() {
    REQUIRE(0 == insertBtree("aa"));
    REQUIRE(0 == eraseTxn("aa"));

    ham_key_t key = ham_make_key((void *)"aa", 3);
    ham_record_t rec = {0};

    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_GEQ_MATCH));
  }

  void greaterThanTest2() {
    teardown();

    ham_parameter_t param[] = {
        {HAM_PARAM_KEY_TYPE, HAM_TYPE_BINARY},
        {HAM_PARAM_KEY_SIZE, 32},
        {0, 0}
    };

    REQUIRE(0 == ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, &param[0]));

    char data[32] = {0};
    ham_key_t key = ham_make_key(&data[0], sizeof(data));
    ham_record_t rec = {0};
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    data[31] = 1;
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));
    char newdata[32] = {0};
    REQUIRE(0 == ::memcmp(key.data, &newdata[0], sizeof(newdata)));
  }

  template<typename Generator>
  void btreeLessThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    gen.generate(0, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));

    for (i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void btreeLessEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void btreeGreaterThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 1; i <= 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
  }

  template<typename Generator>
  void btreeGreaterEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), 0, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void txnLessThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    }

    gen.generate(0, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_LT_MATCH));

    for (i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void txnLessEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void txnGreaterThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 1; i <= 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    }

    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_GT_MATCH));
  }

  template<typename Generator>
  void txnGreaterEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, HAM_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void mixedLessThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    gen.generate(0, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));

    for (i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixedLessEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i - 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixedGreaterThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 1; i <= 5000; i++) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
  }

  template<typename Generator>
  void mixedGreaterEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i <= 10000; i += 2) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (i = 0; i < 10000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i & 1 ? i + 1 : i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(10000, &key);
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
  }

  template<typename Generator>
  void mixed2LessThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 5000; i += 4) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      gen.generate(i + 1, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
      gen.generate(i + 2, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));

      gen.generate(i + 3, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
      REQUIRE(0 == ham_txn_commit(m_txn, 0));
    }
    m_txn = 0;

    gen.generate(0, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));

    for (i = 1; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i - 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixed2GreaterThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 1; i <= 5000; i += 4) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      gen.generate(i + 1, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
      gen.generate(i + 2, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));

      gen.generate(i + 3, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
      REQUIRE(0 == ham_txn_commit(m_txn, 0));
    }
    m_txn = 0;

    for (i = 0; i < 5000; i++) {
      gen.generate(i, &key);

      ham_key_t key2 = {0};
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }

    gen.generate(5000, &key);
    REQUIRE(HAM_KEY_NOT_FOUND
            == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GT_MATCH));
  }

  template<typename Generator>
  void mixed2LessEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 10000; i += 5) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      gen.generate(i + 1, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
      gen.generate(i + 2, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));

      gen.generate(i + 3, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
      REQUIRE(0 == ham_txn_commit(m_txn, 0));

      // skip i + 4
    }
    m_txn = 0;

    for (i = 0; i < 10000; i += 5) {
      ham_key_t key2 = {0};

      gen.generate(i, &key);
      gen2.generate(i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 1, &key);
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 2, &key);
      gen2.generate(i + 2, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 3, &key);
      gen2.generate(i + 3, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 4, &key);
      gen2.generate(i + 3, &key2); // !!
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
    }
  }

  template<typename Generator>
  void mixed2GreaterEqualThanTest() {
    teardown();
    Generator gen, gen2;

    ham_parameter_t envparam[] = {
        {HAM_PARAM_PAGE_SIZE, 1024},
        {0, 0}
    };

    ham_parameter_t dbparam[] = {
        {HAM_PARAM_KEY_TYPE, gen.get_key_type()},
        {HAM_PARAM_RECORD_SIZE, 32},
        {0, 0},
        {0, 0}
    };

    if (gen.get_key_size() > 0) {
      dbparam[2].name = HAM_PARAM_KEY_SIZE;
      dbparam[2].value = gen.get_key_size();
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0664, &envparam[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1,
                    HAM_FORCE_RECORDS_INLINE, &dbparam[0]));

    ham_key_t key = {0};
    char recbuffer[32] = {0};
    ham_record_t rec = ham_make_record(&recbuffer[0], sizeof(recbuffer));

    int i;
    for (i = 0; i < 10000; i += 5) {
      gen.generate(i, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      gen.generate(i + 1, &key);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

      REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
      gen.generate(i + 2, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));

      gen.generate(i + 3, &key);
      REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
      REQUIRE(0 == ham_txn_commit(m_txn, 0));

      // skip i + 4
    }
    m_txn = 0;

    for (i = 0; i < 10000; i += 5) {
      ham_key_t key2 = {0};

      gen.generate(i, &key);
      gen2.generate(i, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 1, &key);
      gen2.generate(i + 1, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 2, &key);
      gen2.generate(i + 2, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      gen.generate(i + 3, &key);
      gen2.generate(i + 3, &key2);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
      REQUIRE(key2.size == key.size);
      REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));

      if (i + 5 < 10000) {
        gen.generate(i + 4, &key);
        gen2.generate(i + 5, &key2); // !!
        REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
        REQUIRE(key2.size == key.size);
        REQUIRE(0 == ::memcmp(key.data, key2.data, key2.size));
      }
    }
  }
};

TEST_CASE("Approx/lessThanTest", "") {
  ApproxFixture f;
  f.lessThanTest();
}

TEST_CASE("Approx/lessOrEqualTest", "") {
  ApproxFixture f;
  f.lessOrEqualTest();
}

TEST_CASE("Approx/greaterThanTest", "") {
  ApproxFixture f;
  f.greaterThanTest();
}

TEST_CASE("Approx/greaterOrEqualTest", "") {
  ApproxFixture f;
  f.greaterOrEqualTest();
}

TEST_CASE("Approx/issue44Test", "") {
  ApproxFixture f;
  f.issue44Test();
}

TEST_CASE("Approx/issue46Test", "") {
  ApproxFixture f;
  f.issue46Test();
}

TEST_CASE("Approx/greaterThanTest2", "") {
  ApproxFixture f;
  f.greaterThanTest2();
}

template<uint16_t Length>
struct BinaryGenerator {
  BinaryGenerator() {
    ::memset(buffer, 0, Length);
  }

  void generate(int i, ham_key_t *key) {
    ::sprintf(buffer, "%05d", i);
    key->data = buffer;
    key->size = Length;
  }

  uint16_t get_key_size() const {
    return (Length);
  }

  uint64_t get_key_type() const {
    return (HAM_TYPE_BINARY);
  }

  char buffer[Length];
};

struct BinaryVarLenGenerator : public BinaryGenerator<32> {
  uint16_t get_key_size() const {
    return (HAM_KEY_SIZE_UNLIMITED);
  }
};

template<uint64_t type, typename T>
struct PodGenerator {
  PodGenerator() : t(0) {
  }

  void generate(int i, ham_key_t *key) {
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

// Btree tests for HAM_FIND_LT_MATCH

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
  f.btreeLessThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeLessThanUint32Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeLessThanUint64Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeLessThanReal32Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeLessThanReal64Test", "") {
  ApproxFixture f;
  f.btreeLessThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Btree tests for HAM_FIND_GT_MATCH

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
  f.btreeGreaterThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.btreeGreaterThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Btree tests for HAM_FIND_LEQ_MATCH

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
  f.btreeLessEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.btreeLessEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Btree tests for HAM_FIND_GEQ_MATCH

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
  f.btreeGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/btreeGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.btreeGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Transaction tests for HAM_FIND_LT_MATCH

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
  f.txnLessThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnLessThanUint32Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnLessThanUint64Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnLessThanReal32Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnLessThanReal64Test", "") {
  ApproxFixture f;
  f.txnLessThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Transaction tests for HAM_FIND_GT_MATCH

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
  f.txnGreaterThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.txnGreaterThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Transaction tests for HAM_FIND_LEQ_MATCH

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
  f.txnLessEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.txnLessEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Transaction tests for HAM_FIND_GEQ_MATCH

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
  f.txnGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/txnGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/txnGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.txnGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_LT_MATCH

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
  f.mixedLessThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedLessThanUint32Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedLessThanUint64Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedLessThanReal32Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedLessThanReal64Test", "") {
  ApproxFixture f;
  f.mixedLessThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_GT_MATCH

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
  f.mixedGreaterThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedGreaterThanUint32Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedGreaterThanUint64Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedGreaterThanReal32Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedGreaterThanReal64Test", "") {
  ApproxFixture f;
  f.mixedGreaterThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_LEQ_MATCH

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
  f.mixedLessEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedLessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedLessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixedLessEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_GEQ_MATCH

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
  f.mixedGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixedGreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixedGreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_LT_MATCH

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
  f.mixed2LessThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2LessThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2LessThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2LessThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2LessThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2LessThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_GT_MATCH

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
  f.mixed2GreaterThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2GreaterThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2GreaterThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_LEQ_MATCH

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
  f.mixed2LessEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2LessEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2LessEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2LessEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

// Mixed tests (Transaction + Btree) for HAM_FIND_GEQ_MATCH

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
  f.mixed2GreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT16, uint16_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanUint32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT32, uint32_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanUint64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<HAM_TYPE_UINT64, uint64_t> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanReal32Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL32, float> >();
}

TEST_CASE("Approx/mixed2GreaterEqualThanReal64Test", "") {
  ApproxFixture f;
  f.mixed2GreaterEqualThanTest<PodGenerator<HAM_TYPE_REAL64, double> >();
}

