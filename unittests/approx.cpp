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
#include "4env/env.h"

using namespace hamsterdb;

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
    REQUIRE(0 == ham_txn_abort(m_txn, 0));
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
    return (be->insert((Transaction *)m_txn, 0, &k, &r, 0));
  }

  ham_status_t insertTxn(const char *s, ham_u32_t flags = 0) {
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

  ham_status_t find(ham_u32_t flags, const char *search, const char *expected) {
    ham_key_t k = {};
    k.data = (void *)search;
    k.size = strlen(search) + 1;
    ham_record_t r = {};

    ham_status_t st = ham_db_find(m_db, m_txn, &k, &r, flags);
    if (st)
      return (st);
    if (strcmp(expected, (const char *)k.data))
      REQUIRE((ham_key_get_intflags(&k) & BtreeKey::kApproximate));
    return (strcmp(expected, (const char *)r.data));
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

