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

#include "../src/config.h"

#include <stdexcept>
#include <string.h>
#include <vector>
#include <string>
#include <algorithm>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/blob.h"
#include "../src/btree.h"
#include "../src/endianswap.h"
#include "../src/cursor.h"
#include "../src/env.h"
#include "../src/btree_cursor.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class ApproxTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  ApproxTest(const char *name = "ApproxTest")
    : hamsterDB_fixture(name) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(ApproxTest, lessThanTest);
    BFC_REGISTER_TEST(ApproxTest, lessOrEqualTest);
    BFC_REGISTER_TEST(ApproxTest, greaterThanTest);
    BFC_REGISTER_TEST(ApproxTest, greaterOrEqualTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_txn_t *m_txn;

public:
  virtual void setup() {
    __super::setup();

    (void)os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(0,
          ham_env_create(&m_env, BFC_OPATH(".test"),
              HAM_ENABLE_TRANSACTIONS, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&m_txn, m_env, 0, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_txn_abort(m_txn, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  ham_status_t insertBtree(const char *s) {
    ham_key_t k = {};
    k.data = (void *)s;
    k.size = strlen(s) + 1;
    ham_record_t r = {};
    r.data = k.data;
    r.size = k.size;

    BtreeIndex *be = ((Database *)m_db)->get_btree();
    return (be->insert((Transaction *)m_txn, &k, &r, 0));
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
      BFC_ASSERT(ham_key_get_intflags(&k) & BtreeKey::KEY_IS_APPROXIMATE);
    return (strcmp(expected, (const char *)r.data));
  }

  void lessThanTest() {
    // btree < nil
    BFC_ASSERT_EQUAL(0, insertBtree("1"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "2", "1"));

    // txn < nil
    BFC_ASSERT_EQUAL(0, insertTxn("2"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "3", "2"));

    // btree < txn
    BFC_ASSERT_EQUAL(0, insertBtree("10"));
    BFC_ASSERT_EQUAL(0, insertTxn("11"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "11", "10"));

    // txn < btree
    BFC_ASSERT_EQUAL(0, insertTxn("20"));
    BFC_ASSERT_EQUAL(0, insertBtree("21"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "21", "20"));

    // btree < btree
    BFC_ASSERT_EQUAL(0, insertBtree("30"));
    BFC_ASSERT_EQUAL(0, insertBtree("31"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "31", "30"));

    // txn < txn
    BFC_ASSERT_EQUAL(0, insertTxn("40"));
    BFC_ASSERT_EQUAL(0, insertTxn("41"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "41", "40"));

    // txn < txn w/ empty node
    BFC_ASSERT_EQUAL(0, insertTxn("50"));
    BFC_ASSERT_EQUAL(0, insertTxn("51"));
    BFC_ASSERT_EQUAL(0, insertTxn("52"));
    BFC_ASSERT_EQUAL(0, eraseTxn("51"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "52", "50"));

    // txn < txn w/ empty node
    BFC_ASSERT_EQUAL(0, insertTxn("60"));
    BFC_ASSERT_EQUAL(0, insertTxn("61"));
    BFC_ASSERT_EQUAL(0, insertTxn("62"));
    BFC_ASSERT_EQUAL(0, eraseTxn("61"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "62", "60"));

    // skip erased btree
    BFC_ASSERT_EQUAL(0, insertBtree("71"));
    BFC_ASSERT_EQUAL(0, eraseTxn("71"));
    BFC_ASSERT_EQUAL(0, insertTxn("70"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "71", "70"));

    // skip 2 erased btree keys
    BFC_ASSERT_EQUAL(0, insertBtree("80"));
    BFC_ASSERT_EQUAL(0, insertBtree("81"));
    BFC_ASSERT_EQUAL(0, eraseTxn("81"));
    BFC_ASSERT_EQUAL(0, insertBtree("82"));
    BFC_ASSERT_EQUAL(0, eraseTxn("82"));
    BFC_ASSERT_EQUAL(0, insertTxn("83"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "83", "80"));

    // overwrite btree
    BFC_ASSERT_EQUAL(0, insertBtree("92"));
    BFC_ASSERT_EQUAL(0, insertTxn("92", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, insertBtree("93"));
    BFC_ASSERT_EQUAL(0, insertTxn("93", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LT_MATCH, "93", "92"));
  }

  void lessOrEqualTest() {
    // btree < nil
    BFC_ASSERT_EQUAL(0, insertBtree("1"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "2", "1"));

    // btree = nil
    BFC_ASSERT_EQUAL(0, insertBtree("2"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "2", "2"));

    // txn < nil
    BFC_ASSERT_EQUAL(0, insertTxn("3"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "4", "3"));

    // txn = nil
    BFC_ASSERT_EQUAL(0, insertTxn("4"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "5", "4"));

    // btree < txn
    BFC_ASSERT_EQUAL(0, insertBtree("10"));
    BFC_ASSERT_EQUAL(0, insertTxn("11"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "11", "11"));

    // txn < btree
    BFC_ASSERT_EQUAL(0, insertTxn("20"));
    BFC_ASSERT_EQUAL(0, insertBtree("21"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "21", "21"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "22", "21"));

    // btree < btree
    BFC_ASSERT_EQUAL(0, insertBtree("30"));
    BFC_ASSERT_EQUAL(0, insertBtree("31"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "31", "31"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "32", "31"));

    // txn < txn
    BFC_ASSERT_EQUAL(0, insertTxn("40"));
    BFC_ASSERT_EQUAL(0, insertTxn("41"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "41", "41"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "42", "41"));

    // txn =
    BFC_ASSERT_EQUAL(0, insertBtree("50"));
    BFC_ASSERT_EQUAL(0, insertTxn("51"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "51", "51"));

    // btree =
    BFC_ASSERT_EQUAL(0, insertTxn("60"));
    BFC_ASSERT_EQUAL(0, insertBtree("61"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "61", "61"));

    // txn < txn w/ empty node
    BFC_ASSERT_EQUAL(0, insertTxn("70"));
    BFC_ASSERT_EQUAL(0, insertTxn("71"));
    BFC_ASSERT_EQUAL(0, eraseTxn("71"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "71", "70"));

    // skip 3 erased btree keys
    BFC_ASSERT_EQUAL(0, insertBtree("80"));
    BFC_ASSERT_EQUAL(0, insertBtree("81"));
    BFC_ASSERT_EQUAL(0, eraseTxn("81"));
    BFC_ASSERT_EQUAL(0, insertBtree("82"));
    BFC_ASSERT_EQUAL(0, eraseTxn("82"));
    BFC_ASSERT_EQUAL(0, insertTxn("83"));
    BFC_ASSERT_EQUAL(0, eraseTxn("83"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "83", "80"));

    // overwrite btree
    BFC_ASSERT_EQUAL(0, insertBtree("92"));
    BFC_ASSERT_EQUAL(0, insertTxn("92", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, insertBtree("93"));
    BFC_ASSERT_EQUAL(0, insertTxn("93", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_LEQ_MATCH, "93", "93"));
  }

  void greaterThanTest() {
    // btree > nil
    BFC_ASSERT_EQUAL(0, insertBtree("2"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "1", "2"));

    // txn > nil
    BFC_ASSERT_EQUAL(0, insertTxn("4"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "3", "4"));

    // btree > txn
    BFC_ASSERT_EQUAL(0, insertTxn("10"));
    BFC_ASSERT_EQUAL(0, insertBtree("11"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "10", "11"));

    // txn > btree
    BFC_ASSERT_EQUAL(0, insertBtree("20"));
    BFC_ASSERT_EQUAL(0, insertTxn("21"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "20", "21"));

    // btree > btree
    BFC_ASSERT_EQUAL(0, insertBtree("30"));
    BFC_ASSERT_EQUAL(0, insertBtree("31"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "30", "31"));

    // txn > txn
    BFC_ASSERT_EQUAL(0, insertTxn("40"));
    BFC_ASSERT_EQUAL(0, insertTxn("41"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "40", "41"));

    // txn > txn w/ empty node
    BFC_ASSERT_EQUAL(0, insertTxn("50"));
    BFC_ASSERT_EQUAL(0, insertTxn("51"));
    BFC_ASSERT_EQUAL(0, eraseTxn("51"));
    BFC_ASSERT_EQUAL(0, insertTxn("52"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "50", "52"));

    // skip 2 erased btree keys
    BFC_ASSERT_EQUAL(0, insertBtree("81"));
    BFC_ASSERT_EQUAL(0, eraseTxn("81"));
    BFC_ASSERT_EQUAL(0, insertBtree("82"));
    BFC_ASSERT_EQUAL(0, eraseTxn("82"));
    BFC_ASSERT_EQUAL(0, insertTxn("83"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GT_MATCH, "80", "83"));
  }

  void greaterOrEqualTest() {
    // btree > nil
    BFC_ASSERT_EQUAL(0, insertBtree("1"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "0", "1"));

    // btree = nil
    BFC_ASSERT_EQUAL(0, insertBtree("3"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "3", "3"));

    // txn > nil
    BFC_ASSERT_EQUAL(0, insertTxn("5"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "4", "5"));

    // txn = nil
    BFC_ASSERT_EQUAL(0, insertTxn("7"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "7", "7"));

    // btree > txn
    BFC_ASSERT_EQUAL(0, insertTxn("11"));
    BFC_ASSERT_EQUAL(0, insertBtree("12"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "11", "11"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "10", "11"));

    // txn > btree
    BFC_ASSERT_EQUAL(0, insertBtree("20"));
    BFC_ASSERT_EQUAL(0, insertTxn("21"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "19", "20"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "20", "20"));

    // btree > btree
    BFC_ASSERT_EQUAL(0, insertBtree("30"));
    BFC_ASSERT_EQUAL(0, insertBtree("31"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "31", "31"));

    // txn > txn
    BFC_ASSERT_EQUAL(0, insertTxn("40"));
    BFC_ASSERT_EQUAL(0, insertTxn("41"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "41", "41"));

    // txn =
    BFC_ASSERT_EQUAL(0, insertBtree("50"));
    BFC_ASSERT_EQUAL(0, insertTxn("51"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "51", "51"));

    // btree =
    BFC_ASSERT_EQUAL(0, insertTxn("60"));
    BFC_ASSERT_EQUAL(0, insertBtree("61"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "61", "61"));

    // txn > txn w/ empty node
    BFC_ASSERT_EQUAL(0, insertTxn("71"));
    BFC_ASSERT_EQUAL(0, eraseTxn("71"));
    BFC_ASSERT_EQUAL(0, insertTxn("72"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "71", "72"));

    // skip erased btree keys
    BFC_ASSERT_EQUAL(0, insertBtree("81"));
    BFC_ASSERT_EQUAL(0, eraseTxn("81"));
    BFC_ASSERT_EQUAL(0, insertBtree("82"));
    BFC_ASSERT_EQUAL(0, eraseTxn("82"));
    BFC_ASSERT_EQUAL(0, insertTxn("83"));
    BFC_ASSERT_EQUAL(0, find(HAM_FIND_GEQ_MATCH, "81", "83"));
  }
};

BFC_REGISTER_FIXTURE(ApproxTest);
