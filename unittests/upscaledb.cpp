/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "1os/file.h"
#include "1errorinducer/errorinducer.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4cursor/cursor_local.h"

#include "utils.h"
#include "os.hpp"

namespace upscaledb {

static int UPS_CALLCONV
my_compare_func(ups_db_t *db,
      const uint8_t *lhs, uint32_t lhs_length,
      const uint8_t *rhs, uint32_t rhs_length) {
  (void)lhs;
  (void)rhs;
  (void)lhs_length;
  (void)rhs_length;
  return (0);
}

static int UPS_CALLCONV
custom_compare_func(ups_db_t *db,
      const uint8_t *lhs, uint32_t lhs_length,
      const uint8_t *rhs, uint32_t rhs_length) {
  REQUIRE(lhs_length == rhs_length);
  REQUIRE(lhs_length == 7);
  return (::memcmp(lhs, rhs, lhs_length));
}

struct my_key_t {
  int32_t val1;
  uint32_t val2;
  uint32_t val3;
  uint32_t val4;
};

struct my_rec_t {
  int32_t val1;
  uint32_t val2[15];
};

struct UpscaledbFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;

  UpscaledbFixture() {
    os::unlink(Utils::opath(".test"));
    REQUIRE(0 == ups_env_create(&m_env, 0, UPS_IN_MEMORY, 0, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~UpscaledbFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
    m_env = 0;
    m_db = 0;
  }

  void versionTest() {
    uint32_t major, minor, revision;

    ups_get_version(0, 0, 0);
    ups_get_version(&major, &minor, &revision);

    REQUIRE((uint32_t)UPS_VERSION_MAJ == major);
    REQUIRE((uint32_t)UPS_VERSION_MIN == minor);
    REQUIRE((uint32_t)UPS_VERSION_REV == revision);
  };

  void openTest() {
    ups_env_t *env;
    ups_parameter_t params[] = {
      { 0x1234567, 0 },
      { 0, 0 }
    };

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(0, "test.db", 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, 0, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, 0, UPS_IN_MEMORY, 0));
    REQUIRE(UPS_FILE_NOT_FOUND ==
        ups_env_open(&env, "xxxx...", 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, "test.db", UPS_IN_MEMORY, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, "test.db", UPS_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, "test.db", UPS_ENABLE_DUPLICATE_KEYS, params));

#ifdef WIN32
    REQUIRE(UPS_IO_ERROR ==
        ups_env_open(&env, "c:\\windows", 0, 0));
#else
    REQUIRE(UPS_IO_ERROR ==
        ups_env_open(&env, "/usr", 0, 0));
#endif
  }

  void getEnvTest() {
    // m_db is already initialized
    REQUIRE(ups_db_get_env(m_db));
  }

  void invHeaderTest() {
    ups_env_t *env;

    REQUIRE(UPS_INV_FILE_HEADER ==
        ups_env_open(&env, Utils::ipath("data/inv-file-header.hdb"), 0, 0));
  }

  void createTest() {
    ups_env_t *env;
    ups_parameter_t cs[] = { { UPS_PARAM_CACHESIZE, 1024 }, { 0, 0 } };
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE,   512 }, { 0, 0 } };

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(0, ".test.db", 0, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, 0, 0, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, 0, UPS_IN_MEMORY, 0, &cs[0]));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, 0, UPS_IN_MEMORY|UPS_READ_ONLY, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, 0, UPS_READ_ONLY, 0, 0));
    REQUIRE(UPS_INV_PAGESIZE ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0, &ps[0]));
#ifdef WIN32
    REQUIRE(UPS_IO_ERROR ==
        ups_env_create(&env, "c:\\windows", 0, 0664, 0));
#else
    REQUIRE(UPS_IO_ERROR ==
        ups_env_create(&env, "/home", 0, 0664, 0));
#endif
  }

  void createPagesizeTest() {
    ups_env_t *env;

    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 512 }, { 0, 0 } };

    REQUIRE(UPS_INV_PAGESIZE ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0644, &ps[0]));

    ps[0].value = 1024;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0644, &ps[0]));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createCloseCreateTest() {
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ups_env_close(env, 0));
    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), 0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createPagesizeReopenTest() {
    ups_env_t *env;
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 1024 * 128 }, { 0, 0 } };

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), 0, 0664, &ps[0]));
    REQUIRE(0 == ups_env_close(env, 0));
    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), 0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void readOnlyTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *cursor;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, UPS_READ_ONLY, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_erase(cursor, 0));

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void invalidPagesizeTest() {
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, 512 },
      { 0, 0 }
    };
    ups_parameter_t p2[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t p3[] = {
      { UPS_PARAM_KEYSIZE, 512 },
      { 0, 0 }
    };

    REQUIRE(UPS_INV_PAGESIZE ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, p1));

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, p2));

    REQUIRE(UPS_INV_KEY_SIZE ==
        ups_env_create_db(env, &db, 1, 0, p3));

    p2[0].value = 15;

    // only page_size of 1k, 2k, multiples of 2k are allowed
    p1[0].value = 1024;
    REQUIRE(0 == ups_env_close(env, 0));
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ups_env_close(env, 0));
    p1[0].value = 2048;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ups_env_close(env, 0));
    p1[0].value = 4096;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ups_env_close(env, 0));
    p1[0].value = 1024 * 3;
    REQUIRE(UPS_INV_PAGESIZE ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, &p1[0]));
  }

  void invalidKeysizeTest() {
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t p2[] = {
      { UPS_PARAM_KEYSIZE, 200 },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0664, p1));

    REQUIRE(UPS_INV_KEY_SIZE ==
        ups_env_create_db(env, &db, 1, 0, p2));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void setCompareTest() {
    REQUIRE(UPS_INV_PARAMETER == ups_db_set_compare_func(0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_db_set_compare_func(m_db, 0));
  }

  void findTest() {
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(0, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, 0, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, 0, 0));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(m_db, 0, &key, &rec, 0));
  }

  void findEmptyRecordTest() {
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, 0));

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    rec.data = (void *)"123";
    rec.size = 12345;
    rec.flags = UPS_RECORD_USER_ALLOC;
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));

    REQUIRE((uint16_t)0 == key.size);
    REQUIRE((void *)0 == key.data);
    REQUIRE((uint32_t)0 == rec.size);
    REQUIRE((void *)0 == rec.data);

    REQUIRE(0 == ups_cursor_close(cursor));
  }


  static int UPS_CALLCONV my_compare_func_u32(ups_db_t *db,
                  const uint8_t *lhs, uint32_t lhs_length,
                  const uint8_t *rhs, uint32_t rhs_length)
  {
    int32_t *l = (int32_t *)lhs;
    int32_t *r = (int32_t *)rhs;
    uint32_t len = (lhs_length < rhs_length ? lhs_length : rhs_length);

    ups_assert(lhs);
    ups_assert(rhs);

    len /= 4;
    while (len > 0) {
      if (*l < *r)
        return -1;
      else if (*l > *r)
        return +1;
      len--;
      l++;
      r++;
    }
    if (lhs_length < rhs_length)
      return (-1);
    else if (rhs_length < lhs_length)
      return (+1);
    return (0);
  }

  void nearFindStressTest() {
    const int RECORD_COUNT_PER_DB = 50000;
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t ps[] = {
      { UPS_PARAM_PAGESIZE,  32 * 1024 }, /* UNIX == WIN now */
      { UPS_PARAM_CACHESIZE, 32 },
      { 0, 0 }
    };
    ups_parameter_t ps2[] = {
      { UPS_PARAM_KEY_SIZE, sizeof(my_key_t) },
      { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
      { 0, 0 }
    };

    ups_key_t key;
    ups_record_t rec;

    my_key_t my_key;
    my_rec_t my_rec;

    teardown();
    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"),
                            UPS_DISABLE_MMAP, 0644, ps));

    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, ps2));
    REQUIRE(0 == ups_db_set_compare_func(db, &my_compare_func_u32));

    /* insert the records: key=2*i; rec=100*i */
    int lower_bound_of_range = 0;
    int upper_bound_of_range = (RECORD_COUNT_PER_DB - 1) * 2;
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    int i;
    for (i = 0; i < RECORD_COUNT_PER_DB; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&my_key, 0, sizeof(my_key));
      ::memset(&my_rec, 0, sizeof(my_rec));

      my_rec.val1 = 100 * i; // record values thus are 50 * key values...
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = UPS_RECORD_USER_ALLOC;

      my_key.val1 = 2 * i;
      key.data = (void *)&my_key;
      key.size = sizeof(my_key);
      key.flags = UPS_KEY_USER_ALLOC;

      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

      /*
      if (i % 1000 == 999) {
        std::cerr << ".";
        if (i % 10000 == 9999 || i <= 10000) {
          std::cerr << "+";
        }
      }
      */
    }
    REQUIRE(0 == ups_cursor_close(cursor));

    // std::cerr << std::endl;

    REQUIRE(0 == ups_db_check_integrity(db, 0));

    my_rec_t *r;
    my_key_t *k;

    /* show record collection */
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < RECORD_COUNT_PER_DB; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE(key.data != (void *)0);
      REQUIRE(rec.data != (void *)0);
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(r->val1 == 100 * i);
      REQUIRE(k->val1 == 2 * i);
    }
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_close(cursor));

    REQUIRE(0 == ups_db_check_integrity(db, 0));

    /*
     * A)
     *
     * now the real thing starts: search for records which match and don't
     * exist, using the various modes.
     * Since we know the keys are all == 0 MOD 2, we know we'll have an EXACT
     * hit for every second entry when we search for keys == 0 MOD 3.
     *
     * B)
     *
     * After a round of that, we do it all over again, but now while we
     * delete
     * every key == 0 MOD 5 at the same time; that is: every second delete
     * should succeed, while it impacts our search hits as any records with
     * key == 0 MOD 10 will be gone by the time we check them out.
     *
     * C)
     *
     * The third round is the specialties corner, where we insert additional
     * records with key == 0 MOD 2 AT THE HIGH END, while searching for an
     * upper and lower non-existing odd key after each insert; at least one
     * of 'em should hit the firnge case of edge-of-page-block with the
     * match landing on the wrong side initially, requiring the internal
     * 'let's jump to the neighbouring block' code to work.
     *
     * D)
     *
     * When we get through that, we do the same at the BOTTOM side of the
     * spectrum.
     *
     * E)
     *
     * And the last part is a bit of random-access simulation, where
     * we search for keys == 0 MOD 3, while we know the state of affairs
     * in the store so we can predict exact match success/failure, but
     * added to this, we traverse a few records up and down from the match
     * using cursor_move() and check to ensure those are all in proper order.
     *
     * The random generator is a simple prime-modulo thingy, which uses a
     * large random number to ensure we're nicely jumping up & down
     * throughout the range.
     */

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (i = lower_bound_of_range / 2 - 7;
        i < upper_bound_of_range / 2 + 7; i++) {
      int looking_for = 3 * i;

      /* determine expected values now; then do all the searches and check 'em */
      bool eq_expect; // EQ key exists?
      int le_keyval; // LE key.
      bool le_expect;
      int lt_keyval; // LT key.
      bool lt_expect;
      int ge_keyval; // GE key.
      bool ge_expect;
      int gt_keyval; // GT key.
      bool gt_expect;

      eq_expect = !(looking_for % 2); // EQ key exists?
      eq_expect &= (looking_for >= lower_bound_of_range
              && looking_for <= upper_bound_of_range);

      le_keyval = looking_for - abs(looking_for % 2); // LE key.
      while (le_keyval > upper_bound_of_range)
        le_keyval -= 2;
      le_expect = (le_keyval >= lower_bound_of_range
              && le_keyval <= upper_bound_of_range);

      lt_keyval = (looking_for - 1) - (abs(looking_for - 1) % 2); // LT key.
      while (lt_keyval > upper_bound_of_range)
        lt_keyval -= 2;
      lt_expect = (lt_keyval >= lower_bound_of_range
              && lt_keyval <= upper_bound_of_range);

      ge_keyval = looking_for + abs(looking_for % 2); // GE key.
      while (ge_keyval < lower_bound_of_range)
        ge_keyval += 2;
      ge_expect = (ge_keyval >= lower_bound_of_range
              && ge_keyval <= upper_bound_of_range);

      gt_keyval = (looking_for + 1) + (abs(looking_for + 1) % 2); // GT key.
      while (gt_keyval < lower_bound_of_range)
        gt_keyval += 2;
      gt_expect = (gt_keyval >= lower_bound_of_range
        && gt_keyval <= upper_bound_of_range);

#define PREP()                      \
      ::memset(&key, 0, sizeof(key));       \
      ::memset(&rec, 0, sizeof(rec));       \
      ::memset(&my_key, 0, sizeof(my_key));     \
      ::memset(&my_rec, 0, sizeof(my_rec));     \
                            \
      my_key.val1 = looking_for;          \
      key.data = (void *)&my_key;         \
      key.size = sizeof(my_key);          \
      key.flags = UPS_KEY_USER_ALLOC;

      PREP();
      REQUIRE((eq_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, 0));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE((k ? k->val1 : 666) ==
            looking_for);
      REQUIRE((r ? r->val1 : 666) ==
            (eq_expect ? looking_for * 50 : 666));

      PREP();
      REQUIRE((lt_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, UPS_FIND_LT_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (lt_expect ? lt_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (lt_expect ? lt_keyval * 50 : 666));

      PREP();
      REQUIRE((gt_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, UPS_FIND_GT_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (gt_expect ? gt_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (gt_expect ? gt_keyval * 50 : 666));

      PREP();
      REQUIRE((le_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, UPS_FIND_LEQ_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (le_expect ? le_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (le_expect ? le_keyval * 50 : 666));

      PREP();
      REQUIRE((ge_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, UPS_FIND_GEQ_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (ge_expect ? ge_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==   
          (ge_expect ? ge_keyval * 50 : 666));

      PREP();
      bool mix_expect = (le_expect || ge_expect);
      REQUIRE((mix_expect ? 0 : UPS_KEY_NOT_FOUND) ==
          ups_cursor_find(cursor, &key, &rec, UPS_FIND_NEAR_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((((k ? k->val1 : 666) == le_keyval)
          || ((k ? k->val1 : 666) == (mix_expect ? ge_keyval : looking_for))));
      REQUIRE((((k ? k->val1 : 666) == le_keyval)
        ? ((r ? r->val1 : 666) == (mix_expect ? le_keyval * 50 : 666))
        : ((r ? r->val1 : 666) == (mix_expect ? ge_keyval * 50 : 666))));

      PREP();
      mix_expect = (lt_expect || gt_expect);
      REQUIRE((mix_expect ? 0 : UPS_KEY_NOT_FOUND) ==
                ups_cursor_find(cursor, &key, &rec,
                        (UPS_FIND_LT_MATCH | UPS_FIND_GT_MATCH)));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((((k ? k->val1 : 666) == lt_keyval)
          || ((k ? k->val1 : 666) == (mix_expect ? gt_keyval : looking_for))));
      REQUIRE((((k ? k->val1 : 666) == lt_keyval)
        ? ((r ? r->val1 : 666) == (mix_expect ? lt_keyval * 50 : 666))
        : ((r ? r->val1 : 666) == (mix_expect ? gt_keyval * 50 : 666))));

#undef PREP
    }
    REQUIRE(0 == ups_cursor_close(cursor));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void nearFindTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 64 * 1024 },  { 0, 0 } };
    ups_parameter_t params[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
        { 0, 0 }
    };
    const int MY_KEY_SIZE = 6554;
    struct my_key_t {
      uint32_t key_val1;
      uint32_t key_surplus[MY_KEY_SIZE/4];
    };
    struct my_rec_t {
      uint32_t rec_val1;
      char rec_val2[512];
    };

    uint32_t keycount = 0;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), 0, 0644, &ps[0]));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 1, 0, &params[0]));
    keycount = 8;
    REQUIRE(0 == ups_db_set_compare_func(db, &my_compare_func_u32));

    ups_key_t key;
    ups_record_t rec;
    const int vals[] = { 1, 7, 3, 2, 9, 55, 42, 660, 14, 11, 37, 99,
      123, 111, 459, 52, 66, 77, 88, 915, 31415, 12719 };

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    my_key_t my_key = {666};
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = UPS_KEY_USER_ALLOC;

    /* empty DB: LT/GT must turn up error */
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(db, 0, &key, &rec, UPS_FIND_EQ_MATCH));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(db, 0, &key, &rec, UPS_FIND_LEQ_MATCH));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(db, 0, &key, &rec, UPS_FIND_GEQ_MATCH));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(db, 0, &key, &rec, UPS_FIND_LT_MATCH));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_find(db, 0, &key, &rec, UPS_FIND_GT_MATCH));

    int fill = 0;
    my_rec_t my_rec = {1000, "hello world!"};
    rec.data = &my_rec;
    rec.size = sizeof(my_rec);
    rec.flags = UPS_RECORD_USER_ALLOC;

    my_key.key_val1 = vals[fill++];

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    /* one record in DB: LT/GT must turn up that one for the
     * right key values */
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_EQ_MATCH));
    REQUIRE(rec.data != key.data);
    my_rec_t *r = (my_rec_t *)rec.data;
    my_key_t *k = (my_key_t *)key.data;
    REQUIRE((unsigned)r->rec_val1 == (unsigned)1000);
    REQUIRE((unsigned)k->key_val1 == (uint32_t)vals[fill-1]);

    ::memset(&rec, 0, sizeof(rec));
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = UPS_KEY_USER_ALLOC;
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_NEAR_MATCH));
    REQUIRE(rec.data != key.data);
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (uint32_t)vals[fill - 1]);
    REQUIRE(ups_key_get_approximate_match_type(&key) == 0);

    ::memset(&rec, 0, sizeof(rec));
    my_key.key_val1 = vals[fill - 1] - 1;
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = UPS_KEY_USER_ALLOC;
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_NEAR_MATCH));
    REQUIRE(rec.data != key.data);
    REQUIRE(rec.data);
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (uint32_t)vals[fill - 1]);
    REQUIRE(ups_key_get_approximate_match_type(&key) == 1);

    ::memset(&rec, 0, sizeof(rec));
    my_key.key_val1 = vals[fill - 1] + 2;
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = UPS_KEY_USER_ALLOC;
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_NEAR_MATCH));
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (uint32_t)vals[fill - 1]);
    REQUIRE(ups_key_get_approximate_match_type(&key) == -1);

    key.data = (void *)&my_key;
    key.size = MY_KEY_SIZE;
    key.flags = UPS_KEY_USER_ALLOC;

    /* add two more records */
    unsigned int i;
    for (i = 0; i < 2; i++) {
      my_rec.rec_val1 = 2000 + i;
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = UPS_RECORD_USER_ALLOC;

      my_key.key_val1 = vals[fill++];
      key.data = (void *)&my_key;
      key.size = MY_KEY_SIZE;
      key.flags = UPS_KEY_USER_ALLOC;

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    /* show record collection */
    const int verify_vals1[] = { 1, 3, 7 };
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < 3; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(k->key_val1 == (uint32_t)verify_vals1[i]);
    }
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_close(cursor));


    /* three records in DB {1, 3, 7}: LT/GT should pick the
     * 'proper' one each time */
    struct search_res_t {
      int rv;
      int keyval;
      int sign;
      int recval;
    };
    struct search_cat_t {
      uint32_t mode;
      const struct search_res_t *cases;
      const char *descr;
    };
    int srch_vals1[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
    const search_res_t srch_res_any1[] = {
      { 0, 1, 1, 1000},
      { 0, 1, 0, 1000},
      { 0, 1, -1, 1000}, /* {2, ...} would've been OK too, but
                  we just happen to know the 'near'
                  internals... */
      { 0, 3, 0, 2001},
      { 0, 3, -1, 2001}, /* be reminded: this is NOT really 'nearest'
                  search, just a kind of 'next-door neighbour
                  search' ... with favorite neighbours ;-) */
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 7, 0, 2000},
      { 0, 7, -1, 2000}
    };
    const search_res_t srch_res_leq1[] = {
      { UPS_KEY_NOT_FOUND, 0, 0, 666},
      { 0, 1, 0, 1000},
      { 0, 1, -1, 1000},
      { 0, 3, 0, 2001},
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 7, 0, 2000},
      { 0, 7, -1, 2000}
    };
    const search_res_t srch_res_lt1[] = {
      { UPS_KEY_NOT_FOUND, 0, 0, 666},
      { UPS_KEY_NOT_FOUND, 1, 0, 666},
      { 0, 1, -1, 1000},
      { 0, 1, -1, 1000},
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 3, -1, 2001},
      { 0, 7, -1, 2000}
    };
    const search_res_t srch_res_geq1[] = {
      { 0, 1, 1, 1000},
      { 0, 1, 0, 1000},
      { 0, 3, 1, 2001},
      { 0, 3, 0, 2001},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 0, 2000},
      { UPS_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_res_t srch_res_gt1[] = {
      { 0, 1, 1, 1000},
      { 0, 3, 1, 2001},
      { 0, 3, 1, 2001},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { UPS_KEY_NOT_FOUND, 7, 0, 666},
      { UPS_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_res_t srch_res_eq1[] = {
      { UPS_KEY_NOT_FOUND, 0, 0, 666},
      { 0, 1, 0, 1000},
      { UPS_KEY_NOT_FOUND, 2, 0, 666},
      { 0, 3, 0, 2001},
      { UPS_KEY_NOT_FOUND, 4, 0, 666},
      { UPS_KEY_NOT_FOUND, 5, 0, 666},
      { UPS_KEY_NOT_FOUND, 6, 0, 666},
      { 0, 7, 0, 2000},
      { UPS_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_cat_t srch_cats[] = {
      { UPS_FIND_NEAR_MATCH, srch_res_any1, "UPS_FIND_NEAR_MATCH '~'" },
      { UPS_FIND_LEQ_MATCH, srch_res_leq1, "UPS_FIND_LEQ_MATCH '<='" },
      { UPS_FIND_LT_MATCH, srch_res_lt1, "UPS_FIND_LT_MATCH '<'" },
      { UPS_FIND_GEQ_MATCH, srch_res_geq1, "UPS_FIND_GEQ_MATCH '>='" },
      { UPS_FIND_GT_MATCH, srch_res_gt1, "UPS_FIND_GT_MATCH '>'" },
      { UPS_FIND_EQ_MATCH, srch_res_eq1, "UPS_FIND_EQ_MATCH '='" },
      { 0 /* = UPS_FIND_EQ_MATCH */, srch_res_eq1,
        "zero default (0) '='" },
    };
    unsigned int j;

    for (j = 1; j < sizeof(srch_cats) / sizeof(srch_cats[0]); j++) {
      const search_res_t *res = srch_cats[j].cases;

      for (i = 0; i < sizeof(srch_vals1) / sizeof(srch_vals1[0]); i++) {
        // announce which test case is checked now; just reporting
        // file+line+func isn't good enough here when things go pear
        // shaped for a specific case...
        //std::cout << "Test: category: " << srch_cats[j].descr
              //<< ", case: " << i << std::endl;

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        my_key.key_val1 = srch_vals1[i];
        key.data = (void *)&my_key;
        key.size = MY_KEY_SIZE;
        key.flags = UPS_KEY_USER_ALLOC;
        int rv = ups_db_find(db, 0, &key, &rec, srch_cats[j].mode);
        r = (my_rec_t *)rec.data;
        k = (my_key_t *)key.data;
        REQUIRE(rv == res[i].rv);
        REQUIRE((r ? r->rec_val1 : 666) == (uint32_t)res[i].recval);
        REQUIRE((k ? k->key_val1 : 666) == (uint32_t)res[i].keyval);
        REQUIRE(ups_key_get_approximate_match_type(&key) == res[i].sign);
      }
    }

    /* add more records: fill one page; then in the next
     * round overflow by one, and then on to three pages, etc. */
    for (i = 0; i < keycount - 3 + 1; i++) {
      my_rec.rec_val1 = 3000 + i;
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = UPS_RECORD_USER_ALLOC;

      my_key.key_val1 = vals[fill++];
      key.data = (void *)&my_key;
      key.size = MY_KEY_SIZE;
      key.flags = UPS_KEY_USER_ALLOC;

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    /* show record collection */
    const int verify_vals2[] = { 1, 2, 3, 7, 9, 14, 42, 55, 660 };
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < 9; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
          ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(k->key_val1 == (uint32_t)verify_vals2[i]);
    }
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_close(cursor));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertTest() {
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(0, 0, &key, &rec, 0));
    key.flags = 0x13;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, 0));
    key.flags = 0;
    rec.flags = 0x13;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, 0));
    rec.flags = 0;
    key.flags = UPS_KEY_USER_ALLOC;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    key.flags = 0;
    rec.flags = UPS_RECORD_USER_ALLOC;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    rec.flags = 0;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE|UPS_DUPLICATE));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, 0, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE_INSERT_BEFORE));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE_INSERT_AFTER));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE_INSERT_FIRST));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE_INSERT_LAST));
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
  }

  void insertDuplicateTest() {
    ups_db_t *db;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE|UPS_OVERWRITE));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 ==
        ups_env_create_db(m_env, &db, 2, UPS_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE | UPS_OVERWRITE));
    REQUIRE(0 ==
        ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_db_close(db, 0));
  }

  void negativeInsertBigKeyTest() {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[0xff] = {0};
    key.size = sizeof(buffer);
    key.data = buffer;

    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_SIZE, 10 },
        { 0, 0 }
    };
    ups_db_t *db;
    REQUIRE(0 == ups_env_create_db(m_env, &db, 13, 0, &p[0]));
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_db_close(db, 0));
  }

  void insertBigKeyTest() {
    ups_key_t key;
    ups_record_t rec;
    char buffer[0xffff];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(buffer, 0, sizeof(buffer));
    key.size = sizeof(buffer);
    key.data = buffer;

    teardown();
    REQUIRE(0 == ups_env_create(&m_env, "test.db", 0, 0644, 0));
    REQUIRE(0 ==
            ups_env_create_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    buffer[0]++;

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 ==
            ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_close(cursor));

    buffer[0]++;

    teardown();

    REQUIRE(0 == ups_env_open(&m_env, "test.db", 0, 0));
    REQUIRE(0 ==
            ups_env_open_db(m_env, &m_db, 1, 0, 0));
    teardown();
    REQUIRE(0 == ups_env_open(&m_env, "test.db", 0, 0));
    REQUIRE(0 ==
            ups_env_open_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
  }

  void eraseTest() {
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_erase(0, 0, &key, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_erase(m_db, 0, 0, 0));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_db_erase(m_db, 0, &key, 0));
  }

  void flushBackendTest() {
    ups_env_t *env1, *env2;
    ups_db_t *db1, *db2;

    ups_key_t key;
    ups_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = &value;
    key.size = sizeof(value);

    REQUIRE(0 ==
        ups_env_create(&env1, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env1, &db1, 111, 0, 0));
    REQUIRE(0 == ups_db_insert(db1, 0, &key, &rec, 0));
    REQUIRE(0 == ups_env_flush(env1, 0));

    /* Exclusive locking is now the default */
    REQUIRE(UPS_WOULD_BLOCK ==
        ups_env_open(&env2, Utils::opath(".test"), 0, 0));
    REQUIRE(UPS_WOULD_BLOCK ==
        ups_env_open(&env2, Utils::opath(".test"), 0, 0));
    REQUIRE(0 == ups_env_close(env1, UPS_AUTO_CLEANUP));
    REQUIRE(0 ==
        ups_env_open(&env2, Utils::opath(".test"), UPS_READ_ONLY, 0));
    REQUIRE(0 == ups_env_open_db(env2, &db2, 111, 0, 0));
    REQUIRE(0 == ups_db_find(db2, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_close(db2, 0));
    REQUIRE(0 == ups_env_close(env2, 0));
  }

  void closeTest() {
    REQUIRE(UPS_INV_PARAMETER == ups_db_close(0, 0));
  }

  void closeWithCursorsTest() {
    ups_cursor_t *c[5];

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], m_db, 0, 0));

    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_db_close(m_db, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_close(c[i]));
    REQUIRE(0 == ups_db_close(m_db, 0));
  }

  void closeWithCursorsAutoCleanupTest() {
    ups_cursor_t *c[5];

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], m_db, 0, 0));

    REQUIRE(0 == ups_db_close(m_db, UPS_AUTO_CLEANUP));
  }

  void compareTest() {
    ups_compare_func_t f = my_compare_func;

    ups_parameter_t params[] = {
      { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
      { 0, 0 }
    };
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(UPS_INV_PARAMETER == ups_db_set_compare_func(m_db, f));

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 111, 0, &params[0]));

    REQUIRE(0 == ups_db_set_compare_func(db, f));
    REQUIRE(f == ((LocalDatabase *)db)->compare_func());

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorCreateTest() {
    ups_cursor_t *cursor;

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_create(&cursor, 0, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_create(0, m_db, 0, 0));
  }

  void cursorCloneTest() {
    ups_cursor_t src, *dest;

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_clone(0, &dest));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_clone(&src, 0));
  }

  void cursorMoveTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(0, 0, 0, 0));
    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_move(cursor, &key, 0, 0));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, 0, UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, 0, UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, 0, UPS_CURSOR_PREVIOUS));

    ups_cursor_close(cursor);
  }

  void cursorReplaceTest() {
    ups_cursor_t *cursor;
    ups_record_t *record=0;

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_overwrite(0, record, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_overwrite(cursor, 0, 0));

    ups_cursor_close(cursor);
  }

  void cursorFindTest() {
    ups_cursor_t *cursor;
    ups_key_t key = {0};

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(0, &key, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, 0, 0, 0));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_find(cursor, &key, 0, 0));

    ups_cursor_close(cursor);
  }

  void cursorInsertTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(0, &key, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(cursor, 0, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(cursor, &key, 0, 0));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void cursorEraseTest() {
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_erase(0, 0));
  }

  void cursorCloseTest() {
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_close(0));
  }

  void cursorGetErasedItemTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = &value;
    key.size = sizeof(value);

    value = 1;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    value = 2;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    value = 1;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));
    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_move(cursor, &key, 0, 0));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void replaceKeyTest() {
    /* in-memory */
    ups_key_t key;
    ups_record_t rec;
    char buffer1[32], buffer2[7];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(buffer1, 0, sizeof(buffer1));
    ::memset(buffer2, 0, sizeof(buffer2));
    rec.size = sizeof(buffer1);
    rec.data = buffer1;

    /* insert a big blob */
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)sizeof(buffer1) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer1, sizeof(buffer1)));

    /* replace with a tiny blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer2);
    rec.data = buffer2;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)sizeof(buffer2) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer2, sizeof(buffer2)));

    /* replace with a big blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer1);
    rec.data = buffer1;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)sizeof(buffer1) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer1, sizeof(buffer1)));

    /* replace with a NULL blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 0;
    rec.data = 0;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)0 == rec.size);
    REQUIRE((void *)0 == rec.data);

    /* replace with a tiny blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer2);
    rec.data = buffer2;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)sizeof(buffer2) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer2, sizeof(buffer2)));

    /* replace with a NULL blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 0;
    rec.data = 0;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)0 == rec.size);
    REQUIRE(rec.data == (void *)0);
  }

  void callocTest() {
    char *p = Memory::callocate<char>(20);

    for (int i = 0; i < 20; i++)
      REQUIRE('\0' == p[i]);

    Memory::release(p);
  }

  void strerrorTest() {
    for (int i = -300; i <= 0; i++) {
      REQUIRE(ups_strerror((ups_status_t)i));
    }
    REQUIRE(0 == strcmp("Unknown error",
          ups_strerror((ups_status_t)-204)));
    REQUIRE(0 == strcmp("Unknown error",
          ups_strerror((ups_status_t)-35)));
    REQUIRE(0 == strcmp("Unknown error",
          ups_strerror((ups_status_t)1)));
  }

  void contextDataTest() {
    void *ptr = (void *)0x13;
    ups_set_context_data(0, 0);
    ups_set_context_data(m_db, ptr);
    REQUIRE((void *)0 == ups_get_context_data(0, 0));
    REQUIRE((void *)0x13 == ups_get_context_data(m_db, 0));
    ups_set_context_data(m_db, 0);
    REQUIRE((void *)0 == ups_get_context_data(m_db, 0));
  }

  void recoveryTest() {
    teardown();

    ups_env_t *env;
    ups_db_t *db;
    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"),
                    UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    ups_key_t key = {};
    ups_record_t rec = {};
    for (unsigned i = 0; i < 10; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"),
                    UPS_AUTO_RECOVERY, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

    for (unsigned i = 0; i < 10; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    }

    for (unsigned i = 0; i < 10; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    }

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void recoveryEnvTest() {
    ups_env_t *env;
    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"),
                UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void insertAppendTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    for (unsigned i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    }
    for (unsigned i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
  }

  void insertPrependTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    for (int i = 100; i >= 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    }
    for (int i = 100; i >= 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
  }

  void cursorInsertAppendTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 0; i < 10000; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    }
    for (unsigned i = 0; i < 10000; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void negativeCursorInsertAppendTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 10; i > 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    }
    for (unsigned i = 1; i <= 10; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void recordCountTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    uint64_t count;
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 1024 * 4 }, { 0, 0 } };
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    teardown();
    REQUIRE(0 ==
            ups_env_create(&m_env, Utils::opath(".test"), 0, 0664, ps));
    REQUIRE(0 ==
            ups_env_create_db(m_env, &m_db, 1, UPS_ENABLE_DUPLICATE_KEYS, 0));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 4000; i > 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    }

    REQUIRE(0 == ups_cursor_close(cursor));

    for (unsigned i = 1; i <= 10; i++) {
      unsigned k = 5;
      key.size = sizeof(k);
      key.data = (void *)&k;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_DUPLICATE));
    }

    REQUIRE(0 == ups_db_check_integrity(m_db, 0));

    count = 0;
    REQUIRE(0 ==
        ups_db_count(m_db, 0, UPS_SKIP_DUPLICATES, &count));
    REQUIRE((unsigned)4000 == count);

    REQUIRE(0 == ups_db_check_integrity(m_db, 0));

    REQUIRE(0 ==
        ups_db_count(m_db, 0, 0, &count));
    REQUIRE((unsigned)(4000 + 10) == count);
  }

  void createDbOpenEnvTest() {
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 22, 0, 0));
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));

    REQUIRE(0 ==
        ups_env_open(&m_env, Utils::opath(".test"), 0, 0));
    REQUIRE(0 == ups_env_open_db(m_env, &m_db, 22, 0, 0));
  }

  void checkDatabaseNameTest() {
    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
    teardown();

    REQUIRE(0 ==
        ups_env_open(&m_env, Utils::opath(".test"), 0, 0));
    REQUIRE(0 ==
        ups_env_open_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ups_db_close(m_db, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open_db(m_env, &m_db, 0xff00, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_open_db(m_env, &m_db, 0xf000, 0, 0));
  }

  void hintingTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    /* UPS_HINT_APPEND is *only* allowed in
     * ups_cursor_insert; not allowed in combination with
     * UPS_HINT_PREPEND */
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec,
          UPS_HINT_PREPEND));

    REQUIRE(0 ==
        ups_cursor_insert(cursor, &key, &rec,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(cursor, &key, &rec,
          UPS_HINT_APPEND|UPS_HINT_PREPEND));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_erase(m_db, 0, &key,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_erase(m_db, 0, &key,
          UPS_HINT_PREPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_erase(cursor,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_erase(cursor,
          UPS_HINT_PREPEND));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, 0,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, 0,
          UPS_HINT_PREPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, &rec,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, &rec,
          UPS_HINT_PREPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_HINT_APPEND));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_HINT_PREPEND));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void directAccessTest() {
    ups_cursor_t *cursor;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 6;
    rec.data = (void *)"hello";

    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_find(cursor, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(cursor, &key, &rec, UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void smallDirectAccessTest() {
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    /* test with an empty record */
    rec.size = 0;
    rec.data = (void *)"";
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)0 == rec.size);

    /* test with a tiny record (<8)*/
    rec.size = 4;
    rec.data = (void *)"hel";
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)4 == rec.size);
    REQUIRE(0 == strcmp("hel", (char *)rec.data));
    ((char *)rec.data)[0] = 'b';
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)4 == rec.size);
    REQUIRE(0 == strcmp("bel", (char *)rec.data));

    /* test with a small record (8)*/
    rec.size = 8;
    rec.data = (void *)"hello wo";
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)8 == rec.size);
    REQUIRE(0 == strcmp("hello wo", (char *)rec.data));
    ((char *)rec.data)[0] = 'b';
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE((unsigned)8 == rec.size);
    REQUIRE(0 == strcmp("bello wo", (char *)rec.data));
  }

  void negativeDirectAccessTest() {
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};
    rec.size = 6;
    rec.data = (void *)"hello";

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec, UPS_DIRECT_ACCESS));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, &rec, UPS_DIRECT_ACCESS));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(cursor, &key, &rec, UPS_DIRECT_ACCESS));

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));

    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"),
            UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(cursor, &key, &rec,
          UPS_DIRECT_ACCESS));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(cursor, &key, &rec,
          UPS_DIRECT_ACCESS));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void unlimitedCacheTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 6;
    rec.data = (void *)"hello";

    REQUIRE(0 == ups_env_create(&env, ".test.db", UPS_CACHE_UNLIMITED, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    REQUIRE(0 == ups_env_open(&env, ".test.db", UPS_CACHE_UNLIMITED, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void overwriteLogDirectoryTest() {
    ups_env_t *env;
    ups_parameter_t ps[] = {
        { UPS_PARAM_LOG_DIRECTORY, (uint64_t)"data" },
        { 0, 0 }
    };

    os::unlink("data/test.db.log0");
    os::unlink("data/test.db.jrn0");
    os::unlink("data/test.db.jrn1");
    REQUIRE(false == os::file_exists("data/test.db.jrn0"));
    REQUIRE(false == os::file_exists("data/test.db.jrn1"));

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath("test.db"),
            UPS_ENABLE_TRANSACTIONS, 0, &ps[0]));
    REQUIRE(0 == ups_env_close(env, 0));
    REQUIRE(true == os::file_exists("data/test.db.jrn0"));
    REQUIRE(true == os::file_exists("data/test.db.jrn1"));

    REQUIRE(0 ==
        ups_env_open(&env, Utils::opath("test.db"),
            UPS_ENABLE_TRANSACTIONS, &ps[0]));

    REQUIRE(0 == ups_env_get_parameters(env, &ps[0]));
    REQUIRE(0 == strcmp("data", (const char *)ps[0].value));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void persistentFlagsTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_parameter_t ps[] = {
        { UPS_PARAM_KEY_SIZE, 7 },
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
        { UPS_PARAM_CUSTOM_COMPARE_NAME, reinterpret_cast<uint64_t>("mycmp") },
        { UPS_PARAM_RECORD_SIZE, 22 },
        { 0, 0 }
    };
    uint32_t flags = UPS_ENABLE_DUPLICATE_KEYS;

    REQUIRE(0 == ups_register_compare("mycmp", custom_compare_func));

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, flags, &ps[0]));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // reopen the database
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

    // check if the flags and parameters were stored persistently
    LocalDatabase *ldb = (LocalDatabase *)db;
    REQUIRE((ldb->get_flags() & flags) == flags);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string s = ldb->btree_index()->test_get_classname();
    REQUIRE(s == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::PaxLayout::BinaryKeyList, upscaledb::DefLayout::DuplicateInlineRecordList>, upscaledb::CallbackCompare>");
#endif

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE((uint64_t)UPS_TYPE_CUSTOM == query[0].value);
    REQUIRE(22u == (unsigned)query[1].value);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    // Variable size keys are not allowed
    ups_record_t rec = {0};
    ups_key_t key = {0};
    key.data = (void *)"12345678";
    key.size = 4;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    rec.size = 22;
    rec.data = (void *)"1234567890123456789012";
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    key.size = 7;
    rec.size = 12;
    REQUIRE(UPS_INV_RECORD_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    rec.size = 22;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void persistentRecordTypeTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_parameter_t ps[] = {
        { UPS_PARAM_RECORD_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 22 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &ps[0]));

    ps[0].value = 0;
    ps[1].value = 0;
    REQUIRE(0 == ups_db_get_parameters(db, ps));
    REQUIRE(UPS_TYPE_UINT32 == (unsigned)ps[0].value);
    REQUIRE(4u == (unsigned)ps[1].value);

    // reopen the database
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

    ps[0].value = 0;
    ps[1].value = 0;
    REQUIRE(0 == ups_db_get_parameters(db, ps));
    REQUIRE(UPS_TYPE_UINT32 == (unsigned)ps[0].value);
    REQUIRE(4u == (unsigned)ps[1].value);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    // Variable size records are not allowed
    ups_record_t rec = {0};
    ups_key_t key = {0};
    rec.data = (void *)"12345678";
    rec.size = 8;
    REQUIRE(UPS_INV_RECORD_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    rec.size = 4;
    rec.data = (void *)&rec.size;
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void invalidKeySizeTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t ps[] = {
        { UPS_PARAM_KEY_SIZE, 0xffff + 1 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_env_create_db(env, &db, 1, 0, &ps[0]));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void recreateInMemoryDatabaseTest() {
    ups_db_t *db;
    ups_env_t *env;

    // create in-memory environment
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                            UPS_IN_MEMORY, 0, 0));
    // create a database (id = 1)
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    // close the database
    REQUIRE(0 == ups_db_close(db, 0));
    // re-create the database (id = 1)
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void disableRecoveryTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_txn_t *txn;

    os::unlink("test.db.jrn0");
    os::unlink("test.db.jrn1");

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        UPS_ENABLE_TRANSACTIONS | UPS_DISABLE_RECOVERY, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    REQUIRE(false == os::file_exists("test.db.jrn0"));
    REQUIRE(false == os::file_exists("test.db.jrn1"));

    // insert a key
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(false == os::file_exists("test.db.jrn0"));
    REQUIRE(false == os::file_exists("test.db.jrn1"));

    // close the database
    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    REQUIRE(false == os::file_exists("test.db.jrn0"));
    REQUIRE(false == os::file_exists("test.db.jrn1"));
  }

  void fileSizeLimitInMemoryTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t params[] = {
        {UPS_PARAM_FILE_SIZE_LIMIT, 3 * 16 * 1024},
        {0, 0}
    };

    REQUIRE(0 == ups_env_create(&env, 0, UPS_IN_MEMORY, 0, &params[0]));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[32] = {0};
    key.data = &buffer[0];
    key.size = sizeof(buffer);

    while (true) {
      *(int *)&buffer[0] += 1; // make key unique
      ups_status_t st = ups_db_insert(db, 0, &key, &rec, 0);
      if (st == UPS_LIMITS_REACHED)
        break;
      REQUIRE(st == UPS_SUCCESS);
    }

    // check integrity
    REQUIRE(0 == ups_db_check_integrity(db, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void fileSizeLimitSplitTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t params[] = {
        {UPS_PARAM_FILE_SIZE_LIMIT, 3 * 16 * 1024}, // 3 pages
        {0, 0}
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        0, 0, &params[0]));

    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[32] = {0};
    key.data = &buffer[0];
    key.size = sizeof(buffer);

    while (true) {
      *(int *)&buffer[0] += 1; // make key unique
      ups_status_t st = ups_db_insert(db, 0, &key, &rec, 0);
      if (st == UPS_LIMITS_REACHED)
        break;
      REQUIRE(st == UPS_SUCCESS);
    }

    // check integrity
    REQUIRE(0 == ups_db_check_integrity(db, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // verify the file size
    File f;
    f.open(Utils::opath("test.db"), 0);
    REQUIRE(f.get_file_size() == (size_t)3 * 16 * 1024);
  }

  void fileSizeLimitBlobTest(bool inmemory) {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t params[] = {
        {UPS_PARAM_FILE_SIZE_LIMIT, 2 * 16 * 1024}, // 2 pages
        {0, 0}
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        inmemory ? UPS_IN_MEMORY : 0, 0, &params[0]));

    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};

    // first insert must succeed
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    // second one fails
    key.data = (void *)"1";
    key.size = 1;
    rec.data = ::calloc(1024, 1);
    rec.size = 1024;
    REQUIRE(UPS_LIMITS_REACHED == ups_db_insert(db, 0, &key, &rec, 0));
    ::free(rec.data);

    // now check the integrity
    REQUIRE(0 == ups_db_check_integrity(db, 0));

    // only one key must be installed!
    uint64_t keycount = 0;
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(keycount == 1u);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // verify the file size
    if (!inmemory) {
      File f;
      f.open(Utils::opath("test.db"), 0);
      REQUIRE(f.get_file_size() == (size_t)2 * 16 * 1024);
    }
  }

  void posixFadviseTest() {
    ups_env_t *env;
    ups_parameter_t pin[] = {
        {UPS_PARAM_POSIX_FADVISE, UPS_POSIX_FADVICE_RANDOM},
        {0, 0}
    };
    ups_parameter_t pout[] = {
        {UPS_PARAM_POSIX_FADVISE, 0},
        {0, 0}
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                            0, 0, &pin[0]));
    REQUIRE(0 == ups_env_get_parameters(env, &pout[0]));
    REQUIRE(UPS_POSIX_FADVICE_RANDOM == pout[0].value);
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // open, make sure the property was not persisted
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_get_parameters(env, &pout[0]));
    REQUIRE(UPS_POSIX_FADVICE_NORMAL == pout[0].value);
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // open with flag
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, &pin[0]));
    REQUIRE(0 == ups_env_get_parameters(env, &pout[0]));
    REQUIRE(UPS_POSIX_FADVICE_RANDOM == pout[0].value);
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  // Open an existing environment and use the ErrorInducer for a failure in
  // mmap. Make sure that the fallback to read() works
  void issue55Test() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    for (int i = 0; i < 100; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t rec = ups_make_record(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    ErrorInducer::activate(true);
    ErrorInducer::get_instance()->add(ErrorInducer::kFileMmap, 1);

    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));

    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    for (int i = 0; i < 100; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t rec = {0};
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    }
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    ErrorInducer::activate(false);
  }

  // create a database with CUSTOM type and callback function, then recover
  void issue64Test() {
#ifndef WIN32
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t params[] = {
        { UPS_PARAM_KEY_SIZE, 7 },
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM },
        { UPS_PARAM_CUSTOM_COMPARE_NAME, reinterpret_cast<uint64_t>("cmp64") },
        { 0, 0 }
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        UPS_ENABLE_TRANSACTIONS | UPS_DONT_FLUSH_TRANSACTIONS,
                        0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &params[0]));
    REQUIRE(0 == ups_db_set_compare_func(db, custom_compare_func));

    // insert a key and commit the transaction
    ups_txn_t *txn;
    ups_key_t key1 = ups_make_key((void *)"hello1", 7);
    ups_key_t key2 = ups_make_key((void *)"hello2", 7);
    ups_record_t rec = ups_make_record((void *)"world", 6);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key1, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key2, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Utils::opath("test.db.jrn0"),
          Utils::opath("test.db.bak0")));
    REQUIRE(true == os::copy(Utils::opath("test.db.jrn1"),
          Utils::opath("test.db.bak1")));
    REQUIRE(true == os::copy(Utils::opath("test.db"),
          Utils::opath("test.db.bak")));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG));

    /* restore the backup files */
    REQUIRE(true == os::copy(Utils::opath("test.db.bak0"),
          Utils::opath("test.db.jrn0")));
    REQUIRE(true == os::copy(Utils::opath("test.db.bak1"),
          Utils::opath("test.db.jrn1")));
    REQUIRE(true == os::copy(Utils::opath("test.db.bak"),
          Utils::opath("test.db")));

    REQUIRE(UPS_NOT_READY == ups_env_open(&env, Utils::opath("test.db"),
                        UPS_AUTO_RECOVERY, 0));
    REQUIRE(0 == ups_register_compare("cmp64", custom_compare_func));
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                        UPS_AUTO_RECOVERY, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
#endif
  }

  void issue66Test() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        UPS_ENABLE_TRANSACTIONS, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    // two transactions: the older one remains active, the newer one will
    // be committed (but not flushed)
    ups_txn_t *txn1;
    ups_txn_t *txn2;
    ups_key_t key1 = ups_make_key((void *)"hello1", 7);
    ups_key_t key2 = ups_make_key((void *)"hello2", 7);
    ups_record_t rec = ups_make_record((void *)"world", 6);
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key1, &rec, 0));

    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn2, &key2, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));

    // now close the database
    REQUIRE(UPS_TXN_STILL_OPEN == ups_db_close(db, 0));

    // and the Environment
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_ABORT));
  }

  void issue47Test() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                        UPS_ENABLE_TRANSACTIONS, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    ups_txn_t *txn;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));

    ups_key_t key1 = ups_make_key((void *)"hello1", 7);
    ups_key_t key2 = ups_make_key((void *)"hello2", 7);
    ups_record_t rec = ups_make_record((void *)"world", 6);
    REQUIRE(0 == ups_db_insert(db, txn, &key1, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key2, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));

    ups_key_t key = {0};
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST));

    REQUIRE(0 == ups_env_flush(env, 0));

    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_ABORT));
  }
};

TEST_CASE("Upscaledb/versionTest", "")
{
  UpscaledbFixture f;
  f.versionTest();
}

TEST_CASE("Upscaledb/openTest", "")
{
  UpscaledbFixture f;
  f.openTest();
}

TEST_CASE("Upscaledb/getEnvTest", "")
{
  UpscaledbFixture f;
  f.getEnvTest();
}

TEST_CASE("Upscaledb/invHeaderTest", "")
{
  UpscaledbFixture f;
  f.invHeaderTest();
}

TEST_CASE("Upscaledb/createTest", "")
{
  UpscaledbFixture f;
  f.createTest();
}

TEST_CASE("Upscaledb/createPagesizeTest", "")
{
  UpscaledbFixture f;
  f.createPagesizeTest();
}

TEST_CASE("Upscaledb/createCloseCreateTest", "")
{
  UpscaledbFixture f;
  f.createCloseCreateTest();
}

TEST_CASE("Upscaledb/createPagesizeReopenTest", "")
{
  UpscaledbFixture f;
  f.createPagesizeReopenTest();
}

TEST_CASE("Upscaledb/readOnlyTest", "")
{
  UpscaledbFixture f;
  f.readOnlyTest();
}

TEST_CASE("Upscaledb/invalidPagesizeTest", "")
{
  UpscaledbFixture f;
  f.invalidPagesizeTest();
}

TEST_CASE("Upscaledb/invalidKeysizeTest", "")
{
  UpscaledbFixture f;
  f.invalidKeysizeTest();
}

TEST_CASE("Upscaledb/setCompareTest", "")
{
  UpscaledbFixture f;
  f.setCompareTest();
}

TEST_CASE("Upscaledb/findTest", "")
{
  UpscaledbFixture f;
  f.findTest();
}

TEST_CASE("Upscaledb/findEmptyRecordTest", "")
{
  UpscaledbFixture f;
  f.findEmptyRecordTest();
}

TEST_CASE("Upscaledb/nearFindTest", "")
{
  UpscaledbFixture f;
  f.nearFindTest();
}

TEST_CASE("Upscaledb/nearFindStressTest", "")
{
  UpscaledbFixture f;
  f.nearFindStressTest();
}

TEST_CASE("Upscaledb/insertTest", "")
{
  UpscaledbFixture f;
  f.insertTest();
}

TEST_CASE("Upscaledb/negativeInsertBigKeyTest", "")
{
  UpscaledbFixture f;
  f.negativeInsertBigKeyTest();
}

TEST_CASE("Upscaledb/insertBigKeyTest", "")
{
  UpscaledbFixture f;
  f.insertBigKeyTest();
}

TEST_CASE("Upscaledb/eraseTest", "")
{
  UpscaledbFixture f;
  f.eraseTest();
}

TEST_CASE("Upscaledb/flushBackendTest", "")
{
  UpscaledbFixture f;
  f.flushBackendTest();
}

TEST_CASE("Upscaledb/closeTest", "")
{
  UpscaledbFixture f;
  f.closeTest();
}

TEST_CASE("Upscaledb/closeWithCursorsTest", "")
{
  UpscaledbFixture f;
  f.closeWithCursorsTest();
}

TEST_CASE("Upscaledb/closeWithCursorsAutoCleanupTest", "")
{
  UpscaledbFixture f;
  f.closeWithCursorsAutoCleanupTest();
}

TEST_CASE("Upscaledb/compareTest", "")
{
  UpscaledbFixture f;
  f.compareTest();
}

TEST_CASE("Upscaledb/cursorCreateTest", "")
{
  UpscaledbFixture f;
  f.cursorCreateTest();
}

TEST_CASE("Upscaledb/cursorCloneTest", "")
{
  UpscaledbFixture f;
  f.cursorCloneTest();
}

TEST_CASE("Upscaledb/cursorMoveTest", "")
{
  UpscaledbFixture f;
  f.cursorMoveTest();
}

TEST_CASE("Upscaledb/cursorReplaceTest", "")
{
  UpscaledbFixture f;
  f.cursorReplaceTest();
}

TEST_CASE("Upscaledb/cursorFindTest", "")
{
  UpscaledbFixture f;
  f.cursorFindTest();
}

TEST_CASE("Upscaledb/cursorInsertTest", "")
{
  UpscaledbFixture f;
  f.cursorInsertTest();
}

TEST_CASE("Upscaledb/cursorEraseTest", "")
{
  UpscaledbFixture f;
  f.cursorEraseTest();
}

TEST_CASE("Upscaledb/cursorCloseTest", "")
{
  UpscaledbFixture f;
  f.cursorCloseTest();
}

TEST_CASE("Upscaledb/cursorGetErasedItemTest", "")
{
  UpscaledbFixture f;
  f.cursorGetErasedItemTest();
}

TEST_CASE("Upscaledb/replaceKeyTest", "")
{
  UpscaledbFixture f;
  f.replaceKeyTest();
}

TEST_CASE("Upscaledb/callocTest", "")
{
  UpscaledbFixture f;
  f.callocTest();
}

TEST_CASE("Upscaledb/strerrorTest", "")
{
  UpscaledbFixture f;
  f.strerrorTest();
}

TEST_CASE("Upscaledb/contextDataTest", "")
{
  UpscaledbFixture f;
  f.contextDataTest();
}

TEST_CASE("Upscaledb/recoveryTest", "")
{
  UpscaledbFixture f;
  f.recoveryTest();
}

TEST_CASE("Upscaledb/recoveryEnvTest", "")
{
  UpscaledbFixture f;
  f.recoveryEnvTest();
}

TEST_CASE("Upscaledb/insertAppendTest", "")
{
  UpscaledbFixture f;
  f.insertAppendTest();
}

TEST_CASE("Upscaledb/insertPrependTest", "")
{
  UpscaledbFixture f;
  f.insertPrependTest();
}

TEST_CASE("Upscaledb/cursorInsertAppendTest", "")
{
  UpscaledbFixture f;
  f.cursorInsertAppendTest();
}

TEST_CASE("Upscaledb/negativeCursorInsertAppendTest", "")
{
  UpscaledbFixture f;
  f.negativeCursorInsertAppendTest();
}

TEST_CASE("Upscaledb/recordCountTest", "")
{
  UpscaledbFixture f;
  f.recordCountTest();
}

TEST_CASE("Upscaledb/createDbOpenEnvTest", "")
{
  UpscaledbFixture f;
  f.createDbOpenEnvTest();
}

TEST_CASE("Upscaledb/checkDatabaseNameTest", "")
{
  UpscaledbFixture f;
  f.checkDatabaseNameTest();
}

TEST_CASE("Upscaledb/hintingTest", "")
{
  UpscaledbFixture f;
  f.hintingTest();
}

TEST_CASE("Upscaledb/directAccessTest", "")
{
  UpscaledbFixture f;
  f.directAccessTest();
}

TEST_CASE("Upscaledb/smallDirectAccessTest", "")
{
  UpscaledbFixture f;
  f.smallDirectAccessTest();
}

TEST_CASE("Upscaledb/negativeDirectAccessTest", "")
{
  UpscaledbFixture f;
  f.negativeDirectAccessTest();
}

TEST_CASE("Upscaledb/unlimitedCacheTest", "")
{
  UpscaledbFixture f;
  f.unlimitedCacheTest();
}

TEST_CASE("Upscaledb/overwriteLogDirectoryTest", "")
{
  UpscaledbFixture f;
  f.overwriteLogDirectoryTest();
}

TEST_CASE("Upscaledb/persistentFlagsTest", "")
{
  UpscaledbFixture f;
  f.persistentFlagsTest();
}

TEST_CASE("Upscaledb/persistentRecordTypeTest", "")
{
  UpscaledbFixture f;
  f.persistentRecordTypeTest();
}

TEST_CASE("Upscaledb/invalidKeySizeTest", "")
{
  UpscaledbFixture f;
  f.invalidKeySizeTest();
}

TEST_CASE("Upscaledb/recreateInMemoryDatabaseTest", "")
{
  UpscaledbFixture f;
  f.recreateInMemoryDatabaseTest();
}

TEST_CASE("Upscaledb/disableRecoveryTest", "")
{
  UpscaledbFixture f;
  f.disableRecoveryTest();
}

TEST_CASE("Upscaledb/fileSizeLimitInMemoryTest", "")
{
  UpscaledbFixture f;
  f.fileSizeLimitInMemoryTest();
}

TEST_CASE("Upscaledb/fileSizeLimitSplitTest", "")
{
  UpscaledbFixture f;
  f.fileSizeLimitSplitTest();
}

TEST_CASE("Upscaledb/fileSizeLimitBlobTest", "")
{
  UpscaledbFixture f;
  f.fileSizeLimitBlobTest(false);
}

TEST_CASE("Upscaledb/fileSizeLimitBlobInMemoryTest", "")
{
  UpscaledbFixture f;
  f.fileSizeLimitBlobTest(true);
}

TEST_CASE("Upscaledb/posixFadviseTest", "")
{
  UpscaledbFixture f;
  f.posixFadviseTest();
}

TEST_CASE("Upscaledb/issue55Test", "")
{
  UpscaledbFixture f;
  f.issue55Test();
}

TEST_CASE("Upscaledb/issue64Test", "")
{
  UpscaledbFixture f;
  f.issue64Test();
}

TEST_CASE("Upscaledb/issue66Test", "")
{
  UpscaledbFixture f;
  f.issue66Test();
}

TEST_CASE("Upscaledb/issue47Test", "")
{
  UpscaledbFixture f;
  f.issue47Test();
}

} // namespace upscaledb
