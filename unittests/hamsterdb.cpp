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

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/version.h"
#include "../src/serial.h"
#include "../src/btree_index.h"
#include "../src/env.h"
#include "../src/page.h"
#include "../src/cursor.h"
#include "../src/cache.h"

namespace hamsterdb {

static int HAM_CALLCONV
my_compare_func(ham_db_t *db,
      const ham_u8_t *lhs, ham_size_t lhs_length,
      const ham_u8_t *rhs, ham_size_t rhs_length) {
  (void)lhs;
  (void)rhs;
  (void)lhs_length;
  (void)rhs_length;
  return (0);
}

struct HamsterdbFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  HamsterdbFixture() {
    os::unlink(Globals::opath(".test"));
    REQUIRE(0 == ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~HamsterdbFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    m_env = 0;
    m_db = 0;
  }

  void versionTest() {
    ham_u32_t major, minor, revision;

    ham_get_version(0, 0, 0);
    ham_get_version(&major, &minor, &revision);

    REQUIRE((ham_u32_t)HAM_VERSION_MAJ == major);
    REQUIRE((ham_u32_t)HAM_VERSION_MIN == minor);
    REQUIRE((ham_u32_t)HAM_VERSION_REV == revision);
  };

  void licenseTest() {
    const char *licensee = 0, *product = 0;

    ham_get_license(0, 0);
    ham_get_license(&licensee, &product);

    REQUIRE(0 == strcmp(HAM_LICENSEE, licensee));
    REQUIRE(0 == strcmp(HAM_PRODUCT_NAME, product));
  };

  void openTest() {
    ham_env_t *env;
    ham_parameter_t params[] = {
      { 0x1234567, 0 },
      { 0, 0 }
    };

    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(0, "test.db", 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, 0, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, 0, HAM_IN_MEMORY, 0));
    REQUIRE(HAM_FILE_NOT_FOUND ==
        ham_env_open(&env, "xxxx...", 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, "test.db", HAM_IN_MEMORY, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, "test.db", HAM_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, "test.db", HAM_ENABLE_DUPLICATE_KEYS, params));

#ifdef WIN32
    REQUIRE(HAM_IO_ERROR ==
        ham_env_open(&env, "c:\\windows", 0, 0));
#else
    REQUIRE(HAM_IO_ERROR ==
        ham_env_open(&env, "/usr", 0, 0));
#endif
  }

  void getEnvTest() {
    // m_db is already initialized
    REQUIRE(ham_db_get_env(m_db));
  }

  void invHeaderTest() {
    ham_env_t *env;

    REQUIRE(HAM_INV_FILE_HEADER ==
        ham_env_open(&env, Globals::ipath("data/inv-file-header.hdb"), 0, 0));
  }

  void invVersionTest() {
    ham_env_t *env;

    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&env, Globals::ipath("data/inv-file-version.hdb"), 0, 0));
  }

  void createTest() {
    ham_env_t *env;
    ham_parameter_t cs[] = { { HAM_PARAM_CACHESIZE, 1024 }, { 0, 0 } };
    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE,   512 }, { 0, 0 } };

    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(0, ".test.db", 0, 0664, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, 0, 0, 0664, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, 0, HAM_IN_MEMORY | HAM_CACHE_STRICT, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, ".test.db",
          HAM_CACHE_UNLIMITED | HAM_CACHE_STRICT, 0644, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, ".test.db", HAM_CACHE_UNLIMITED, 0, &cs[0]));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, ".test.db",
            HAM_CACHE_UNLIMITED | HAM_CACHE_STRICT, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open(&env, ".test.db", HAM_CACHE_UNLIMITED, &cs[0]));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, 0, HAM_IN_MEMORY, 0, &cs[0]));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, 0, HAM_IN_MEMORY|HAM_READ_ONLY, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, 0, HAM_READ_ONLY, 0, 0));
    REQUIRE(HAM_INV_PAGESIZE ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0, &ps[0]));
#ifdef WIN32
    REQUIRE(HAM_IO_ERROR ==
        ham_env_create(&env, "c:\\windows", 0, 0664, 0));
#else
    REQUIRE(HAM_IO_ERROR ==
        ham_env_create(&env, "/home", 0, 0664, 0));
#endif
  }

  void createPagesizeTest() {
    ham_env_t *env;

    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE, 512 }, { 0, 0 } };

    REQUIRE(HAM_INV_PAGESIZE ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0644, &ps[0]));

    ps[0].value = 1024;
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0644, &ps[0]));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void createCloseCreateTest() {
    ham_env_t *env;

    REQUIRE(0 == ham_env_create(&env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ham_env_close(env, 0));
    REQUIRE(0 == ham_env_open(&env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void createPagesizeReopenTest() {
    ham_env_t *env;
    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE, 1024 * 128 }, { 0, 0 } };

    REQUIRE(0 == ham_env_create(&env, Globals::opath(".test"), 0, 0664, &ps[0]));
    REQUIRE(0 == ham_env_close(env, 0));
    REQUIRE(0 == ham_env_open(&env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void readOnlyTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *cursor;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ham_env_create(&env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, HAM_READ_ONLY, 0));
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));

    REQUIRE(HAM_WRITE_PROTECTED ==
        ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(HAM_WRITE_PROTECTED ==
        ham_db_erase(db, 0, &key, 0));
    REQUIRE(HAM_WRITE_PROTECTED ==
        ham_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(HAM_WRITE_PROTECTED ==
        ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(HAM_WRITE_PROTECTED ==
        ham_cursor_erase(cursor, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void invalidPagesizeTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 512 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p3[] = {
      { HAM_PARAM_KEYSIZE, 512 },
      { 0, 0 }
    };

    REQUIRE(HAM_INV_PAGESIZE ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, p1));

    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, p2));

    REQUIRE(HAM_INV_KEYSIZE ==
        ham_env_create_db(env, &db, 1, 0, p3));

    p2[0].value = 15;

    // only pagesize of 1k, 2k, multiples of 2k are allowed
    p1[0].value = 1024;
    REQUIRE(0 == ham_env_close(env, 0));
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ham_env_close(env, 0));
    p1[0].value = 2048;
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ham_env_close(env, 0));
    p1[0].value = 4096;
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, &p1[0]));
    REQUIRE(0 == ham_env_close(env, 0));
    p1[0].value = 1024 * 3;
    REQUIRE(HAM_INV_PAGESIZE ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, &p1[0]));
  }

  void invalidKeysizeTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEYSIZE, 200 },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0664, p1));

    REQUIRE(HAM_INV_KEYSIZE ==
        ham_env_create_db(env, &db, 1, 0, p2));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void getErrorTest() {
    REQUIRE(0 == ham_db_get_error(0));
    REQUIRE(0 == ham_db_get_error(m_db));
  }

  void setCompareTest() {
    REQUIRE(HAM_INV_PARAMETER == ham_db_set_compare_func(0, 0));
  }

  void findTest() {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(0, 0, &key, &rec, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, 0, &rec, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, &key, 0, 0));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void findEmptyRecordTest() {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, 0));

    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    rec.data = (void *)"123";
    rec.size = 12345;
    rec.flags = HAM_RECORD_USER_ALLOC;
    REQUIRE(0 ==
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));

    REQUIRE((ham_u16_t)0 == key.size);
    REQUIRE((void *)0 == key.data);
    REQUIRE((ham_size_t)0 == rec.size);
    REQUIRE((void *)0 == rec.data);

    REQUIRE(0 == ham_cursor_close(cursor));
  }


  static int HAM_CALLCONV my_compare_func_u32(ham_db_t *db,
                  const ham_u8_t *lhs, ham_size_t lhs_length,
                  const ham_u8_t *rhs, ham_size_t rhs_length)
  {
    ham_s32_t *l = (ham_s32_t *)lhs;
    ham_s32_t *r = (ham_s32_t *)rhs;
    ham_size_t len = (lhs_length < rhs_length ? lhs_length : rhs_length);

    ham_assert(lhs);
    ham_assert(rhs);

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
#if defined(HAM_DEBUG)
    const int RECORD_COUNT_PER_DB = 20000;
#else
    const int RECORD_COUNT_PER_DB = 50000;
#endif
    ham_env_t *env;
    ham_db_t *db;
    struct my_key_t {
      ham_s32_t val1;
      ham_u32_t val2;
      ham_u32_t val3;
      ham_u32_t val4;
    };
    struct my_rec_t {
      ham_s32_t val1;
      ham_u32_t val2[15];
    };
    ham_parameter_t ps[] = {
      { HAM_PARAM_PAGESIZE,  32 * 1024 }, /* UNIX == WIN now */
      { HAM_PARAM_CACHESIZE, 32 },
      { 0, 0 }
    };
    ham_parameter_t ps2[] = {
      { HAM_PARAM_KEYSIZE,  sizeof(my_key_t) },
      { 0, 0 }
    };

    ham_key_t key;
    ham_record_t rec;

    my_key_t my_key;
    my_rec_t my_rec;

    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"),
            HAM_DISABLE_MMAP, 0644, ps));

    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, ps2));
    REQUIRE(0 ==
        ham_db_set_compare_func(db, &my_compare_func_u32));

    /* insert the records: key=2*i; rec=100*i */
    int lower_bound_of_range = 0;
    int upper_bound_of_range = (RECORD_COUNT_PER_DB - 1) * 2;
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
    int i;
    for (i = 0; i < RECORD_COUNT_PER_DB; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&my_key, 0, sizeof(my_key));
      ::memset(&my_rec, 0, sizeof(my_rec));

      my_rec.val1 = 100 * i; // record values thus are 50 * key values...
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = HAM_RECORD_USER_ALLOC;

      my_key.val1 = 2 * i;
      key.data = (void *)&my_key;
      key.size = sizeof(my_key);
      key.flags = HAM_KEY_USER_ALLOC;

      REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));

      /*
      if (i % 1000 == 999) {
        std::cerr << ".";
        if (i % 10000 == 9999 || i <= 10000) {
          std::cerr << "+";
        }
      }
      */
    }
    REQUIRE(0 == ham_cursor_close(cursor));

    // std::cerr << std::endl;

    REQUIRE(0 == ham_db_check_integrity(db, NULL));

    my_rec_t *r;
    my_key_t *k;

    /* show record collection */
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < RECORD_COUNT_PER_DB; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE(key.data != (void *)0);
      REQUIRE(rec.data != (void *)0);
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(r->val1 == 100 * i);
      REQUIRE(k->val1 == 2 * i);
    }
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(0 == ham_cursor_close(cursor));

    REQUIRE(0 == ham_db_check_integrity(db, NULL));

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

    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
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
      key.flags = HAM_KEY_USER_ALLOC;

      PREP();
      REQUIRE((eq_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, 0));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE((k ? k->val1 : 666) ==
            looking_for);
      REQUIRE((r ? r->val1 : 666) ==
            (eq_expect ? looking_for * 50 : 666));

      PREP();
      REQUIRE((lt_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, HAM_FIND_LT_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (lt_expect ? lt_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (lt_expect ? lt_keyval * 50 : 666));

      PREP();
      REQUIRE((gt_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, HAM_FIND_GT_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (gt_expect ? gt_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (gt_expect ? gt_keyval * 50 : 666));

      PREP();
      REQUIRE((le_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, HAM_FIND_LEQ_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (le_expect ? le_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==
          (le_expect ? le_keyval * 50 : 666));

      PREP();
      REQUIRE((ge_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, HAM_FIND_GEQ_MATCH));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      // key is untouched when no match found at all
      REQUIRE((k ? k->val1 : 666) ==
          (ge_expect ? ge_keyval : looking_for));
      REQUIRE((r ? r->val1 : 666) ==   
          (ge_expect ? ge_keyval * 50 : 666));

      PREP();
      bool mix_expect = (le_expect || ge_expect);
      REQUIRE((mix_expect ? 0 : HAM_KEY_NOT_FOUND) ==
          ham_cursor_find(cursor, &key, &rec, HAM_FIND_NEAR_MATCH));
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
      REQUIRE((mix_expect ? 0 : HAM_KEY_NOT_FOUND) ==
        ham_cursor_find(cursor, &key, &rec, (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)));
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
    REQUIRE(0 == ham_cursor_close(cursor));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void nearFindTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE, 64 * 1024 },  { 0, 0 } };
    const int MY_KEY_SIZE = 6554;
    struct my_key_t {
      ham_u32_t key_val1;
      ham_u32_t key_surplus[MY_KEY_SIZE/4];
    };
    struct my_rec_t {
      ham_u32_t rec_val1;
      char rec_val2[512];
    };

    ham_size_t keycount = 0;
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), 0, 0644, &ps[0]));
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 1, HAM_ENABLE_EXTENDED_KEYS, 0));
    keycount = 8;
    REQUIRE(0 ==
        ham_db_set_compare_func(db, &my_compare_func_u32));

    ham_key_t key;
    ham_record_t rec;
    const int vals[] = { 1, 7, 3, 2, 9, 55, 42, 660, 14, 11, 37, 99,
      123, 111, 459, 52, 66, 77, 88, 915, 31415, 12719 };

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    my_key_t my_key = {666};
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = HAM_KEY_USER_ALLOC;

    /* empty DB: LT/GT must turn up error */
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(db, 0, &key, &rec, HAM_FIND_LT_MATCH));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(db, 0, &key, &rec, HAM_FIND_GT_MATCH));

    int fill = 0;
    my_rec_t my_rec = {1000, "hello world!"};
    rec.data = &my_rec;
    rec.size = sizeof(my_rec);
    rec.flags = HAM_RECORD_USER_ALLOC;

    my_key.key_val1 = vals[fill++];

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));

    /* one record in DB: LT/GT must turn up that one for the
     * right key values */
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));
    REQUIRE(rec.data != key.data);
    my_rec_t *r = (my_rec_t *)rec.data;
    my_key_t *k = (my_key_t *)key.data;
    REQUIRE((unsigned)r->rec_val1 == (unsigned)1000);
    REQUIRE((unsigned)k->key_val1 == (ham_u32_t)vals[fill-1]);

    ::memset(&rec, 0, sizeof(rec));
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = HAM_KEY_USER_ALLOC;
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
    REQUIRE(rec.data != key.data);
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (ham_u32_t)vals[fill-1]);
    REQUIRE(ham_key_get_approximate_match_type(&key) == 0);

    ::memset(&rec, 0, sizeof(rec));
    my_key.key_val1 = vals[fill - 1] - 1;
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = HAM_KEY_USER_ALLOC;
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
    REQUIRE(rec.data != key.data);
    REQUIRE(rec.data);
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (ham_u32_t)vals[fill-1]);
    REQUIRE(ham_key_get_approximate_match_type(&key) == 1);

    ::memset(&rec, 0, sizeof(rec));
    my_key.key_val1 = vals[fill - 1] + 2;
    key.data = &my_key;
    key.size = MY_KEY_SIZE;
    key.flags = HAM_KEY_USER_ALLOC;
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
    r = (my_rec_t *)rec.data;
    k = (my_key_t *)key.data;
    REQUIRE(r->rec_val1 == (unsigned)1000);
    REQUIRE(k->key_val1 == (ham_u32_t)vals[fill - 1]);
    REQUIRE(ham_key_get_approximate_match_type(&key) == -1);

    key.data = (void *)&my_key;
    key.size = MY_KEY_SIZE;
    key.flags = HAM_KEY_USER_ALLOC;

    /* add two more records */
    unsigned int i;
    for (i = 0; i < 2; i++) {
      my_rec.rec_val1 = 2000 + i;
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = HAM_RECORD_USER_ALLOC;

      my_key.key_val1 = vals[fill++];
      key.data = (void *)&my_key;
      key.size = MY_KEY_SIZE;
      key.flags = HAM_KEY_USER_ALLOC;

      REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    }

    /* show record collection */
    const int verify_vals1[] = { 1, 3, 7 };
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < 3; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(k->key_val1 == (ham_u32_t)verify_vals1[i]);
    }
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(0 == ham_cursor_close(cursor));


    /* three records in DB {1, 3, 7}: LT/GT should pick the
     * 'proper' one each time */
    struct search_res_t {
      int rv;
      int keyval;
      int sign;
      int recval;
    };
    struct search_cat_t {
      ham_u32_t mode;
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
      { HAM_KEY_NOT_FOUND, 0, 0, 666},
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
      { HAM_KEY_NOT_FOUND, 0, 0, 666},
      { HAM_KEY_NOT_FOUND, 1, 0, 666},
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
      { HAM_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_res_t srch_res_gt1[] = {
      { 0, 1, 1, 1000},
      { 0, 3, 1, 2001},
      { 0, 3, 1, 2001},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { 0, 7, 1, 2000},
      { HAM_KEY_NOT_FOUND, 7, 0, 666},
      { HAM_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_res_t srch_res_eq1[] = {
      { HAM_KEY_NOT_FOUND, 0, 0, 666},
      { 0, 1, 0, 1000},
      { HAM_KEY_NOT_FOUND, 2, 0, 666},
      { 0, 3, 0, 2001},
      { HAM_KEY_NOT_FOUND, 4, 0, 666},
      { HAM_KEY_NOT_FOUND, 5, 0, 666},
      { HAM_KEY_NOT_FOUND, 6, 0, 666},
      { 0, 7, 0, 2000},
      { HAM_KEY_NOT_FOUND, 8, 0, 666}
    };
    const search_cat_t srch_cats[] = {
      { HAM_FIND_NEAR_MATCH, srch_res_any1, "HAM_FIND_NEAR_MATCH '~'" },
      { HAM_FIND_LEQ_MATCH, srch_res_leq1, "HAM_FIND_LEQ_MATCH '<='" },
      { HAM_FIND_LT_MATCH, srch_res_lt1, "HAM_FIND_LT_MATCH '<'" },
      { HAM_FIND_GEQ_MATCH, srch_res_geq1, "HAM_FIND_GEQ_MATCH '>='" },
      { HAM_FIND_GT_MATCH, srch_res_gt1, "HAM_FIND_GT_MATCH '>'" },
      { HAM_FIND_EXACT_MATCH, srch_res_eq1, "HAM_FIND_EXACT_MATCH '='" },
      { 0 /* = HAM_FIND_EXACT_MATCH */, srch_res_eq1,
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
        key.flags = HAM_KEY_USER_ALLOC;
        int rv = ham_db_find(db, 0, &key, &rec, srch_cats[j].mode);
        r = (my_rec_t *)rec.data;
        k = (my_key_t *)key.data;
        REQUIRE(rv == res[i].rv);
        REQUIRE((r ? r->rec_val1 : 666) ==
            (ham_u32_t)res[i].recval);
        REQUIRE((k ? k->key_val1 : 666) ==
            (ham_u32_t)res[i].keyval);
        REQUIRE(ham_key_get_approximate_match_type(&key) ==
            res[i].sign);
      }
    }

    /* add more records: fill one page; then in the next
     * round overflow by one, and then on to three pages, etc. */
    for (i = 0; i < keycount - 3 + 1; i++) {
      my_rec.rec_val1 = 3000 + i;
      rec.data = &my_rec;
      rec.size = sizeof(my_rec);
      rec.flags = HAM_RECORD_USER_ALLOC;

      my_key.key_val1 = vals[fill++];
      key.data = (void *)&my_key;
      key.size = MY_KEY_SIZE;
      key.flags = HAM_KEY_USER_ALLOC;

      REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    }

    /* show record collection */
    const int verify_vals2[] = { 1, 2, 3, 7, 9, 14, 42, 55, 660 };
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
    for (i = 0; i < 9; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      r = (my_rec_t *)rec.data;
      k = (my_key_t *)key.data;
      REQUIRE(k->key_val1 == (ham_u32_t)verify_vals2[i]);
    }
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(0 == ham_cursor_close(cursor));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertTest() {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(0, 0, &key, &rec, 0));
    key.flags = 0x13;
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, 0));
    key.flags = 0;
    rec.flags = 0x13;
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, 0));
    rec.flags = 0;
    key.flags = HAM_KEY_USER_ALLOC;
    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    key.flags = 0;
    rec.flags = HAM_RECORD_USER_ALLOC;
    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    rec.flags = 0;
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE|HAM_DUPLICATE));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, 0, &rec, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_BEFORE));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_AFTER));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_FIRST));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_LAST));
    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
  }

  void insertDuplicateTest() {
    ham_db_t *db;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE|HAM_OVERWRITE));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 ==
        ham_env_create_db(m_env, &db, 2, HAM_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(db, 0, &key, &rec, HAM_DUPLICATE | HAM_OVERWRITE));
    REQUIRE(0 ==
        ham_db_insert(db, 0, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_db_close(db, 0));
  }

  void negativeInsertBigKeyTest() {
    ham_key_t key;
    ham_record_t rec;
    char buffer[0xffff];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(buffer, 0, sizeof(buffer));
    key.size = sizeof(buffer);
    key.data = buffer;

    REQUIRE(HAM_INV_KEYSIZE ==
            ham_db_insert(m_db, 0, &key, &rec, 0));

    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(HAM_INV_KEYSIZE ==
            ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void insertBigKeyTest() {
    ham_key_t key;
    ham_record_t rec;
    char buffer[0xffff];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(buffer, 0, sizeof(buffer));
    key.size = sizeof(buffer);
    key.data = buffer;

    teardown();
    REQUIRE(0 == ham_env_create(&m_env, "test.db", 0, 0644, 0));
    REQUIRE(0 ==
            ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_EXTENDED_KEYS, 0));

    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));

    buffer[0]++;

    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 ==
            ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_cursor_close(cursor));

    buffer[0]++;

    teardown();

    REQUIRE(0 == ham_env_open(&m_env, "test.db", 0, 0));
    REQUIRE(0 ==
            ham_env_open_db(m_env, &m_db, 1, HAM_ENABLE_EXTENDED_KEYS, 0));
    teardown();
    REQUIRE(0 == ham_env_open(&m_env, "test.db", 0, 0));
    REQUIRE(0 ==
            ham_env_open_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void eraseTest() {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_erase(0, 0, &key, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_erase(m_db, 0, 0, 0));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_erase(m_db, 0, &key, 0));
  }

  void flushBackendTest() {
    ham_env_t *env1, *env2;
    ham_db_t *db1, *db2;

    ham_key_t key;
    ham_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = &value;
    key.size = sizeof(value);

    REQUIRE(0 ==
        ham_env_create(&env1, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env1, &db1, 111, 0, 0));
    REQUIRE(0 == ham_db_insert(db1, 0, &key, &rec, 0));
    REQUIRE(0 == ham_env_flush(env1, 0));

    /* Exclusive locking is now the default */
    REQUIRE(HAM_WOULD_BLOCK ==
        ham_env_open(&env2, Globals::opath(".test"), 0, 0));
    REQUIRE(HAM_WOULD_BLOCK ==
        ham_env_open(&env2, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_close(env1, HAM_AUTO_CLEANUP));
    REQUIRE(0 ==
        ham_env_open(&env2, Globals::opath(".test"), HAM_READ_ONLY, 0));
    REQUIRE(0 == ham_env_open_db(env2, &db2, 111, 0, 0));
    REQUIRE(0 == ham_db_find(db2, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_close(db2, 0));
    REQUIRE(0 == ham_env_close(env2, 0));
  }

  void closeTest() {
    REQUIRE(HAM_INV_PARAMETER == ham_db_close(0, 0));
  }

  void closeWithCursorsTest() {
    ham_cursor_t *c[5];

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, 0, 0));

    REQUIRE(HAM_CURSOR_STILL_OPEN == ham_db_close(m_db, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
    REQUIRE(0 == ham_db_close(m_db, 0));
  }

  void closeWithCursorsAutoCleanupTest() {
    ham_cursor_t *c[5];

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, 0, 0));

    REQUIRE(0 == ham_db_close(m_db, HAM_AUTO_CLEANUP));
  }

  void compareTest() {
    ham_compare_func_t f = my_compare_func;

    REQUIRE(0 == ham_db_set_compare_func(m_db, f));
    REQUIRE(f == ((LocalDatabase *)m_db)->get_compare_func());
  }

  void cursorCreateTest() {
    ham_cursor_t *cursor;

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_create(&cursor, 0, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_create(0, m_db, 0, 0));
  }

  void cursorCloneTest() {
    ham_cursor_t src, *dest;

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_clone(0, &dest));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_clone(&src, 0));
  }

  void cursorMoveTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_move(0, 0, 0, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_move(cursor, &key, 0, 0));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_PREVIOUS));

    ham_cursor_close(cursor);
  }

  void cursorReplaceTest() {
    ham_cursor_t *cursor;
    ham_record_t *record=0;

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_overwrite(0, record, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_overwrite(cursor, 0, 0));

    ham_cursor_close(cursor);
  }

  void cursorFindTest() {
    ham_cursor_t *cursor;
    ham_key_t *key = 0;

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(0, key, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, 0, 0, 0));

    ham_cursor_close(cursor);
  }

  void cursorInsertTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_insert(0, &key, &rec, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_insert(cursor, 0, &rec, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_insert(cursor, &key, 0, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void cursorEraseTest() {
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_erase(0, 0));
  }

  void cursorCloseTest() {
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_close(0));
  }

  void cursorGetErasedItemTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = &value;
    key.size = sizeof(value);

    value = 1;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    value = 2;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    value = 1;
    REQUIRE(0 == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_move(cursor, &key, 0, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void replaceKeyTest() {
    /* in-memory */
    ham_key_t key;
    ham_record_t rec;
    char buffer1[32], buffer2[7];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(buffer1, 0, sizeof(buffer1));
    ::memset(buffer2, 0, sizeof(buffer2));
    rec.size = sizeof(buffer1);
    rec.data = buffer1;

    /* insert a big blob */
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)sizeof(buffer1) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer1, sizeof(buffer1)));

    /* replace with a tiny blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer2);
    rec.data = buffer2;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)sizeof(buffer2) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer2, sizeof(buffer2)));

    /* replace with a big blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer1);
    rec.data = buffer1;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)sizeof(buffer1) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer1, sizeof(buffer1)));

    /* replace with a NULL blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 0;
    rec.data = 0;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)0 == rec.size);
    REQUIRE((void *)0 == rec.data);

    /* replace with a tiny blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = sizeof(buffer2);
    rec.data = buffer2;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)sizeof(buffer2) == rec.size);
    REQUIRE(0 == ::memcmp(rec.data, buffer2, sizeof(buffer2)));

    /* replace with a NULL blob */
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 0;
    rec.data = 0;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)0 == rec.size);
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
      REQUIRE(ham_strerror((ham_status_t)i));
    }
    REQUIRE(0 == strcmp("Unknown error",
          ham_strerror((ham_status_t)-204)));
    REQUIRE(0 == strcmp("Unknown error",
          ham_strerror((ham_status_t)-35)));
    REQUIRE(0 == strcmp("Unknown error",
          ham_strerror((ham_status_t)1)));
  }

  void contextDataTest() {
    void *ptr = (void *)0x13;
    ham_set_context_data(0, 0);
    ham_set_context_data(m_db, ptr);
    REQUIRE((void *)0 == ham_get_context_data(0, 0));
    REQUIRE((void *)0x13 == ham_get_context_data(m_db, 0));
    ham_set_context_data(m_db, 0);
    REQUIRE((void *)0 == ham_get_context_data(m_db, 0));
  }

  void recoveryTest() {
    ham_env_t *old = m_env;
    REQUIRE(0 ==
            ham_env_create(&m_env, Globals::opath(".test"),
                    HAM_ENABLE_RECOVERY, 0664, 0));
    REQUIRE(0 == ham_env_close(m_env, 0));
    m_env = old;
  }

  void recoveryNegativeTest() {
    ham_env_t *old = m_env;
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY | HAM_IN_MEMORY, 0664, 0));
    m_env = old;
  }

  void recoveryEnvTest() {
    ham_env_t *env;
    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath(".test"), HAM_ENABLE_RECOVERY, 0664, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void recoveryEnvNegativeTest() {
    ham_env_t *env;
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_create(&env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY | HAM_IN_MEMORY, 0664, 0));
  }

  void insertAppendTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (unsigned i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }
    for (unsigned i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
  }

  void insertPrependTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 100; i >= 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }
    for (int i = 100; i >= 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
  }

  void cursorInsertAppendTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 0; i < 10000; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    }
    for (unsigned i = 0; i < 10000; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void negativeCursorInsertAppendTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 10; i > 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    }
    for (unsigned i = 1; i <= 10; i++) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE((unsigned)key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, key.size));
    }
    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void recordCountTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ham_u64_t count;
    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE, 1024 * 4 }, { 0, 0 } };
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    teardown();
    REQUIRE(0 ==
            ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, ps));
    REQUIRE(0 ==
            ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATE_KEYS, 0));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    for (unsigned i = 4000; i > 0; i--) {
      key.size = sizeof(i);
      key.data = (void *)&i;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    }

    REQUIRE(0 == ham_cursor_close(cursor));

    for (unsigned i = 1; i <= 10; i++) {
      unsigned k = 5;
      key.size = sizeof(k);
      key.data = (void *)&k;
      rec.size = sizeof(i);
      rec.data = (void *)&i;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    count = 0;
    REQUIRE(0 ==
        ham_db_get_key_count(m_db, 0, HAM_SKIP_DUPLICATES, &count));
    REQUIRE((unsigned)4000 == count);

    REQUIRE(0 ==
        ham_db_get_key_count(m_db, 0, 0, &count));
    REQUIRE((unsigned)(4000 + 10) == count);
  }

  void createDbOpenEnvTest() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 22, 0, 0));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 22, 0, 0));
  }

  void checkDatabaseNameTest() {
    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    teardown();

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open_db(m_env, &m_db, 0xff00, 0, 0));
    /* right now it's allowed to create the DUMMY_DATABASE from scratch
     * - that's not really a problem...
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open_db(m_env, m_db,
            HAM_DUMMY_DATABASE_NAME, 0, 0));
     */
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_open_db(m_env, &m_db,
            HAM_DUMMY_DATABASE_NAME + 1, 0, 0));
  }

  void hintingTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    /* HAM_HINT_APPEND is *only* allowed in
     * ham_cursor_insert; not allowed in combination with
     * HAM_HINT_PREPEND */
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_insert(m_db, 0, &key, &rec,
          HAM_HINT_PREPEND));

    REQUIRE(0 ==
        ham_cursor_insert(cursor, &key, &rec,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_insert(cursor, &key, &rec,
          HAM_HINT_APPEND|HAM_HINT_PREPEND));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_erase(m_db, 0, &key,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_erase(m_db, 0, &key,
          HAM_HINT_PREPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_erase(cursor,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_erase(cursor,
          HAM_HINT_PREPEND));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, 0,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, 0,
          HAM_HINT_PREPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, &rec,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, &rec,
          HAM_HINT_PREPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_HINT_APPEND));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_HINT_PREPEND));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void directAccessTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 6;
    rec.data = (void *)"hello";

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_find(cursor, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(cursor, &key, &rec, HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)6 == rec.size);
    REQUIRE(0 == strcmp("hello", (char *)rec.data));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void smallDirectAccessTest() {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    /* test with an empty record */
    rec.size = 0;
    rec.data = (void *)"";
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)0 == rec.size);

    /* test with a tiny record (<8)*/
    rec.size = 4;
    rec.data = (void *)"hel";
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)4 == rec.size);
    REQUIRE(0 == strcmp("hel", (char *)rec.data));
    ((char *)rec.data)[0] = 'b';
    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)4 == rec.size);
    REQUIRE(0 == strcmp("bel", (char *)rec.data));

    /* test with a small record (8)*/
    rec.size = 8;
    rec.data = (void *)"hello wo";
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)8 == rec.size);
    REQUIRE(0 == strcmp("hello wo", (char *)rec.data));
    ((char *)rec.data)[0] = 'b';
    REQUIRE(0 ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE((unsigned)8 == rec.size);
    REQUIRE(0 == strcmp("bello wo", (char *)rec.data));
  }

  void negativeDirectAccessTest() {
    ham_cursor_t *cursor;
    ham_key_t key = {};
    ham_record_t rec = {};
    rec.size = 6;
    rec.data = (void *)"hello";

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, &key, &rec, HAM_DIRECT_ACCESS));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, &rec, HAM_DIRECT_ACCESS));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_move(cursor, &key, &rec, HAM_DIRECT_ACCESS));

    REQUIRE(0 == ham_cursor_close(cursor));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_db_find(m_db, 0, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_find(cursor, &key, &rec,
          HAM_DIRECT_ACCESS));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_move(cursor, &key, &rec,
          HAM_DIRECT_ACCESS));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void unlimitedCacheTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.size = 6;
    rec.data = (void *)"hello";

    REQUIRE(0 == ham_env_create(&env, ".test.db", HAM_CACHE_UNLIMITED, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    REQUIRE(0 == ham_env_open(&env, ".test.db", HAM_CACHE_UNLIMITED, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void openVersion1x() {
    ham_env_t *env;

    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&env, Globals::opath("data/sample-db1-1.x.hdb"), 0, 0));
    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&env, Globals::opath("data/sample-db1-2.0.hdb"), 0, 0));
  }

  void overwriteLogDirectoryTest() {
    ham_env_t *env;
    ham_parameter_t ps[] = {
        { HAM_PARAM_LOG_DIRECTORY, (ham_u64_t)"data" },
        { 0, 0 }
    };

    os::unlink("data/test.db.log0");
    os::unlink("data/test.db.jrn0");
    os::unlink("data/test.db.jrn1");
    REQUIRE(false == os::file_exists("data/test.db.log0"));
    REQUIRE(false == os::file_exists("data/test.db.jrn0"));
    REQUIRE(false == os::file_exists("data/test.db.jrn1"));

    REQUIRE(0 ==
        ham_env_create(&env, Globals::opath("test.db"),
            HAM_ENABLE_TRANSACTIONS, 0, &ps[0]));
    REQUIRE(0 == ham_env_close(env, 0));
    REQUIRE(true == os::file_exists("data/test.db.log0"));
    REQUIRE(true == os::file_exists("data/test.db.jrn0"));
    REQUIRE(true == os::file_exists("data/test.db.jrn1"));

    REQUIRE(0 ==
        ham_env_open(&env, Globals::opath("test.db"),
            HAM_ENABLE_TRANSACTIONS, &ps[0]));

    REQUIRE(0 == ham_env_get_parameters(env, &ps[0]));
    REQUIRE(0 == strcmp("data", (const char *)ps[0].value));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void persistentFlagsTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_parameter_t ps[] = {
        { HAM_PARAM_KEYSIZE, 8 },
        { 0, 0 }
    };
    ham_u32_t flags = HAM_ENABLE_EXTENDED_KEYS
                    | HAM_ENABLE_DUPLICATE_KEYS
                    | HAM_DISABLE_VARIABLE_KEYS;

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, flags, &ps[0]));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen the database
    REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

    // check if the flags and parameters were stored persistently
    LocalDatabase *ldb = (LocalDatabase *)db;
    REQUIRE((ldb->get_rt_flags() & flags) == flags);

    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));

    // Variable size keys are not allowed
    ham_record_t rec = {0};
    ham_key_t key = {0};
    key.data = (void *)"12345678";
    key.size = 4;
    REQUIRE(HAM_INV_KEYSIZE == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(HAM_INV_KEYSIZE == ham_cursor_insert(cursor, &key, &rec, 0));
    key.size = 8;
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }
};

TEST_CASE("Hamsterdb/versionTest", "")
{
  HamsterdbFixture f;
  f.versionTest();
}

TEST_CASE("Hamsterdb/licenseTest", "")
{
  HamsterdbFixture f;
  f.licenseTest();
}

TEST_CASE("Hamsterdb/openTest", "")
{
  HamsterdbFixture f;
  f.openTest();
}

TEST_CASE("Hamsterdb/getEnvTest", "")
{
  HamsterdbFixture f;
  f.getEnvTest();
}

TEST_CASE("Hamsterdb/invHeaderTest", "")
{
  HamsterdbFixture f;
  f.invHeaderTest();
}

TEST_CASE("Hamsterdb/invVersionTest", "")
{
  HamsterdbFixture f;
  f.invVersionTest();
}

TEST_CASE("Hamsterdb/createTest", "")
{
  HamsterdbFixture f;
  f.createTest();
}

TEST_CASE("Hamsterdb/createPagesizeTest", "")
{
  HamsterdbFixture f;
  f.createPagesizeTest();
}

TEST_CASE("Hamsterdb/createCloseCreateTest", "")
{
  HamsterdbFixture f;
  f.createCloseCreateTest();
}

TEST_CASE("Hamsterdb/createPagesizeReopenTest", "")
{
  HamsterdbFixture f;
  f.createPagesizeReopenTest();
}

TEST_CASE("Hamsterdb/readOnlyTest", "")
{
  HamsterdbFixture f;
  f.readOnlyTest();
}

TEST_CASE("Hamsterdb/invalidPagesizeTest", "")
{
  HamsterdbFixture f;
  f.invalidPagesizeTest();
}

TEST_CASE("Hamsterdb/invalidKeysizeTest", "")
{
  HamsterdbFixture f;
  f.invalidKeysizeTest();
}

TEST_CASE("Hamsterdb/getErrorTest", "")
{
  HamsterdbFixture f;
  f.getErrorTest();
}

TEST_CASE("Hamsterdb/setCompareTest", "")
{
  HamsterdbFixture f;
  f.setCompareTest();
}

TEST_CASE("Hamsterdb/findTest", "")
{
  HamsterdbFixture f;
  f.findTest();
}

TEST_CASE("Hamsterdb/findEmptyRecordTest", "")
{
  HamsterdbFixture f;
  f.findEmptyRecordTest();
}

TEST_CASE("Hamsterdb/nearFindTest", "")
{
  HamsterdbFixture f;
  f.nearFindTest();
}

TEST_CASE("Hamsterdb/nearFindStressTest", "")
{
  HamsterdbFixture f;
  f.nearFindStressTest();
}

TEST_CASE("Hamsterdb/insertTest", "")
{
  HamsterdbFixture f;
  f.insertTest();
}

TEST_CASE("Hamsterdb/negativeInsertBigKeyTest", "")
{
  HamsterdbFixture f;
  f.negativeInsertBigKeyTest();
}

TEST_CASE("Hamsterdb/insertBigKeyTest", "")
{
  HamsterdbFixture f;
  f.insertBigKeyTest();
}

TEST_CASE("Hamsterdb/eraseTest", "")
{
  HamsterdbFixture f;
  f.eraseTest();
}

TEST_CASE("Hamsterdb/flushBackendTest", "")
{
  HamsterdbFixture f;
  f.flushBackendTest();
}

TEST_CASE("Hamsterdb/closeTest", "")
{
  HamsterdbFixture f;
  f.closeTest();
}

TEST_CASE("Hamsterdb/closeWithCursorsTest", "")
{
  HamsterdbFixture f;
  f.closeWithCursorsTest();
}

TEST_CASE("Hamsterdb/closeWithCursorsAutoCleanupTest", "")
{
  HamsterdbFixture f;
  f.closeWithCursorsAutoCleanupTest();
}

TEST_CASE("Hamsterdb/compareTest", "")
{
  HamsterdbFixture f;
  f.compareTest();
}

TEST_CASE("Hamsterdb/cursorCreateTest", "")
{
  HamsterdbFixture f;
  f.cursorCreateTest();
}

TEST_CASE("Hamsterdb/cursorCloneTest", "")
{
  HamsterdbFixture f;
  f.cursorCloneTest();
}

TEST_CASE("Hamsterdb/cursorMoveTest", "")
{
  HamsterdbFixture f;
  f.cursorMoveTest();
}

TEST_CASE("Hamsterdb/cursorReplaceTest", "")
{
  HamsterdbFixture f;
  f.cursorReplaceTest();
}

TEST_CASE("Hamsterdb/cursorFindTest", "")
{
  HamsterdbFixture f;
  f.cursorFindTest();
}

TEST_CASE("Hamsterdb/cursorInsertTest", "")
{
  HamsterdbFixture f;
  f.cursorInsertTest();
}

TEST_CASE("Hamsterdb/cursorEraseTest", "")
{
  HamsterdbFixture f;
  f.cursorEraseTest();
}

TEST_CASE("Hamsterdb/cursorCloseTest", "")
{
  HamsterdbFixture f;
  f.cursorCloseTest();
}

TEST_CASE("Hamsterdb/cursorGetErasedItemTest", "")
{
  HamsterdbFixture f;
  f.cursorGetErasedItemTest();
}

TEST_CASE("Hamsterdb/replaceKeyTest", "")
{
  HamsterdbFixture f;
  f.replaceKeyTest();
}

TEST_CASE("Hamsterdb/callocTest", "")
{
  HamsterdbFixture f;
  f.callocTest();
}

TEST_CASE("Hamsterdb/strerrorTest", "")
{
  HamsterdbFixture f;
  f.strerrorTest();
}

TEST_CASE("Hamsterdb/contextDataTest", "")
{
  HamsterdbFixture f;
  f.contextDataTest();
}

TEST_CASE("Hamsterdb/recoveryTest", "")
{
  HamsterdbFixture f;
  f.recoveryTest();
}

TEST_CASE("Hamsterdb/recoveryNegativeTest", "")
{
  HamsterdbFixture f;
  f.recoveryNegativeTest();
}

TEST_CASE("Hamsterdb/recoveryEnvTest", "")
{
  HamsterdbFixture f;
  f.recoveryEnvTest();
}

TEST_CASE("Hamsterdb/recoveryEnvNegativeTest", "")
{
  HamsterdbFixture f;
  f.recoveryEnvNegativeTest();
}

TEST_CASE("Hamsterdb/insertAppendTest", "")
{
  HamsterdbFixture f;
  f.insertAppendTest();
}

TEST_CASE("Hamsterdb/insertPrependTest", "")
{
  HamsterdbFixture f;
  f.insertPrependTest();
}

TEST_CASE("Hamsterdb/cursorInsertAppendTest", "")
{
  HamsterdbFixture f;
  f.cursorInsertAppendTest();
}

TEST_CASE("Hamsterdb/negativeCursorInsertAppendTest", "")
{
  HamsterdbFixture f;
  f.negativeCursorInsertAppendTest();
}

TEST_CASE("Hamsterdb/recordCountTest", "")
{
  HamsterdbFixture f;
  f.recordCountTest();
}

TEST_CASE("Hamsterdb/createDbOpenEnvTest", "")
{
  HamsterdbFixture f;
  f.createDbOpenEnvTest();
}

TEST_CASE("Hamsterdb/checkDatabaseNameTest", "")
{
  HamsterdbFixture f;
  f.checkDatabaseNameTest();
}

TEST_CASE("Hamsterdb/hintingTest", "")
{
  HamsterdbFixture f;
  f.hintingTest();
}

TEST_CASE("Hamsterdb/directAccessTest", "")
{
  HamsterdbFixture f;
  f.directAccessTest();
}

TEST_CASE("Hamsterdb/smallDirectAccessTest", "")
{
  HamsterdbFixture f;
  f.smallDirectAccessTest();
}

TEST_CASE("Hamsterdb/negativeDirectAccessTest", "")
{
  HamsterdbFixture f;
  f.negativeDirectAccessTest();
}

TEST_CASE("Hamsterdb/unlimitedCacheTest", "")
{
  HamsterdbFixture f;
  f.unlimitedCacheTest();
}

TEST_CASE("Hamsterdb/openVersion1x", "")
{
  HamsterdbFixture f;
  f.openVersion1x();
}

TEST_CASE("Hamsterdb/overwriteLogDirectoryTest", "")
{
  HamsterdbFixture f;
  f.overwriteLogDirectoryTest();
}

TEST_CASE("Hamsterdb/persistentFlagsTest", "")
{
  HamsterdbFixture f;
  f.persistentFlagsTest();
}

} // namespace hamsterdb
