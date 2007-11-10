/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.hpp>

static int
my_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    (void)lhs;
    (void)rhs;
    (void)lhs_length;
    (void)rhs_length;
    return (0);
}

static int
my_prefix_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length,
               ham_size_t lhs_real_length,
               const ham_u8_t *rhs, ham_size_t rhs_length,
               ham_size_t rhs_real_length)
{
    (void)lhs;
    (void)rhs;
    (void)lhs_length;
    (void)rhs_length;
    (void)lhs_real_length;
    (void)rhs_real_length;
    return (0);
}

class CppApiTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CppApiTest);
    CPPUNIT_TEST      (keyTest);
    CPPUNIT_TEST      (recordTest);
    CPPUNIT_TEST      (compareTest);
    CPPUNIT_TEST      (createOpenCloseDbTest);
    CPPUNIT_TEST      (insertFindEraseTest);
    CPPUNIT_TEST      (cursorTest);
    CPPUNIT_TEST      (envTest);
    CPPUNIT_TEST      (envGetDatabaseNamesTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()
    { 
    }
    
    void tearDown() 
    { 
    }

    void keyTest(void)
    {
        void *p=(void *)"123";
        void *q=(void *)"234";
        ham::key k1, k2(p, 4, HAM_KEY_USER_ALLOC);

        CPPUNIT_ASSERT_EQUAL((void *)0, k1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, k1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0, k1.get_flags());

        CPPUNIT_ASSERT_EQUAL(p, k2.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, k2.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_KEY_USER_ALLOC, k2.get_flags());

        k1=k2;
        CPPUNIT_ASSERT_EQUAL(p, k1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, k1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_KEY_USER_ALLOC, k1.get_flags());

        ham::key k3(k1);
        CPPUNIT_ASSERT_EQUAL(p, k3.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, k3.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_KEY_USER_ALLOC, k3.get_flags());

        k1.set_data(q);
        k1.set_size(2);
        k1.set_flags(0);
        CPPUNIT_ASSERT_EQUAL(q, k1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)2, k1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0, k1.get_flags());
    }

    void recordTest(void)
    {
        void *p=(void *)"123";
        void *q=(void *)"234";
        ham::record r1, r2(p, 4, HAM_RECORD_USER_ALLOC);

        CPPUNIT_ASSERT_EQUAL((void *)0, r1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, r1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0, r1.get_flags());

        CPPUNIT_ASSERT_EQUAL(p, r2.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, r2.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_RECORD_USER_ALLOC, r2.get_flags());

        r1=r2;
        CPPUNIT_ASSERT_EQUAL(p, r1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, r1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_RECORD_USER_ALLOC, r1.get_flags());

        ham::record r3(r1);
        CPPUNIT_ASSERT_EQUAL(p, r3.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)4, r3.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_RECORD_USER_ALLOC, r3.get_flags());

        r1.set_data(q);
        r1.set_size(2);
        r1.set_flags(0);
        CPPUNIT_ASSERT_EQUAL(q, r1.get_data());
        CPPUNIT_ASSERT_EQUAL((ham_size_t)2, r1.get_size());
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0, r1.get_flags());
    }

    void compareTest(void)
    {
        ham::db db;
        db.create(".test");
        db.set_compare_func(my_compare_func);
        db.set_prefix_compare_func(my_prefix_compare_func);
        db.close();
    }

    void createOpenCloseDbTest(void)
    {
        ham::db db;
        db.create(".test");
        db.close();
        db.open(".test");
    }

    void insertFindEraseTest(void)
    {
        ham::db db;
        ham::key k;
        ham::record r, out;

        k.set_data((void *)"12345");
        k.set_size(6);
        r.set_data((void *)"12345");
        r.set_size(6);

        db.create(".test");
        db.insert(&k, &r);
        out=db.find(&k);
        CPPUNIT_ASSERT_EQUAL(r.get_size(), out.get_size());
        CPPUNIT_ASSERT_EQUAL(0,
                        ::memcmp(r.get_data(), out.get_data(), out.get_size()));
        db.erase(&k);
        try {
            out=db.find(&k);
        }
        catch(ham::error &e) {
            CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, e.get_errno());
        }

        db.close();
        db.close();
        db.close();
        db.open(".test");
    }

    void cursorTest(void)
    {
        ham::db db;
        ham::key k((void *)"12345", 5), k2;
        ham::record r((void *)"12345", 5), r2;

        db.create(".test");
        ham::cursor c(&db);

        db.insert(&k, &r);
        ham::cursor clone=c.clone();

        c.move_first(&k2, &r2);
        CPPUNIT_ASSERT_EQUAL(k.get_size(), k2.get_size());
        CPPUNIT_ASSERT_EQUAL(r.get_size(), r2.get_size());

        c.move_last(&k2, &r2);
        CPPUNIT_ASSERT_EQUAL(k.get_size(), k2.get_size());
        CPPUNIT_ASSERT_EQUAL(r.get_size(), r2.get_size());

        try {
            c.move_next();
        }
        catch (ham::error &e) {
            CPPUNIT_ASSERT_EQUAL(e.get_errno(), HAM_KEY_NOT_FOUND);
        }

        try {
            c.move_previous();
        }
        catch (ham::error &e) {
            CPPUNIT_ASSERT_EQUAL(e.get_errno(), HAM_KEY_NOT_FOUND);
        }

        c.find(&k);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)1, c.get_duplicate_count());

        c.erase();
    }

    void envTest(void)
    {
        ham::env env;

        env.create(".test");
        env.close();
        env.close();
        env.close();
        env.open(".test");

        ham::db db1=env.create_db(1);
        db1.close();
        db1=env.open_db(1);
        env.rename_db(1, 2);

        try {
            env.erase_db(2);
        }
        catch (ham::error &e) {
            CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN, e.get_errno());
        }
        db1.close();
        env.erase_db(2);
    }
    
    void envGetDatabaseNamesTest(void)
    {
        ham::env env;
        std::vector<ham_u16_t> v;

        env.create(".test");

        v=env.get_database_names();
        CPPUNIT_ASSERT_EQUAL((size_t)0, v.size());

        ham::db db1=env.create_db(1);
        v=env.get_database_names();
        CPPUNIT_ASSERT_EQUAL((size_t)1, v.size());
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)1, v[0]);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(CppApiTest);

