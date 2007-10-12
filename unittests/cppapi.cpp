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
    CPPUNIT_TEST      (createOpenCloseDbTest);
    CPPUNIT_TEST      (insertFindEraseTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()
    { 
    }
    
    void tearDown() 
    { 
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

        ham_key_t ktmp1=k;
        ham_key_t ktmp2(k);
        ham_record_t rtmp1=r;
        ham_record_t rtmp2(r);

        db.create(".test");
        db.insert(&k, &r);
        db.find(&k, &out);
        CPPUNIT_ASSERT_EQUAL(r.get_size(), out.get_size());
        CPPUNIT_ASSERT_EQUAL(0,
                        ::memcmp(r.get_data(), out.get_data(), out.get_size()));
        db.erase(&k);
        try {
            db.find(&k, &out);
        }
        catch(error &e) {
            CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, e.get_errno());
        }

        db.close();
        db.open(".test");
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(CppApiTest);

