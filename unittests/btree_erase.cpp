/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/version.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class EraseTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    EraseTest(ham_u32_t flags=0, const char *name="EraseTest")
        : hamsterDB_fixture(name),
        m_db(0), m_flags(flags)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(EraseTest, collapseRootTest);
        BFC_REGISTER_TEST(EraseTest, shiftFromRightTest);
        BFC_REGISTER_TEST(EraseTest, shiftFromLeftTest);
        BFC_REGISTER_TEST(EraseTest, mergeWithLeftTest);
    }

protected:
    ham_db_t *m_db;
    ham_u32_t m_flags;

public:
    virtual void setup()
    {
        __super::setup();

        os::unlink(BFC_OPATH(".test"));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags, 0644));
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    void prepare(int num_inserts)
    {
        ham_key_t key;
        ham_record_t rec;

        ham_parameter_t ps[]={
            {HAM_PARAM_PAGESIZE,   1024},
            {HAM_PARAM_KEYSIZE,   128},
            {0, 0}
        };

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_create_ex(m_db, BFC_OPATH(".test"), m_flags, 0644, &ps[0]));

        for (int i=0; i<num_inserts*10; i+=10) {
            key.data=&i;
            rec.data=&i;
            key.size=sizeof(i);
            rec.size=sizeof(i);

            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
        }
    }

    void collapseRootTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        prepare(8);

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_erase(m_db, 0, &key, 0));

        for (int i=0; i<80; i+=10) {
            key.data=&i;
            key.size=sizeof(i);

            BFC_ASSERT_EQUAL(0,
                    ham_erase(m_db, 0, &key, 0));
        }
    }

    void shiftFromRightTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));
        int i=0;

        prepare(8);

        key.data=&i;
        key.size=sizeof(i);

        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void shiftFromLeftTest() {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        int i;

        prepare(8);

        i=21;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        i=22;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        i=23;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        i=70;
        key.data=&i;
        key.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));

        i=60;
        key.data=&i;
        key.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));

        i=50;
        key.data=&i;
        key.size=sizeof(i);
        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void mergeWithLeftTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        prepare(8);

        for (int i=70; i>=50; i-=10) {
            key.data=&i;
            key.size=sizeof(i);

            BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        }
    }
};

class InMemoryEraseTest : public EraseTest
{
public:
    InMemoryEraseTest()
        : EraseTest(HAM_IN_MEMORY_DB, "InMemoryEraseTest")
    {
    }
};

BFC_REGISTER_FIXTURE(EraseTest);
BFC_REGISTER_FIXTURE(InMemoryEraseTest);

