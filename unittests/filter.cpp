/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include <ham/hamsterdb_int.h>
#include "memtracker.h"
#include "../src/db.h"
#include "os.hpp"

typedef struct simple_filter_t
{
    int written;
    int read;
    int closed;
} simple_filter_t;

static ham_status_t
my_xor_pre_cb(ham_env_t *, ham_file_filter_t *filter, 
        ham_u8_t *buffer, ham_size_t size)
{
    char ch=*(char *)filter->userdata;
    for (ham_size_t i=0; i<size; i++)
        buffer[i]^=ch;
    return (0);
}

static ham_status_t
my_xor_post_cb(ham_env_t *, ham_file_filter_t *filter, 
        ham_u8_t *buffer, ham_size_t size)
{
    char ch=*(char *)filter->userdata;
    for (ham_size_t i=0; i<size; i++)
        buffer[i]^=ch;
    return (0);
}

static ham_status_t
my_file_pre_cb(ham_env_t *, ham_file_filter_t *filter, ham_u8_t *, ham_size_t)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->written++;
    return (0);
}

static ham_status_t
my_file_post_cb(ham_env_t *, ham_file_filter_t *filter, ham_u8_t *, ham_size_t)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->read++;
    return (0);
}

static void
my_file_close_cb(ham_env_t *, ham_file_filter_t *filter)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->closed++;
}

static ham_status_t
my_record_pre_cb(ham_db_t *, ham_record_filter_t *filter, ham_record_t *)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->written++;
    return (0);
}

static ham_status_t
my_record_post_cb(ham_db_t *, ham_record_filter_t *filter, ham_record_t *)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->read++;
    return (0);
}

static void
my_record_close_cb(ham_db_t *, ham_record_filter_t *filter)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->closed++;
}

class FilterTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(FilterTest);
    CPPUNIT_TEST      (addRemoveFileTest);
    CPPUNIT_TEST      (addRemoveRecordTest);
    CPPUNIT_TEST      (simpleFileFilterTest);
    CPPUNIT_TEST      (cascadedFileFilterTest);
    CPPUNIT_TEST      (simpleRecordFilterTest);
    CPPUNIT_TEST      (aesFilterTest);
    CPPUNIT_TEST      (aesFilterInMemoryTest);
    CPPUNIT_TEST      (aesTwiceFilterTest);
    CPPUNIT_TEST      (negativeAesFilterTest);
    CPPUNIT_TEST      (zlibFilterTest);
    CPPUNIT_TEST      (zlibFilterEmptyRecordTest);
    CPPUNIT_TEST      (zlibEnvFilterTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_u32_t m_flags;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        m_flags=0;

        os::unlink(".test");
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    void tearDown() 
    { 
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void addRemoveFileTest()
    {
        ham_env_t *env;
        ham_file_filter_t filter1, filter2, filter3;
        memset(&filter1, 0, sizeof(filter1));
        memset(&filter2, 0, sizeof(filter2));
        memset(&filter3, 0, sizeof(filter3));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_add_file_filter(0, &filter1));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_add_file_filter(env, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_remove_file_filter(0, &filter1));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_remove_file_filter(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter2._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter3));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter2._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter2);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter3));
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter1));
        CPPUNIT_ASSERT(0==env_get_file_filter(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void addRemoveRecordTest()
    {
        ham_record_filter_t filter1, filter2, filter3;
        memset(&filter1, 0, sizeof(filter1));
        memset(&filter2, 0, sizeof(filter2));
        memset(&filter3, 0, sizeof(filter3));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_add_record_filter(0, &filter1));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_add_record_filter(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_remove_record_filter(0, &filter1));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_remove_record_filter(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter1));
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter2._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter3));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter2._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter2);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter3));
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter1));
        CPPUNIT_ASSERT(0==db_get_record_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void simpleFileFilterTest()
    {
        ham_env_t *env;
        ham_db_t *db;

        simple_filter_t sf;
        ham_file_filter_t filter;
        memset(&filter, 0, sizeof(filter));
        memset(&sf, 0, sizeof(sf));
        filter.userdata=(void *)&sf;
        filter.before_write_cb=my_file_pre_cb;
        filter.after_read_cb=my_file_post_cb;
        filter.close_cb=my_file_close_cb;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 333, 0, 0));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(1, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);

        memset(&sf, 0, sizeof(sf));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(0, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void cascadedFileFilterTest()
    {
        ham_env_t *env;
        ham_db_t *db;

        char ch1=0x13, ch2=0x15;
        ham_file_filter_t filter1, filter2;
        memset(&filter1, 0, sizeof(filter1));
        filter1.userdata=(void *)&ch1;
        filter1.before_write_cb=my_xor_pre_cb;
        filter1.after_read_cb=my_xor_post_cb;
        memset(&filter2, 0, sizeof(filter2));
        filter2.userdata=(void *)&ch2;
        filter2.before_write_cb=my_xor_pre_cb;
        filter2.after_read_cb=my_xor_post_cb;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void simpleRecordFilterTest()
    {
        simple_filter_t sf;
        ham_record_filter_t filter;
        memset(&filter, 0, sizeof(filter));
        memset(&sf, 0, sizeof(sf));
        filter.userdata=(void *)&sf;
        filter.before_insert_cb=my_record_pre_cb;
        filter.after_read_cb=my_record_post_cb;
        filter.close_cb=my_record_close_cb;

        CPPUNIT_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"123";
        rec.size=3;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(1, sf.written);
        CPPUNIT_ASSERT_EQUAL(0, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);

        memset(&sf, 0, sizeof(sf));
        CPPUNIT_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(0, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);
    }

    void aesFilterTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};
        ham_u8_t aeskey2[16]={0x14};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(HAM_ACCESS_DENIED, 
                ham_env_enable_encryption(env, aeskey2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void aesFilterInMemoryTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_IN_MEMORY_DB, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void aesTwiceFilterTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey1[16]={0x13};
        ham_u8_t aeskey2[16]={0x14};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey1, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_ALREADY_INITIALIZED, 
                ham_env_enable_encryption(env, aeskey2, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void negativeAesFilterTest(void)
    {
#ifndef HAM_DISABLE_ENCRYPTION
        ham_env_t *env=(ham_env_t *)1;
        ham_db_t *db;
        ham_u8_t aeskey[16]={0x13};

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                        ham_env_enable_encryption(0, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN, 
                        ham_env_enable_encryption(env, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void zlibFilterTest()
    {
#ifndef HAM_DISABLE_COMPRESSION
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"hello world 12345 12345 12345 12345 12345";
        rec.size=(ham_size_t)strlen((char *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_enable_compression(0, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_enable_compression(m_db, 9999, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        rec.flags=HAM_RECORD_USER_ALLOC;
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }
    
    void zlibFilterEmptyRecordTest(void)
    {
#ifndef HAM_DISABLE_COMPRESSION
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }

    void zlibEnvFilterTest()
    {
#ifndef HAM_DISABLE_COMPRESSION
        ham_env_t *env;
        ham_db_t *db[3];
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        rec.data=(void *)"123";
        rec.size=(ham_size_t)strlen("123");

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[1]));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[2]));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[0], 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[1], 334, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[2], 335, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(db[0], 3, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(db[1], 8, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[0], 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[1], 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[2], 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db[0], 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db[1], 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db[2], 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[0], 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[1], 334, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[2], 335, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(db[0], 3, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_compression(db[1], 8, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db[0], 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db[1], 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db[2], 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[1]));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[2]));
#endif
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(FilterTest);

