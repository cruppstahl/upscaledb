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

#include <stdexcept>
#include <cstring>
#include <ham/hamsterdb_int.h>
#include "memtracker.h"
#include "../src/db.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"

using namespace bfc;

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

class FilterTest : public fixture
{
public:
    FilterTest()
        : fixture("FilterTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(FilterTest, addRemoveFileTest);
        BFC_REGISTER_TEST(FilterTest, addRemoveRecordTest);
        BFC_REGISTER_TEST(FilterTest, simpleFileFilterTest);
        BFC_REGISTER_TEST(FilterTest, cascadedFileFilterTest);
        BFC_REGISTER_TEST(FilterTest, simpleRecordFilterTest);
        BFC_REGISTER_TEST(FilterTest, aesFilterTest);
        BFC_REGISTER_TEST(FilterTest, aesFilterInMemoryTest);
        BFC_REGISTER_TEST(FilterTest, aesTwiceFilterTest);
        BFC_REGISTER_TEST(FilterTest, negativeAesFilterTest);
        BFC_REGISTER_TEST(FilterTest, zlibFilterTest);
        BFC_REGISTER_TEST(FilterTest, zlibFilterEmptyRecordTest);
        BFC_REGISTER_TEST(FilterTest, zlibEnvFilterTest);
    }

protected:
    ham_db_t *m_db;
    ham_u32_t m_flags;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        m_flags=0;

        os::unlink(".test");
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    void teardown() 
    { 
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void addRemoveFileTest()
    {
        ham_env_t *env;
        ham_file_filter_t filter1, filter2, filter3;
        memset(&filter1, 0, sizeof(filter1));
        memset(&filter2, 0, sizeof(filter2));
        memset(&filter3, 0, sizeof(filter3));

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_add_file_filter(0, &filter1));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_add_file_filter(env, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_remove_file_filter(0, &filter1));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_remove_file_filter(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        BFC_ASSERT(filter1._next==0);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        BFC_ASSERT(filter1._next==&filter2);
        BFC_ASSERT(filter2._prev==&filter1);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter2._next==0);
        BFC_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter3));
        BFC_ASSERT(filter1._next==&filter2);
        BFC_ASSERT(filter2._prev==&filter1);
        BFC_ASSERT(filter2._next==&filter3);
        BFC_ASSERT(filter3._prev==&filter2);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter3._next==0);
        BFC_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter2));
        BFC_ASSERT(filter1._next==&filter3);
        BFC_ASSERT(filter3._prev==&filter1);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter3._next==0);
        BFC_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter3));
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter1._next==0);
        BFC_ASSERT_EQUAL(&filter1, env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_remove_file_filter(env, &filter1));
        BFC_ASSERT(0==env_get_file_filter(env));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void addRemoveRecordTest()
    {
        ham_record_filter_t filter1, filter2, filter3;
        memset(&filter1, 0, sizeof(filter1));
        memset(&filter2, 0, sizeof(filter2));
        memset(&filter3, 0, sizeof(filter3));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_add_record_filter(0, &filter1));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_add_record_filter(m_db, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_remove_record_filter(0, &filter1));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_remove_record_filter(m_db, 0));

        BFC_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter1));
        BFC_ASSERT(filter1._next==0);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter2));
        BFC_ASSERT(filter1._next==&filter2);
        BFC_ASSERT(filter2._prev==&filter1);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter2._next==0);
        BFC_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter3));
        BFC_ASSERT(filter1._next==&filter2);
        BFC_ASSERT(filter2._prev==&filter1);
        BFC_ASSERT(filter2._next==&filter3);
        BFC_ASSERT(filter3._prev==&filter2);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter3._next==0);
        BFC_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter2));
        BFC_ASSERT(filter1._next==&filter3);
        BFC_ASSERT(filter3._prev==&filter1);
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter3._next==0);
        BFC_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter3));
        BFC_ASSERT(filter1._prev==0);
        BFC_ASSERT(filter1._next==0);
        BFC_ASSERT_EQUAL(&filter1, db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_remove_record_filter(m_db, &filter1));
        BFC_ASSERT(0==db_get_record_filter(m_db));

        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 333, 0, 0));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(1, sf.written);
        BFC_ASSERT_EQUAL(1, sf.read);
        BFC_ASSERT_EQUAL(1, sf.closed);

        memset(&sf, 0, sizeof(sf));
        BFC_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(0, sf.read);
        BFC_ASSERT_EQUAL(0, sf.closed);

        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(1, sf.read);
        BFC_ASSERT_EQUAL(0, sf.closed);

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(1, sf.read);
        BFC_ASSERT_EQUAL(1, sf.closed);

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter1));
        BFC_ASSERT_EQUAL(0, ham_env_add_file_filter(env, &filter2));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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

        BFC_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter));

        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"123";
        rec.size=3;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        BFC_ASSERT_EQUAL(1, sf.written);
        BFC_ASSERT_EQUAL(0, sf.read);
        BFC_ASSERT_EQUAL(1, sf.closed);

        memset(&sf, 0, sizeof(sf));
        BFC_ASSERT_EQUAL(0, ham_add_record_filter(m_db, &filter));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(0, sf.read);
        BFC_ASSERT_EQUAL(0, sf.closed);

        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(1, sf.read);
        BFC_ASSERT_EQUAL(0, sf.closed);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, sf.written);
        BFC_ASSERT_EQUAL(1, sf.read);
        BFC_ASSERT_EQUAL(1, sf.closed);
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        BFC_ASSERT_EQUAL(HAM_ACCESS_DENIED, 
                ham_env_enable_encryption(env, aeskey2, 0));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_IN_MEMORY_DB, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey1, 0));
        BFC_ASSERT_EQUAL(HAM_ALREADY_INITIALIZED, 
                ham_env_enable_encryption(env, aeskey2, 0));

        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void negativeAesFilterTest(void)
    {
#ifndef HAM_DISABLE_ENCRYPTION
        ham_env_t *env=(ham_env_t *)1;
        ham_db_t *db;
        ham_u8_t aeskey[16]={0x13};

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                        ham_env_enable_encryption(0, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN, 
                        ham_env_enable_encryption(env, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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

        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_enable_compression(0, 0, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_enable_compression(m_db, 9999, 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        rec.flags=HAM_RECORD_USER_ALLOC;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }
    
    void zlibFilterEmptyRecordTest(void)
    {
#ifndef HAM_DISABLE_COMPRESSION
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0664));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(m_db, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
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

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db[0]));
        BFC_ASSERT_EQUAL(0, ham_new(&db[1]));
        BFC_ASSERT_EQUAL(0, ham_new(&db[2]));

        BFC_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db[0], 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db[1], 334, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db[2], 335, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_enable_compression(db[0], 3, 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(db[1], 8, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db[0], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db[1], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db[2], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db[0], 0));
        BFC_ASSERT_EQUAL(0, ham_close(db[1], 0));
        BFC_ASSERT_EQUAL(0, ham_close(db[2], 0));

        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db[0], 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db[1], 334, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db[2], 335, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(db[0], 3, 0));
        BFC_ASSERT_EQUAL(0, ham_enable_compression(db[1], 8, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db[0], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db[1], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db[2], 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db[0]));
        BFC_ASSERT_EQUAL(0, ham_delete(db[1]));
        BFC_ASSERT_EQUAL(0, ham_delete(db[2]));
#endif
    }

};

BFC_REGISTER_FIXTURE(FilterTest);

