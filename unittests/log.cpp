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
#include <vector>
#include <sstream>

#include "../src/internal_fwd_decl.h"
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/os.h"
#include "../src/db.h"
#include "../src/device.h"
#include "../src/env.h"
#include "../src/btree.h"
#include "../src/btree_key.h"
#include "../src/freelist.h"
#include "../src/cache.h"
#include "memtracker.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

/* this function pointer is defined in changeset.c */
typedef void (*hook_func_t)(void);
extern hook_func_t g_CHANGESET_POST_LOG_HOOK;

class LogTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    LogTest()
        : hamsterDB_fixture("LogTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(LogTest, structHeaderTest);
        BFC_REGISTER_TEST(LogTest, structEntryTest);
        BFC_REGISTER_TEST(LogTest, structLogTest);
        BFC_REGISTER_TEST(LogTest, createCloseTest);
        BFC_REGISTER_TEST(LogTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(LogTest, negativeCreateTest);
        BFC_REGISTER_TEST(LogTest, negativeOpenTest);
        BFC_REGISTER_TEST(LogTest, appendWriteTest);
        BFC_REGISTER_TEST(LogTest, clearTest);
        BFC_REGISTER_TEST(LogTest, iterateOverEmptyLogTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogOneEntryTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogMultipleEntryTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        m_alloc=memtracker_new();
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS, 0644));
    
        m_env=db_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    ham_log_t *disconnect_log_and_create_new_log(void)
    {
        ham_log_t *log;
        ham_env_t *env=db_get_env(m_db);

        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, log_create(env, 0644, 0, &log));
        BFC_ASSERT_NULL(log);

        /* 
         * make sure db->log is already NULL, i.e. disconnected. 
         * Otherwise an BFC ASSERT for log_close() will segfault 
         * the teardown() code, which will try to close the db->log 
         * all over AGAIN! 
         */
        log = env_get_log(env);
        env_set_log(env, NULL);
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, log_create(env, 0644, 0, &log));
        BFC_ASSERT_NOTNULL(log);
        return log;
    }

    void structHeaderTest()
    {
        log_header_t hdr;

        log_header_set_magic(&hdr, 0x1234);
        BFC_ASSERT_EQUAL((ham_u32_t)0x1234, log_header_get_magic(&hdr));

        log_header_set_lsn(&hdr, 0x888ull);
        BFC_ASSERT_EQUAL((ham_u64_t)0x888ull, log_header_get_lsn(&hdr));
    }

    void structEntryTest()
    {
        log_entry_t e;

        log_entry_set_lsn(&e, 0x13);
        BFC_ASSERT_EQUAL((ham_u64_t)0x13, log_entry_get_lsn(&e));

        log_entry_set_txn_id(&e, 0x15);
        BFC_ASSERT_EQUAL((ham_u64_t)0x15, log_entry_get_txn_id(&e));

        log_entry_set_offset(&e, 0x22);
        BFC_ASSERT_EQUAL((ham_u64_t)0x22, log_entry_get_offset(&e));

        log_entry_set_data_size(&e, 0x16);
        BFC_ASSERT_EQUAL((ham_u64_t)0x16, log_entry_get_data_size(&e));

        log_entry_set_flags(&e, 0xff000000);
        BFC_ASSERT_EQUAL((ham_u32_t)0xff000000, log_entry_get_flags(&e));

        log_entry_set_type(&e, 13u);
        BFC_ASSERT_EQUAL(13u, log_entry_get_type(&e));
    }

    void structLogTest(void)
    {
        ham_log_t log;

        BFC_ASSERT_NOTNULL(env_get_log(m_env));

        log_set_allocator(&log, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL((mem_allocator_t *)m_alloc, 
                        log_get_allocator(&log));

        log_set_flags(&log, 0x13);
        BFC_ASSERT_EQUAL((ham_u32_t)0x13, log_get_flags(&log));

        log_set_fd(&log, (ham_fd_t)0x20);
        BFC_ASSERT_EQUAL((ham_fd_t)0x20, log_get_fd(&log));
        log_set_fd(&log, HAM_INVALID_FD);

        log_set_lsn(&log, 0x17ull);
        BFC_ASSERT_EQUAL((ham_u64_t)0x17ull, log_get_lsn(&log));
    }

    void createCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();

        BFC_ASSERT_EQUAL(0u, log_get_flags(log));
        /* TODO make sure that the file exists and 
         * contains only the header */

        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));

        BFC_ASSERT_EQUAL(0, log_open(m_env, 0, &log));
        BFC_ASSERT(log!=0);
        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void negativeCreateTest(void)
    {
        ham_log_t *log;
        const char *oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "/::asdf");
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                log_create(m_env, 0644, 0, &log));
        BFC_ASSERT_EQUAL((ham_log_t *)0, log);
        env_set_filename(m_env, oldfilename);
    }

    void negativeOpenTest(void)
    {
        ham_fd_t fd;
        ham_log_t *log;
        const char *oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                log_open(m_env, 0, &log));

        /* if log_open() fails, it will call log_close() internally and 
         * log_close() overwrites the header structure. therefore we have
         * to patch the file before we start the test. */
        BFC_ASSERT_EQUAL(0, os_open("data/log-broken-magic.log0", 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, (void *)"x", 1));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        env_set_filename(m_env, "data/log-broken-magic");
        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, 
                log_open(m_env, 0, &log));

        env_set_filename(m_env, oldfilename);
    }

    void appendWriteTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        BFC_ASSERT_EQUAL(0, log_append_write(log, txn, 1,
                                0, data, sizeof(data)));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void clearTest(void)
    {
        ham_bool_t isempty;
        ham_u8_t data[1024]={0};
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, log_append_write(log, txn, 1,
                                0, data, sizeof(data)));

        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);

        BFC_ASSERT_EQUAL(0, log_clear(log));
        BFC_ASSERT_EQUAL(0, log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void iterateOverEmptyLogTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);

        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t *txn;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, log_append_write(log, txn, 1,
                                0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, log_open(m_env, 0, &log));
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_txn_id(&entry));
        BFC_ASSERT_EQUAL((ham_u32_t)1024, log_entry_get_data_size(&entry));
        BFC_ASSERT_NOTNULL(data);
        BFC_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_WRITE, 
                        log_entry_get_type(&entry));

        BFC_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void checkLogEntry(ham_log_t *log, log_entry_t *entry, ham_u64_t lsn, 
                ham_u64_t txn_id, ham_u32_t type, ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, log_entry_get_lsn(entry));
        BFC_ASSERT_EQUAL(txn_id, log_entry_get_txn_id(entry));
        if (log_entry_get_data_size(entry)==0) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            allocator_free(log_get_allocator(log), data);
        }
        BFC_ASSERT_EQUAL(type, log_entry_get_type(entry));
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        ham_log_t *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            ham_page_t *page;
            page=page_new(m_env);
            BFC_ASSERT_EQUAL(0, page_alloc(page));
            BFC_ASSERT_EQUAL(0, log_append_page(log, page, 1+i));
            BFC_ASSERT_EQUAL(0, page_free(page));
            page_delete(page);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, log_open(m_env, 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(log, &entry, 5, 0, LOG_ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)log_entry_get_data_size(&entry));
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(log, &entry, 4, 0, LOG_ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)log_entry_get_data_size(&entry));
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(log, &entry, 3, 0, LOG_ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)log_entry_get_data_size(&entry));
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(log, &entry, 2, 0, LOG_ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)log_entry_get_data_size(&entry));
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(log, &entry, 1, 0, LOG_ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)log_entry_get_data_size(&entry));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }
};

struct LogEntry
{
    LogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, 
                ham_u64_t _offset, ham_u64_t _data_size)
    :   lsn(_lsn), txn_id(_txn_id), offset(_offset),
        type(LOG_ENTRY_TYPE_WRITE), data_size(_data_size)
    {
    }

    ham_u64_t lsn;
    ham_u64_t txn_id;
    ham_u64_t offset;
    ham_u32_t type;
    ham_u64_t data_size;
};

class LogHighLevelTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    LogHighLevelTest()
        : hamsterDB_fixture("LogHighLevelTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseEnvTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenFullLogTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenFullLogRecoverTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenCloseEnvTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenFullLogEnvTest);
        BFC_REGISTER_TEST(LogHighLevelTest, createCloseOpenFullLogEnvRecoverTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverAllocatePageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverAllocateMultiplePageTest);
#if 0
        BFC_REGISTER_TEST(LogHighLevelTest, allocatePageFromFreelistTest);
        BFC_REGISTER_TEST(LogHighLevelTest, allocateClearedPageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, singleInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, doubleInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, splitInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, singleEraseTest);
        BFC_REGISTER_TEST(LogHighLevelTest, eraseMergeTest);
        BFC_REGISTER_TEST(LogHighLevelTest, cursorOverwriteTest);
        BFC_REGISTER_TEST(LogHighLevelTest, singleBlobTest);
        BFC_REGISTER_TEST(LogHighLevelTest, largeBlobTest);
        BFC_REGISTER_TEST(LogHighLevelTest, insertDuplicateTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedPageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedPageTest2);
        BFC_REGISTER_TEST(LogHighLevelTest, redoInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, redoMultipleInsertsTest);
        BFC_REGISTER_TEST(LogHighLevelTest, negativeAesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterRecoverTest);
#endif
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        m_alloc=memtracker_new();
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        //db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS
                        | HAM_ENABLE_RECOVERY
                        | HAM_ENABLE_DUPLICATES, 0644));

        m_env=db_get_env(m_db);
    }

    void open(void)
    {
        // open without recovery and transactions (they imply recovery)!
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    void createCloseTest(void)
    {
        BFC_ASSERT_NOTNULL(env_get_log(m_env));
    }

    void createCloseEnvTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT(env_get_log(env) != 0);
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT(env_get_log(env)==0);
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        m_env=db_get_env(m_db);
        BFC_ASSERT(env_get_log(m_env)!=0);
    }

    void createCloseOpenFullLogRecoverTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(env_get_pagesize(m_env));
        memset(buffer, 0, env_get_pagesize(m_env));
        ham_size_t ps=env_get_pagesize(m_env);

        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 2,
                                ps, buffer, ps));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);

        /* make sure that the log file was deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(m_env);
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);

        free(buffer);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(env_get_pagesize(m_env));
        memset(buffer, 0, env_get_pagesize(m_env));

        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, env_get_pagesize(m_env)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));

        free(buffer);
    }

    void createCloseOpenCloseEnvTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT(env_get_log(env)==0);

        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT(env_get_log(env)!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenFullLogEnvTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(env_get_pagesize(m_env));
        memset(buffer, 0, env_get_pagesize(m_env));

        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, env_get_pagesize(m_env)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT(env_get_log(env)==0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));

        free(buffer);
    }

    void createCloseOpenFullLogEnvRecoverTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(env_get_pagesize(m_env));
        memset(buffer, 0, env_get_pagesize(m_env));

        BFC_ASSERT_EQUAL(0, log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, env_get_pagesize(m_env)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(env);
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));

        free(buffer);
    }

    static void copyLog(void)
    {
        assert(true==os::copy(BFC_OPATH(".test.log0"), 
                    BFC_OPATH(".test2.log0")));
    }

    static void restoreLog(void)
    {
        assert(true==os::copy(BFC_OPATH(".test2.log0"), 
                    BFC_OPATH(".test.log0")));
    }

    void compareLog(const char *filename, LogEntry e)
    {
        std::vector<LogEntry> v;
        v.push_back(e);
        compareLog(filename, v);
    }

    void compareLog(const char *filename, std::vector<LogEntry> &vec) 
    {
        log_entry_t entry;
        log_iterator_t it={0};
        ham_u8_t *data;
        size_t size=0;
        ham_log_t *log; 
        ham_env_t *env;
        std::vector<LogEntry>::iterator vit=vec.begin();

        /* for traversing the logfile we need a temp. Env handle */
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, filename, 0, 0664));
        BFC_ASSERT_EQUAL(0, log_open(env, 0, &log));

        while (1) {
            BFC_ASSERT_EQUAL(0, log_get_entry(log, &it, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;

            if (vit==vec.end()) {
                BFC_ASSERT_EQUAL(0ull, log_entry_get_lsn(&entry));
                break;
            }
            size++;

            BFC_ASSERT_EQUAL((*vit).lsn, log_entry_get_lsn(&entry));
            BFC_ASSERT_EQUAL((*vit).txn_id, log_entry_get_txn_id(&entry));
            BFC_ASSERT_EQUAL((*vit).offset, log_entry_get_offset(&entry));
            BFC_ASSERT_EQUAL((*vit).type, log_entry_get_type(&entry));
            BFC_ASSERT_EQUAL((*vit).data_size, log_entry_get_data_size(&entry));

            allocator_free(log_get_allocator(log), data);

            vit++;
        }

        BFC_ASSERT_EQUAL(vec.size(), size);

        BFC_ASSERT_EQUAL(0, log_close(log, HAM_TRUE));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void recoverAllocatePageTest(void)
    {
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT_EQUAL(ps*2, page_get_self(page));
        for (int i=0; i<200; i++)
            page_get_payload(page)[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, changeset_flush(env_get_changeset(m_env), 1));
        changeset_clear(env_get_changeset(m_env));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now truncate the file - after all we want to make sure that 
         * the log appends the new page */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*2));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        compareLog(BFC_OPATH(".test2"), LogEntry(1, 0, ps*2, ps));

        /* recover and make sure that the page exists */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, m_db, ps*2, 0));
        /* verify that the page contains the marker */
        for (int i=0; i<200; i++)
            BFC_ASSERT_EQUAL((ham_u8_t)i, page_get_payload(page)[i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(1ull, log_get_lsn(env_get_log(m_env)));
    }

    void recoverAllocateMultiplePageTest(void)
    {
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page[10];

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], m_db, 0, PAGE_IGNORE_FREELIST));
            BFC_ASSERT_EQUAL(ps*(2+i), page_get_self(page[i]));
            for (int j=0; j<200; j++)
                page_get_payload(page[i])[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, changeset_flush(env_get_changeset(m_env), 33));
        changeset_clear(env_get_changeset(m_env));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now truncate the file - after all we want to make sure that 
         * the log appends the new page */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*2));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        std::vector<LogEntry> vec;
        for (int i=0; i<10; i++)
            vec.push_back(LogEntry(33, 0, ps*(2+i), ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* recover and make sure that the pages exists */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], m_db, ps*(2+i), 0));
            /* verify that the pages contain the markers */
            for (int j=0; j<200; j++)
                BFC_ASSERT_EQUAL((ham_u8_t)(i+j), page_get_payload(page[i])[j]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(33ull, log_get_lsn(env_get_log(m_env)));
    }

    void recoverModifiedPageTest(void)
    {
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT_EQUAL(ps*2, page_get_self(page));
        BFC_ASSERT_EQUAL(0, changeset_flush(env_get_changeset(m_env), 1));
        changeset_clear(env_get_changeset(m_env));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* modify the page - we want that the page in the file contains
         * a unique marker, but the page in the log does not */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, m_db, ps, 0));
        for (int i=0; i<200; i++)
            page_get_payload(page)[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, page_flush(page));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* now truncate the file - after all we want to make sure that 
         * the log appends the new page */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*2));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        compareLog(BFC_OPATH(".test2"), LogEntry(1, 0, ps*2, ps));

        /* recover and make sure that the page exists */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, m_db, ps, 0));
        /* verify that the page does NOT contain the marker */
        for (int i=0; i<200; i++)
            BFC_ASSERT_EQUAL(0, page_get_payload(page)[i]);
    }

    void negativeAesFilterTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        /* close m_db, otherwise ham_env_create fails */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_u8_t aeskey[16] ={0x13};

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_NOT_INITIALIZED, 
                    ham_env_enable_encryption(env, aeskey, 0));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 
                    0, 0664));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
#endif
    }

    void aesFilterTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        /* close m_db, otherwise ham_env_create fails */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 
                    HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_open(env, BFC_OPATH(".test"), 
                    HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void aesFilterRecoverTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        /* close m_db, otherwise ham_env_create fails on win32 */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 
                    HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }
};

BFC_REGISTER_FIXTURE(LogTest);
BFC_REGISTER_FIXTURE(LogHighLevelTest);

