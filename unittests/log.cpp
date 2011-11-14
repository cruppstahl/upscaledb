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
extern "C" {
typedef void (*hook_func_t)(void);
extern hook_func_t g_CHANGESET_POST_LOG_HOOK;
}

class LogTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    LogTest()
        : hamsterDB_fixture("LogTest")
    {
        testrunner::get_instance()->register_fixture(this);
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
    
        m_env=ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    Log *disconnect_log_and_create_new_log(void)
    {
        ham_env_t *env=ham_get_env(m_db);
        Log *log=new Log(env);
        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, log->create());
        delete log;

        log = env_get_log(env);
        BFC_ASSERT_EQUAL(0, log->close());
        BFC_ASSERT_EQUAL(0, log->create());
        BFC_ASSERT_NOTNULL(log);
        return (log);
    }

    void createCloseTest(void)
    {
        Log *log = disconnect_log_and_create_new_log();

        /* TODO make sure that the file exists and 
         * contains only the header */

        BFC_ASSERT_EQUAL(true, log->is_empty());

        BFC_ASSERT_EQUAL(0, log->close());
    }

    void createCloseOpenCloseTest(void)
    {
        Log *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(true, log->is_empty());
        BFC_ASSERT_EQUAL(0, log->close());

        BFC_ASSERT_EQUAL(0, log->open());
        BFC_ASSERT_EQUAL(true, log->is_empty());
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void negativeCreateTest(void)
    {
        Log *log=new Log(m_env);
        std::string oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "/::asdf");
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, log->create());
        env_set_filename(m_env, oldfilename);
        delete log;
    }

    void negativeOpenTest(void)
    {
        Log *log=new Log(m_env);
        ham_fd_t fd;
        std::string oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, log->open());

        /* if log->open() fails, it will call log->close() internally and 
         * log->close() overwrites the header structure. therefore we have
         * to patch the file before we start the test. */
        BFC_ASSERT_EQUAL(0, os_open("data/log-broken-magic.log0", 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, (void *)"x", 1));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        env_set_filename(m_env, "data/log-broken-magic");
        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, log->open());

        env_set_filename(m_env, oldfilename);
        delete log;
    }

    void appendWriteTest(void)
    {
        Log *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, data, sizeof(data)));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void clearTest(void)
    {
        ham_u8_t data[1024]={0};
        Log *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(true, log->is_empty());

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, data, sizeof(data)));
        BFC_ASSERT_EQUAL(false, log->is_empty());

        BFC_ASSERT_EQUAL(0, log->clear());
        BFC_ASSERT_EQUAL(true, log->is_empty());

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void iterateOverEmptyLogTest(void)
    {
        Log *log = disconnect_log_and_create_new_log();

        Log::Iterator iter=0;

        Log::Entry entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, entry.lsn);
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);

        BFC_ASSERT_EQUAL(0, log->close());
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t *txn;
        Log *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, log->close(true));

        BFC_ASSERT_EQUAL(0, log->open());
        BFC_ASSERT(log!=0);

        Log::Iterator iter=0;

        Log::Entry entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)1, entry.lsn);
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));
        BFC_ASSERT_EQUAL((ham_u32_t)1024, entry.data_size);
        BFC_ASSERT_NOTNULL(data);
        BFC_ASSERT_EQUAL((ham_u32_t)Log::ENTRY_TYPE_WRITE, entry.type);

        if (data)
            allocator_free(env_get_allocator(m_env), data);

        BFC_ASSERT_EQUAL((ham_u64_t)1, log->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void checkLogEntry(Log *log, Log::Entry *entry, ham_u64_t lsn, 
                ham_u32_t type, ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, entry->lsn);
        if (entry->data_size==0) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            allocator_free(env_get_allocator(m_env), data);
        }
        BFC_ASSERT_EQUAL(type, entry->type);
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        Log *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            ham_page_t *page;
            page=page_new(m_env);
            BFC_ASSERT_EQUAL(0, page_alloc(page));
            BFC_ASSERT_EQUAL(0, log->append_page(page, 1+i));
            BFC_ASSERT_EQUAL(0, page_free(page));
            page_delete(page);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=ham_get_env(m_db);
        BFC_ASSERT_EQUAL((Log *)0, env_get_log(m_env));
        log=new Log(m_env);
        BFC_ASSERT_EQUAL(0, log->open());
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        Log::Iterator iter=0;

        Log::Entry entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 5, Log::ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 4, Log::ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 3, Log::ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 2, Log::ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 1, Log::ENTRY_TYPE_WRITE, data);
        BFC_ASSERT_EQUAL(env_get_pagesize(m_env), 
                        (ham_size_t)entry.data_size);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }
};

struct LogEntry
{
    LogEntry(ham_u64_t _lsn, ham_u64_t _offset, ham_u64_t _data_size)
    :   lsn(_lsn), offset(_offset),
        type(Log::ENTRY_TYPE_WRITE), data_size(_data_size)
    {
    }

    ham_u64_t lsn;
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
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedPageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedMultiplePageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverMixedAllocatedModifiedPageTest);
        /* modify header page by using the freelist */
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedHeaderPageTest);
        /* modify header page by creating a database */
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedHeaderPageTest2);
        /* modify header page by erasing a database */
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedHeaderPageTest3);
        BFC_REGISTER_TEST(LogHighLevelTest, recoverModifiedFreelistTest);
        BFC_REGISTER_TEST(LogHighLevelTest, negativeAesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterRecoverTest);
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

        m_env=ham_get_env(m_db);
    }

    void open(void)
    {
        // open without recovery and transactions (they imply recovery)!
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        if (m_db) {
            BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
            ham_delete(m_db);
        }
        m_db=0;
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
        m_env=ham_get_env(m_db);
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
                    env_get_log(m_env)->append_write(2, ps, buffer, ps));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=ham_get_env(m_db);

        /* make sure that the log file was deleted and that the lsn is 1 */
        Log *log=env_get_log(m_env);
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log->get_fd(), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(Log::Header), filesize);

        free(buffer);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(env_get_pagesize(m_env));
        memset(buffer, 0, env_get_pagesize(m_env));

        BFC_ASSERT_EQUAL(0, 
                    env_get_log(m_env)->append_write(1,
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
                    env_get_log(m_env)->append_write(1,
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

        BFC_ASSERT_EQUAL(0, env_get_log(m_env)->append_write(1,
                                0, buffer, env_get_pagesize(m_env)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        Log *log=env_get_log(env);
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log->get_fd(), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(Log::Header), filesize);

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
        Log::Entry entry;
        Log::Iterator iter=0;
        ham_u8_t *data;
        size_t size=0;
        Log *log; 
        ham_env_t *env;
        std::vector<LogEntry>::iterator vit=vec.begin();

        /* for traversing the logfile we need a temp. Env handle */
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_env_create(env, filename, 0, 0664));
        log=env_get_log(env);
        BFC_ASSERT_EQUAL((Log *)0, log);
        log=new Log(env);
        BFC_ASSERT_EQUAL(0, log->open());

        while (1) {
            BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
            if (entry.lsn==0)
                break;

            if (vit==vec.end()) {
                BFC_ASSERT_EQUAL(0ull, entry.lsn);
                break;
            }
            size++;

            BFC_ASSERT_EQUAL((*vit).lsn, entry.lsn);
            BFC_ASSERT_EQUAL((*vit).offset, entry.offset);
            BFC_ASSERT_EQUAL((*vit).type, entry.type);
            BFC_ASSERT_EQUAL((*vit).data_size, entry.data_size);

            if (data)
                allocator_free(env_get_allocator(env), data);

            vit++;
        }

        if (data)
            allocator_free(env_get_allocator(env), data);
        BFC_ASSERT_EQUAL(vec.size(), size);

        BFC_ASSERT_EQUAL(0, log->close(true));
        delete log;
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void recoverAllocatePageTest(void)
    {
#ifndef WIN32
        Database *db=(Database *)m_db;
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, db, 0, PAGE_IGNORE_FREELIST));
        page_set_dirty(page);
        BFC_ASSERT_EQUAL(ps*2, page_get_self(page));
        for (int i=0; i<200; i++)
            page_get_payload(page)[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(1));
        env_get_changeset(m_env).clear();
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
        compareLog(BFC_OPATH(".test2"), LogEntry(1, ps*2, ps));

        /* recover and make sure that the page exists */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, db, ps*2, 0));
        /* verify that the page contains the marker */
        for (int i=0; i<200; i++)
            BFC_ASSERT_EQUAL((ham_u8_t)i, page_get_payload(page)[i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(1ull, env_get_log(m_env)->get_lsn());

        env_get_changeset(m_env).clear();
#endif
    }

    void recoverAllocateMultiplePageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page_set_dirty(page[i]);
            BFC_ASSERT_EQUAL(ps*(2+i), page_get_self(page[i]));
            for (int j=0; j<200; j++)
                page_get_payload(page[i])[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(33));
        env_get_changeset(m_env).clear();
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
            vec.push_back(LogEntry(33, ps*(2+i), ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* recover and make sure that the pages exists */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            /* verify that the pages contain the markers */
            for (int j=0; j<200; j++)
                BFC_ASSERT_EQUAL((ham_u8_t)(i+j), page_get_payload(page[i])[j]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(33ull, env_get_log(m_env)->get_lsn());

        env_get_changeset(m_env).clear();
#endif
	}

    void recoverModifiedPageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page;
        Database *db=(Database *)m_db;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, db, 0, PAGE_IGNORE_FREELIST));
        page_set_dirty(page);
        BFC_ASSERT_EQUAL(ps*2, page_get_self(page));
        for (int i=0; i<200; i++)
            page_get_payload(page)[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(2));
        env_get_changeset(m_env).clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now modify the file - after all we want to make sure that 
         * the recovery overwrites the modification */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, ps*2, "XXXXXXXXXXXXXXXXXXXX", 20));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        compareLog(BFC_OPATH(".test2"), LogEntry(2, ps*2, ps));

        /* recover and make sure that the page is ok */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, db, ps*2, 0));
        /* verify that the page does not contain the "XXX..." */
        for (int i=0; i<20; i++)
            BFC_ASSERT_NOTEQUAL('X', page_get_raw_payload(page)[i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(2ull, env_get_log(m_env)->get_lsn());

        env_get_changeset(m_env).clear();
#endif
	}

    void recoverModifiedMultiplePageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page_set_dirty(page[i]);
            BFC_ASSERT_EQUAL(ps*(2+i), page_get_self(page[i]));
            for (int j=0; j<200; j++)
                page_get_payload(page[i])[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(5));
        env_get_changeset(m_env).clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now modify the file - after all we want to make sure that 
         * the recovery overwrites the modification */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, os_pwrite(fd, ps*(2+i), 
                                "XXXXXXXXXXXXXXXXXXXX", 20));
        }
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        std::vector<LogEntry> vec;
        for (int i=0; i<10; i++)
            vec.push_back(LogEntry(5, ps*(2+i), ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* recover and make sure that the page is ok */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        /* verify that the pages does not contain the "XXX..." */
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            for (int j=0; j<20; j++)
                BFC_ASSERT_NOTEQUAL('X', page_get_raw_payload(page[i])[i]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(5ull, env_get_log(m_env)->get_lsn());

        env_get_changeset(m_env).clear();
#endif
    }

    void recoverMixedAllocatedModifiedPageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page_set_dirty(page[i]);
            BFC_ASSERT_EQUAL(ps*(2+i), page_get_self(page[i]));
            for (int j=0; j<200; j++)
                page_get_payload(page[i])[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(6));
        env_get_changeset(m_env).clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now modify the file - after all we want to make sure that 
         * the recovery overwrites the modification, and then truncate
         * the file */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, os_pwrite(fd, ps*(2+i), 
                                "XXXXXXXXXXXXXXXXXXXX", 20));
        }
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*7));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one alloc-page entry */
        std::vector<LogEntry> vec;
        for (int i=0; i<10; i++)
            vec.push_back(LogEntry(6, ps*(2+i), ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* recover and make sure that the pages are ok */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        /* verify that the pages do not contain the "XXX..." */
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            for (int j=0; j<20; j++)
                BFC_ASSERT_NOTEQUAL('X', page_get_raw_payload(page[i])[i]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(6ull, env_get_log(m_env)->get_lsn());

        env_get_changeset(m_env).clear();
#endif
    }

    void recoverModifiedHeaderPageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=env_get_pagesize(m_env);
        ham_page_t *page;
        Database *db=(Database *)m_db;

        /* modify the header page by updating the freelist */
        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_env, db, ps, DB_CHUNKSIZE, HAM_FALSE));

        /* flush and backup the logs */
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(9));
        env_get_changeset(m_env).clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* now modify the file - after all we want to make sure that 
         * the recovery overwrites the modification */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, ps-20,
                                "XXXXXXXXXXXXXXXXXXXX", 20));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* make sure that the log has one entry - the header file */
        compareLog(BFC_OPATH(".test2"), LogEntry(9, 0, ps));

        /* recover and make sure that the header page was restored */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);
        page=env_get_header_page(m_env);
        /* verify that the page does not contain the "XXX..." */
        for (int i=0; i<20; i++)
            BFC_ASSERT_NOTEQUAL('X', page_get_raw_payload(page)[ps-20+i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(9ull, env_get_log(m_env)->get_lsn());
#endif
    }

    void recoverModifiedHeaderPageTest2(void)
    {
#ifndef WIN32
        teardown(); m_env=0;

        ham_env_t *env;
        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));

        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;

        /* modify the header page by creating a database; then stop
         * creating backups of the logfile and remove the database again */
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        ham_size_t ps=env_get_pagesize(env);
        g_CHANGESET_POST_LOG_HOOK=0;
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_erase_db(env, 333, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        /* verify that the database does not exist */
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* make sure that the log has two entries - the header file 
         * and the root page of the new database */
        std::vector<LogEntry> vec;
        vec.push_back(LogEntry(1, ps, ps));
        vec.push_back(LogEntry(1, 0, ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* now modify the file and remove the root page of the new database */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*2));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* open the database again and recover; the modified header
         * page and the allocated root page must be re-created */
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db, 333, 0, 0));

        /* verify the lsn */
        BFC_ASSERT_EQUAL(1ull, env_get_log(env)->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        ham_delete(db);
        ham_env_delete(env);
#endif
    }

    void recoverModifiedHeaderPageTest3(void)
    {
#ifndef WIN32
        teardown(); m_env=0;

        ham_env_t *env;
        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));

        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;

        /* modify the header page by erasing a database; then stop
         * creating backups of the logfile; then re-create the database */
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        ham_size_t ps=env_get_pagesize(env);
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_erase_db(env, 333, 0));
        g_CHANGESET_POST_LOG_HOOK=0;
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        /* verify that the database exists */
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        /* restore the backupped logfiles */
        restoreLog();

        /* make sure that the log has one entry - the header file */
        std::vector<LogEntry> vec;
        vec.push_back(LogEntry(2, 0, ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* open the database again and recover; the database must be
         * erased again */
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                ham_env_open_db(env, db, 333, 0, 0));

        /* verify the lsn */
        BFC_ASSERT_EQUAL(2ull, env_get_log(env)->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        ham_delete(db);
        ham_env_delete(env);
#endif
    }

    void recoverModifiedFreelistTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_offset_t o=env_get_usable_pagesize(m_env)*8*DB_CHUNKSIZE;
        ham_size_t ps=env_get_pagesize(m_env);
        Database *db=(Database *)m_db;

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_env, db, 3*o, DB_CHUNKSIZE, HAM_FALSE));

        /* flush and backup the logs */
        BFC_ASSERT_EQUAL(0, env_get_changeset(m_env).flush(19));
        env_get_changeset(m_env).clear();
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)0;
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* now truncate the file - we want to make sure that the freelist
         * pages are restored after recovery */
        ham_fd_t fd;
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, ps*2));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        /* restore the backupped logfiles */
        restoreLog();

        /* make sure that the log has created and updated all the 
         * freelist pages */
        std::vector<LogEntry> vec;
        for (int i=0; i<5; i++)
            if (i!=1) /* 2nd page is root-page of the btree */
                vec.push_back(LogEntry(19, ps*i, ps));
        compareLog(BFC_OPATH(".test2"), vec);

        /* recover and make sure that the freelist was restored */
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        db=(Database *)m_db;
        m_env=ham_get_env(m_db);

        /*
         * The hinters must be disabled for this test to succeed; at least
         * they need to be instructed to kick in late.
         */
        db->set_data_access_mode(
                db->get_data_access_mode() & 
                        ~(HAM_DAM_SEQUENTIAL_INSERT
                         | HAM_DAM_RANDOM_WRITE));

        ham_offset_t addr;
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, m_env, db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(3*o, addr);
        env_get_changeset(m_env).clear();

        /* verify the lsn */
        BFC_ASSERT_EQUAL(19ull, env_get_log(m_env)->get_lsn());
#endif
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
#ifndef WIN32
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
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        g_CHANGESET_POST_LOG_HOOK=0;
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        /* restore the backupped logfiles */
        restoreLog();

        BFC_ASSERT_EQUAL(0, ham_env_open(env, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        BFC_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
#endif
#endif
    }

    void aesFilterRecoverTest()
    {
#ifndef WIN32
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
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        g_CHANGESET_POST_LOG_HOOK=0;
        BFC_ASSERT_EQUAL(0, ham_erase(db, 0, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_DONT_CLEAR_LOG));

        /* restore the backupped logfiles */
        restoreLog();

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
#endif
    }
};

BFC_REGISTER_FIXTURE(LogTest);
BFC_REGISTER_FIXTURE(LogHighLevelTest);

