/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
    Environment *m_env;

public:
    virtual void setup() 
    { 
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS, 0644));
    
        m_env=(Environment *)ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    Log *disconnect_log_and_create_new_log(void)
    {
        Log *log=new Log(m_env);
        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, log->create());
        delete log;

        log=m_env->get_log();
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
        Environment *env=(Environment *)m_env;
        Log *log=new Log(env);
        std::string oldfilename=env->get_filename();
        env->set_filename("/::asdf");
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, log->create());
        env->set_filename(oldfilename);
        delete log;
    }

    void negativeOpenTest(void)
    {
        Environment *env=(Environment *)m_env;
        Log *log=new Log(env);
        ham_fd_t fd;
        std::string oldfilename=env->get_filename();
        env->set_filename("xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, log->open());

        /* if log->open() fails, it will call log->close() internally and 
         * log->close() overwrites the header structure. therefore we have
         * to patch the file before we start the test. */
        BFC_ASSERT_EQUAL(0, os_open("data/log-broken-magic.log0", 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, (void *)"x", 1));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        env->set_filename("data/log-broken-magic");
        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, log->open());

        env->set_filename(oldfilename);
        delete log;
    }

    void appendWriteTest(void)
    {
        Log *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, 0, data, sizeof(data)));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void clearTest(void)
    {
        ham_u8_t data[1024]={0};
        Log *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(true, log->is_empty());

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, 0, data, sizeof(data)));
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
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, log->append_write(1, 0, 0, buffer, sizeof(buffer)));
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
        BFC_ASSERT_EQUAL((ham_u32_t)0, entry.flags);

        if (data)
            m_env->get_allocator()->free(data);

        BFC_ASSERT_EQUAL((ham_u64_t)1, log->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log->close());
    }

    void checkLogEntry(Log *log, Log::Entry *entry, ham_u64_t lsn, 
                ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, entry->lsn);
        if (entry->data_size==0) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            m_env->get_allocator()->free(data);
        }
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        Log *log=m_env->get_log();

        for (int i=0; i<5; i++) {
            Page *page;
            page=new Page(m_env);
            BFC_ASSERT_EQUAL(0, page->allocate());
            BFC_ASSERT_EQUAL(0, log->append_page(page, 1+i, 5-i));
            BFC_ASSERT_EQUAL(0, page->free());
            delete page;
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        BFC_ASSERT_EQUAL((Log *)0, m_env->get_log());
        log=new Log(m_env);
        BFC_ASSERT_EQUAL(0, log->open());
        m_env->set_log(log);
        BFC_ASSERT(log!=0);

        Log::Iterator iter=0;

        Log::Entry entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 5, data);
        BFC_ASSERT_EQUAL(m_env->get_pagesize(), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 4, data);
        BFC_ASSERT_EQUAL(m_env->get_pagesize(), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 3, data);
        BFC_ASSERT_EQUAL(m_env->get_pagesize(), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 2, data);
        BFC_ASSERT_EQUAL(m_env->get_pagesize(), 
                        (ham_size_t)entry.data_size);
        BFC_ASSERT_EQUAL(0, log->get_entry(&iter, &entry, &data));
        checkLogEntry(log, &entry, 1, data);
        BFC_ASSERT_EQUAL(m_env->get_pagesize(), 
                        (ham_size_t)entry.data_size);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }
};

struct LogEntry
{
    LogEntry(ham_u64_t _lsn, ham_u64_t _offset, ham_u64_t _data_size)
    :   lsn(_lsn), offset(_offset), data_size(_data_size)
    {
    }

    ham_u64_t lsn;
    ham_u64_t offset;
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
        BFC_REGISTER_TEST(LogHighLevelTest, negativeAesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterRecoverTest);
    }

protected:
    ham_db_t *m_db;
    Environment *m_env;

public:
    virtual void setup() 
    { 
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS
                        | HAM_ENABLE_RECOVERY
                        | HAM_ENABLE_DUPLICATES, 0644));

        m_env=(Environment *)ham_get_env(m_db);
    }

    void open(void)
    {
        // open without recovery and transactions (they imply recovery)!
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        if (m_db) {
            BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
            ham_delete(m_db);
        }
        m_db=0;
    }

    void createCloseTest(void)
    {
        BFC_ASSERT_NOTNULL(m_env->get_log());
    }

    void createCloseEnvTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT(((Environment *)env)->get_log() != 0);
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT(((Environment *)env)->get_log()==0);
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);
        BFC_ASSERT(m_env->get_log()!=0);
    }

    void createCloseOpenFullLogRecoverTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(m_env->get_pagesize());
        memset(buffer, 0, m_env->get_pagesize());
        ham_size_t ps=m_env->get_pagesize();

        BFC_ASSERT_EQUAL(0, 
                    m_env->get_log()->append_write(2, 0, ps, buffer, ps));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* make sure that the log file was deleted and that the lsn is 1 */
        Log *log=m_env->get_log();
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log->get_fd(), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(Log::Header), filesize);

        free(buffer);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(m_env->get_pagesize());
        memset(buffer, 0, m_env->get_pagesize());

        BFC_ASSERT_EQUAL(0, 
                    m_env->get_log()->append_write(1, 0,
                                0, buffer, m_env->get_pagesize()));
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
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT(((Environment *)env)->get_log()==0);

        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT(((Environment *)env)->get_log()!=0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenFullLogEnvTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(m_env->get_pagesize());
        memset(buffer, 0, m_env->get_pagesize());

        BFC_ASSERT_EQUAL(0, 
                    m_env->get_log()->append_write(1, 0,
                                0, buffer, m_env->get_pagesize()));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT(((Environment *)env)->get_log()==0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));

        free(buffer);
    }

    void createCloseOpenFullLogEnvRecoverTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        ham_u8_t *buffer=(ham_u8_t *)malloc(m_env->get_pagesize());
        memset(buffer, 0, m_env->get_pagesize());

        BFC_ASSERT_EQUAL(0, m_env->get_log()->append_write(1, 0,
                                0, buffer, m_env->get_pagesize()));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        Log *log=((Environment *)env)->get_log();
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
        log=((Environment *)env)->get_log();
        BFC_ASSERT_EQUAL((Log *)0, log);
        log=new Log((Environment *)env);
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
            BFC_ASSERT_EQUAL((*vit).data_size, entry.data_size);

            if (data)
                ((Environment *)env)->get_allocator()->free(data);

            vit++;
        }

        if (data)
            ((Environment *)env)->get_allocator()->free(data);
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
        ham_size_t ps=m_env->get_pagesize();
        Page *page;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, db, 0, PAGE_IGNORE_FREELIST));
        page->set_dirty(true);
        BFC_ASSERT_EQUAL(ps*2, page->get_self());
        for (int i=0; i<200; i++)
            page->get_payload()[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, m_env->get_changeset().flush(1));
        m_env->get_changeset().clear();
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
        m_env=(Environment *)ham_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, db, ps*2, 0));
        /* verify that the page contains the marker */
        for (int i=0; i<200; i++)
            BFC_ASSERT_EQUAL((ham_u8_t)i, page->get_payload()[i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(1ull, m_env->get_log()->get_lsn());

        m_env->get_changeset().clear();
#endif
    }

    void recoverAllocateMultiplePageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=m_env->get_pagesize();
        Page *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page[i]->set_dirty(true);
            BFC_ASSERT_EQUAL(ps*(2+i), page[i]->get_self());
            for (int j=0; j<200; j++)
                page[i]->get_payload()[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, m_env->get_changeset().flush(33));
        m_env->get_changeset().clear();
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
        m_env=(Environment *)ham_get_env(m_db);
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            /* verify that the pages contain the markers */
            for (int j=0; j<200; j++)
                BFC_ASSERT_EQUAL((ham_u8_t)(i+j), page[i]->get_payload()[j]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(33ull, m_env->get_log()->get_lsn());

        m_env->get_changeset().clear();
#endif
	}

    void recoverModifiedPageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=m_env->get_pagesize();
        Page *page;
        Database *db=(Database *)m_db;

        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, db, 0, PAGE_IGNORE_FREELIST));
        page->set_dirty(true);
        BFC_ASSERT_EQUAL(ps*2, page->get_self());
        for (int i=0; i<200; i++)
            page->get_payload()[i]=(ham_u8_t)i;
        BFC_ASSERT_EQUAL(0, m_env->get_changeset().flush(2));
        m_env->get_changeset().clear();
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
        m_env=(Environment *)ham_get_env(m_db);
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, db, ps*2, 0));
        /* verify that the page does not contain the "XXX..." */
        for (int i=0; i<20; i++)
            BFC_ASSERT_NOTEQUAL('X', page->get_raw_payload()[i]);

        /* verify the lsn */
        BFC_ASSERT_EQUAL(2ull, m_env->get_log()->get_lsn());

        m_env->get_changeset().clear();
#endif
	}

    void recoverModifiedMultiplePageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=m_env->get_pagesize();
        Page *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page[i]->set_dirty(true);
            BFC_ASSERT_EQUAL(ps*(2+i), page[i]->get_self());
            for (int j=0; j<200; j++)
                page[i]->get_payload()[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, m_env->get_changeset().flush(5));
        m_env->get_changeset().clear();
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
        m_env=(Environment *)ham_get_env(m_db);
        /* verify that the pages does not contain the "XXX..." */
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            for (int j=0; j<20; j++)
                BFC_ASSERT_NOTEQUAL('X', page[i]->get_raw_payload()[i]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(5ull, m_env->get_log()->get_lsn());

        m_env->get_changeset().clear();
#endif
    }

    void recoverMixedAllocatedModifiedPageTest(void)
    {
#ifndef WIN32
        g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
        ham_size_t ps=m_env->get_pagesize();
        Page *page[10];
        Database *db=(Database *)m_db;

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    db_alloc_page(&page[i], db, 0, PAGE_IGNORE_FREELIST));
            page[i]->set_dirty(true);
            BFC_ASSERT_EQUAL(ps*(2+i), page[i]->get_self());
            for (int j=0; j<200; j++)
                page[i]->get_payload()[j]=(ham_u8_t)(i+j);
        }
        BFC_ASSERT_EQUAL(0, m_env->get_changeset().flush(6));
        m_env->get_changeset().clear();
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
        m_env=(Environment *)ham_get_env(m_db);
        /* verify that the pages do not contain the "XXX..." */
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, db_fetch_page(&page[i], db, ps*(2+i), 0));
            for (int j=0; j<20; j++)
                BFC_ASSERT_NOTEQUAL('X', page[i]->get_raw_payload()[i]);
        }

        /* verify the lsn */
        BFC_ASSERT_EQUAL(6ull, m_env->get_log()->get_lsn());

        m_env->get_changeset().clear();
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
        char buffer[1024]={0};

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        rec.data=buffer;
        rec.size=sizeof(buffer);
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

