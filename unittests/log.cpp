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
        BFC_REGISTER_TEST(LogTest, iterateOverLogMultipleEntryWithDataTest);
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
        ham_log_t *log;
        const char *oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                log_open(m_env, 0, &log));

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

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
    }

    void checkLogEntry(log_entry_t *entry, ham_u64_t lsn, ham_u64_t txn_id, 
                    ham_u32_t type, ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, log_entry_get_lsn(entry));
        BFC_ASSERT_EQUAL(txn_id, log_entry_get_txn_id(entry));
        if (log_entry_get_data_size(entry)==0) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            allocator_free((mem_allocator_t *)m_alloc, data);
        }
        BFC_ASSERT_EQUAL(type, log_entry_get_type(entry));
    }

    void iterateOverLogMultipleEntryTest(void)
    {
#if 0 /* TODO */
        ham_txn_t *txn;
        ham_log_t *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            // ham_txn_begin and ham_txn_abort will automatically add a 
            // log entry
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
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
        checkLogEntry(&entry, 13, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 12, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 11, 5, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 10, 5, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  9, 4, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  8, 4, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  7, 3, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  6, 3, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  5, 2, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  4, 2, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  3, 1, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  2, 1, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, log_get_entry(log, &iter, &entry, &data));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }

    void iterateOverLogMultipleEntryWithDataTest(void)
    {
#if 0 /* TODO */
        ham_txn_t *txn;
        ham_u8_t buffer[20];
        ham_log_t *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            memset(buffer, (char)i, sizeof(buffer));
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, 
                            log_append_write(log, txn, i, buffer, i));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, log_open(m_env, 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        log_entry_t entry;
        ham_u8_t *data;

        int writes=4;

        while (1) {
            BFC_ASSERT_EQUAL(0, 
                    log_get_entry(log, &iter, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;

            if (log_entry_get_type(&entry)==LOG_ENTRY_TYPE_WRITE) {
                ham_u8_t cmp[20];
                memset(cmp, (char)writes, sizeof(cmp));
                BFC_ASSERT_EQUAL((ham_u64_t)writes, 
                        log_entry_get_data_size(&entry));
                BFC_ASSERT_EQUAL((ham_u64_t)writes, 
                        log_entry_get_offset(&entry));
                BFC_ASSERT_EQUAL(0, memcmp(data, cmp, 
                        (int)log_entry_get_data_size(&entry)));
                writes--;
            }

            if (data) {
                BFC_ASSERT(log_entry_get_data_size(&entry)!=0);
                allocator_free(((mem_allocator_t *)m_alloc), data);
            }
        }

        BFC_ASSERT_EQUAL(-1, writes);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }
    
};

class LogEntry : public log_entry_t
{
public:
    LogEntry(log_entry_t *entry, ham_u8_t *data) { 
        memcpy(&m_entry, entry, sizeof(m_entry));
        if (data)
            m_data.insert(m_data.begin(), data, 
                    data+log_entry_get_data_size(entry));
    }

    LogEntry(ham_u64_t txn_id, ham_u8_t type, ham_offset_t offset,
            ham_u64_t data_size, ham_u8_t *data=0) {
        memset(&m_entry, 0, sizeof(m_entry));
        log_entry_set_txn_id(&m_entry, txn_id);
        log_entry_set_type(&m_entry, type);
        log_entry_set_offset(&m_entry, offset);
        log_entry_set_data_size(&m_entry, data_size);
        if (data)
            m_data.insert(m_data.begin(), data, data+data_size);
    }

    std::vector<ham_u8_t> m_data;
    log_entry_t m_entry;

    std::string to_str()
    {
        std::ostringstream o(std::ostringstream::out);
        o << "txn:" << log_entry_get_txn_id(&m_entry);
        o << ", offset:" << log_entry_get_offset(&m_entry);
        o << ", datasize:" << log_entry_get_data_size(&m_entry);
        return o.str();
    }
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
#if 0
        BFC_REGISTER_TEST(LogHighLevelTest, allocatePageTest);
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
    typedef std::vector<LogEntry> log_vector_t;

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

    class log_assert_monitor: public bfc_assert_monitor
    {
    protected:
        log_vector_t *lhs;
        log_vector_t *rhs;

    public:
        log_assert_monitor(log_vector_t *l, log_vector_t *r)
            : lhs(l), rhs(r)
        {}
        virtual ~log_assert_monitor()
        {}

        virtual void handler(bfc::error &err)
        {
            std::ostringstream o(std::ostringstream::out);

            o << std::endl;

            log_vector_t::iterator itl=lhs->begin();
            log_vector_t::iterator itr=rhs->begin(); 
            int entry;
            for (entry = 1; itl != lhs->end(); ++itl, ++itr, entry++)
            {
                //o << "[" << entry << "]\t" << (*itl).to_str() << std::endl << "    vs.\t" << (*itr).to_str() << std::endl;
                o << "[" << entry << "]\t" << (*itr).to_str() << std::endl;
            }

            // augment error message:
            err.m_message += o.str();
        }
    };

    void compareLogs(log_vector_t *lhs, log_vector_t *rhs)
    {
        BFC_ASSERT_EQUAL((unsigned)lhs->size(), (unsigned)rhs->size());

        log_assert_monitor assert_monitor(lhs, rhs);
        push_assert_monitor(assert_monitor);

        log_vector_t::iterator itl=lhs->begin();
        log_vector_t::iterator itr=rhs->begin(); 
        int entry;
        for (entry = 1; itl!=lhs->end(); ++itl, ++itr, entry++) 
        {
            BFC_ASSERT_EQUAL_I(log_entry_get_txn_id(&(*itl).m_entry), 
                    log_entry_get_txn_id(&(*itr).m_entry), entry); 
            BFC_ASSERT_EQUAL_I(log_entry_get_type(&(*itl).m_entry), 
                    log_entry_get_type(&(*itr).m_entry), entry); 
            BFC_ASSERT_EQUAL_I(log_entry_get_offset(&(*itl).m_entry), 
                    log_entry_get_offset(&(*itr).m_entry), entry); 
            BFC_ASSERT_EQUAL_I(log_entry_get_data_size(&(*itl).m_entry), 
                    log_entry_get_data_size(&(*itr).m_entry), entry); 

            if ((*itl).m_data.size()) {
                void *pl=&(*itl).m_data[0];
                void *pr=&(*itr).m_data[0];
                BFC_ASSERT_EQUAL_I(0, memcmp(pl, pr, 
                    (size_t)log_entry_get_data_size(&(*itl).m_entry)), entry);
            }
        }

        pop_assert_monitor();
    }

    log_vector_t readLog()
    {
        log_vector_t vec;
        ham_log_t *log;
        BFC_ASSERT_EQUAL(0, log_open(m_env, 0, &log));
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        while (1) {
            BFC_ASSERT_EQUAL(0, 
                            log_get_entry(log, &iter, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;
            
            /*
            printf("lsn: %d, txn: %d, type: %d, offset: %d, size %d\n",
                        (int)log_entry_get_lsn(&entry),
                        (int)log_entry_get_txn_id(&entry),
                        (int)log_entry_get_type(&entry),
                        (int)log_entry_get_offset(&entry),
                        (int)log_entry_get_data_size(&entry));
                        */

            vec.push_back(LogEntry(&entry, data));
            if (data)
                allocator_free(((mem_allocator_t *)m_alloc), data);
        }

        BFC_ASSERT_EQUAL(0, log_close(log, HAM_FALSE));
        return (vec);
    }

    void createCloseTest(void)
    {
        BFC_ASSERT(env_get_log(m_env)!=0);
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
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 2,
                                0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);

        /* make sure that the log file was deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(m_env);
        BFC_ASSERT(log!=0);
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
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
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, 
                    log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY));
        BFC_ASSERT(env_get_log(env)==0);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenFullLogEnvRecoverTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_u8_t buffer[1024]={0};
        BFC_ASSERT_EQUAL(0, log_append_write(env_get_log(m_env), txn, 1,
                                0, buffer, sizeof(buffer)));
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
    }

    void allocatePageTest(void)
    {
        ham_page_t *page;
        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, 0, 0));
        compareLogs(&exp, &vec);
    }

    void allocatePageFromFreelistTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        ham_page_t *page;
        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, 
                        PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, db_free_page(page, DB_MOVE_TO_FREELIST));
        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, PAGE_CLEAR_WITH_ZERO));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        compareLogs(&exp, &vec);
    }

    void allocateClearedPageTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        ham_page_t *page;
        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, 
                        PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        compareLogs(&exp, &vec);
    }

    void insert(const char *name, const char *data, ham_u32_t flags=0)
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.data=(void *)name;
        key.size=(ham_u16_t)strlen(name)+1;
        rec.data=(void *)data;
        rec.size=(ham_u16_t)strlen(data)+1;

        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, flags));
    }

    void find(const char *name, const char *data, ham_status_t result=0)
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.data=(void *)name;
        key.size=(ham_u16_t)strlen(name)+1;

        BFC_ASSERT_EQUAL(result, ham_find(m_db, 0, &key, &rec, 0));
        if (result==0)
            BFC_ASSERT_EQUAL(0, ::strcmp(data, (const char *)rec.data));
    }

    void erase(const char *name)
    {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        key.data=(void *)name;
        key.size=(ham_u16_t)strlen(name)+1;

        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void singleInsertTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "b");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void doubleInsertTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "b");
        insert("b", "c");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void splitInsertTest(void)
    {
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, 1024}, 
            {HAM_PARAM_KEYSIZE,   200}, 
            {0, 0}
        };
        ham_size_t ps=1024;
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_RECOVERY, 0644, p));
        insert("a", "1");
        insert("b", "2");
        insert("c", "3");
        insert("d", "4");
        insert("e", "5");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;

        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*3, ps));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void singleEraseTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "b");
        erase("a");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void eraseMergeTest(void)
    {
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, 1024}, 
            {HAM_PARAM_KEYSIZE,   200}, 
            {0, 0}
        };
        ham_size_t ps=1024;
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                                HAM_ENABLE_RECOVERY, 0644, p));
        insert("a", "1");
        insert("b", "2");
        insert("c", "3");
        insert("d", "4");
        insert("e", "5");
        erase("e");
        erase("d");
        erase("c");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void cursorOverwriteTest(void)
    {
        ham_key_t key; memset(&key, 0, sizeof(key));
        ham_record_t rec; memset(&rec, 0, sizeof(rec));
        ham_size_t ps=os_get_pagesize();
        insert("a", "1");
        ham_cursor_t *c;
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(c, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_overwrite(c, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_close(m_db, HAM_DONT_CLEAR_LOG|HAM_AUTO_CLEANUP));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void singleBlobTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "1111111110111111111011111111101111111110");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        compareLogs(&exp, &vec);
    }

    void largeBlobTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        char *p=(char *)malloc(ps/4);
        memset(p, 'a', ps/4);
        p[ps/4-1]=0;
        insert("a", p);
        free(p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        compareLogs(&exp, &vec);
    }

    void insertDuplicateTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "1");
        insert("a", "2", HAM_DUPLICATE);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void recoverModifiedPageTest(void)
    {
        ham_txn_t *txn;
        ham_page_t *page;

        /* allocate page, write before-image, modify, commit (= write
         * after-image */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, db_alloc_page(&page, m_db, 0, 0));
        BFC_ASSERT(page!=0);
        ham_offset_t address=page_get_self(page);
        ham_u8_t *p=page_get_payload(page);
        memset(p, 0, env_get_usable_pagesize(m_env));
        p[0]=1;
        page_set_dirty(page);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* fetch page again, modify, abort -> first modification is still
         * available, second modification is reverted */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, env_fetch_page(&page, m_env, address, 0));
        BFC_ASSERT(page!=0);
        p=page_get_payload(page);
        p[0]=2;
        page_set_dirty(page);
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

        /* check modifications */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, env_fetch_page(&page, m_env, address, 0));
        BFC_ASSERT(page!=0);
        p=page_get_payload(page);
        BFC_ASSERT_EQUAL((ham_u8_t)1, p[0]);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void recoverModifiedPageTest2(void)
    {
        ham_txn_t *txn;
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();

        /* insert a key */
        insert("a", "1");

        /* fetch the page with the key, overwrite it with garbage, then
         * abort */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, env_fetch_page(&page, m_env, ps, 0));
        BFC_ASSERT(page!=0);
        btree_node_t *node=page_get_btree_node(page);
        btree_key_t *entry=btree_node_get_key(m_db, node, 0);
        BFC_ASSERT_EQUAL((ham_u8_t)'a', key_get_key(entry)[0]);
        key_get_key(entry)[0]='b';
        page_set_dirty(page);
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

        /* now fetch the original key */
        ham_key_t key;
        ham_record_t record;
        memset(&key, 0, sizeof(key));
        key.data=(void *)"a";
        key.size=2; /* zero-terminated "a" */
        memset(&record, 0, sizeof(record));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &record, 0));
    }

    void redoInsertTest(void)
    {
        ham_page_t *page;

        /* insert a key */
        insert("x", "2");

        /* now walk through all pages and set them un-dirty, so they
         * are not written to the file */
        page=cache_get_totallist(env_get_cache(m_env)); 
        while (page) {
            page_set_undirty(page);
            page=page_get_next(page, PAGE_LIST_CACHED);
        }

        /* close the database (without deleting the log) */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now reopen and recover */
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

        /* and make sure that the inserted item is found */
        find("x", "2");
    }

    void redoMultipleInsertsTest(void)
    {
        ham_page_t *page;

        /* insert keys */
        insert("x", "2");
        insert("y", "3");
        insert("z", "4");

        /* now walk through all pages and set them un-dirty, so they
         * are not written to the file */
        page=cache_get_totallist(env_get_cache(m_env)); 
        while (page) {
            page_set_undirty(page);
            page=page_get_next(page, PAGE_LIST_CACHED);
        }

        /* close the database (without deleting the log) */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now reopen and recover */
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), 
                            HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

        /* and make sure that the inserted item is found */
        find("x", "2");
        find("y", "3");
        find("z", "4");
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

