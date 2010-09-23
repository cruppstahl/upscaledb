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
        BFC_REGISTER_TEST(LogTest, appendTxnBeginTest);
        BFC_REGISTER_TEST(LogTest, appendTxnAbortTest);
        BFC_REGISTER_TEST(LogTest, appendTxnCommitTest);
        BFC_REGISTER_TEST(LogTest, appendCheckpointTest);
        BFC_REGISTER_TEST(LogTest, appendFlushPageTest);
        BFC_REGISTER_TEST(LogTest, appendPreWriteTest);
        BFC_REGISTER_TEST(LogTest, appendWriteTest);
        BFC_REGISTER_TEST(LogTest, insertCheckpointTest);
        BFC_REGISTER_TEST(LogTest, insertTwoCheckpointsTest);
        BFC_REGISTER_TEST(LogTest, clearTest);
        BFC_REGISTER_TEST(LogTest, iterateOverEmptyLogTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogOneEntryTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogMultipleEntryTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogMultipleEntrySwapTest);
        BFC_REGISTER_TEST(LogTest, iterateOverLogMultipleEntrySwapTwiceTest);
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

        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
            ham_log_create((mem_allocator_t *)m_alloc, env,
                BFC_OPATH(".test"), 0644, 0, &log));
        BFC_ASSERT_NULL(log);

        /* 
         * make sure db->log is already NULL, i.e. disconnected. 
         * Otherwise an BFC ASSERT for ham_log_close() will segfault 
         * the teardown() code, which will try to close the db->log 
         * all over AGAIN! 
         */
        log = env_get_log(env);
        env_set_log(env, NULL);
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, 
            ham_log_create((mem_allocator_t *)m_alloc, env,
                BFC_OPATH(".test"), 0644, 0, &log));
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

        log_entry_set_type(&e, LOG_ENTRY_TYPE_CHECKPOINT);
        BFC_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_CHECKPOINT, 
                log_entry_get_type(&e));
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

        log_set_state(&log, 0x88);
        BFC_ASSERT_EQUAL((ham_u32_t)0x88, log_get_state(&log));

        log_set_current_fd(&log, 1);
        BFC_ASSERT_EQUAL((unsigned)1, log_get_current_fd(&log));

        log_set_fd(&log, 0, (ham_fd_t)0x20);
        BFC_ASSERT_EQUAL((ham_fd_t)0x20, log_get_fd(&log, 0));
        log_set_fd(&log, 1, (ham_fd_t)0x21);
        BFC_ASSERT_EQUAL((ham_fd_t)0x21, log_get_fd(&log, 1));

        log_set_lsn(&log, 0x99);
        BFC_ASSERT_EQUAL((ham_u64_t)0x99, log_get_lsn(&log));

        log_set_last_checkpoint_lsn(&log, 0x100);
        BFC_ASSERT_EQUAL((ham_u64_t)0x100, 
                        log_get_last_checkpoint_lsn(&log));

        for (int i=0; i<2; i++) {
            log_set_open_txn(&log, i, 0x15+i);
            BFC_ASSERT_EQUAL((ham_size_t)0x15+i, 
                    log_get_open_txn(&log, i));
            log_set_closed_txn(&log, i, 0x25+i);
            BFC_ASSERT_EQUAL((ham_size_t)0x25+i, 
                    log_get_closed_txn(&log, i));
        }
    }

    void createCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();

        BFC_ASSERT_EQUAL(0u, log_get_flags(log));
        BFC_ASSERT_EQUAL((ham_offset_t)1, log_get_lsn(log));
        /* TODO make sure that the two files exist and 
         * contain only the header */

        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));

        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        BFC_ASSERT(log!=0);
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void negativeCreateTest(void)
    {
        ham_log_t *log;
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_log_create((mem_allocator_t *)m_alloc, m_env,
                        "/::asdf", 0644, 0, &log));
        BFC_ASSERT_EQUAL((ham_log_t *)0, log);
    }

    void negativeOpenTest(void)
    {
        ham_log_t *log;
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        "xxx$$test", 0, &log));

        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        "data/log-broken-magic", 0, &log));
    }

    void appendTxnBeginTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));

        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendTxnAbortTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_log_append_txn_abort(log, txn));
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendTxnCommitTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, txn));
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendCheckpointTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, ham_log_append_checkpoint(log));
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendFlushPageTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        ham_page_t *page;
        page=page_new(m_env);
        BFC_ASSERT_EQUAL(0, page_alloc(page));
        page_set_dirty(page);

        BFC_ASSERT_EQUAL(0, ham_log_append_flush_page(log, page));
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, page_free(page));
        page_delete(page);
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendPreWriteTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        BFC_ASSERT_EQUAL(0, ham_log_append_prewrite(log, txn, 
                                0, data, sizeof(data)));
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendWriteTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        BFC_ASSERT_EQUAL(0, ham_log_append_write(log, txn, 
                                0, data, sizeof(data)));
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void insertCheckpointTest(void)
    {
        int i;
        ham_log_t *log = disconnect_log_and_create_new_log();
        log_set_threshold(log, 5);
        BFC_ASSERT_EQUAL((ham_size_t)5, log_get_threshold(log));

        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

        for (i=0; i<=6; i++) {
            ham_txn_t *txn;
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));
            BFC_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, txn));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        /* check that the following logs are written to the other file */
        BFC_ASSERT_EQUAL((ham_size_t)1, log_get_current_fd(log));

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void insertTwoCheckpointsTest(void)
    {
        int i;
        ham_log_t *log = disconnect_log_and_create_new_log();
        log_set_threshold(log, 5);
        BFC_ASSERT_EQUAL((ham_size_t)5, log_get_threshold(log));

        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

        for (i=0; i<=10; i++) {
            ham_txn_t *txn;
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));
            BFC_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, txn));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        /* check that the following logs are written to the first file */
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void clearTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));

        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_log_clear(log));
        BFC_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverEmptyLogTest(void)
    {
        ham_log_t *log = disconnect_log_and_create_new_log();

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t *txn;
        ham_log_t *log = disconnect_log_and_create_new_log();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, txn));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_txn_id(&entry));
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);
        BFC_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_TXN_BEGIN, 
                        log_entry_get_type(&entry));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
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
        ham_txn_t *txn;
        ham_log_t *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            // ham_txn_begin and ham_txn_abort will automatically add a 
            // log entry
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 13, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 12, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 11, 5, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 10, 5, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  9, 4, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  8, 4, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  7, 3, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  6, 3, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  5, 2, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  4, 2, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  3, 1, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  2, 1, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry,  1, 0, LOG_ENTRY_TYPE_PREWRITE, data);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTest(void)
    {
        ham_txn_t *txn;
        ham_log_t *log=env_get_log(m_env);
        log_set_threshold(log, 5);

        for (int i=0; i<=7; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        log_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 18, 8, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 17, 8, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 16, 7, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 15, 7, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 14, 6, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 13, 6, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 12, 0, LOG_ENTRY_TYPE_CHECKPOINT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 11, 5, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 10, 5, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 9, 4, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 8, 4, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 7, 3, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 6, 3, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 5, 2, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 4, 2, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 3, 1, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 2, 1, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 1, 0, LOG_ENTRY_TYPE_PREWRITE, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTwiceTest(void)
    {
        ham_txn_t *txn;
        ham_log_t *log=env_get_log(m_env);
        log_set_threshold(log, 5);

        for (int i=0; i<=10; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        log_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 25, 11, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 24, 11, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 23, 0, LOG_ENTRY_TYPE_CHECKPOINT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 22, 10, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 21, 10, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 20, 9, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 19, 9, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 18, 8, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 17, 8, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 16, 7, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 15, 7, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 14, 6, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        checkLogEntry(&entry, 13, 6, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntryWithDataTest(void)
    {
        ham_txn_t *txn;
        ham_u8_t buffer[20];
        ham_log_t *log=env_get_log(m_env);

        for (int i=0; i<5; i++) {
            memset(buffer, (char)i, sizeof(buffer));
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, 
                            ham_log_append_write(log, txn, i, buffer, i));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        env_set_log(m_env, log);
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        log_entry_t entry;
        ham_u8_t *data;

        int writes=4;

        while (1) {
            BFC_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));
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

public:
    static std::string log_entry_type2str(int type)
{
switch (type)
{
case LOG_ENTRY_TYPE_TXN_BEGIN                :
return "LOG_ENTRY_TYPE_TXN_BEGIN";

case LOG_ENTRY_TYPE_TXN_ABORT                :
return "LOG_ENTRY_TYPE_TXN_ABORT";
                
case LOG_ENTRY_TYPE_TXN_COMMIT               :
return "LOG_ENTRY_TYPE_TXN_COMMIT";

case LOG_ENTRY_TYPE_PREWRITE                 :
return "LOG_ENTRY_TYPE_PREWRITE";

case LOG_ENTRY_TYPE_WRITE                    :
return "LOG_ENTRY_TYPE_WRITE";

case LOG_ENTRY_TYPE_CHECKPOINT               :
return "LOG_ENTRY_TYPE_CHECKPOINT";

case LOG_ENTRY_TYPE_FLUSH_PAGE              :
return "LOG_ENTRY_TYPE_FLUSH_PAGE";

default:
return "LOG_ENTRY_TYPE_???";
}
}

    std::string to_str()
    {
        std::ostringstream o(std::ostringstream::out);
        o << "txn:" << log_entry_get_txn_id(&m_entry);
        o << ", type:" << log_entry_get_type(&m_entry) << "(" << log_entry_type2str(log_entry_get_type(&m_entry)) << ")";
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
        BFC_REGISTER_TEST(LogHighLevelTest, txnBeginAbortTest);
        BFC_REGISTER_TEST(LogHighLevelTest, txnBeginCommitTest);
        BFC_REGISTER_TEST(LogHighLevelTest, multipleTxnBeginCommitTest);
        BFC_REGISTER_TEST(LogHighLevelTest, multipleTxnReadonlyBeginCommitTest);
        BFC_REGISTER_TEST(LogHighLevelTest, allocatePageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, allocatePageFromFreelistTest);
        BFC_REGISTER_TEST(LogHighLevelTest, allocateClearedPageTest);
        BFC_REGISTER_TEST(LogHighLevelTest, singleInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, doubleInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, splitInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, insertAfterCheckpointTest);
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
        BFC_REGISTER_TEST(LogHighLevelTest, redoMultipleInsertsCheckpointTest);
        BFC_REGISTER_TEST(LogHighLevelTest, undoInsertTest);
        BFC_REGISTER_TEST(LogHighLevelTest, undoMultipleInsertsTest);
        BFC_REGISTER_TEST(LogHighLevelTest, undoMultipleInsertsCheckpointTest);
        BFC_REGISTER_TEST(LogHighLevelTest, negativeAesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterTest);
        BFC_REGISTER_TEST(LogHighLevelTest, aesFilterRecoverTest);
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
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));
        BFC_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        while (1) {
            BFC_ASSERT_EQUAL(0, 
                            ham_log_get_entry(log, &iter, &entry, &data));
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

            // skip CHECKPOINTs, they are not interesting for our tests
            if (log_entry_get_type(&entry)==LOG_ENTRY_TYPE_CHECKPOINT)
                continue;

            vec.push_back(LogEntry(&entry, data));
            if (data)
                allocator_free(((mem_allocator_t *)m_alloc), data);
        }

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
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
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT(env_get_log(env) != 0);  /* since the hack/patch to load the first db/page at env create/open, there's a log as well */
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
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));
        m_env=db_get_env(m_db);

        /* make sure that the log files are deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(m_env);
        BFC_ASSERT(log!=0);
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 0), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 1), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
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
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT(env_get_log(env)!=0); /* since the hack/patch to load the first db/page at env create/open, there's a log as well */
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
        BFC_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(env_get_log(m_env), txn));
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
        BFC_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(env_get_log(m_env), txn));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(env);
        BFC_ASSERT(log!=0);
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));
        ham_u64_t filesize;
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 0), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
        BFC_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 1), &filesize));
        BFC_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void txnBeginAbortTest(void)
    {
        ham_txn_t *txn;
        ham_size_t pagesize=os_get_pagesize();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_ABORT, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void txnBeginCommitTest(void)
    {
        ham_txn_t *txn;
        ham_size_t pagesize=os_get_pagesize();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void multipleTxnBeginCommitTest(void)
    {
        /* 
         * currently hamsterdb only supports one open transaction,
         * therefore this test is removed
         */
#if 0
        ham_txn_t *txn[3];
        ham_size_t pagesize=os_get_pagesize();
        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn[i], m_db, 0));
        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(0, txn_commit(txn[i], 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        for (int i=0; i<3; i++)
            exp.push_back(LogEntry(3-i, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        for (int i=0; i<3; i++)
            exp.push_back(LogEntry(3-i, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
#endif
    }

    void multipleTxnReadonlyBeginCommitTest(void)
    {
        ham_txn_t *txn;
        ham_size_t pagesize=os_get_pagesize();
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, HAM_TXN_READ_ONLY));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void allocatePageTest(void)
    {
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();
        BFC_ASSERT_EQUAL(0, 
                db_alloc_page(&page, m_db, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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

        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*1, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*3, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*3, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_PREWRITE, ps*3, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void insertAfterCheckpointTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        log_set_threshold(env_get_log(m_env), 5);
        insert("a", "1"); insert("b", "2"); insert("c", "3");
        insert("d", "4"); insert("e", "5"); insert("f", "6");
        insert("g", "1"); insert("h", "2"); insert("i", "3");
        insert("j", "4"); insert("k", "5"); insert("l", "6");
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        open();
        log_vector_t vec=readLog();
        log_vector_t exp;

        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
        exp.push_back(LogEntry(12, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(12, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(12, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(11, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(11, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(11, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
        exp.push_back(LogEntry(11, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        // ignored in readLog()
        // exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_CHECKPOINT, 0, 0));
        exp.push_back(LogEntry(10, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(10, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(10, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(9, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(9, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(9, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_WRITE, ps*3, ps));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_WRITE, ps*1, ps));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(6, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_WRITE, ps*3, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_PREWRITE, ps*3, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(5, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(4, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(3, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));

        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
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
        BFC_ASSERT_EQUAL(0, ham_log_add_page_before(page));
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

    void redoMultipleInsertsCheckpointTest(void)
    {
        ham_page_t *page;
        log_set_threshold(env_get_log(m_env), 5);

        /* insert keys */
        insert("1", "1");
        insert("2", "2");
        insert("3", "3");
        insert("4", "4");
        insert("5", "5");
        insert("6", "6");
        insert("7", "7");

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

        /* and make sure that the inserted items are found */
        find("1", "1");
        find("2", "2");
        find("3", "3");
        find("4", "4");
        find("5", "5");
        find("6", "6");
        find("7", "7");
    }

    void patchLogfile(const char *filename, ham_u64_t txn_id)
    {
        bool created=false;

        if (!m_env) {
            BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
            created=true;
        }

        int found=0;
        ham_log_t *log;
        BFC_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, m_env,
                        BFC_OPATH(".test"), 0, &log));

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        while (1) {
            BFC_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;
            if (data) {
                allocator_free((mem_allocator_t *)m_alloc, data);
                data=0;
            }
            if (log_entry_get_type(&entry)==LOG_ENTRY_TYPE_TXN_COMMIT
                    && log_entry_get_txn_id(&entry)==txn_id) {
                log_entry_set_flags(&entry, 0);
                log_entry_set_type(&entry, LOG_ENTRY_TYPE_TXN_ABORT);
                BFC_ASSERT_EQUAL(0, os_pwrite(log_get_fd(log, iter._fdidx),
                            iter._offset, &entry, sizeof(entry)));
                found=1;
                break;
            }
        }

        BFC_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        BFC_ASSERT_EQUAL(1, found);

        // clean up 
        if (created) {
            ham_env_delete(m_env);
            m_env=0;
        }
    }

    void undoInsertTest(void)
    {
        /* insert two keys, we will then undo the second one */
        insert("x", "2");
        insert("y", "3");
        /* close the database (without deleting the log) */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        m_env=0;

        /* now walk through the logfile and patch the COMMIT entry of the
         * second insert (txn-id=2) to ABORT */
        patchLogfile(BFC_OPATH(".test"), 2);

        /* now reopen and recover */
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));
        m_env=db_get_env(m_db);

        /* and make sure that the inserted item is found, but not the 
         * aborted item */
        find("x", "2");
        find("y", "3", HAM_KEY_NOT_FOUND);
    }

    void undoMultipleInsertsTest(void)
    {
        /* insert two keys, we will then undo the second one */
        insert("1", "2");
        insert("2", "3");
        insert("3", "4");
        /* close the database (without deleting the log) */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        m_env=0;

        /* now walk through the logfile and patch the COMMIT entry of the
         * second and third insert (txn-id=2, 3) to ABORT */
        patchLogfile(BFC_OPATH(".test"), 2);
        patchLogfile(BFC_OPATH(".test"), 3);

        /* now reopen and recover */
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));
        m_env=db_get_env(m_db);

        /* and make sure that the inserted item is found, but not the 
         * aborted item */
        find("1", "2");
        find("2", "3", HAM_KEY_NOT_FOUND);
        find("3", "4", HAM_KEY_NOT_FOUND);
    }

    void undoMultipleInsertsCheckpointTest(void)
    {
        log_set_threshold(env_get_log(m_env), 5);

        /* insert two keys, we will then undo the second one */
        insert("1", "2");
        insert("2", "3");
        insert("3", "4");
        insert("4", "5");
        insert("5", "6");
        insert("6", "7");
        /* close the database (without deleting the log) */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        m_env=0;

        /* now walk through the logfile and patch the COMMIT entry of the
         * last insert (txn-id=7) to ABORT */
        patchLogfile(BFC_OPATH(".test"), 6);

        /* now reopen and recover */
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));
        m_env=db_get_env(m_db);

        /* and make sure that the inserted item is found, but not the 
         * aborted item */
        find("1", "2");
        find("2", "3");
        find("3", "4");
        find("4", "5");
        find("5", "6");
        find("6", "7", HAM_KEY_NOT_FOUND);
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

