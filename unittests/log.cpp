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
#include <ham/hamsterdb.h>
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/os.h"
#include "../src/db.h"
#include "memtracker.h"
#include "os.hpp"

class LogTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LogTest);
    CPPUNIT_TEST      (structHeaderTest);
    CPPUNIT_TEST      (structEntryTest);
    CPPUNIT_TEST      (structLogTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseOpenCloseTest);
    CPPUNIT_TEST      (negativeCreateTest);
    CPPUNIT_TEST      (negativeOpenTest);
    CPPUNIT_TEST      (appendTxnBeginTest);
    CPPUNIT_TEST      (appendTxnAbortTest);
    CPPUNIT_TEST      (appendTxnCommitTest);
    CPPUNIT_TEST      (appendCheckpointTest);
    CPPUNIT_TEST      (appendFlushPageTest);
    CPPUNIT_TEST      (appendWriteTest);
    CPPUNIT_TEST      (appendOverwriteTest);
    CPPUNIT_TEST      (insertCheckpointTest);
    CPPUNIT_TEST      (insertTwoCheckpointsTest);
    CPPUNIT_TEST      (clearTest);
    CPPUNIT_TEST      (iterateOverEmptyLogTest);
    CPPUNIT_TEST      (iterateOverLogOneEntryTest);
    CPPUNIT_TEST      (iterateOverLogMultipleEntryTest);
    CPPUNIT_TEST      (iterateOverLogMultipleEntrySwapTest);
    CPPUNIT_TEST      (iterateOverLogMultipleEntrySwapTwiceTest);
    CPPUNIT_TEST      (iterateOverLogMultipleEntryWithDataTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        (void)os::unlink(".test");

        m_alloc=memtracker_new();
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 0, 0644));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    void structHeaderTest()
    {
        log_header_t hdr;

        log_header_set_magic(&hdr, 0x1234);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x1234, log_header_get_magic(&hdr));
    }

    void structEntryTest()
    {
        log_entry_t e;

        log_entry_set_lsn(&e, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x13, log_entry_get_lsn(&e));

        log_entry_set_prev_lsn(&e, 0x14);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x14, log_entry_get_prev_lsn(&e));

        log_entry_set_txn_id(&e, 0x15);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x15, log_entry_get_txn_id(&e));

        log_entry_set_data_size(&e, 0x16);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x16, log_entry_get_data_size(&e));

        log_entry_set_flags(&e, 0xff000000);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0xff000000, log_entry_get_flags(&e));

        log_entry_set_type(&e, LOG_ENTRY_TYPE_CHECKPOINT);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_CHECKPOINT, 
                log_entry_get_type(&e));
    }

    void structLogTest(void)
    {
        ham_log_t log;

        log_set_allocator(&log, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL((mem_allocator_t *)m_alloc, 
                        log_get_allocator(&log));

        log_set_flags(&log, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x13, log_get_flags(&log));

        log_set_state(&log, 0x88);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x88, log_get_state(&log));

        log_set_current_fd(&log, 0x89);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0x89, log_get_current_fd(&log));

        log_set_fd(&log, 0, (ham_fd_t)0x20);
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x20, log_get_fd(&log, 0));
        log_set_fd(&log, 1, (ham_fd_t)0x21);
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x21, log_get_fd(&log, 1));

        log_set_lsn(&log, 0x99);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x99, log_get_lsn(&log));

        log_set_current_file(&log, 7);
        CPPUNIT_ASSERT_EQUAL(7, log_get_current_file(&log));

        for (int i=0; i<2; i++) {
            log_set_open_txn(&log, i, 0x15+i);
            CPPUNIT_ASSERT_EQUAL((ham_size_t)0x15+i, 
                    log_get_open_txn(&log, i));
            log_set_closed_txn(&log, i, 0x25+i);
            CPPUNIT_ASSERT_EQUAL((ham_size_t)0x25+i, 
                    log_get_closed_txn(&log, i));
        }
    }

    void createCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT(log!=0);

        CPPUNIT_ASSERT_EQUAL(0u, log_get_flags(log));
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)1, log_get_lsn(log));
        /* TODO make sure that the two files exist and 
         * contain only the header */

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void negativeCreateTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        "/::asdf", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL((ham_log_t *)0, log);
    }

    void negativeOpenTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        "xxx$$test", 0, &log));

        CPPUNIT_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        "data/log-broken-magic", 0, &log));
    }

    void appendTxnBeginTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));

        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendTxnAbortTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_abort(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendTxnCommitTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendCheckpointTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_checkpoint(log));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendFlushPageTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        ham_page_t *page;
        page=page_new(m_db);
        CPPUNIT_ASSERT_EQUAL(0, page_alloc(page, db_get_pagesize(m_db)));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_flush_page(log, page));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, page_free(page));
        page_delete(page);
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendWriteTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_write(log, &txn, 
                                data, sizeof(data)));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendOverwriteTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));

        ham_u8_t old_data[100], new_data[100];
        for (int i=0; i<100; i++) {
            old_data[i]=(ham_u8_t)i;
            new_data[i]=(ham_u8_t)i+1;
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_overwrite(log, &txn, 
                    old_data, new_data, sizeof(old_data)));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void insertCheckpointTest(void)
    {
        int i;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        log_set_threshold(log, 5);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)5, log_get_threshold(log));

        CPPUNIT_ASSERT_EQUAL(0, log_get_current_file(log));

        for (i=0; i<=6; i++) {
            ham_txn_t txn;
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        /* check that the following logs are written to the other file */
        CPPUNIT_ASSERT_EQUAL(1, log_get_current_file(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void insertTwoCheckpointsTest(void)
    {
        int i;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        log_set_threshold(log, 5);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)5, log_get_threshold(log));

        CPPUNIT_ASSERT_EQUAL(0, log_get_current_file(log));

        for (i=0; i<=10; i++) {
            ham_txn_t txn;
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        /* check that the following logs are written to the first file */
        CPPUNIT_ASSERT_EQUAL(0, log_get_current_file(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void clearTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_last_lsn(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_clear(log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverEmptyLogTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        CPPUNIT_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));
        CPPUNIT_ASSERT_EQUAL((ham_u8_t *)0, data);

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t txn;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc,
                       ".test", 0644, 0, &log));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        CPPUNIT_ASSERT_EQUAL(0, ham_log_get_entry(log, &iter, &entry, &data));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_lsn(&entry));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(&txn));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_entry_get_txn_id(&entry));
        CPPUNIT_ASSERT_EQUAL((ham_u8_t *)0, data);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_TXN_BEGIN, 
                        log_entry_get_type(&entry));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        ham_txn_t txn;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)5-i, log_entry_get_lsn(&entry));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)5-i, log_entry_get_txn_id(&entry));
            CPPUNIT_ASSERT_EQUAL((ham_u8_t *)0, data);
            CPPUNIT_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_TXN_BEGIN, 
                        log_entry_get_type(&entry));
        }

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_get_entry(log, &iter, &entry, &data));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogMultipleEntrySwapTest(void)
    {
        ham_txn_t txn;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        log_set_threshold(log, 5);

        for (int i=0; i<=7; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        int found_txn_begin =0;
        int found_txn_commit=0;
        int found_checkpoint=0;
        while (1) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));

            if (log_entry_get_lsn(&entry)==0)
                break;
            if (LOG_ENTRY_TYPE_TXN_BEGIN==log_entry_get_type(&entry)) {
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)8-found_txn_begin, 
                                log_entry_get_txn_id(&entry));
                CPPUNIT_ASSERT_EQUAL((ham_u8_t *)0, data);
                found_txn_begin++;
            }
            else if (LOG_ENTRY_TYPE_TXN_COMMIT==log_entry_get_type(&entry)) {
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)8-found_txn_commit, 
                                log_entry_get_txn_id(&entry));
                found_txn_commit++;
            }
            else if (LOG_ENTRY_TYPE_CHECKPOINT==log_entry_get_type(&entry)) {
                found_checkpoint++;
            }
            else
                CPPUNIT_ASSERT(!"unknown log_entry_type");
        }
        CPPUNIT_ASSERT_EQUAL(8, found_txn_begin);
        CPPUNIT_ASSERT_EQUAL(8, found_txn_commit);
        CPPUNIT_ASSERT_EQUAL(1, found_checkpoint);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_get_entry(log, &iter, &entry, &data));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogMultipleEntrySwapTwiceTest(void)
    {
        ham_txn_t txn;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        log_set_threshold(log, 5);

        for (int i=0; i<=10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        int found_txn_begin =0;
        int found_txn_commit=0;
        int found_checkpoint=0;

        for (int i=24; i>=0; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));

            if (log_entry_get_lsn(&entry)==0)
                break;

            if (LOG_ENTRY_TYPE_TXN_BEGIN==log_entry_get_type(&entry)) {
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)11-found_txn_begin, 
                                log_entry_get_txn_id(&entry));
                CPPUNIT_ASSERT_EQUAL((ham_u8_t *)0, data);
                found_txn_begin++;
            }
            else if (LOG_ENTRY_TYPE_TXN_COMMIT==log_entry_get_type(&entry)) {
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)11-found_txn_commit, 
                                log_entry_get_txn_id(&entry));
                found_txn_commit++;
            }
            else if (LOG_ENTRY_TYPE_CHECKPOINT==log_entry_get_type(&entry)) {
                found_checkpoint++;
            }
            else
                CPPUNIT_ASSERT(!"unknown log_entry_type");
        }
        CPPUNIT_ASSERT_EQUAL(6, found_txn_begin);
        CPPUNIT_ASSERT_EQUAL(6, found_txn_commit);
        CPPUNIT_ASSERT_EQUAL(1, found_checkpoint);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_get_entry(log, &iter, &entry, &data));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void iterateOverLogMultipleEntryWithDataTest(void)
    {
        ham_txn_t txn;
        ham_u8_t buffer[20];
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));

        for (int i=0; i<5; i++) {
            memset(buffer, (char)i, sizeof(buffer));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_write(log, &txn, buffer, i));
            CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(&txn));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, 
                        ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;

        int writes=4;

        while (1) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_log_get_entry(log, &iter, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;

            if (log_entry_get_type(&entry)==LOG_ENTRY_TYPE_WRITE) {
                ham_u8_t cmp[20];
                memset(cmp, (char)writes, sizeof(cmp));
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)writes, 
                        log_entry_get_data_size(&entry));
                CPPUNIT_ASSERT_EQUAL(0, memcmp(data, cmp, 
                        (int)log_entry_get_data_size(&entry)));
                writes--;
            }

            if (data)
                ham_mem_free(m_db, data);
        }

        CPPUNIT_ASSERT_EQUAL(-1, writes);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }
    
};

class LogEntry : public log_entry_t
{
public:
    LogEntry(ham_db_t *db, log_entry_t *entry, ham_u8_t *data)
    : m_data(data), m_db(db) { 
        memcpy(&m_entry, entry, sizeof(m_entry));
    }

    ~LogEntry() { 
        if (m_data)
            ham_mem_free(m_db, m_data);
    }

    ham_u8_t *m_data;
    log_entry_t m_entry;

private:
    ham_db_t *m_db;
};

class LogHighLevelTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LogHighLevelTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    typedef std::vector<LogEntry> log_vector_t;

    void setUp()
    { 
        (void)os::unlink(".test");

        m_alloc=memtracker_new();
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_RECOVERY, 0644));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    log_vector_t readLog()
    {
        log_vector_t vec;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, ".test", 0, &log));
        CPPUNIT_ASSERT(log!=0);

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        while (1) {
            CPPUNIT_ASSERT_EQUAL(0, 
                            ham_log_get_entry(log, &iter, &entry, &data));
            if (log_entry_get_lsn(&entry)==0)
                break;
            // skip CHECKPOINTs, they are not interesting for our tests
            if (log_entry_get_type(&entry)==LOG_ENTRY_TYPE_CHECKPOINT)
                continue;

            LogEntry le(m_db, &entry, data);
            vec.push_back(le);
        }

        return (vec);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);
CPPUNIT_TEST_SUITE_REGISTRATION(LogHighLevelTest);

