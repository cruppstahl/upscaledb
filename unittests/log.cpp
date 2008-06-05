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
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/os.h"
#include "../src/db.h"
#include "../src/btree.h"
#include "../src/keys.h"
#include "../src/freelist.h"
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
    CPPUNIT_TEST      (appendPreWriteTest);
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

        log_entry_set_txn_id(&e, 0x15);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x15, log_entry_get_txn_id(&e));

        log_entry_set_offset(&e, 0x22);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x22, log_entry_get_offset(&e));

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

        CPPUNIT_ASSERT_EQUAL((ham_log_t *)0, db_get_log(m_db));

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

        log_set_last_checkpoint_lsn(&log, 0x100);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x100, 
                        log_get_last_checkpoint_lsn(&log));

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
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));

        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_abort(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)3, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_closed_txn(log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_open_txn(log, 1));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_closed_txn(log, 1));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendCheckpointTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_checkpoint(log));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendFlushPageTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        ham_page_t *page;
        page=page_new(m_db);
        CPPUNIT_ASSERT_EQUAL(0, page_alloc(page, db_get_pagesize(m_db)));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_flush_page(log, page));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, page_free(page));
        page_delete(page);
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendPreWriteTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_prewrite(log, &txn, 
                                0, data, sizeof(data)));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendWriteTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        ham_u8_t data[100];
        for (int i=0; i<100; i++)
            data[i]=(ham_u8_t)i;

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_write(log, &txn, 
                                0, data, sizeof(data)));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
    }

    void appendOverwriteTest(void)
    {
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_create((mem_allocator_t *)m_alloc, 
                        ".test", 0644, 0, &log));
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        ham_u8_t old_data[100], new_data[100];
        for (int i=0; i<100; i++) {
            old_data[i]=(ham_u8_t)i;
            new_data[i]=(ham_u8_t)i+1;
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_overwrite(log, &txn, 
                    0, old_data, new_data, sizeof(old_data)));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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

        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

        for (i=0; i<=6; i++) {
            ham_txn_t txn;
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        }

        /* check that the following logs are written to the other file */
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, log_get_current_fd(log));

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

        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

        for (i=0; i<=10; i++) {
            ham_txn_t txn;
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        }

        /* check that the following logs are written to the first file */
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));

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
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(0, isempty);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2, log_get_lsn(log));

        CPPUNIT_ASSERT_EQUAL(0, ham_log_clear(log));
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
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

        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_commit(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_log_append_txn_begin(log, &txn));
            CPPUNIT_ASSERT_EQUAL(0, 
                            ham_log_append_write(log, &txn, i, buffer, i));
            CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
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
                CPPUNIT_ASSERT_EQUAL((ham_u64_t)writes, 
                        log_entry_get_offset(&entry));
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
};

class LogHighLevelTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LogHighLevelTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseEnvTest);
    CPPUNIT_TEST      (createCloseOpenCloseTest);
    CPPUNIT_TEST      (createCloseOpenFullLogTest);
    CPPUNIT_TEST      (createCloseOpenFullLogRecoverTest);
    CPPUNIT_TEST      (createCloseOpenCloseEnvTest);
    CPPUNIT_TEST      (createCloseOpenFullLogEnvTest);
    CPPUNIT_TEST      (createCloseOpenFullLogEnvRecoverTest);
    CPPUNIT_TEST      (txnBeginAbortTest);
    CPPUNIT_TEST      (txnBeginCommitTest);
    CPPUNIT_TEST      (multipleTxnBeginCommitTest);
    CPPUNIT_TEST      (multipleTxnReadonlyBeginCommitTest);
    CPPUNIT_TEST      (allocatePageTest);
    CPPUNIT_TEST      (allocatePageFromFreelistTest);
    CPPUNIT_TEST      (allocateClearedPageTest);
    CPPUNIT_TEST      (singleInsertTest);
    CPPUNIT_TEST      (doubleInsertTest);
    CPPUNIT_TEST      (splitInsertTest);
    CPPUNIT_TEST      (insertAfterCheckpointTest);
    CPPUNIT_TEST      (singleEraseTest);
    CPPUNIT_TEST      (eraseMergeTest);
    CPPUNIT_TEST      (cursorOverwriteTest);
    CPPUNIT_TEST      (singleBlobTest);
    CPPUNIT_TEST      (largeBlobTest);
    CPPUNIT_TEST      (insertDuplicateTest);
    CPPUNIT_TEST      (recoverModifiedPageTest);
    CPPUNIT_TEST      (recoverModifiedPageTest2);
    CPPUNIT_TEST      (redoInsertTest);
    CPPUNIT_TEST      (redoMultipleInsertsTest);
    CPPUNIT_TEST      (redoMultipleInsertsCheckpointTest);
    CPPUNIT_TEST      (undoInsertTest);
    CPPUNIT_TEST      (undoMultipleInsertsTest);
    CPPUNIT_TEST      (undoMultipleInsertsCheckpointTest);
    CPPUNIT_TEST      (aesFilterTest);
    CPPUNIT_TEST      (aesFilterRecoverTest);
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
                ham_create(m_db, ".test", 
                    HAM_ENABLE_RECOVERY|HAM_ENABLE_DUPLICATES, 0644));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    void compareLogs(log_vector_t *lhs, log_vector_t *rhs)
    {
        CPPUNIT_ASSERT_EQUAL(lhs->size(), rhs->size());

        log_vector_t::iterator itl=lhs->begin();
        log_vector_t::iterator itr=rhs->begin(); 
        for (; itl!=lhs->end(); ++itl, ++itr) {
            CPPUNIT_ASSERT_EQUAL(log_entry_get_txn_id(&(*itl).m_entry), 
                    log_entry_get_txn_id(&(*itr).m_entry)); 
            CPPUNIT_ASSERT_EQUAL(log_entry_get_type(&(*itl).m_entry), 
                    log_entry_get_type(&(*itr).m_entry)); 
            CPPUNIT_ASSERT_EQUAL(log_entry_get_offset(&(*itl).m_entry), 
                    log_entry_get_offset(&(*itr).m_entry)); 
            CPPUNIT_ASSERT_EQUAL(log_entry_get_data_size(&(*itl).m_entry), 
                    log_entry_get_data_size(&(*itr).m_entry)); 

            if ((*itl).m_data.size()) {
                void *pl=&(*itl).m_data[0];
                void *pr=&(*itr).m_data[0];
                CPPUNIT_ASSERT_EQUAL(0, memcmp(pl, pr, 
                            log_entry_get_data_size(&(*itl).m_entry)));
            }
        }
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
                ham_mem_free(m_db, data);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_FALSE));
        return (vec);
    }

    void createCloseTest(void)
    {
        CPPUNIT_ASSERT(db_get_log(m_db)!=0);
    }

    void createCloseEnvTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_RECOVERY, 0664));
        CPPUNIT_ASSERT(env_get_log(env)==0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        CPPUNIT_ASSERT(env_get_log(env)!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT(env_get_log(env)!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT(env_get_log(env)==0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT(db_get_log(m_db)==0);
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT(db_get_log(m_db)!=0);
    }

    void createCloseOpenFullLogRecoverTest(void)
    {
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(db_get_log(m_db), &txn));
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        ham_log_t *log=db_get_log(m_db);
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));
        ham_u64_t filesize;
        CPPUNIT_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 0), &filesize));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
        CPPUNIT_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 1), &filesize));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
    }

    void createCloseOpenFullLogTest(void)
    {
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(db_get_log(m_db), &txn));
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        CPPUNIT_ASSERT_EQUAL(HAM_NEED_RECOVERY,
                ham_open(m_db, ".test", HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT(db_get_log(m_db)==0);
    }

    void createCloseOpenCloseEnvTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_RECOVERY, 0664));
        CPPUNIT_ASSERT(env_get_log(env)==0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, m_db, 333, 0, 0));
        CPPUNIT_ASSERT(env_get_log(env)!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT(env_get_log(env)!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT(env_get_log(env)==0);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open(env, ".test", HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT(env_get_log(env)!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenFullLogEnvTest(void)
    {
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(db_get_log(m_db), &txn));
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, ".test", HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT(env_get_log(env)==0);
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenFullLogEnvRecoverTest(void)
    {
        ham_txn_t txn;
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_append_txn_begin(db_get_log(m_db), &txn));
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        ham_env_t *env;
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open(env, ".test", HAM_AUTO_RECOVERY));

        /* make sure that the log files are deleted and that the lsn is 1 */
        ham_log_t *log=env_get_log(env);
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, log_get_lsn(log));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, log_get_current_fd(log));
        ham_u64_t filesize;
        CPPUNIT_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 0), &filesize));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);
        CPPUNIT_ASSERT_EQUAL(0, os_get_filesize(log_get_fd(log, 1), &filesize));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)sizeof(log_header_t), filesize);

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void txnBeginAbortTest(void)
    {
        ham_txn_t txn;
        ham_size_t pagesize=os_get_pagesize();
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_ABORT, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void txnBeginCommitTest(void)
    {
        ham_txn_t txn;
        ham_size_t pagesize=os_get_pagesize();
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(1, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void multipleTxnBeginCommitTest(void)
    {
        ham_txn_t txn[3];
        ham_size_t pagesize=os_get_pagesize();
        for (int i=0; i<3; i++)
            CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn[i], m_db, 0));
        for (int i=0; i<3; i++)
            CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn[i], 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        for (int i=0; i<3; i++)
            exp.push_back(LogEntry(3-i, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        for (int i=0; i<3; i++)
            exp.push_back(LogEntry(3-i, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void multipleTxnReadonlyBeginCommitTest(void)
    {
        ham_txn_t txn;
        ham_size_t pagesize=os_get_pagesize();
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, HAM_TXN_READ_ONLY));
        CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0));
        exp.push_back(LogEntry(2, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, pagesize, pagesize));
        compareLogs(&exp, &vec);
    }

    void allocatePageTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        ham_page_t *page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps*2, ps));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_PREWRITE, ps, ps));
        compareLogs(&exp, &vec);
    }

    void allocatePageFromFreelistTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        ham_page_t *page=db_alloc_page(m_db, 0, 
                        PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, db_free_page(page, DB_MOVE_TO_FREELIST));
        page=db_alloc_page(m_db, 0, PAGE_CLEAR_WITH_ZERO);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
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
        ham_page_t *page=db_alloc_page(m_db, 0, 
                        PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
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
        key.size=strlen(name)+1;
        rec.data=(void *)data;
        rec.size=strlen(data)+1;

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, flags));
    }

    void find(const char *name, const char *data, ham_status_t result=0)
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.data=(void *)name;
        key.size=strlen(name)+1;

        CPPUNIT_ASSERT_EQUAL(result, ham_find(m_db, 0, &key, &rec, 0));
        if (result==0)
            CPPUNIT_ASSERT_EQUAL(0, ::strcmp(data, (const char *)rec.data));
    }

    void erase(const char *name)
    {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        key.data=(void *)name;
        key.size=strlen(name)+1;

        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void singleInsertTest(void)
    {
        ham_size_t ps=os_get_pagesize();
        insert("a", "b");
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, ".test", HAM_ENABLE_RECOVERY, 0644, p));
        insert("a", "1");
        insert("b", "2");
        insert("c", "3");
        insert("d", "4");
        insert("e", "5");
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;

        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*3, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
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
        log_set_threshold(db_get_log(m_db), 5);
        insert("a", "1"); insert("b", "2"); insert("c", "3");
        insert("d", "4"); insert("e", "5"); insert("f", "6");
        insert("g", "1"); insert("h", "2"); insert("i", "3");
        insert("j", "4"); insert("k", "5"); insert("l", "6");
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, ".test", HAM_ENABLE_RECOVERY, 0644, p));
        insert("a", "1");
        insert("b", "2");
        insert("c", "3");
        insert("d", "4");
        insert("e", "5");
        erase("e");
        erase("d");
        erase("c");
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_WRITE, ps, ps));
        exp.push_back(LogEntry(8, LOG_ENTRY_TYPE_TXN_BEGIN, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_TXN_COMMIT, 0, 0, 0));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_WRITE, ps*3, ps));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_WRITE, ps*2, ps));
        exp.push_back(LogEntry(7, LOG_ENTRY_TYPE_WRITE, ps, ps));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_cursor_move(c, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_cursor_overwrite(c, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_close(m_db, HAM_DONT_CLEAR_LOG|HAM_AUTO_CLEANUP));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        log_vector_t vec=readLog();
        log_vector_t exp;
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, 0, 0, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps*2, 0));
        exp.push_back(LogEntry(0, LOG_ENTRY_TYPE_FLUSH_PAGE, ps, 0));

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
        ham_txn_t txn;
        ham_page_t *page;

        /* allocate page, write before-image, modify, commit (= write
         * after-image */
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        page=db_alloc_page(m_db, 0, 0);
        CPPUNIT_ASSERT(page!=0);
        ham_offset_t address=page_get_self(page);
        ham_u8_t *p=page_get_payload(page);
        memset(p, 0, db_get_usable_pagesize(m_db));
        p[0]=1;
        page_set_dirty(page);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_add_page_before(page));
        CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn, 0));

        /* fetch page again, modify, abort -> first modification is still
         * available, second modification is reverted */
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        page=db_fetch_page(m_db, address, 0);
        CPPUNIT_ASSERT(page!=0);
        p=page_get_payload(page);
        p[0]=2;
        page_set_dirty(page);
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));

        /* check modifications */
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        page=db_fetch_page(m_db, address, 0);
        CPPUNIT_ASSERT(page!=0);
        p=page_get_payload(page);
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)1, p[0]);
        CPPUNIT_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void recoverModifiedPageTest2(void)
    {
        ham_txn_t txn;
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();

        /* insert a key */
        insert("a", "1");

        /* fetch the page with the key, overwrite it with garbage, then
         * abort */
        CPPUNIT_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        page=db_fetch_page(m_db, ps, 0);
        CPPUNIT_ASSERT(page!=0);
        btree_node_t *node=ham_page_get_btree_node(page);
        int_key_t *entry=btree_node_get_key(m_db, node, 0);
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)'a', key_get_key(entry)[0]);
        key_get_key(entry)[0]='b';
        page_set_dirty(page);
        CPPUNIT_ASSERT_EQUAL(0, txn_abort(&txn, 0));

        /* now fetch the original key */
        ham_key_t key;
        ham_record_t record;
        memset(&key, 0, sizeof(key));
        key.data=(void *)"a";
        key.size=2; /* zero-terminated "a" */
        memset(&record, 0, sizeof(record));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &record, 0));
    }

    void redoInsertTest(void)
    {
        ham_page_t *page;

        /* insert a key */
        insert("x", "2");

        /* now walk through all pages and set them un-dirty, so they
         * are not written to the file */
        page=cache_get_totallist(db_get_cache(m_db)); 
        while (page) {
            page_set_undirty(page);
            page=page_get_next(page, PAGE_LIST_CACHED);
        }

        /* close the database (without deleting the log) */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

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
        page=cache_get_totallist(db_get_cache(m_db)); 
        while (page) {
            page_set_undirty(page);
            page=page_get_next(page, PAGE_LIST_CACHED);
        }

        /* close the database (without deleting the log) */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

        /* and make sure that the inserted item is found */
        find("x", "2");
        find("y", "3");
        find("z", "4");
    }

    void redoMultipleInsertsCheckpointTest(void)
    {
        ham_page_t *page;
        log_set_threshold(db_get_log(m_db), 5);

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
        page=cache_get_totallist(db_get_cache(m_db)); 
        while (page) {
            page_set_undirty(page);
            page=page_get_next(page, PAGE_LIST_CACHED);
        }

        /* close the database (without deleting the log) */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

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
        int found=0;
        ham_log_t *log;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_log_open((mem_allocator_t *)m_alloc, ".test", 0, &log));

        log_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        log_entry_t entry;
        ham_u8_t *data;
        while (1) {
            CPPUNIT_ASSERT_EQUAL(0, 
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
                CPPUNIT_ASSERT_EQUAL(0, os_pwrite(log_get_fd(log, iter._fdidx),
                            iter._offset, &entry, sizeof(entry)));
                found=1;
                break;
            }
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log, HAM_TRUE));
        CPPUNIT_ASSERT_EQUAL(1, found);
    }

    void undoInsertTest(void)
    {
        /* insert two keys, we will then undo the second one */
        insert("x", "2");
        insert("y", "3");
        /* close the database (without deleting the log) */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        /* now walk through the logfile and patch the COMMIT entry of the
         * second insert (txn-id=2) to ABORT */
        patchLogfile(".test", 2);

        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

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
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        /* now walk through the logfile and patch the COMMIT entry of the
         * second and third insert (txn-id=2, 3) to ABORT */
        patchLogfile(".test", 2);
        patchLogfile(".test", 3);

        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

        /* and make sure that the inserted item is found, but not the 
         * aborted item */
        find("1", "2");
        find("2", "3", HAM_KEY_NOT_FOUND);
        find("3", "4", HAM_KEY_NOT_FOUND);
    }

    void undoMultipleInsertsCheckpointTest(void)
    {
        log_set_threshold(db_get_log(m_db), 5);

        /* insert two keys, we will then undo the second one */
        insert("1", "2");
        insert("2", "3");
        insert("3", "4");
        insert("4", "5");
        insert("5", "6");
        insert("6", "7");
        /* close the database (without deleting the log) */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        /* now walk through the logfile and patch the COMMIT entry of the
         * last insert (txn-id=7) to ABORT */
        patchLogfile(".test", 6);

        /* now reopen and recover */
        CPPUNIT_ASSERT_EQUAL(0,
                ham_open(m_db, ".test", HAM_AUTO_RECOVERY|HAM_ENABLE_RECOVERY));

        /* and make sure that the inserted item is found, but not the 
         * aborted item */
        find("1", "2");
        find("2", "3");
        find("3", "4");
        find("4", "5");
        find("5", "6");
        find("6", "7", HAM_KEY_NOT_FOUND);
    }

    void aesFilterTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        /* close m_db, otherwise ham_env_create fails on win32 */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 
                    HAM_ENABLE_RECOVERY, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 
                    HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

    void aesFilterRecoverTest()
    {
#ifndef HAM_DISABLE_ENCRYPTION
        /* close m_db, otherwise ham_env_create fails on win32 */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_db_t *db;

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16] ={0x13};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 
                    HAM_ENABLE_RECOVERY, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_DONT_CLEAR_LOG));

        CPPUNIT_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_env_open(env, ".test", HAM_ENABLE_RECOVERY));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open(env, ".test", HAM_AUTO_RECOVERY));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_enable_encryption(env, aeskey, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);
CPPUNIT_TEST_SUITE_REGISTRATION(LogHighLevelTest);

