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
#include "../src/db.h"
#include "../src/log.h"
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

        log_entry_set_size(&e, 0x16);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x16, log_entry_get_size(&e));

        log_entry_set_flags(&e, 0xff000000);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0xff000000, log_entry_get_flags(&e));

        log_entry_set_type(&e, LOG_ENTRY_TYPE_CHECKPOINT);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)LOG_ENTRY_TYPE_CHECKPOINT, 
                log_entry_get_type(&e));

        log_entry_set_last_checkpoint(&e, 0x17);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x17,
                log_entry_get_last_checkpoint(&e));

        CPPUNIT_ASSERT(log_entry_get_data(&e)!=0);
    }

    void structLogTest(void)
    {
        ham_log_t log;

        log_set_db(&log, m_db);
        CPPUNIT_ASSERT_EQUAL(m_db, log_get_db(&log));

        log_set_flags(&log, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x13, log_get_flags(&log));

        log_set_fd(&log, 0, (ham_fd_t)0x20);
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x20, log_get_fd(&log, 0));
        log_set_fd(&log, 1, (ham_fd_t)0x21);
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x21, log_get_fd(&log, 1));
        log_swap_fds(&log);
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x21, log_get_fd(&log, 0));
        CPPUNIT_ASSERT_EQUAL((ham_fd_t)0x20, log_get_fd(&log, 1));

        log_set_lsn(&log, 0x99);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x99, log_get_lsn(&log));
    }

    void createCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log=ham_log_create(m_db, ".test", 0644, 0);
        CPPUNIT_ASSERT(log!=0);

        CPPUNIT_ASSERT_EQUAL(m_db, log_get_db(log));
        CPPUNIT_ASSERT_EQUAL(0u, log_get_flags(log));
        /* TODO make sure that the two files exist and 
         * contain only the header */

        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);

        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_bool_t isempty;
        ham_log_t *log=ham_log_create(m_db, ".test", 0644, 0);
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log));

        log=ham_log_open(m_db, ".test", 0);
        CPPUNIT_ASSERT(log!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_is_empty(log, &isempty));
        CPPUNIT_ASSERT_EQUAL(1, isempty);
        CPPUNIT_ASSERT_EQUAL(0, ham_log_close(log));
    }

    void negativeCreateTest(void)
    {
        ham_log_t *log=ham_log_create(m_db, "/.test", 0644, 0);
        CPPUNIT_ASSERT_EQUAL((ham_log_t *)0, log);
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, ham_get_error(m_db));
    }

    void negativeOpenTest(void)
    {
        ham_log_t *log=ham_log_open(m_db, "xxx$$test", 0);
        CPPUNIT_ASSERT_EQUAL((ham_log_t *)0, log);
        CPPUNIT_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, ham_get_error(m_db));

        log=ham_log_open(m_db, "data/log-broken-magic", 0);
        CPPUNIT_ASSERT_EQUAL((ham_log_t *)0, log);
        CPPUNIT_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, ham_get_error(m_db));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(LogTest);

