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
#include "../src/journal.h"
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

class JournalTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    JournalTest()
        : hamsterDB_fixture("JournalTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(JournalTest, structHeaderTest);
        BFC_REGISTER_TEST(JournalTest, structEntryTest);
        BFC_REGISTER_TEST(JournalTest, structJournalTest);
        BFC_REGISTER_TEST(JournalTest, createCloseTest);
        BFC_REGISTER_TEST(JournalTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(JournalTest, negativeCreateTest);
        BFC_REGISTER_TEST(JournalTest, negativeOpenTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnBeginTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnAbortTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnCommitTest);
        BFC_REGISTER_TEST(JournalTest, appendInsertTest);
        BFC_REGISTER_TEST(JournalTest, appendEraseTest);
        BFC_REGISTER_TEST(JournalTest, clearTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverEmptyLogTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogOneEntryTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntryTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntrySwapTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntrySwapTwiceTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntryWithDataTest);
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
                        HAM_ENABLE_TRANSACTIONS|HAM_ENABLE_RECOVERY, 0644));
    
        m_env=db_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    journal_t *disconnect_and_create_new_journal(void)
    {
        journal_t *log;
        ham_env_t *env=db_get_env(m_db);

        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
            journal_create(env, 0644, 0, &log));
        BFC_ASSERT_NULL(log);

        /* 
         * make sure db->journal is already NULL, i.e. disconnected. 
         * Otherwise an BFC ASSERT for journal_close() will segfault 
         * the teardown() code, which will try to close the db->journal 
         * all over AGAIN! 
         */
        log = env_get_journal(env);
        env_set_journal(env, NULL);
        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, 
            journal_create(env, 0644, 0, &log));
        BFC_ASSERT_NOTNULL(log);
        env_set_journal(env, log);
        return (log);
    }

    void structHeaderTest()
    {
        journal_header_t hdr;

        journal_header_set_magic(&hdr, 0x1234);
        BFC_ASSERT_EQUAL((ham_u32_t)0x1234, journal_header_get_magic(&hdr));

        journal_header_set_lsn(&hdr, 0x888ull);
        BFC_ASSERT_EQUAL((ham_u64_t)0x888ull, journal_header_get_lsn(&hdr));
    }

    void structEntryTest()
    {
        journal_entry_t e;

        journal_entry_set_lsn(&e, 0x13);
        BFC_ASSERT_EQUAL((ham_u64_t)0x13, journal_entry_get_lsn(&e));

        journal_entry_set_txn_id(&e, 0x15);
        BFC_ASSERT_EQUAL((ham_u64_t)0x15, journal_entry_get_txn_id(&e));

        journal_entry_set_followup_size(&e, 0x16);
        BFC_ASSERT_EQUAL((ham_u64_t)0x16, journal_entry_get_followup_size(&e));
        journal_entry_set_followup_size(&e, 0);

        journal_entry_set_flags(&e, 0xff000000);
        BFC_ASSERT_EQUAL((ham_u32_t)0xff000000, journal_entry_get_flags(&e));

        journal_entry_set_type(&e, JOURNAL_ENTRY_TYPE_INSERT);
        BFC_ASSERT_EQUAL((ham_u32_t)JOURNAL_ENTRY_TYPE_INSERT, 
                journal_entry_get_type(&e));
    }

    void structJournalTest(void)
    {
        journal_t log;

        BFC_ASSERT_NOTNULL(env_get_journal(m_env));

        journal_set_allocator(&log, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL((mem_allocator_t *)m_alloc, 
                        journal_get_allocator(&log));

        journal_set_current_fd(&log, 1);
        BFC_ASSERT_EQUAL((unsigned)1, journal_get_current_fd(&log));

        journal_set_fd(&log, 0, (ham_fd_t)0x20);
        BFC_ASSERT_EQUAL((ham_fd_t)0x20, journal_get_fd(&log, 0));
        journal_set_fd(&log, 1, (ham_fd_t)0x21);
        BFC_ASSERT_EQUAL((ham_fd_t)0x21, journal_get_fd(&log, 1));

        journal_set_lsn(&log, 0x99);
        BFC_ASSERT_EQUAL((ham_u64_t)0x99, journal_get_lsn(&log));

        journal_set_last_checkpoint_lsn(&log, 0x100);
        BFC_ASSERT_EQUAL((ham_u64_t)0x100, 
                        journal_get_last_checkpoint_lsn(&log));

        for (int i=0; i<2; i++) {
            journal_set_open_txn(&log, i, 0x15+i);
            BFC_ASSERT_EQUAL((ham_size_t)0x15+i, 
                    journal_get_open_txn(&log, i));
            journal_set_closed_txn(&log, i, 0x25+i);
            BFC_ASSERT_EQUAL((ham_size_t)0x25+i, 
                    journal_get_closed_txn(&log, i));
        }
    }

    void createCloseTest(void)
    {
        ham_bool_t isempty;
        journal_t *log = disconnect_and_create_new_journal();

        BFC_ASSERT_EQUAL((ham_offset_t)1, journal_get_lsn(log));
        /* TODO make sure that the two files exist and 
         * contain only the header */

        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        // do not close the journal - it will be closed in teardown()
    }

    void createCloseOpenCloseTest(void)
    {
        ham_bool_t isempty;
        journal_t *log=env_get_journal(m_env);
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, journal_open(m_env, 0, &log));
        BFC_ASSERT(log!=0);
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        env_set_journal(m_env, log);
    }

    void negativeCreateTest(void)
    {
        journal_t *log;
        const char *oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "/::asdf");
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                journal_create(m_env, 0644, 0, &log));
        env_set_filename(m_env, oldfilename);
        BFC_ASSERT_EQUAL((journal_t *)0, log);
    }

    void negativeOpenTest(void)
    {
        ham_fd_t fd;
        journal_t *log;
        const char *oldfilename=env_get_filename(m_env);
        env_set_filename(m_env, "xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                    journal_open(m_env, 0, &log));
        BFC_ASSERT_EQUAL((journal_t *)0, log);

        /* if journal_open() fails, it will call journal_close() internally and 
         * journal_close() overwrites the header structure. therefore we have
         * to patch the file before we start the test. */
        BFC_ASSERT_EQUAL(0, os_open("data/log-broken-magic.jrn0", 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, (void *)"x", 1));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        env_set_filename(m_env, "data/log-broken-magic");
        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, 
                    journal_open(m_env,  0, &log));
        env_set_filename(m_env, oldfilename);
        BFC_ASSERT_EQUAL((journal_t *)0, log);
    }

    void appendTxnBeginTest(void)
    {
        ham_bool_t isempty;
        journal_t *log = disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_size_t)1, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendTxnAbortTest(void)
    {
        ham_bool_t isempty;
        journal_t *log = disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)1, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, journal_append_txn_abort(log, txn));
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)3, journal_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)1, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendTxnCommitTest(void)
    {
        ham_bool_t isempty;
        journal_t *log = disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)1, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, journal_append_txn_commit(log, txn));
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)3, journal_get_lsn(log));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)1, journal_get_closed_txn(log, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_open_txn(log, 1));
        BFC_ASSERT_EQUAL((ham_size_t)0, journal_get_closed_txn(log, 1));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendInsertTest(void)
    {
        journal_t *log = disconnect_and_create_new_journal();
        ham_txn_t *txn;
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"key1";
        key.size=5;
        rec.data=(void *)"rec1";
        rec.size=5;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                    journal_append_insert(log, txn, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL((ham_u64_t)3, journal_get_lsn(log));
        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        /* verify that the insert entry was written correctly */
        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        journal_entry_t entry;
        journal_entry_insert_t *ins;
        BFC_ASSERT_EQUAL(0,  // this is the txn
                    journal_get_entry(log, &iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL(0,  // this is the insert
                    journal_get_entry(log, &iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL(5, journal_entry_insert_get_key_size(ins));
        BFC_ASSERT_EQUAL(5u, journal_entry_insert_get_record_size(ins));
        BFC_ASSERT_EQUAL(0ull, 
                    journal_entry_insert_get_record_partial_size(ins));
        BFC_ASSERT_EQUAL(0ull, 
                    journal_entry_insert_get_record_partial_offset(ins));
        BFC_ASSERT_EQUAL((unsigned)HAM_OVERWRITE, 
                    journal_entry_insert_get_flags(ins));
        BFC_ASSERT_EQUAL(0, 
                    strcmp("key1", 
                            (char *)journal_entry_insert_get_key_data(ins)));
        BFC_ASSERT_EQUAL(0, 
                    strcmp("rec1", 
                            (char *)journal_entry_insert_get_record_data(ins)));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendEraseTest(void)
    {
        journal_t *log = disconnect_and_create_new_journal();
        ham_txn_t *txn;
        ham_key_t key={0};
        key.data=(void *)"key1";
        key.size=5;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                    journal_append_erase(log, txn, &key, 1, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)3, journal_get_lsn(log));
        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        /* verify that the insert entry was written correctly */
        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
        journal_entry_t entry;
        journal_entry_erase_t *er;
        BFC_ASSERT_EQUAL(0, // this is the txn
                    journal_get_entry(log, &iter, &entry, (void **)&er));
        BFC_ASSERT_EQUAL(0, // this is the erase
                    journal_get_entry(log, &iter, &entry, (void **)&er));
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL(5, journal_entry_erase_get_key_size(er));
        BFC_ASSERT_EQUAL(0u, journal_entry_erase_get_flags(er));
        BFC_ASSERT_EQUAL(1u, journal_entry_erase_get_dupe(er));
        BFC_ASSERT_EQUAL(0, 
                    strcmp("key1", 
                            (char *)journal_entry_erase_get_key_data(er)));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void clearTest(void)
    {
        ham_bool_t isempty;
        journal_t *log = disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(0, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));

        BFC_ASSERT_EQUAL(0, journal_clear(log));
        BFC_ASSERT_EQUAL(0, journal_is_empty(log, &isempty));
        BFC_ASSERT_EQUAL(1, isempty);
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));

        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        BFC_ASSERT_EQUAL((ham_u64_t)2, journal_get_lsn(log));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void iterateOverEmptyLogTest(void)
    {
        journal_t *log = disconnect_and_create_new_journal();

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        journal_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, 
                    journal_get_entry(log, &iter, &entry, (void **)&data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, journal_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t *txn;
        journal_t *log = disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(1ull, journal_get_lsn(log));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, journal_append_txn_begin(log, txn));
        BFC_ASSERT_EQUAL(0, journal_close(log, HAM_TRUE));

        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        BFC_ASSERT_EQUAL(2ull, journal_get_lsn(log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

        journal_entry_t entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, 
                    journal_get_entry(log, &iter, &entry, (void **)&data));
        BFC_ASSERT_EQUAL((ham_u64_t)1, journal_entry_get_lsn(&entry));
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));
        BFC_ASSERT_EQUAL((ham_u64_t)1, journal_entry_get_txn_id(&entry));
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);
        BFC_ASSERT_EQUAL((ham_u32_t)JOURNAL_ENTRY_TYPE_TXN_BEGIN, 
                        journal_entry_get_type(&entry));

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void checkJournalEntry(journal_entry_t *entry, ham_u64_t lsn, 
                    ham_u64_t txn_id, ham_u32_t type, ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, journal_entry_get_lsn(entry));
        BFC_ASSERT_EQUAL(txn_id, journal_entry_get_txn_id(entry));
        if (!journal_entry_get_followup_size(entry)) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            allocator_free((mem_allocator_t *)m_alloc, data);
        }
        BFC_ASSERT_EQUAL(type, journal_entry_get_type(entry));
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        ham_txn_t *txn;
        journal_t *log=env_get_journal(m_env);

        for (int i=0; i<5; i++) {
            // ham_txn_begin and ham_txn_abort will automatically add a 
            // journal entry
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

#if 0 /* TODO */
        journal_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 13, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 12, 0, LOG_ENTRY_TYPE_FLUSH_PAGE, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 11, 5, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 10, 5, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  9, 4, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  8, 4, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  7, 3, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  6, 3, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  5, 2, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  4, 2, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  3, 1, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  2, 1, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry,  1, 0, LOG_ENTRY_TYPE_PREWRITE, data);
#endif

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTest(void)
    {
        ham_txn_t *txn;
        journal_t *log=env_get_journal(m_env);
        journal_set_threshold(log, 5);

        for (int i=0; i<=7; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));

#if 0 /* TODO */
        journal_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 18, 8, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 17, 8, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 16, 7, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 15, 7, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 14, 6, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 13, 6, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 12, 0, LOG_ENTRY_TYPE_CHECKPOINT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 11, 5, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 10, 5, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 9, 4, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 8, 4, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 7, 3, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 6, 3, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 5, 2, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 4, 2, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 3, 1, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 2, 1, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 1, 0, LOG_ENTRY_TYPE_PREWRITE, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
#endif

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTwiceTest(void)
    {
        ham_txn_t *txn;
        journal_t *log=env_get_journal(m_env);
        journal_set_threshold(log, 5);

        for (int i=0; i<=10; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
#if 0 /* TODO */
        journal_entry_t entry;
        ham_u8_t *data;

        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 25, 11, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 24, 11, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 23, 0, LOG_ENTRY_TYPE_CHECKPOINT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 22, 10, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 21, 10, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 20, 9, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 19, 9, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 18, 8, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 17, 8, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 16, 7, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 15, 7, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 14, 6, LOG_ENTRY_TYPE_TXN_ABORT, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        checkJournalEntry(&entry, 13, 6, LOG_ENTRY_TYPE_TXN_BEGIN, data);
        BFC_ASSERT_EQUAL(0, journal_get_entry(log, &iter, &entry, &data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, log_entry_get_lsn(&entry));
#endif

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntryWithDataTest(void)
    {
        ham_txn_t *txn;
        ham_u8_t buffer[20];
        journal_t *log=env_get_journal(m_env);

        for (int i=0; i<5; i++) {
            memset(buffer, (char)i, sizeof(buffer));
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
#if 0 /* TODO */
            BFC_ASSERT_EQUAL(0, 
                            journal_append_insert(log, txn, i, buffer, i));
#endif
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=db_get_env(m_db);
        BFC_ASSERT_EQUAL(0, 
                journal_open(m_env, 0, &log));
        env_set_journal(m_env, log);
        BFC_ASSERT(log!=0);

        journal_iterator_t iter;
        memset(&iter, 0, sizeof(iter));
#if 0 /* TODO */
        journal_entry_t entry;
        ham_u8_t *data;

        int writes=4;

        while (1) {
            BFC_ASSERT_EQUAL(0, 
                    journal_get_entry(log, &iter, &entry, &data));
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
#endif
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }
    
};


BFC_REGISTER_FIXTURE(JournalTest);

