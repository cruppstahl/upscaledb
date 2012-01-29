/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

struct LogEntry
{
    LogEntry()
    :   lsn(0), txn_id(0), type(0), dbname(0) {
    }

    LogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, 
                ham_u32_t _type, ham_u16_t _dbname, const char *_name=0)
    :   lsn(_lsn), txn_id(_txn_id), type(_type), dbname(_dbname) {
        if (_name)
            name=_name;
    }

    ham_u64_t lsn;
    ham_u64_t txn_id;
    ham_u32_t type;
    ham_u16_t dbname;
    std::string name;
};

struct InsertLogEntry : public LogEntry
{
    InsertLogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, ham_u16_t _dbname, 
                ham_key_t *_key, ham_record_t *_record)
    : LogEntry(_lsn, _txn_id, Journal::ENTRY_TYPE_INSERT, _dbname),
        key(_key), record(_record)
    {
    }

    ham_key_t *key;
    ham_record_t *record;
};

struct EraseLogEntry : public LogEntry
{
    EraseLogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, ham_u16_t _dbname, 
                ham_key_t *_key)
    : LogEntry(_lsn, _txn_id, Journal::ENTRY_TYPE_INSERT, _dbname),
        key(_key)
    {
    }

    ham_key_t *key;
};

class JournalTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    JournalTest()
        : hamsterDB_fixture("JournalTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(JournalTest, createCloseTest);
        BFC_REGISTER_TEST(JournalTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(JournalTest, negativeCreateTest);
        BFC_REGISTER_TEST(JournalTest, negativeOpenTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnBeginTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnAbortTest);
        BFC_REGISTER_TEST(JournalTest, appendTxnCommitTest);
        BFC_REGISTER_TEST(JournalTest, appendInsertTest);
        BFC_REGISTER_TEST(JournalTest, appendPartialInsertTest);
        BFC_REGISTER_TEST(JournalTest, appendEraseTest);
        BFC_REGISTER_TEST(JournalTest, clearTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverEmptyLogTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogOneEntryTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntryTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntrySwapTest);
        BFC_REGISTER_TEST(JournalTest, iterateOverLogMultipleEntrySwapTwiceTest);
        BFC_REGISTER_TEST(JournalTest, recoverVerifyTxnIdsTest);
        BFC_REGISTER_TEST(JournalTest, recoverCommittedTxnsTest);
        BFC_REGISTER_TEST(JournalTest, recoverAutoAbortTxnsTest);
        BFC_REGISTER_TEST(JournalTest, recoverSkipAlreadyFlushedTest);
        BFC_REGISTER_TEST(JournalTest, recoverInsertTest);
        BFC_REGISTER_TEST(JournalTest, recoverEraseTest);
        BFC_REGISTER_TEST(JournalTest, lsnOverflowTest);
    }

protected:
    ham_db_t *m_db;
    Environment *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        m_alloc=memtracker_new();
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_DUPLICATES
                        |HAM_ENABLE_TRANSACTIONS
                        |HAM_ENABLE_RECOVERY, 0644));
    
        m_env=(Environment *)ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL((unsigned long)0, memtracker_get_leaks(m_alloc));
    }

    Journal *disconnect_and_create_new_journal(void)
    {
        Environment *env=(Environment *)ham_get_env(m_db);
        Journal *j=new Journal(env);

        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, j->create());
        delete (j);

        /* 
         * make sure db->journal is already NULL, i.e. disconnected. 
         * Otherwise an BFC ASSERT for journal_close() will segfault 
         * the teardown() code, which will try to close the db->journal 
         * all over AGAIN! 
         */
        j=env->get_journal();
        env->set_journal(NULL);
        BFC_ASSERT_EQUAL(0, j->close());
        delete j;

        j=new Journal(env);
        BFC_ASSERT_EQUAL(0, j->create());
        BFC_ASSERT_NOTNULL(j);
        env->set_journal(j);
        return (j);
    }

    void createCloseTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();

        BFC_ASSERT_EQUAL((ham_offset_t)1, j->get_lsn());
        /* TODO make sure that the two files exist and 
         * contain only the header */

        BFC_ASSERT_EQUAL(true, j->is_empty());

        // do not close the journal - it will be closed in teardown()
    }

    void createCloseOpenCloseTest(void)
    {
        Journal *j=m_env->get_journal();
        BFC_ASSERT_EQUAL(true, j->is_empty());
        BFC_ASSERT_EQUAL(0, j->close(true));

        BFC_ASSERT_EQUAL(0, j->open());
        BFC_ASSERT_EQUAL(true, j->is_empty());
        m_env->set_journal(j);
    }

    void negativeCreateTest(void)
    {
        Journal *j=new Journal(m_env);
        std::string oldfilename=m_env->get_filename();
        m_env->set_filename("/::asdf");
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, j->create());
        m_env->set_filename(oldfilename);
        delete (j);
    }

    void negativeOpenTest(void)
    {
        ham_fd_t fd;
        Journal *j=new Journal(m_env);
        std::string oldfilename=m_env->get_filename();
        m_env->set_filename("xxx$$test");
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, j->open());

        /* if journal::open() fails, it will call journal::close() 
         * internally and journal::close() overwrites the header structure. 
         * therefore we have to patch the file before we start the test. */
        BFC_ASSERT_EQUAL(0, os_open("data/log-broken-magic.jrn0", 0, &fd));
        BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, (void *)"x", 1));
        BFC_ASSERT_EQUAL(0, os_close(fd, 0));

        m_env->set_filename("data/log-broken-magic");
        BFC_ASSERT_EQUAL(HAM_LOG_INV_FILE_HEADER, j->open());
        m_env->set_filename(oldfilename);
        delete j;
    }

    void appendTxnBeginTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(true, j->is_empty());

        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, 
                ham_txn_begin(&txn, (ham_env_t *)m_env, "name", 0, 0));

        BFC_ASSERT_EQUAL((ham_size_t)1, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)2, j->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendTxnAbortTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(true, j->is_empty());

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)2, j->get_lsn());
        BFC_ASSERT_EQUAL((ham_size_t)1, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        ham_u64_t lsn;
        BFC_ASSERT_EQUAL(0, env_get_incremented_lsn(m_env, &lsn));
        BFC_ASSERT_EQUAL(0, j->append_txn_abort(txn, lsn));
        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)1, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendTxnCommitTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(true, j->is_empty());

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)2, j->get_lsn());
        BFC_ASSERT_EQUAL((ham_size_t)1, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        ham_u64_t lsn;
        BFC_ASSERT_EQUAL(0, env_get_incremented_lsn(m_env, &lsn));
        BFC_ASSERT_EQUAL(0, j->append_txn_commit(txn, lsn));
        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)1, j->m_closed_txn[0]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_open_txn[1]);
        BFC_ASSERT_EQUAL((ham_size_t)0, j->m_closed_txn[1]);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendInsertTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        ham_txn_t *txn;
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"key1";
        key.size=5;
        rec.data=(void *)"rec1";
        rec.size=5;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));

        ham_u64_t lsn;
        BFC_ASSERT_EQUAL(0, env_get_incremented_lsn(m_env, &lsn));
        BFC_ASSERT_EQUAL(0, 
                    j->append_insert((Database *)m_db, txn, &key, &rec, 
                                HAM_OVERWRITE, lsn));
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        BFC_ASSERT_EQUAL(0, j->close(true));

        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        /* verify that the insert entry was written correctly */
        Journal::Iterator iter;
        memset(&iter, 0, sizeof(iter));
        JournalEntry entry;
        JournalEntryInsert *ins;
        BFC_ASSERT_EQUAL(0,  // this is the txn
                    j->get_entry(&iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL(0,  // this is the insert
                    j->get_entry(&iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL((ham_u64_t)2, entry.lsn);
        BFC_ASSERT_EQUAL(5, ins->key_size);
        BFC_ASSERT_EQUAL(5u, ins->record_size);
        BFC_ASSERT_EQUAL(0ull, ins->record_partial_size);
        BFC_ASSERT_EQUAL(0ull, ins->record_partial_offset);
        BFC_ASSERT_EQUAL((unsigned)HAM_OVERWRITE, ins->insert_flags);
        BFC_ASSERT_EQUAL(0, strcmp("key1", (char *)ins->get_key_data()));
        BFC_ASSERT_EQUAL(0, strcmp("rec1", (char *)ins->get_record_data()));

        j->alloc_free(ins);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendPartialInsertTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        ham_txn_t *txn;
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"key1";
        key.size=5;
        rec.data=(void *)"rec1";
        rec.size=15;
        rec.partial_size=5;
        rec.partial_offset=10;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));

        ham_u64_t lsn;
        BFC_ASSERT_EQUAL(0, env_get_incremented_lsn(m_env, &lsn));
        BFC_ASSERT_EQUAL(0, 
                    j->append_insert((Database *)m_db, txn, &key, &rec, 
                                HAM_PARTIAL, lsn));
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        BFC_ASSERT_EQUAL(0, j->close(true));

        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        /* verify that the insert entry was written correctly */
        Journal::Iterator iter;
        memset(&iter, 0, sizeof(iter));
        JournalEntry entry;
        JournalEntryInsert *ins;
        BFC_ASSERT_EQUAL(0,  // this is the txn
                    j->get_entry(&iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL(0,  // this is the insert
                    j->get_entry(&iter, &entry, (void **)&ins));
        BFC_ASSERT_EQUAL((ham_u64_t)2, entry.lsn);
        BFC_ASSERT_EQUAL(5, ins->key_size);
        BFC_ASSERT_EQUAL(15u, ins->record_size);
        BFC_ASSERT_EQUAL(5u, ins->record_partial_size);
        BFC_ASSERT_EQUAL(10u, ins->record_partial_offset);
        BFC_ASSERT_EQUAL((unsigned)HAM_PARTIAL, ins->insert_flags);
        BFC_ASSERT_EQUAL(0, strcmp("key1", (char *)ins->get_key_data()));
        BFC_ASSERT_EQUAL(0, strcmp("rec1", (char *)ins->get_record_data()));

		j->alloc_free(ins);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void appendEraseTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        ham_txn_t *txn;
        ham_key_t key={0};
        key.data=(void *)"key1";
        key.size=5;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));

        ham_u64_t lsn;
        BFC_ASSERT_EQUAL(0, env_get_incremented_lsn(m_env, &lsn));
        BFC_ASSERT_EQUAL(0, j->append_erase((Database *)m_db, 
                    txn, &key, 1, 0, lsn));
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        BFC_ASSERT_EQUAL(0, j->close(true));

        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        /* verify that the erase entry was written correctly */
        Journal::Iterator iter;
        memset(&iter, 0, sizeof(iter));
        JournalEntry entry;
        JournalEntryErase *er;
        BFC_ASSERT_EQUAL(0, // this is the txn
                    j->get_entry(&iter, &entry, (void **)&er));
        BFC_ASSERT_EQUAL(0, // this is the erase
                    j->get_entry(&iter, &entry, (void **)&er));
        BFC_ASSERT_EQUAL((ham_u64_t)2, entry.lsn);
        BFC_ASSERT_EQUAL(5, er->key_size);
        BFC_ASSERT_EQUAL(0u, er->erase_flags);
        BFC_ASSERT_EQUAL(1u, er->duplicate);
        BFC_ASSERT_EQUAL(0, strcmp("key1", (char *)er->get_key_data()));

        j->alloc_free(er);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void clearTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(true, j->is_empty());

        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(false, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)2, j->get_lsn());

        BFC_ASSERT_EQUAL(0, j->clear());
        BFC_ASSERT_EQUAL(true, j->is_empty());
        BFC_ASSERT_EQUAL((ham_u64_t)2, j->get_lsn());

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());

        BFC_ASSERT_EQUAL(0, j->close());
        BFC_ASSERT_EQUAL(0, j->open());
        BFC_ASSERT_EQUAL((ham_u64_t)3, j->get_lsn());
        m_env->set_journal(j);
    }

    void iterateOverEmptyLogTest(void)
    {
        Journal *j=disconnect_and_create_new_journal();

        Journal::Iterator iter;
        memset(&iter, 0, sizeof(iter));

        JournalEntry entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, 
                    j->get_entry(&iter, &entry, (void **)&data));
        BFC_ASSERT_EQUAL((ham_u64_t)0, entry.lsn);
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);
    }

    void iterateOverLogOneEntryTest(void)
    {
        ham_txn_t *txn;
        Journal *j=disconnect_and_create_new_journal();
        BFC_ASSERT_EQUAL(1ull, j->get_lsn());
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        BFC_ASSERT_EQUAL(0, 
                j->append_txn_begin(txn, m_env, 0, j->get_lsn()));
        BFC_ASSERT_EQUAL(0, j->close(true));

        BFC_ASSERT_EQUAL(0, j->open());
        BFC_ASSERT_EQUAL(2ull, j->get_lsn());
        m_env->set_journal(j);

        Journal::Iterator iter;
        memset(&iter, 0, sizeof(iter));

        JournalEntry entry;
        ham_u8_t *data;
        BFC_ASSERT_EQUAL(0, 
                    j->get_entry(&iter, &entry, (void **)&data));
        BFC_ASSERT_EQUAL((ham_u64_t)1, entry.lsn);
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));
        BFC_ASSERT_EQUAL((ham_u64_t)1, entry.txn_id);
        BFC_ASSERT_EQUAL((ham_u8_t *)0, data);
        BFC_ASSERT_EQUAL((ham_u32_t)Journal::ENTRY_TYPE_TXN_BEGIN, 
                        entry.type);

        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    }

    void checkJournalEntry(JournalEntry *entry, ham_u64_t lsn, 
                    ham_u64_t txn_id, ham_u32_t type, ham_u8_t *data)
    {
        BFC_ASSERT_EQUAL(lsn, entry->lsn);
        BFC_ASSERT_EQUAL(txn_id, entry->txn_id);
        if (!entry->followup_size) {
            BFC_ASSERT_NULL(data);
        }
        else {
            BFC_ASSERT_NOTNULL(data);
            allocator_free((mem_allocator_t *)m_alloc, data);
        }
        BFC_ASSERT_EQUAL(type, entry->type);
    }

    void compareJournal(Journal *journal, LogEntry *vec, unsigned size)
    {
        Journal::Iterator it;
        JournalEntry entry;
        void *aux;
        unsigned s=0;

        do {
            BFC_ASSERT_EQUAL(0, journal->get_entry(&it, &entry, &aux));
            if (entry.lsn==0)
                break;

            if (s==size) {
                BFC_ASSERT_EQUAL(0ull, entry.lsn);
                break;
            }
#if 0
            std::cout << "vector: lsn=" << (*vit).lsn << "; txn=" 
                      << (*vit).txn_id << "; type=" << (*vit).type 
                      << "; dbname=" << (*vit).dbname << std::endl;
            std::cout << "journl: lsn=" << entry.lsn 
                      << "; txn=" << entry.txn_id
                      << "; type=" << entry.type
                      << "; dbname=" << entry.dbname 
                      << std::endl
                      << std::endl;
#endif

            s++;

            BFC_ASSERT_EQUAL(vec->lsn, entry.lsn);
            BFC_ASSERT_EQUAL(vec->txn_id, entry.txn_id);
            BFC_ASSERT_EQUAL(vec->type, entry.type);
            BFC_ASSERT_EQUAL(vec->dbname, entry.dbname);
            if (vec->name.size()) {
                BFC_ASSERT_NOTNULL(aux);
                BFC_ASSERT_EQUAL(0, strcmp((char *)aux, vec->name.c_str()));
            }

            if (aux)
                journal->alloc_free(aux);

            vec++;

        } while (1);

        if (aux)
            journal->alloc_free(aux);
        BFC_ASSERT_EQUAL(s, size);
    }

    void iterateOverLogMultipleEntryTest(void)
    {
        ham_txn_t *txn;
        Journal *j=disconnect_and_create_new_journal();
        unsigned p=0;

        LogEntry vec[20];
        for (int i=0; i<5; i++) {
            // ham_txn_begin and ham_txn_abort will automatically add a 
            // journal entry
            char name[16];
            sprintf(name, "name%d", i);
            BFC_ASSERT_EQUAL(0, 
                    ham_txn_begin(&txn, (ham_env_t *)m_env, name, 0, 0));
            vec[p++]=LogEntry(1+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_BEGIN, 0, &name[0]);
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
            vec[p++]=LogEntry(2+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_ABORT, 0);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        compareJournal(j, vec, p);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTest(void)
    {
        ham_txn_t *txn;
        Journal *j=disconnect_and_create_new_journal();
        j->m_threshold=5;
        unsigned p=0;
        LogEntry vec[20];

        for (int i=0; i<=7; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
            vec[p++]=LogEntry(1+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
            vec[p++]=LogEntry(2+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_ABORT, 0);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        compareJournal(j, vec, p);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void iterateOverLogMultipleEntrySwapTwiceTest(void)
    {
        ham_txn_t *txn;
        Journal *j=disconnect_and_create_new_journal();
        j->m_threshold=5;

        unsigned p=0;
        LogEntry vec[20];

        for (int i=0; i<=10; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
            if (i>=5)
                vec[p++]=LogEntry(1+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
            if (i>=5)
                vec[p++]=LogEntry(2+i*2, txn_get_id(txn), 
                            Journal::ENTRY_TYPE_TXN_ABORT, 0);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);

        compareJournal(j, vec, p);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void verifyJournalIsEmpty(void)
    {
        ham_offset_t size;
        Journal *j=m_env->get_journal();
        BFC_ASSERT_EQUAL(0, os_get_filesize(j->m_fd[0], &size));
        BFC_ASSERT_EQUAL(sizeof(Journal::Header), size);
        BFC_ASSERT_EQUAL(0, os_get_filesize(j->m_fd[1], &size));
        BFC_ASSERT_EQUAL(sizeof(Journal::Header), size);
    }

    void recoverVerifyTxnIdsTest(void)
    {
        ham_txn_t *txn;
        LogEntry vec[20];
        unsigned p=0;

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, txn_get_id(txn));
            vec[p++]=LogEntry(1+i*2, txn_get_id(txn), 
                        Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            ham_u64_t txnid=txn_get_id(txn);
            BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
            vec[p++]=LogEntry(2+i*2, txnid,
                        Journal::ENTRY_TYPE_TXN_COMMIT, 0);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));

        /* reopen the database */
        BFC_ASSERT_EQUAL(HAM_NEED_RECOVERY, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_ENABLE_RECOVERY));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* verify the lsn */
        Journal *j=m_env->get_journal();
        BFC_ASSERT_EQUAL(11ull, j->get_lsn());
        BFC_ASSERT_EQUAL(5ull, m_env->get_txn_id());

        /* create another transaction and make sure that the transaction
         * IDs and the lsn's continue seamlessly */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        BFC_ASSERT_EQUAL(6ull, txn_get_id(txn));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void recoverCommittedTxnsTest(void)
    {
        ham_txn_t *txn[5];
        LogEntry vec[20];
        unsigned p=0;
        ham_key_t key={0};
        ham_record_t rec={0};
        Journal *j=new Journal(m_env);
        ham_u64_t lsn=2;

        /* create a couple of transaction which insert a key, and commit 
         * them */
        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn[i], (ham_env_t *)m_env, 0, 0, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn[i], &key, &rec, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_INSERT, 0xf000);
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_COMMIT, 0);
            BFC_ASSERT_EQUAL(0, ham_txn_commit(txn[i], 0));
        }

        /* backup the journal files; then re-create the Environment from the 
         * journal */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        delete j;
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);
        compareJournal(j, vec, p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* now verify that the committed transactions were re-played from
         * the journal */
        for (int i=0; i<5; i++) {
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        }
    }

    void recoverAutoAbortTxnsTest(void)
    {
#ifndef WIN32
        ham_txn_t *txn[5];
        LogEntry vec[20];
        unsigned p=0;
        ham_key_t key={0};
        ham_record_t rec={0};
        Journal *j=new Journal(m_env);
        ham_u64_t lsn=2;

        /* create a couple of transaction which insert a key, but do not 
         * commit them! */
        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn[i], (ham_env_t *)m_env, 0, 0, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn[i], &key, &rec, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_INSERT, 0xf000);
        }

        /* backup the journal files; then re-create the Environment from the 
         * journal */
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.jrn0"), 
                    BFC_OPATH(".test.bak0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.jrn1"), 
                    BFC_OPATH(".test.bak1")));
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_txn_commit(txn[i], 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak0"), 
                    BFC_OPATH(".test.jrn0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak1"), 
                    BFC_OPATH(".test.jrn1")));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        delete j;
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);
        compareJournal(j, vec, p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* by re-creating the database we make sure that it's definitely
         * empty */
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 0, 0644));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        /* now open and recover */
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak0"), 
                    BFC_OPATH(".test.jrn0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak1"), 
                    BFC_OPATH(".test.jrn1")));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* now verify that the transactions were actually aborted */
        for (int i=0; i<5; i++) {
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                        ham_find(m_db, 0, &key, &rec, 0));
        }
#endif
    }

    void recoverSkipAlreadyFlushedTest(void)
    {
#ifndef WIN32
        ham_txn_t *txn[2];
        LogEntry vec[20];
        unsigned p=0;
        ham_key_t key={0};
        ham_record_t rec={0};
        Journal *j=m_env->get_journal();
        ham_u64_t lsn=2;

        /* create two transactions which insert a key, but only flush the
         * first; instead, manually append the "commit" of the second
         * transaction to the journal (but not to the database!) */
        for (int i=0; i<2; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn[i], (ham_env_t *)m_env, 0, 0, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_BEGIN, 0);
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn[i], &key, &rec, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_INSERT, 0xf000);
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_COMMIT, 0);
            if (i==0)
                BFC_ASSERT_EQUAL(0, ham_txn_commit(txn[i], 0));
            else
                BFC_ASSERT_EQUAL(0, j->append_txn_commit(txn[i], lsn-1));
        }

        /* backup the journal files; then re-create the Environment from the 
         * journal */
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.jrn0"), 
                    BFC_OPATH(".test.bak0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.jrn1"), 
                    BFC_OPATH(".test.bak1")));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn[1], 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak0"), 
                    BFC_OPATH(".test.jrn0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak1"), 
                    BFC_OPATH(".test.jrn1")));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);
        compareJournal(j, vec, p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        /* now open and recover */
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak0"), 
                    BFC_OPATH(".test.jrn0")));
        BFC_ASSERT_EQUAL(true, os::copy(BFC_OPATH(".test.bak1"), 
                    BFC_OPATH(".test.jrn1")));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* now verify that the transactions were both committed */
        for (int i=0; i<2; i++) {
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        }
#endif
    }

    void recoverInsertTest(void)
    {
        ham_txn_t *txn[2];
        LogEntry vec[200];
        unsigned p=0;
        ham_key_t key={0};
        ham_record_t rec={0};
        Journal *j=new Journal(m_env);
        ham_u64_t lsn=2;

        /* create two transactions with many keys that are inserted */
        for (int i=0; i<2; i++) {
            BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn[i], (ham_env_t *)m_env, 0, 0, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i]), 
                        Journal::ENTRY_TYPE_TXN_BEGIN, 0);
        }
        for (int i=0; i<100; i++) {
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn[i&1], &key, &rec, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn[i&1]), 
                        Journal::ENTRY_TYPE_INSERT, 0xf000);
        }
        /* commit the first txn, abort the second */
        vec[p++]=LogEntry(lsn++, txn_get_id(txn[0]), 
                    Journal::ENTRY_TYPE_TXN_COMMIT, 0);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn[0], 0));
        vec[p++]=LogEntry(lsn++, txn_get_id(txn[1]), 
                    Journal::ENTRY_TYPE_TXN_ABORT, 0);
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn[1], 0));

        /* backup the journal files; then re-create the Environment from the 
         * journal */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        delete j;
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);
        compareJournal(j, vec, p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS|HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* now verify that the committed transaction was re-played from
         * the journal */
        for (int i=0; i<100; i++) {
            key.data=&i;
            key.size=sizeof(i);
            if (i&1)
                BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                            ham_find(m_db, 0, &key, &rec, 0));
            else
                BFC_ASSERT_EQUAL(0, 
                            ham_find(m_db, 0, &key, &rec, 0));
        }
    }

    void recoverEraseTest(void)
    {
        ham_txn_t *txn;
        LogEntry vec[200];
        unsigned p=0;
        ham_key_t key={0};
        ham_record_t rec={0};
        Journal *j=new Journal(m_env);
        ham_u64_t lsn=2;

        /* create a transaction with many keys that are inserted, mostly
         * duplicates */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        vec[p++]=LogEntry(lsn++, txn_get_id(txn), 
                    Journal::ENTRY_TYPE_TXN_BEGIN, 0);
        for (int i=0; i<100; i++) {
            int val=i%10; 
            key.data=&val;
            key.size=sizeof(val);
            BFC_ASSERT_EQUAL(0, 
                        ham_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn), 
                        Journal::ENTRY_TYPE_INSERT, 0xf000);
        }
        /* now delete them all */
        for (int i=0; i<10; i++) {
            key.data=&i;
            key.size=sizeof(i);
            BFC_ASSERT_EQUAL(0, 
                        ham_erase(m_db, txn, &key, 0));
            vec[p++]=LogEntry(lsn++, txn_get_id(txn), 
                        Journal::ENTRY_TYPE_ERASE, 0xf000);
        }
        /* commit the txn */
        vec[p++]=LogEntry(lsn++, txn_get_id(txn), 
                    Journal::ENTRY_TYPE_TXN_COMMIT, 0);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* backup the journal files; then re-create the Environment from the 
         * journal */
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        m_env=(Environment *)ham_get_env(m_db);
        delete j;
        j=new Journal(m_env);
        BFC_ASSERT_EQUAL(0, j->open());
        m_env->set_journal(j);
        compareJournal(j, vec, p);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_DONT_CLEAR_LOG));
        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS
                        |HAM_AUTO_RECOVERY));
        m_env=(Environment *)ham_get_env(m_db);

        /* verify that the journal is empty */
        verifyJournalIsEmpty();

        /* now verify that the committed transaction was re-played from
         * the journal; the database must be empty */
        ham_u64_t keycount;
        BFC_ASSERT_EQUAL(0, ham_get_key_count(m_db, 0, 0, &keycount));
        BFC_ASSERT_EQUAL(0ull, keycount);
    }

    void lsnOverflowTest(void)
    {
        Journal *j=m_env->get_journal();
        j->m_lsn=0xffffffffffffffffull-1;
        ham_txn_t *txn;

        /* this one must work */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
        /* this one must fail */
        BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED, ham_txn_commit(txn, 0));

        /* and now it has to work again */
        j->m_lsn=3;
    }

};


BFC_REGISTER_FIXTURE(JournalTest);

