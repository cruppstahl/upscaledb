/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/endianswap.h"

#include "../src/journal.h"
#include "../src/txn.h"
#include "os.hpp"

using namespace hamsterdb;

namespace hamsterdb {

struct LogEntry {
  LogEntry()
    : lsn(0), txn_id(0), type(0), dbname(0) {
  }

  LogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, ham_u32_t _type,
        ham_u16_t _dbname, const char *_name = "")
    : lsn(_lsn), txn_id(_txn_id), type(_type), dbname(_dbname) {
    strcpy(name, _name);
  }

  ham_u64_t lsn;
  ham_u64_t txn_id;
  ham_u32_t type;
  ham_u16_t dbname;
  char name[256];
};

struct InsertLogEntry : public LogEntry {
  InsertLogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, ham_u16_t _dbname,
        ham_key_t *_key, ham_record_t *_record)
    : LogEntry(_lsn, _txn_id, Journal::ENTRY_TYPE_INSERT, _dbname),
      key(_key), record(_record) {
  }

  ham_key_t *key;
  ham_record_t *record;
};

struct EraseLogEntry : public LogEntry {
  EraseLogEntry(ham_u64_t _lsn, ham_u64_t _txn_id, ham_u16_t _dbname,
        ham_key_t *_key)
    : LogEntry(_lsn, _txn_id, Journal::ENTRY_TYPE_INSERT, _dbname),
      key(_key) {
  }

  ham_key_t *key;
};

struct JournalFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalEnvironment *m_lenv;

  JournalFixture() {
    setup();
  }

  ~JournalFixture() {
    teardown();
  }

  void setup() {
    (void)os::unlink(Globals::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY, 0644, 0));
    REQUIRE(0 ==
            ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATE_KEYS, 0));

    m_lenv = (LocalEnvironment *)m_env;
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  Journal *disconnect_and_create_new_journal() {
    Journal *j = new Journal(m_lenv);

    REQUIRE(HAM_WOULD_BLOCK == j->create());
    delete (j);

    /*
     * make sure db->journal is already NULL, i.e. disconnected.
     * Otherwise an BFC ASSERT for journal_close() will segfault
     * the teardown() code, which will try to close the db->journal
     * all over AGAIN!
     */
    j = m_lenv->get_journal();
    m_lenv->test_set_journal(NULL);
    REQUIRE(0 == j->close());
    delete j;

    j = new Journal(m_lenv);
    REQUIRE(0 == j->create());
    REQUIRE(j);
    m_lenv->test_set_journal(j);
    return (j);
  }

  void createCloseTest() {
    Journal *j = disconnect_and_create_new_journal();

    REQUIRE((ham_u64_t)1 == j->test_get_lsn());
    /* TODO make sure that the two files exist and
     * contain only the header */

    REQUIRE(true == j->is_empty());

    // do not close the journal - it will be closed in teardown()
  }

  void createCloseOpenCloseTest() {
    Journal *j = m_lenv->get_journal();
    REQUIRE(true == j->is_empty());
    REQUIRE(0 == j->close(true));

    REQUIRE(0 == j->open());
    REQUIRE(true == j->is_empty());
    m_lenv->test_set_journal(j);
  }

  void negativeCreateTest() {
    Journal *j = new Journal(m_lenv);
    std::string oldfilename = m_lenv->get_filename();
    m_lenv->test_set_filename("/::asdf");
    REQUIRE(HAM_IO_ERROR == j->create());
    m_lenv->test_set_filename(oldfilename);
    delete (j);
  }

  void negativeOpenTest() {
    ham_fd_t fd;
    Journal *j = new Journal(m_lenv);
    std::string oldfilename = m_lenv->get_filename();
    m_lenv->test_set_filename("xxx$$test");
    REQUIRE(HAM_FILE_NOT_FOUND == j->open());

    /* if journal::open() fails, it will call journal::close()
     * internally and journal::close() overwrites the header structure.
     * therefore we have to patch the file before we start the test. */
    REQUIRE(0 == os_open("data/log-broken-magic.jrn0", 0, &fd));
    REQUIRE(0 == os_pwrite(fd, 0, (void *)"x", 1));
    REQUIRE(0 == os_close(fd));

    m_lenv->test_set_filename("data/log-broken-magic");
    REQUIRE(HAM_LOG_INV_FILE_HEADER == j->open());
    m_lenv->test_set_filename(oldfilename);
    delete j;
  }

  void appendTxnBeginTest() {
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(true == j->is_empty());

    REQUIRE((ham_u32_t)0 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, "name", 0, 0));

    REQUIRE((ham_u32_t)1 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)2 == j->test_get_lsn());

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendTxnAbortTest() {
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(true == j->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)2 == j->test_get_lsn());
    REQUIRE((ham_u32_t)1 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    ham_u64_t lsn = m_lenv->get_incremented_lsn();
    REQUIRE(0 == j->append_txn_abort((Transaction *)txn, lsn));
    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    REQUIRE((ham_u32_t)0 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)1 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendTxnCommitTest() {
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(true == j->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)2 == j->test_get_lsn());
    REQUIRE((ham_u32_t)1 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    ham_u64_t lsn = m_lenv->get_incremented_lsn();
    REQUIRE(0 == j->append_txn_commit((Transaction *)txn, lsn));
    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    REQUIRE((ham_u32_t)0 == j->m_open_txn[0]);
    REQUIRE((ham_u32_t)1 == j->m_closed_txn[0]);
    REQUIRE((ham_u32_t)0 == j->m_open_txn[1]);
    REQUIRE((ham_u32_t)0 == j->m_closed_txn[1]);

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendInsertTest() {
    Journal *j = disconnect_and_create_new_journal();
    ham_txn_t *txn;
    ham_key_t key = {};
    ham_record_t rec = {};
    key.data = (void *)"key1";
    key.size = 5;
    rec.data = (void *)"rec1";
    rec.size = 5;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t lsn = m_lenv->get_incremented_lsn();
    REQUIRE(0 ==
          j->append_insert((Database *)m_db, (Transaction *)txn,
              &key, &rec, HAM_OVERWRITE, lsn));
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    REQUIRE(0 == j->close(true));

    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    /* verify that the insert entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    REQUIRE(0 ==  // this is the txn
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE(0 ==  // this is the insert
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE((ham_u64_t)2 == entry.lsn);
    PJournalEntryInsert *ins = (PJournalEntryInsert *)auxbuffer.get_ptr();
    REQUIRE(5 == ins->key_size);
    REQUIRE(5u == ins->record_size);
    REQUIRE(0ull == ins->record_partial_size);
    REQUIRE(0ull == ins->record_partial_offset);
    REQUIRE((unsigned)HAM_OVERWRITE == ins->insert_flags);
    REQUIRE(0 == strcmp("key1", (char *)ins->get_key_data()));
    REQUIRE(0 == strcmp("rec1", (char *)ins->get_record_data()));

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendPartialInsertTest() {
    Journal *j = disconnect_and_create_new_journal();
    ham_txn_t *txn;
    ham_key_t key = {};
    ham_record_t rec = {};
    key.data = (void *)"key1";
    key.size = 5;
    rec.data = (void *)"rec1";
    rec.size = 15;
    rec.partial_size = 5;
    rec.partial_offset = 10;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t lsn = m_lenv->get_incremented_lsn();
    REQUIRE(0 ==
          j->append_insert((Database *)m_db, (Transaction *)txn,
              &key, &rec, HAM_PARTIAL, lsn));
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    REQUIRE(0 == j->close(true));

    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    /* verify that the insert entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    REQUIRE(0 ==  // this is the txn
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE(0 ==  // this is the insert
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE((ham_u64_t)2 == entry.lsn);
    PJournalEntryInsert *ins = (PJournalEntryInsert *)auxbuffer.get_ptr();
    REQUIRE(5 == ins->key_size);
    REQUIRE(15u == ins->record_size);
    REQUIRE(5u == ins->record_partial_size);
    REQUIRE(10u == ins->record_partial_offset);
    REQUIRE((unsigned)HAM_PARTIAL == ins->insert_flags);
    REQUIRE(0 == strcmp("key1", (char *)ins->get_key_data()));
    REQUIRE(0 == strcmp("rec1", (char *)ins->get_record_data()));

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendEraseTest() {
    Journal *j = disconnect_and_create_new_journal();
    ham_txn_t *txn;
    ham_key_t key = {};
    key.data = (void *)"key1";
    key.size = 5;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t lsn = m_lenv->get_incremented_lsn();
    REQUIRE(0 == j->append_erase((Database *)m_db,
          (Transaction *)txn, &key, 1, 0, lsn));
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    REQUIRE(0 == j->close(true));

    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    /* verify that the erase entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    REQUIRE(0 == // this is the txn
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE(0 == // this is the erase
          j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE((ham_u64_t)2 == entry.lsn);
    PJournalEntryErase *er = (PJournalEntryErase *)auxbuffer.get_ptr();
    REQUIRE(5 == er->key_size);
    REQUIRE(0u == er->erase_flags);
    REQUIRE(1u == er->duplicate);
    REQUIRE(0 == strcmp("key1", (char *)er->get_key_data()));

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void clearTest() {
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(true == j->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(false == j->is_empty());
    REQUIRE((ham_u64_t)2 == j->test_get_lsn());

    REQUIRE(0 == j->clear());
    REQUIRE(true == j->is_empty());
    REQUIRE((ham_u64_t)2 == j->test_get_lsn());

    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());

    REQUIRE(0 == j->close());
    REQUIRE(0 == j->open());
    REQUIRE((ham_u64_t)3 == j->test_get_lsn());
    m_lenv->test_set_journal(j);
  }

  void iterateOverEmptyLogTest() {
    Journal *j = disconnect_and_create_new_journal();

    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));

    PJournalEntry entry;
    ByteArray auxbuffer;
    REQUIRE(0 == j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE((ham_u64_t)0 == entry.lsn);
    REQUIRE(0 == auxbuffer.get_size());
  }

  void iterateOverLogOneEntryTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(1ull == j->test_get_lsn());
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 ==
        j->append_txn_begin((Transaction *)txn, m_lenv,
            0, j->test_get_lsn()));
    REQUIRE(0 == j->close(true));

    REQUIRE(0 == j->open());
    REQUIRE(2ull == j->test_get_lsn());
    m_lenv->test_set_journal(j);

    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));

    PJournalEntry entry;
    ByteArray auxbuffer;
    REQUIRE(0 == j->get_entry(&iter, &entry, &auxbuffer));
    REQUIRE((ham_u64_t)1 == entry.lsn);
    REQUIRE((ham_u64_t)1 == ((Transaction *)txn)->get_id());
    REQUIRE((ham_u64_t)1 == entry.txn_id);
    REQUIRE(0 == auxbuffer.get_size());
    REQUIRE((ham_u32_t)Journal::ENTRY_TYPE_TXN_BEGIN ==
            entry.type);

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void compareJournal(Journal *journal, LogEntry *vec, unsigned size) {
    Journal::Iterator it;
    PJournalEntry entry;
    ByteArray auxbuffer;
    unsigned s = 0;

    do {
      REQUIRE(0 == journal->get_entry(&it, &entry, &auxbuffer));
      if (entry.lsn == 0)
        break;

      if (s == size) {
        REQUIRE(0ull == entry.lsn);
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

      REQUIRE(vec->lsn == entry.lsn);
      REQUIRE(vec->txn_id == entry.txn_id);
      REQUIRE(vec->type == entry.type);
      REQUIRE(vec->dbname == entry.dbname);
      if (strlen(vec->name)) {
        REQUIRE(auxbuffer.get_size());
        REQUIRE(0 == strcmp((char *)auxbuffer.get_ptr(), vec->name));
      }

      vec++;
    } while (1);

    REQUIRE(s == size);
  }

  void iterateOverLogMultipleEntryTest() {
    ham_txn_t *txn;
    disconnect_and_create_new_journal();
    unsigned p = 0;

    LogEntry vec[20];
    for (int i = 0; i < 5; i++) {
      // ham_txn_begin and ham_txn_abort will automatically add a
      // journal entry
      char name[16];
      sprintf(name, "name%d", i);
      REQUIRE(0 ==
          ham_txn_begin(&txn, m_env, name, 0, 0));
      vec[p++] = LogEntry(1 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_BEGIN, 0, &name[0]);
      REQUIRE(0 == ham_txn_abort(txn, 0));
      vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_ABORT, 0);
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    compareJournal(j, vec, p);
  }

  void iterateOverLogMultipleEntrySwapTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    j->m_threshold = 5;
    unsigned p = 0;
    LogEntry vec[20];

    for (int i = 0; i <= 7; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
      vec[p++] = LogEntry(1 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      REQUIRE(0 == ham_txn_abort(txn, 0));
      vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_ABORT, 0);
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    compareJournal(j, vec, p);
  }

  void iterateOverLogMultipleEntrySwapTwiceTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    j->m_threshold = 5;

    unsigned p = 0;
    LogEntry vec[20];

    for (int i = 0; i <= 10; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
      if (i >= 5)
        vec[p++] = LogEntry(1 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      REQUIRE(0 == ham_txn_abort(txn, 0));
      if (i >= 5)
        vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::ENTRY_TYPE_TXN_ABORT, 0);
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);

    compareJournal(j, vec, p);
  }

  void verifyJournalIsEmpty() {
    ham_u64_t size;
    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = m_lenv->get_journal();
    REQUIRE(j);
    REQUIRE(0 == os_get_filesize(j->m_fd[0], &size));
    REQUIRE(sizeof(Journal::PEnvironmentHeader) == size);
    REQUIRE(0 == os_get_filesize(j->m_fd[1], &size));
    REQUIRE(sizeof(Journal::PEnvironmentHeader) == size);
  }

  void recoverVerifyTxnIdsTest() {
    ham_txn_t *txn;
    LogEntry vec[20];
    unsigned p = 0;

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
      REQUIRE((ham_u64_t)(i + 1) == ((Transaction *)txn)->get_id());
      vec[p++] = LogEntry(1 + i * 2, ((Transaction *)txn)->get_id(),
            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      ham_u64_t txnid = ((Transaction *)txn)->get_id();
      REQUIRE(0 == ham_txn_commit(txn, 0));
      vec[p++] = LogEntry(2 + i * 2, txnid,
            Journal::ENTRY_TYPE_TXN_COMMIT, 0);
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* reopen the database */
    REQUIRE(HAM_NEED_RECOVERY ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_ENABLE_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    m_lenv = (LocalEnvironment *)m_env;

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* verify the lsn */
    Journal *j = m_lenv->get_journal();
    REQUIRE(11ull == j->test_get_lsn());
    REQUIRE(5ull == m_lenv->test_get_txn_id());

    /* create another transaction and make sure that the transaction
     * IDs and the lsn's continue seamlessly */
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(6ull == ((Transaction *)txn)->get_id());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void recoverCommittedTxnsTest() {
    ham_txn_t *txn[5];
    LogEntry vec[20];
    unsigned p = 0;
    ham_key_t key = {};
    ham_record_t rec = {};
    Journal *j = new Journal(m_lenv);
    ham_u64_t lsn = 2;

    /* create a couple of transaction which insert a key, and commit
     * them */
    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_INSERT, 1);
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_COMMIT, 0);
      REQUIRE(0 == ham_txn_commit(txn[i], 0));
    }

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    delete j;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the committed transactions were re-played from
     * the journal */
    for (int i = 0; i < 5; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    }
  }

  void recoverAutoAbortTxnsTest() {
#ifndef WIN32
    ham_txn_t *txn[5];
    LogEntry vec[20];
    unsigned p = 0;
    ham_key_t key = {};
    ham_record_t rec = {};
    Journal *j = new Journal(m_lenv);
    ham_u64_t lsn = 2;

    /* create a couple of transaction which insert a key, but do not
     * commit them! */
    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_INSERT, 1);
    }

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Globals::opath(".test.jrn0"),
          Globals::opath(".test.bak0")));
    REQUIRE(true == os::copy(Globals::opath(".test.jrn1"),
          Globals::opath(".test.bak1")));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_txn_commit(txn[i], 0));
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(true == os::copy(Globals::opath(".test.bak0"),
          Globals::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Globals::opath(".test.bak1"),
          Globals::opath(".test.jrn1")));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    delete j;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* by re-creating the database we make sure that it's definitely
     * empty */
    REQUIRE(0 ==
          ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    /* now open and recover */
    REQUIRE(true == os::copy(Globals::opath(".test.bak0"),
          Globals::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Globals::opath(".test.bak1"),
          Globals::opath(".test.jrn1")));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the transactions were actually aborted */
    for (int i = 0; i < 5; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(m_db, 0, &key, &rec, 0));
    }
#endif
  }

  void recoverSkipAlreadyFlushedTest() {
#ifndef WIN32
    ham_txn_t *txn[2];
    LogEntry vec[20];
    unsigned p = 0;
    ham_key_t key = {};
    ham_record_t rec = {};
    Journal *j = m_lenv->get_journal();
    ham_u64_t lsn = 2;

    /* create two transactions which insert a key, but only flush the
     * first; instead, manually append the "commit" of the second
     * transaction to the journal (but not to the database!) */
    for (int i = 0; i < 2; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_INSERT, 1);
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_COMMIT, 0);
      if (i == 0)
        REQUIRE(0 == ham_txn_commit(txn[i], 0));
      else
        REQUIRE(0 ==
            j->append_txn_commit((Transaction *)txn[i], lsn - 1));
    }

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Globals::opath(".test.jrn0"),
          Globals::opath(".test.bak0")));
    REQUIRE(true == os::copy(Globals::opath(".test.jrn1"),
          Globals::opath(".test.bak1")));
    REQUIRE(0 == ham_txn_commit(txn[1], 0));
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(true == os::copy(Globals::opath(".test.bak0"),
          Globals::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Globals::opath(".test.bak1"),
          Globals::opath(".test.jrn1")));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* now open and recover */
    REQUIRE(true == os::copy(Globals::opath(".test.bak0"),
          Globals::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Globals::opath(".test.bak1"),
          Globals::opath(".test.jrn1")));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_lenv = (LocalEnvironment *)m_env;

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the transactions were both committed */
    for (int i = 0; i < 2; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    }
#endif
  }

  void recoverInsertTest() {
    ham_txn_t *txn[2];
    LogEntry vec[200];
    unsigned p = 0;
    ham_key_t key = {};
    ham_record_t rec = {};
    Journal *j = new Journal(m_lenv);
    ham_u64_t lsn = 2;

    /* create two transactions with many keys that are inserted */
    for (int i = 0; i < 2; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::ENTRY_TYPE_TXN_BEGIN, 0);
    }
    for (int i = 0; i < 100; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i&1], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i&1])->get_id(),
            Journal::ENTRY_TYPE_INSERT, 1);
    }
    /* commit the first txn, abort the second */
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn[0])->get_id(),
          Journal::ENTRY_TYPE_TXN_COMMIT, 0);
    REQUIRE(0 == ham_txn_commit(txn[0], 0));
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn[1])->get_id(),
          Journal::ENTRY_TYPE_TXN_ABORT, 0);
    REQUIRE(0 == ham_txn_abort(txn[1], 0));

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    delete j;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the committed transaction was re-played from
     * the journal */
    for (int i = 0; i < 100; i++) {
      key.data = &i;
      key.size = sizeof(i);
      if (i & 1)
        REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(m_db, 0, &key, &rec, 0));
      else
        REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    }
  }

  void recoverEraseTest() {
    ham_txn_t *txn;
    LogEntry vec[200];
    unsigned p = 0;
    ham_key_t key = {};
    ham_record_t rec = {};
    Journal *j = new Journal(m_lenv);
    ham_u64_t lsn = 2;

    /* create a transaction with many keys that are inserted, mostly
     * duplicates */
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
          Journal::ENTRY_TYPE_TXN_BEGIN, 0);
    for (int i = 0; i < 100; i++) {
      int val = i % 10;
      key.data = &val;
      key.size = sizeof(val);
      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
            Journal::ENTRY_TYPE_INSERT, 1);
    }
    /* now delete them all */
    for (int i = 0; i < 10; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_erase(m_db, txn, &key, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
            Journal::ENTRY_TYPE_ERASE, 1);
    }
    /* commit the txn */
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
          Journal::ENTRY_TYPE_TXN_COMMIT, 0);
    REQUIRE(0 == ham_txn_commit(txn, 0));

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    delete j;
    j = new Journal(m_lenv);
    REQUIRE(0 == j->open());
    m_lenv->test_set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the committed transaction was re-played from
     * the journal; the database must be empty */
    ham_u64_t keycount;
    REQUIRE(0 == ham_db_get_key_count(m_db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);
  }
};

TEST_CASE("Journal/createCloseTest", "")
{
  JournalFixture f;
  f.createCloseTest();
}

TEST_CASE("Journal/createCloseOpenCloseTest", "")
{
  JournalFixture f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("Journal/negativeCreate", "")
{
  JournalFixture f;
  f.negativeCreateTest();
}

TEST_CASE("Journal/negativeOpen", "")
{
  JournalFixture f;
  f.negativeOpenTest();
}

TEST_CASE("Journal/appendTxnBegin", "")
{
  JournalFixture f;
  f.appendTxnBeginTest();
}

TEST_CASE("Journal/appendTxnAbort", "")
{
  JournalFixture f;
  f.appendTxnAbortTest();
}

TEST_CASE("Journal/appendTxnCommit", "")
{
  JournalFixture f;
  f.appendTxnCommitTest();
}

TEST_CASE("Journal/appendInsert", "")
{
  JournalFixture f;
  f.appendInsertTest();
}

TEST_CASE("Journal/appendPartialInsert", "")
{
  JournalFixture f;
  f.appendPartialInsertTest();
}

TEST_CASE("Journal/appendErase", "")
{
  JournalFixture f;
  f.appendEraseTest();
}

TEST_CASE("Journal/appendClear", "")
{
  JournalFixture f;
  f.clearTest();
}

TEST_CASE("Journal/iterateOverEmptyLog", "")
{
  JournalFixture f;
  f.iterateOverEmptyLogTest();
}

TEST_CASE("Journal/iterateOverLogOneEntry", "")
{
  JournalFixture f;
  f.iterateOverLogOneEntryTest();
}

TEST_CASE("Journal/iterateOverLogMultipleEntry", "")
{
  JournalFixture f;
  f.iterateOverLogMultipleEntryTest();
}

TEST_CASE("Journal/iterateOverLogMultipleEntrySwap", "")
{
  JournalFixture f;
  f.iterateOverLogMultipleEntrySwapTest();
}

TEST_CASE("Journal/iterateOverLogMultipleEntrySwapTwice", "")
{
  JournalFixture f;
  f.iterateOverLogMultipleEntrySwapTwiceTest();
}

TEST_CASE("Journal/recoverVerifyTxnIds", "")
{
  JournalFixture f;
  f.recoverVerifyTxnIdsTest();
}

TEST_CASE("Journal/recoverCommittedTxns", "")
{
  JournalFixture f;
  f.recoverCommittedTxnsTest();
}

TEST_CASE("Journal/recoverAutoAbortedTxns", "")
{
  JournalFixture f;
  f.recoverAutoAbortTxnsTest();
}

TEST_CASE("Journal/recoverSkipAlreadyFlushed", "")
{
  JournalFixture f;
  f.recoverSkipAlreadyFlushedTest();
}

TEST_CASE("Journal/recoverInsertTest", "")
{
  JournalFixture f;
  f.recoverInsertTest();
}

TEST_CASE("Journal/recoverEraseTest", "")
{
  JournalFixture f;
  f.recoverEraseTest();
}

} // namespace hamsterdb

