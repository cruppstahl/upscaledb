/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "3rdparty/catch/catch.hpp"

#include "os.hpp"
#include "utils.h"

#include "2lsn_manager/lsn_manager.h"
#include "2lsn_manager/lsn_manager_test.h"
#include "3journal/journal.h"
#include "4txn/txn.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

using namespace hamsterdb;

namespace hamsterdb {

static bool g_changeset_flushed = false;
extern void (*g_CHANGESET_POST_LOG_HOOK)(void);
static void
changeset_post_log_hook() {
  g_changeset_flushed = true;
}

struct LogEntry {
  LogEntry()
    : lsn(0), txn_id(0), type(0), dbname(0) {
  }

  LogEntry(uint64_t _lsn, uint64_t _txn_id, uint32_t _type,
        uint16_t _dbname, const char *_name = "")
    : lsn(_lsn), txn_id(_txn_id), type(_type), dbname(_dbname) {
    if (_name)
      name = _name;
  }

  // for sorting by lsn
  bool operator<(const LogEntry &other) const {
    return (lsn < other.lsn);
  }

  uint64_t lsn;
  uint64_t txn_id;
  uint32_t type;
  uint16_t dbname;
  std::string name;
};

struct InsertLogEntry : public LogEntry {
  InsertLogEntry(uint64_t _lsn, uint64_t _txn_id, uint16_t _dbname,
        ham_key_t *_key, ham_record_t *_record)
    : LogEntry(_lsn, _txn_id, Journal::kEntryTypeInsert, _dbname),
      key(_key), record(_record) {
  }

  ham_key_t *key;
  ham_record_t *record;
};

struct EraseLogEntry : public LogEntry {
  EraseLogEntry(uint64_t _lsn, uint64_t _txn_id, uint16_t _dbname,
        ham_key_t *_key)
    : LogEntry(_lsn, _txn_id, Journal::kEntryTypeInsert, _dbname),
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

  uint64_t get_lsn() {
    LsnManagerTest test(((LocalEnvironment *)m_env)->lsn_manager());
    return (test.lsn());
  }

  void setup(bool flush_when_committed = true) {
    (void)os::unlink(Utils::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                (flush_when_committed ? HAM_FLUSH_WHEN_COMMITTED : 0)
                | HAM_ENABLE_TRANSACTIONS
                | HAM_ENABLE_RECOVERY, 0644, 0));
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

    REQUIRE_CATCH(j->create(), HAM_WOULD_BLOCK);
    delete j;

    /*
     * setting the journal to NULL calls close() and deletes the
     * old journal
     */
    j = m_lenv->journal();
    LocalEnvironmentTest test = m_lenv->test();
    test.set_journal(NULL);

    j = new Journal(m_lenv);
    j->create();
    test = m_lenv->test();
    test.set_journal(j);
    return (j);
  }

  void createCloseTest() {
    Journal *j = disconnect_and_create_new_journal();

    /* TODO make sure that the two files exist and
     * contain only the header */

    REQUIRE(true == j->is_empty());

    // do not close the journal - it will be closed in teardown()
  }

  void negativeCreateTest() {
    Journal *j = new Journal(m_lenv);
    std::string oldfilename = m_lenv->config().filename;
    EnvironmentTest test = ((Environment *)m_lenv)->test();
    test.set_filename("/::asdf");
    REQUIRE_CATCH(j->create(), HAM_IO_ERROR);
    test.set_filename(oldfilename);
    j->close();
    delete (j);
  }

  void negativeOpenTest() {
    Journal *j = new Journal(m_lenv);
    std::string oldfilename = m_lenv->config().filename;
    EnvironmentTest test = ((Environment *)m_lenv)->test();
    test.set_filename("xxx$$test");
    REQUIRE_CATCH(j->open(), HAM_FILE_NOT_FOUND);

    /* if journal::open() fails, it will call journal::close()
     * internally and journal::close() overwrites the header structure.
     * therefore we have to patch the file before we start the test. */
    File f;
    f.open("data/log-broken-magic.jrn0", 0);
    f.pwrite(0, (void *)"x", 1);
    f.close();

    test.set_filename("data/log-broken-magic");
    REQUIRE_CATCH(j->open(), HAM_LOG_INV_FILE_HEADER);
    test.set_filename(oldfilename);
    j->close();
    delete j;
  }

  void appendTxnBeginTest() {
    Journal *j = disconnect_and_create_new_journal();
    JournalTest test = j->test();
    REQUIRE(true == j->is_empty());

    REQUIRE((uint32_t)0 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, "name", 0, 0));

    REQUIRE((uint32_t)1 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

    j->flush_buffer(0);
    j->flush_buffer(1);

    REQUIRE(false == j->is_empty());
    REQUIRE((uint64_t)3 == get_lsn());

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendTxnAbortTest() {
    Journal *j = disconnect_and_create_new_journal();
    JournalTest test = j->test();
    REQUIRE(true == j->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    j->flush_buffer(0);
    j->flush_buffer(1);

    REQUIRE(false == j->is_empty());
    REQUIRE((uint64_t)3 == get_lsn());
    REQUIRE((uint32_t)1 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

    uint64_t lsn = m_lenv->next_lsn();
    j->append_txn_abort((LocalTransaction *)txn, lsn);
    REQUIRE(false == j->is_empty());
    REQUIRE((uint64_t)4 == get_lsn());
    REQUIRE((uint32_t)0 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)1 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void appendTxnCommitTest() {
    Journal *j = disconnect_and_create_new_journal();
    JournalTest test = j->test();
    REQUIRE(true == j->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    j->flush_buffer(0);
    j->flush_buffer(1);

    REQUIRE(false == j->is_empty());
    REQUIRE((uint64_t)3 == get_lsn());
    REQUIRE((uint32_t)1 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

    uint64_t lsn = m_lenv->next_lsn();
    j->append_txn_commit((LocalTransaction *)txn, lsn);
    REQUIRE(false == j->is_empty());
    // simulate a txn flush
    j->transaction_flushed((LocalTransaction *)txn);
    REQUIRE((uint64_t)4 == get_lsn());
    REQUIRE((uint32_t)0 == test.state()->open_txn[0]);
    REQUIRE((uint32_t)1 == test.state()->closed_txn[0]);
    REQUIRE((uint32_t)0 == test.state()->open_txn[1]);
    REQUIRE((uint32_t)0 == test.state()->closed_txn[1]);

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

    uint64_t lsn = m_lenv->next_lsn();
    j->append_insert((Database *)m_db, (LocalTransaction *)txn,
              &key, &rec, HAM_OVERWRITE, lsn);
    REQUIRE((uint64_t)4 == get_lsn());
    j->close(true);
    j->open();

    /* verify that the insert entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    j->get_entry(&iter, &entry, &auxbuffer); // this is the txn
    j->get_entry(&iter, &entry, &auxbuffer); // this is the insert
    REQUIRE((uint64_t)3 == entry.lsn);
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
    rec.size = 1024;
    rec.partial_size = 5;
    rec.partial_offset = 10;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    uint64_t lsn = m_lenv->next_lsn();
    j->append_insert((Database *)m_db, (LocalTransaction *)txn,
              &key, &rec, HAM_PARTIAL, lsn);
    REQUIRE((uint64_t)4 == get_lsn());
    j->close(true);
    j->open();

    /* verify that the insert entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    j->get_entry(&iter, &entry, &auxbuffer); // this is the txn
    j->get_entry(&iter, &entry, &auxbuffer); // this is the insert
    REQUIRE((uint64_t)3 == entry.lsn);
    PJournalEntryInsert *ins = (PJournalEntryInsert *)auxbuffer.get_ptr();
    REQUIRE(auxbuffer.get_size() == sizeof(PJournalEntryInsert) - 1
                                    + ins->key_size + ins->record_partial_size);
    REQUIRE(5 == ins->key_size);
    REQUIRE(1024u == ins->record_size);
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

    uint64_t lsn = m_lenv->next_lsn();
    j->append_erase((Database *)m_db, (LocalTransaction *)txn, &key, 1, 0, lsn);
    REQUIRE((uint64_t)4 == get_lsn());
    j->close(true);
    j->open();

    /* verify that the erase entry was written correctly */
    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));
    PJournalEntry entry;
    ByteArray auxbuffer;
    j->get_entry(&iter, &entry, &auxbuffer); // this is the txn
    j->get_entry(&iter, &entry, &auxbuffer); // this is the erase
    REQUIRE((uint64_t)3 == entry.lsn);
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

    j->flush_buffer(0);
    j->flush_buffer(1);

    REQUIRE(false == j->is_empty());
    REQUIRE((uint64_t)3 == get_lsn());

    j->clear();
    REQUIRE(true == j->is_empty());
    REQUIRE((uint64_t)3 == get_lsn());

    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE((uint64_t)4 == get_lsn());

    j->close();
    j->open();
    REQUIRE((uint64_t)4 == get_lsn());
  }

  void iterateOverEmptyLogTest() {
    Journal *j = disconnect_and_create_new_journal();

    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));

    PJournalEntry entry;
    ByteArray auxbuffer;
    j->get_entry(&iter, &entry, &auxbuffer);
    REQUIRE((uint64_t)0 == entry.lsn);
    REQUIRE(0 == auxbuffer.get_size());
  }

  void iterateOverLogOneEntryTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    REQUIRE(2ull == get_lsn());
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    j->append_txn_begin((LocalTransaction *)txn, 0, get_lsn());
    j->close(true);
    j->open();
    REQUIRE(3ull == get_lsn());

    Journal::Iterator iter;
    memset(&iter, 0, sizeof(iter));

    PJournalEntry entry;
    ByteArray auxbuffer;
    j->get_entry(&iter, &entry, &auxbuffer);
    REQUIRE((uint64_t)2 == entry.lsn);
    REQUIRE((uint64_t)1 == ((Transaction *)txn)->get_id());
    REQUIRE((uint64_t)1 == entry.txn_id);
    REQUIRE(0 == auxbuffer.get_size());
    REQUIRE((uint32_t)Journal::kEntryTypeTxnBegin == entry.type);

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void compareJournal(Journal *journal, LogEntry *vec, size_t size) {
    Journal::Iterator it;
    PJournalEntry entry;
    ByteArray auxbuffer;

    std::vector<LogEntry> entries;

    while (true) {
      journal->get_entry(&it, &entry, &auxbuffer);
      if (entry.lsn == 0)
        break;

      // skip Changesets
      if (entry.type == Journal::kEntryTypeChangeset)
        continue;

      // txn_begin can include a transaction name
      if (entry.type == Journal::kEntryTypeTxnBegin) {
        LogEntry le(entry.lsn, entry.txn_id, entry.type, entry.dbname,
                    auxbuffer.get_size() > 0
                        ? (char *)auxbuffer.get_ptr()
                        : "");
        entries.push_back(le);
      }
      else {
        LogEntry le(entry.lsn, entry.txn_id, entry.type, entry.dbname);
        entries.push_back(le);
      }
    }

    // sort by lsn
    std::sort(entries.begin(), entries.end());

    // now compare against the entries supplied by the user
    for (size_t i = 0; i < size; i++) {
      REQUIRE(vec[i].lsn == entries[i].lsn);
      REQUIRE(vec[i].txn_id == entries[i].txn_id);
      REQUIRE(vec[i].type == entries[i].type);
      REQUIRE(vec[i].dbname == entries[i].dbname);
      REQUIRE(vec[i].name == entries[i].name);
    }

    REQUIRE(entries.size() == (size_t)size);
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
      REQUIRE(0 == ham_txn_begin(&txn, m_env, name, 0, 0));
      vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnBegin, 0, &name[0]);
      vec[p++] = LogEntry(3 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnAbort, 0);
      REQUIRE(0 == ham_txn_abort(txn, 0));
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);

    compareJournal(j, vec, p);
  }

  void iterateOverLogMultipleEntrySwapTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    JournalTest test = j->test();
    test.state()->threshold = 5;
    unsigned p = 0;
    LogEntry vec[20];

    for (int i = 0; i <= 7; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
      vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnBegin, 0);
      vec[p++] = LogEntry(3 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnAbort, 0);
      REQUIRE(0 == ham_txn_abort(txn, 0));
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);

    compareJournal(j, vec, p);
  }

  void iterateOverLogMultipleEntrySwapTwiceTest() {
    ham_txn_t *txn;
    Journal *j = disconnect_and_create_new_journal();
    JournalTest test = j->test();
    test.state()->threshold = 5;

    unsigned p = 0;
    LogEntry vec[20];

    for (int i = 0; i <= 10; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
      if (i >= 5) {
        vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnBegin, 0);
        vec[p++] = LogEntry(3 + i * 2, ((Transaction *)txn)->get_id(),
              Journal::kEntryTypeTxnAbort, 0);
      }
      REQUIRE(0 == ham_txn_abort(txn, 0));
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);

    compareJournal(j, vec, p);
  }

  void verifyJournalIsEmpty() {
    uint64_t size;
    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = m_lenv->journal();
    REQUIRE(j);
    JournalTest test = j->test();
    size = test.state()->files[0].get_file_size();
    REQUIRE(0 == size);
    size = test.state()->files[1].get_file_size();
    REQUIRE(0 == size);
  }

  void recoverVerifyTxnIdsTest() {
    ham_txn_t *txn;
    LogEntry vec[20];
    unsigned p = 0;

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_env, 0, 0, 0));
      REQUIRE((uint64_t)(i + 1) == ((Transaction *)txn)->get_id());
      vec[p++] = LogEntry(1 + i * 2, ((Transaction *)txn)->get_id(),
            Journal::kEntryTypeTxnBegin, 0);
      vec[p++] = LogEntry(2 + i * 2, ((Transaction *)txn)->get_id(),
            Journal::kEntryTypeTxnCommit, 0);
      REQUIRE(0 == ham_txn_commit(txn, 0));
    }

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* reopen the database */
    REQUIRE(HAM_NEED_RECOVERY ==
        ham_env_open(&m_env, Utils::opath(".test"),
                HAM_FLUSH_WHEN_COMMITTED
                | HAM_ENABLE_TRANSACTIONS
                | HAM_ENABLE_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
                HAM_FLUSH_WHEN_COMMITTED
                | HAM_ENABLE_TRANSACTIONS
                | HAM_AUTO_RECOVERY, 0));
    m_lenv = (LocalEnvironment *)m_env;

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* verify the lsn */
    //Journal *j = m_lenv->journal();
    // TODO 12 on linux, 11 on Win32 - wtf?
    // REQUIRE(12ull == get_lsn());
    REQUIRE(5ull == ((LocalTransactionManager *)(m_lenv->txn_manager()))->test_get_txn_id());

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
    uint64_t lsn = 2;

    /* create a couple of transaction which insert a key, and commit
     * them */
    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnBegin, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeInsert, 1);
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnCommit, 0);
      REQUIRE(0 == ham_txn_commit(txn[i], 0));
    }

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j->close();
    delete j;
    j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
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
    uint64_t lsn = 2;

    /* create a couple of transaction which insert a key, but do not
     * commit them! */
    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnBegin, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeInsert, 1);
    }

    m_lenv = (LocalEnvironment *)m_env;
    m_lenv->journal()->flush_buffer(0);
    m_lenv->journal()->flush_buffer(1);

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_txn_commit(txn[i], 0));
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    
    Journal *j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* by re-creating the database we make sure that it's definitely
     * empty */
    REQUIRE(0 ==
          ham_env_create(&m_env, Utils::opath(".test"),
                HAM_FLUSH_WHEN_COMMITTED, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    /* now open and recover */
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
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

  void recoverTempTxns() {
#ifndef WIN32
    ham_key_t key = {};
    ham_record_t rec = {};

    /* insert keys with anonymous transactions */
    for (int i = 0; i < 5; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    m_lenv = (LocalEnvironment *)m_env;
    m_lenv->journal()->flush_buffer(0);
    m_lenv->journal()->flush_buffer(1);

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* by re-creating the database we make sure that it's definitely
     * empty */
    REQUIRE(0 ==
          ham_env_create(&m_env, Utils::opath(".test"),
                HAM_FLUSH_WHEN_COMMITTED, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    /* now open and recover */
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the transactions were committed */
    for (int i = 0; i < 5; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
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
    Journal *j = m_lenv->journal();
    uint64_t lsn = 2;

    /* create two transactions which insert a key, but only flush the
     * first; instead, manually append the "commit" of the second
     * transaction to the journal (but not to the database!) */
    for (int i = 0; i < 2; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnBegin, 0);
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeInsert, 1);
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnCommit, 0);
      if (i == 0)
        REQUIRE(0 == ham_txn_commit(txn[i], 0));
      else
        j->append_txn_commit((LocalTransaction *)txn[i], lsn - 1);
    }

    j->flush_buffer(0);
    j->flush_buffer(1);

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));
    REQUIRE(0 == ham_txn_commit(txn[1], 0));
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);
    compareJournal(j, vec, p);
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    /* now open and recover */
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
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
    ham_key_t key = {0};
    ham_record_t rec = {0};
    uint64_t lsn = 2;

    /* create two transactions with many keys that are inserted */
    for (int i = 0; i < 2; i++) {
      REQUIRE(0 == ham_txn_begin(&txn[i], m_env, 0, 0, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i])->get_id(),
            Journal::kEntryTypeTxnBegin, 0);
    }
    for (int i = 0; i < 100; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn[i & 1], &key, &rec, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn[i & 1])->get_id(),
            Journal::kEntryTypeInsert, 1);
    }
    /* commit the first txn, abort the second */
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn[0])->get_id(),
          Journal::kEntryTypeTxnCommit, 0);
    REQUIRE(0 == ham_txn_commit(txn[0], 0));
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn[1])->get_id(),
          Journal::kEntryTypeTxnAbort, 0);
    REQUIRE(0 == ham_txn_abort(txn[1], 0));

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));
    m_lenv = (LocalEnvironment *)m_env;

    Journal *j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);
    compareJournal(j, vec, p);

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
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
    ham_key_t key = {0};
    ham_record_t rec = {0};
    uint64_t lsn = 2;

    /* create a transaction with many keys that are inserted, mostly
     * duplicates */
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
          Journal::kEntryTypeTxnBegin, 0);
    for (int i = 0; i < 100; i++) {
      int val = i % 10;
      key.data = &val;
      key.size = sizeof(val);
      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
            Journal::kEntryTypeInsert, 1);
    }
    /* now delete them all */
    for (int i = 0; i < 10; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_erase(m_db, txn, &key, 0));
      vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
            Journal::kEntryTypeErase, 1);
    }
    /* commit the txn */
    vec[p++] = LogEntry(lsn++, ((Transaction *)txn)->get_id(),
          Journal::kEntryTypeTxnCommit, 0);
    REQUIRE(0 == ham_txn_commit(txn, 0));

    /* backup the journal files; then re-create the Environment from the
     * journal */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"), 0, 0));

    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = new Journal(m_lenv);
    j->open();
    m_lenv->test().set_journal(j);
    compareJournal(j, vec, p);

    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* verify that the journal is empty */
    verifyJournalIsEmpty();

    /* now verify that the committed transaction was re-played from
     * the journal; the database must be empty */
    uint64_t keycount;
    REQUIRE(0 == ham_db_get_key_count(m_db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);
  }

  void recoverAfterChangesetTest() {
#ifndef WIN32
    ham_txn_t *txn;

    // do not immediately flush the changeset after a commit
    teardown();
    setup(false);

    g_changeset_flushed = false;
    g_CHANGESET_POST_LOG_HOOK = changeset_post_log_hook;

    int i = 0;
    while (!g_changeset_flushed) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

      ham_key_t key = ham_make_key((void *)"key", 4);
      ham_record_t rec = ham_make_record(&i, sizeof(i));

      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      REQUIRE(0 == ham_txn_commit(txn, 0));

      i++;
    }

    /* backup the files */
    REQUIRE(true == os::copy(Utils::opath(".test"),
          Utils::opath(".test.bak")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));

    /* close the environment, then restore the files */
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(true == os::copy(Utils::opath(".test.bak"),
          Utils::opath(".test")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));

    /* open the environment */
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* now verify that the database is complete */
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    ham_status_t st;
    int j = 0;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(0 == strcmp("key", (const char *)key.data));
      REQUIRE(key.size == 4);
      REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
      REQUIRE(rec.size == sizeof(j));
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(i == j);
    REQUIRE(0 == ham_cursor_close(cursor));
#endif
  }

  void recoverAfterChangesetAndCommitTest() {
#ifndef WIN32
    ham_txn_t *txn;

    // do not immediately flush the changeset after a commit
    teardown();
    setup(false);

    g_changeset_flushed = false;
    g_CHANGESET_POST_LOG_HOOK = changeset_post_log_hook;

    int i = 0;
    while (!g_changeset_flushed) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

      ham_key_t key = ham_make_key((void *)"key", 4);
      ham_record_t rec = ham_make_record(&i, sizeof(i));

      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      REQUIRE(0 == ham_txn_commit(txn, 0));

      i++;
    }

    // changeset was flushed, now add another commit
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    ham_key_t key = ham_make_key((void *)"kez", 4);
    ham_record_t rec = ham_make_record((void *)"rec", 4);
    REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_txn_commit(txn, 0));
    i++;

    /* backup the files */
    REQUIRE(true == os::copy(Utils::opath(".test"),
          Utils::opath(".test.bak")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));

    /* close the environment, then restore the files */
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(true == os::copy(Utils::opath(".test.bak"),
          Utils::opath(".test")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));

    /* open the environment */
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* now verify that the database is complete */
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    ham_status_t st;
    int j = 0;
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(key.size == 4);
      if (j <= 64) {
        REQUIRE(0 == strcmp("key", (const char *)key.data));
        REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
        REQUIRE(rec.size == sizeof(j));
      }
      else {
        REQUIRE(0 == strcmp("kez", (const char *)key.data));
      }
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(i == j);
    REQUIRE(0 == ham_cursor_close(cursor));
#endif
  }

  void recoverAfterChangesetAndCommit2Test() {
#ifndef WIN32
    ham_txn_t *txn;
    ham_txn_t *longtxn;

    // do not immediately flush the changeset after a commit
    teardown();
    setup(false);

    REQUIRE(0 == ham_txn_begin(&longtxn, m_env, 0, 0, 0));

    int i = 0;
    // txn's are only flushed if the oldest txn is committed, and this is
    // not the case here. Therefore, the CHANGESET_POST_LOG_HOOK is never
    // invoked. Just write 100 transactions instead of testing against
    // g_changeset_flushed
    for (i = 0; i < 100; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

      ham_key_t key = ham_make_key((void *)"key", 4);
      ham_record_t rec = ham_make_record(&i, sizeof(i));

      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      REQUIRE(0 == ham_txn_commit(txn, 0));
    }

    // now commit the previous transaction
    ham_key_t key = ham_make_key((void *)"kez", 4);
    ham_record_t rec = ham_make_record((void *)"rec", 4);
    REQUIRE(0 == ham_db_insert(m_db, longtxn, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_txn_commit(longtxn, 0));
    i++;

    /* backup the files */
    REQUIRE(true == os::copy(Utils::opath(".test"),
          Utils::opath(".test.bak")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));

    /* close the environment, then restore the files */
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(true == os::copy(Utils::opath(".test.bak"),
          Utils::opath(".test")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));

    /* open the environment */
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* now verify that the database is complete */
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    ham_status_t st;
    int j = 0;
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(key.size == 4);
      if (j < 100) {
        REQUIRE(0 == strcmp("key", (const char *)key.data));
        REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
        REQUIRE(rec.size == sizeof(j));
      }
      else {
        REQUIRE(0 == strcmp("kez", (const char *)key.data));
      }
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(i == j);
    REQUIRE(0 == ham_cursor_close(cursor));
#endif
  }

  void recoverWithCorruptChangesetTest() {
#ifndef WIN32
    ham_txn_t *txn;

    // do not immediately flush the changeset after a commit
    teardown();
    setup(false);

    g_changeset_flushed = false;
    g_CHANGESET_POST_LOG_HOOK = changeset_post_log_hook;

    int i = 0;
    while (!g_changeset_flushed) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

      ham_key_t key = ham_make_key((void *)"key", 4);
      ham_record_t rec = ham_make_record(&i, sizeof(i));

      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      REQUIRE(0 == ham_txn_commit(txn, 0));

      i++;
    }

    // changeset was flushed, now add another commit
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    ham_key_t key = ham_make_key((void *)"kez", 4);
    ham_record_t rec = ham_make_record((void *)"rec", 4);
    REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_txn_commit(txn, 0));
    i++;

    /* backup the files */
    REQUIRE(true == os::copy(Utils::opath(".test"),
          Utils::opath(".test.bak")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));

    /* close the environment */
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    /* make sure that the changesets is corrupt by truncating the file */
    File f;
    f.open(".test.bak1", 0);
    uint64_t file_size = f.get_file_size();
    REQUIRE(file_size == 0x913cul);
    f.truncate(file_size - 60);
    f.close();

    /* restore the files */
    REQUIRE(true == os::copy(Utils::opath(".test.bak"),
          Utils::opath(".test")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));

    /* open the environment */
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));

    /* now verify that the database is complete */
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    ham_status_t st;
    int j = 0;
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(key.size == 4);
      if (j <= 64) {
        REQUIRE(0 == strcmp("key", (const char *)key.data));
        REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
        REQUIRE(rec.size == sizeof(j));
      }
      else {
        REQUIRE(0 == strcmp("kez", (const char *)key.data));
      }
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(i == j);
    REQUIRE(0 == ham_cursor_close(cursor));
#endif
  }

  void recoverFromRecoveryTest() {
#ifndef WIN32
    ham_txn_t *txn;

    // do not immediately flush the changeset after a commit
    teardown();
    setup(false);

    // need a second database
    ham_db_t *db2;
    REQUIRE(0 == ham_env_create_db(m_env, &db2, 2,
                            HAM_ENABLE_DUPLICATE_KEYS, 0));

    // add 5 commits
    int i;
    for (i = 0; i < 5; i++) {
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

      ham_key_t key = ham_make_key((void *)"key", 4);
      ham_record_t rec = ham_make_record(&i, sizeof(i));

      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, HAM_DUPLICATE));
      REQUIRE(0 == ham_txn_commit(txn, 0));
    }

    // changeset was flushed, now add another commit in the other database,
    // to make sure that it affects a different page
    i = 0;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    ham_key_t key = ham_make_key((void *)"key", 4);
    ham_record_t rec = ham_make_record(&i, sizeof(i));
    REQUIRE(0 == ham_db_insert(db2, txn, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_txn_commit(txn, 0));

    /* backup the files */
    REQUIRE(true == os::copy(Utils::opath(".test"),
          Utils::opath(".test.bak")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn0"),
          Utils::opath(".test.bak0")));
    REQUIRE(true == os::copy(Utils::opath(".test.jrn1"),
          Utils::opath(".test.bak1")));

    /* close the environment */
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    /* restore the files */
    REQUIRE(true == os::copy(Utils::opath(".test.bak"),
          Utils::opath(".test")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak0"),
          Utils::opath(".test.jrn0")));
    REQUIRE(true == os::copy(Utils::opath(".test.bak1"),
          Utils::opath(".test.jrn1")));

    /* make sure that recovery will fail */
    ErrorInducer::activate(true);
    ErrorInducer::get_instance()->add(ErrorInducer::kChangesetFlush, 3);

    /* open the environment, perform recovery */
    REQUIRE(HAM_INTERNAL_ERROR ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));

    /* disable error inducer, try again */
    ErrorInducer::activate(false);
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &db2, 2, 0, 0));

    /* now verify that the database is complete */
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    ham_status_t st;
    int j = 0;
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(0 == strcmp("key", (const char *)key.data));
      REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
      REQUIRE(rec.size == sizeof(j));
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(j == 5);
    REQUIRE(0 == ham_cursor_close(cursor));

    REQUIRE(0 == ham_cursor_create(&cursor, db2, 0, 0));
    j = 0;
    while ((st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT)) == 0) {
      REQUIRE(0 == strcmp("key", (const char *)key.data));
      REQUIRE(0 == memcmp(&j, rec.data, sizeof(j)));
      REQUIRE(rec.size == sizeof(j));
      j++;
    }
    REQUIRE(st == HAM_KEY_NOT_FOUND);
    REQUIRE(j == 1);
    REQUIRE(0 == ham_cursor_close(cursor));
#endif
  }

  void switchThresholdTest() {
    teardown();

    ham_parameter_t params[] = {
      {HAM_PARAM_JOURNAL_SWITCH_THRESHOLD, 33}, 
      {0, 0}
    };

    REQUIRE(0 == ham_env_create(&m_env, Utils::opath(".test"),
                HAM_ENABLE_TRANSACTIONS, 0644, &params[0]));

    // verify threshold through ham_env_get_parameters
    params[0].value = 0;
    REQUIRE(0 == ham_env_get_parameters(m_env, &params[0]));
    REQUIRE(params[0].value == 33);

    // verify threshold in the Journal object
    m_lenv = (LocalEnvironment *)m_env;
    Journal *j = m_lenv->journal();
    JournalTest test = j->test();
    test.state()->threshold = 5;

    // open w/o parameter
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, 0));
    params[0].value = 0;
    REQUIRE(0 == ham_env_get_parameters(m_env, &params[0]));
    REQUIRE(params[0].value == 0);

    // open w/ parameter
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    params[0].value = 44;
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"),
                    HAM_ENABLE_TRANSACTIONS, &params[0]));
    params[0].value = 0;
    REQUIRE(0 == ham_env_get_parameters(m_env, &params[0]));
    REQUIRE(params[0].value == 44);
  }

  void issue45Test() {
    ham_txn_t *txn;
    ham_key_t key = {0};
    ham_record_t rec = {0};

    /* create a transaction with one insert */
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    key.data = (void *)"aaaaa";
    key.size = 6;
    REQUIRE(0 == ham_db_insert(m_db, txn, &key, &rec, 0));

    /* reopen and recover. issue 45 causes a segfault */
    REQUIRE(0 == ham_env_close(m_env,
                HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));
    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),
                HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
  }
};

TEST_CASE("Journal/createCloseTest", "")
{
  JournalFixture f;
  f.createCloseTest();
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

TEST_CASE("Journal/recoverTempTxns", "")
{
  JournalFixture f;
  f.recoverTempTxns();
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

TEST_CASE("Journal/recoverAfterChangesetTest", "")
{
  JournalFixture f;
  f.recoverAfterChangesetTest();
}

TEST_CASE("Journal/recoverAfterChangesetAndCommitTest", "")
{
  JournalFixture f;
  f.recoverAfterChangesetAndCommitTest();
}

TEST_CASE("Journal/recoverAfterChangesetAndCommit2Test", "")
{
  JournalFixture f;
  f.recoverAfterChangesetAndCommit2Test();
}

TEST_CASE("Journal/recoverWithCorruptChangesetTest", "")
{
  JournalFixture f;
  f.recoverWithCorruptChangesetTest();
}

TEST_CASE("Journal/recoverFromRecoveryTest", "")
{
  JournalFixture f;
  f.recoverFromRecoveryTest();
}

TEST_CASE("Journal/switchThresholdTest", "")
{
  JournalFixture f;
  f.switchThresholdTest();
}

TEST_CASE("Journal/issue45Test", "")
{
  JournalFixture f;
  f.issue45Test();
}

} // namespace hamsterdb

