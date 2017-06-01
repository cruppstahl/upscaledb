/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "2lsn_manager/lsn_manager.h"
#include "3journal/journal.h"
#include "4txn/txn_local.h"

#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

namespace upscaledb {

struct JournalEntry {
  JournalEntry(uint64_t lsn_, uint64_t txnid_, uint32_t dbid_,
                  uint32_t type_, const char *key_, const char *record_,
                  uint32_t flags_)
    : lsn(lsn_), txnid(txnid_), dbid(dbid_), type(type_), key(key_),
      record(record_), duplicate(0), flags(flags_) {
  }

  JournalEntry(uint64_t lsn_, uint64_t txnid_, uint32_t dbid_,
                  uint32_t type_, const char *key_, uint32_t duplicate_)
    : lsn(lsn_), txnid(txnid_), dbid(dbid_), type(type_), key(key_),
      record(0), duplicate(duplicate_), flags(0) {
  }

  JournalEntry(uint64_t lsn_, uint64_t txnid_, uint32_t type_)
    : lsn(lsn_), txnid(txnid_), dbid(0), type(type_), key(0),
      record(0), duplicate(0), flags(0) {
  }

  uint64_t lsn;
  uint64_t txnid;
  uint32_t dbid;
  uint32_t type;
  const char *key;
  const char *record;
  uint32_t duplicate;
  uint32_t flags;
};

struct JournalProxy {
  JournalProxy(Journal *j)
    : journal(j) {
  }

  JournalProxy(LocalEnv *env)
    : journal(env->journal.get()) {
  }

  JournalProxy &require_create(uint32_t status = 0) {
    if (status) {
      REQUIRE_CATCH(journal->create(), status);
    }
    else {
      journal->create();
    }
    return *this;
  }

  JournalProxy &require_open(uint32_t status = 0) {
    if (status) {
      REQUIRE_CATCH(journal->open(), status);
    }
    else {
      journal->open();
    }
    return *this;
  }

  JournalProxy &require_entries(std::vector<JournalEntry> entries) {
    Journal::Iterator iter;
    PJournalEntry entry;
    ByteArray auxbuffer;
    int adjust = 0;

    // if vector is empty then make sure that the journal is also empty!
    if (entries.empty()) {
      journal->test_read_entry(&iter, &entry, &auxbuffer);
      REQUIRE(entry.lsn == 0);
      REQUIRE(auxbuffer.size() == 0);
      return *this;
    }

#if 0
    do {
      journal->test_read_entry(&iter, &entry, &auxbuffer);
      if (entry.lsn == 0)
        break;
      printf("lsn %u: txn %u, type %u\n", (unsigned)entry.lsn,
                      (unsigned)entry.txn_id, (unsigned)entry.type);
    } while (1);

    printf("----\n");

    for (auto e : entries) {
      printf("lsn %u: txn %u, type %u\n", (unsigned)e.lsn,
                      (unsigned)e.txnid, (unsigned)e.type);
    }

    return *this;
#endif

    bool starting = true;

    for (auto e : entries) {
      journal->test_read_entry(&iter, &entry, &auxbuffer);
      if (e.lsn == 0)
        continue;

      // skip Changesets
      while (entry.type == Journal::kEntryTypeChangeset && entry.lsn > 0) {
        if (!starting)
          adjust++;
        journal->test_read_entry(&iter, &entry, &auxbuffer);
      }

      starting = false;

      e.lsn += adjust;

      REQUIRE(e.lsn == entry.lsn);

      if (e.type == Journal::kEntryTypeInsert) {
        PJournalEntryInsert *ins = (PJournalEntryInsert *)auxbuffer.data();
        if (e.key)
          REQUIRE(ins->key_size == ::strlen(e.key) + 1);
        if (e.record)
          REQUIRE(ins->record_size == ::strlen(e.record) + 1);
        REQUIRE(ins->insert_flags == e.flags);
        if (ins->key_size > 0 && e.key)
          REQUIRE(0 == ::strcmp((char *)ins->key_data(), e.key));
        if (ins->record_size > 0 && e.record)
          REQUIRE(0 == ::strcmp((char *)ins->record_data(), e.record));
        continue;
      }

      if (e.type == Journal::kEntryTypeErase) {
        PJournalEntryErase *er = (PJournalEntryErase *)auxbuffer.data();
        if (e.key)
          REQUIRE(er->key_size == ::strlen(e.key) + 1);
        REQUIRE(er->duplicate == e.duplicate);
        // REQUIRE(er->erase_flags == e.flags);
        if (er->key_size > 0 && e.key)
          REQUIRE(0 == ::strcmp((char *)er->key_data(), e.key));
        continue;
      }

      if (e.type == Journal::kEntryTypeTxnBegin) {
        REQUIRE(auxbuffer.size() == 0);
        REQUIRE(entry.txn_id == e.txnid);
        continue;
      }

      if (e.type == Journal::kEntryTypeTxnAbort) {
        REQUIRE(auxbuffer.size() == 0);
        REQUIRE(entry.txn_id == e.txnid);
        continue;
      }

      if (e.type == Journal::kEntryTypeTxnCommit) {
        REQUIRE(auxbuffer.size() == 0);
        REQUIRE(entry.txn_id == e.txnid);
        continue;
      }

      REQUIRE(!"not yet implemented");
    }
    return *this;
  }

  JournalProxy &require_close(bool dontclearlog = false) {
    journal->close(dontclearlog);
    return *this;
  }

  JournalProxy &require_empty(bool empty = true) {
    REQUIRE(journal->is_empty() == empty);
    return *this;
  }

  JournalProxy &require_num_transactions(uint32_t value) {
    REQUIRE(journal->state.num_transactions == value);
    return *this;
  }

  JournalProxy &flush_buffers() {
    journal->test_flush_buffers();
    return *this;
  }

  JournalProxy &append_txn_begin(ups_txn_t *txn, const char *name,
                  uint64_t lsn) {
    journal->append_txn_begin((LocalTxn *)txn, name, lsn);
    return *this;
  }

  JournalProxy &append_txn_commit(ups_txn_t *txn, uint64_t lsn) {
    journal->append_txn_commit((LocalTxn *)txn, lsn);
    return *this;
  }

  JournalProxy &append_insert(ups_db_t *db, ups_txn_t *txn, const char *key,
                  const char *record, uint32_t flags, uint64_t lsn) {
    ups_key_t k = ups_make_key((void *)key, (uint16_t)(::strlen(key) + 1));
    ups_record_t r = ups_make_record((void *)record,
                            (uint32_t)(::strlen(record) + 1));
    journal->append_insert((Db *)db, (LocalTxn *)txn, &k, &r, flags, lsn);
    return *this;
  }

  JournalProxy &append_erase(ups_db_t *db, ups_txn_t *txn, const char *key,
                  uint32_t duplicate, uint32_t flags, uint64_t lsn) {
    ups_key_t k = ups_make_key((void *)key, (uint16_t)(::strlen(key) + 1));
    journal->append_erase((Db *)db, (LocalTxn *)txn, &k, duplicate, flags, lsn);
    return *this;
  }

  JournalProxy &clear() {
    journal->clear();
    return *this;
  }

  Journal *journal;
};

struct JournalFixture : BaseFixture {
  JournalFixture(uint32_t flags = 0) {
    require_create(flags | UPS_ENABLE_TRANSACTIONS, 0,
                    UPS_ENABLE_DUPLICATE_KEYS, 0);
  }

  uint64_t current_lsn() {
    return lenv()->lsn_manager.current;
  }

  uint64_t next_lsn() {
    return lenv()->lsn_manager.next();
  }

  void require_current_lsn(uint64_t lsn) {
    REQUIRE(lenv()->lsn_manager.current == lsn);
  }

  void require_file_size(const char *filename, uint64_t size) {
    File f;
    f.open(filename, 0);
    REQUIRE(f.file_size() == size);
    f.close();
  }

  uint64_t txnid(ups_txn_t *txn) {
    return ((Txn *)txn)->id;
  }

  void backup() {
    REQUIRE(true == os::copy("test.db", "test.db.bak"));
    REQUIRE(true == os::copy("test.db.jrn0", "test.db.bak0"));
    REQUIRE(true == os::copy("test.db.jrn1", "test.db.bak1"));
  }

  void restore() {
    REQUIRE(true == os::copy("test.db.bak", "test.db"));
    REQUIRE(true == os::copy("test.db.bak0", "test.db.jrn0"));
    REQUIRE(true == os::copy("test.db.bak1", "test.db.jrn1"));
  }

  // TODO why??
  Journal *reset_journal() {
    // setting the journal to NULL calls close() and deletes the
    // old journal
    lenv()->journal.reset(0);

    Journal *j = new Journal(lenv());
    j->create();
    lenv()->journal.reset(j);
    return j;
  }

  void createCloseTest() {
    JournalProxy jp(reset_journal());
    jp.require_empty();

    // do not close the journal - it will be closed in teardown()
  }

  void negativeCreateTest() {
    ScopedPtr<Journal> j(new Journal(lenv()));
    JournalProxy jp(j.get());

    std::string oldfilename = lenv()->config.filename;
    lenv()->config.filename = "/::asdf";

    jp.require_create(UPS_IO_ERROR);
    j->close();
    REQUIRE_CATCH(j->create(), UPS_IO_ERROR);
    lenv()->config.filename = oldfilename;
  }

  void negativeOpenTest() {
    ScopedPtr<Journal> j(new Journal(lenv()));
    JournalProxy jp(j.get());

    std::string oldfilename = lenv()->config.filename;
    lenv()->config.filename = "/::asdf";
    jp.require_open(UPS_FILE_NOT_FOUND);

    // if journal::open() fails, it will call journal::close()
    // internally and journal::close() overwrites the header structure.
    // therefore we have to patch the file before we start the test.
    File f;
    f.open("data/log-broken-magic.jrn0", 0);
    f.pwrite(0, (void *)"x", 1);
    f.close();

    lenv()->config.filename = "data/log-broken-magic";
    jp.require_open(UPS_LOG_INV_FILE_HEADER);
    lenv()->config.filename = oldfilename;
  }

  void appendTxnBeginTest() {
    JournalProxy jp(reset_journal());
    jp.require_empty()
      .require_num_transactions(0);

    TxnProxy tp(env, "name");
    jp.require_num_transactions(0)
      .flush_buffers() 
      .require_empty(true);

    require_current_lsn(3);
  }

  void appendTxnCommitTest() {
    JournalProxy jp(reset_journal());
    TxnProxy tp(env, "name");
    require_current_lsn(3);
    jp.flush_buffers()
      .require_empty(true)
      .append_txn_commit(tp.txn, 3)
      .require_empty(false);
  }

  void appendInsertTest() {
    JournalProxy jp(reset_journal());
    TxnProxy tp(env);
    jp.append_insert(db, tp.txn, "key1", "rec1", UPS_OVERWRITE, next_lsn());
    require_current_lsn(4);
    jp.require_close(true)
      .require_open()
      .require_entries( {
        { 3, 1, 1, Journal::kEntryTypeInsert, "key1", "rec1", UPS_OVERWRITE }
      });
  }

  void appendEraseTest() {
    JournalProxy jp(reset_journal());
    TxnProxy tp(env);
    jp.append_erase(db, tp.txn, "key1", 1, 0, next_lsn());
    require_current_lsn(4);
    jp.require_close(true)
      .require_open()
      .require_entries( {
        { 3, 1, 1, Journal::kEntryTypeErase, "key1", 1 }
      });
  }

  void appendClearTest() {
    JournalProxy jp(reset_journal());
    TxnProxy tp(env);
    jp.flush_buffers()
      .require_empty(true)
      .clear()
      .require_empty(true);
    require_current_lsn(3);
    tp.abort();
    require_current_lsn(3);
    jp.require_close()
      .require_open();
    require_current_lsn(3);
  }

  void iterateOverEmptyLogTest() {
    JournalProxy jp(reset_journal());
    jp.require_entries( { } );
  }

  void iterateOverLogOneEntryTest() {
    JournalProxy jp(reset_journal());
    TxnProxy tp(env);
    jp.append_txn_commit(tp.txn, next_lsn());
    jp.require_close(true)
      .require_open()
      .require_entries( {
        { 3, 1, 0, Journal::kEntryTypeTxnCommit, nullptr, nullptr, 0 }
      });
  }

  void iterateOverLogMultipleEntryTest() {
    JournalProxy jp(reset_journal());

    std::vector<JournalEntry> vec;
    for (uint64_t i = 0; i < 5; i++) {
      // ups_txn_begin and ups_txn_commit will automatically add a
      // journal entry
      TxnProxy tp(env, nullptr, true); // calls begin/commit
      vec.push_back( { 2 + i * 2, tp.id(), Journal::kEntryTypeTxnBegin } );
      vec.push_back( { 3 + i * 2, tp.id(), Journal::kEntryTypeTxnCommit } );
    }

    // reopen the journal, verify entries
    jp.require_close(true)
      .require_open()
      .require_entries(vec);
  }

  void iterateOverLogMultipleEntrySwapTest() {
    JournalProxy jp(reset_journal());
    jp.journal->state.threshold = 5;

    std::vector<JournalEntry> vec;
    for (uint64_t i = 0; i <= 7; i++) {
      // ups_txn_begin and ups_txn_commit will automatically add a
      // journal entry
      TxnProxy tp(env, nullptr, true); // calls begin/commit
      vec.push_back( { 2 + i * 2, tp.id(), Journal::kEntryTypeTxnBegin } );
      vec.push_back( { 3 + i * 2, tp.id(), Journal::kEntryTypeTxnCommit } );
    }

    // reopen the journal, verify entries
    jp.require_close(true)
      .require_open()
      .require_entries(vec);
  }

  void iterateOverLogMultipleEntrySwapTwiceTest() {
    JournalProxy jp(reset_journal());
    jp.journal->state.threshold = 5;
    uint64_t lsn = 2;

    std::vector<JournalEntry> vec;
    for (uint64_t i = 0; i <= 10; i++) {
      TxnProxy tp(env, nullptr, true); // calls begin/commit
      // ups_txn_begin and ups_txn_commit will automatically add a
      // journal entry
      vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnBegin } );
      vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnCommit } );
    }

    // reopen the journal, verify entries
    jp.require_close(true)
      .require_open()
      .require_entries(vec);
  }

  void verifyJournalIsEmpty() {
    uint64_t size;
    Journal *j = lenv()->journal.get();
    REQUIRE(j != 0);
    size = j->state.files[0].file_size();
    REQUIRE(0 == size);
    size = j->state.files[1].file_size();
    REQUIRE(0 == size);
  }

  void recoverVerifyTxnIdsTest() {
    for (uint64_t i = 0; i < 5; i++) {
      TxnProxy tp(env, nullptr, true); // calls begin/commit
    }

    // reopen the Environment (and perform recovery)
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    require_open(UPS_ENABLE_TRANSACTIONS, nullptr, UPS_NEED_RECOVERY);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    require_current_lsn(11);

    REQUIRE(5ull == ((LocalTxnManager *)(lenv()->txn_manager.get()))->_txn_id);

    // create another transaction and make sure that the transaction
    // IDs and the lsn's continue seamlessly
    TxnProxy tp2(env);
    REQUIRE(tp2.id() == 6);
  }

  void recoverCommittedTxnsTest() {
    std::vector<uint8_t> record;
    std::vector<JournalEntry> vec;
    uint64_t lsn = 2;

    for (uint64_t i = 0; i < 5; i++) {
      TxnProxy tp(env, nullptr, true); // calls begin/commit
      DbProxy dbp(db);
      dbp.require_insert(tp.txn, (uint32_t)i, record);
      vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnBegin } );
      vec.push_back( { lsn++, tp.id(), 1, Journal::kEntryTypeInsert,
                             nullptr, nullptr, 0 } );
      vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnCommit } );
    }

    // reopen the journal, verify entries
    JournalProxy jp(lenv());
    jp.require_close(true)
      .require_open()
      .require_entries(vec);

    // now perform recovery
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);

    // The journal must be empty now
    jp = JournalProxy(lenv());
    jp.require_empty(true);

    for (uint32_t i = 0; i < 5; i++) {
      DbProxy dbp(db);
      dbp.require_find(i, record);
    }
  }

  void recoverAutoAbortTxnsTest() {
#ifndef WIN32
    // create a couple of transaction which insert a key, but do not
    // commit them!
    ups_txn_t *txn[5];
    std::vector<uint8_t> record;
    std::vector<JournalEntry> vec;

    JournalProxy jp(lenv());
    jp.clear();

    for (uint64_t i = 0; i < 5; i++) {
      REQUIRE(0 == ups_txn_begin(&txn[i], env, nullptr, 0, 0));
      DbProxy dbp(db);
      dbp.require_insert(txn[i], (uint32_t)i, record);
    }

    jp.flush_buffers();

    // backup the journal files; then re-create the Environment from the
    // journal
    backup();
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_txn_abort(txn[i], 0));
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    restore();
    require_open(); // no transactions -> no recovery!
    jp = JournalProxy(new Journal(lenv()));
    jp.require_close(true)
      .require_open()
      .require_entries(vec)
      .require_close(true);
    delete jp.journal;
    jp.journal = nullptr;

    // now open and perform recovery
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    jp = JournalProxy(lenv());
    jp.require_empty();
    for (uint32_t i = 0; i < 5; i++) {
      DbProxy dbp(db);
      dbp.require_find(i, record, UPS_KEY_NOT_FOUND);
    }
#endif
  }

  void recoverTempTxns() {
#ifndef WIN32
    std::vector<uint8_t> record;

    for (uint64_t i = 0; i < 5; i++) {
      DbProxy dbp(db);
      dbp.require_insert((uint32_t)i, record);
    }

    JournalProxy jp(lenv());
    jp.flush_buffers();

    backup();
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    restore();
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    jp = JournalProxy(lenv());
    jp.require_empty();

    for (uint32_t i = 0; i < 5; i++) {
      DbProxy dbp(db);
      dbp.require_find(i, record);
    }
#endif
  }

  void recoverInsertTest() {
    std::vector<JournalEntry> vec;
    JournalProxy jp(lenv());
    ups_txn_t *txn[2];
    uint64_t lsn = 2;

    // create two transactions...
    for (int i = 0; i < 2; i++) {
      REQUIRE(0 == ups_txn_begin(&txn[i], env, nullptr, 0, 0));
      if (i == 0)
        vec.push_back( { lsn++, txnid(txn[0]), Journal::kEntryTypeTxnBegin } );
      else
        lsn++;
    }

    // with many keys
    std::vector<uint8_t> record;
    DbProxy dbp(db);
    for (int i = 0; i < 100; i++) {
      dbp.require_insert(txn[i & 1], (uint32_t)i, record);
      if ((i & 1) == 0)
        vec.push_back( { lsn++, txnid(txn[0]), 1, Journal::kEntryTypeInsert,
                             nullptr, nullptr, 0 } );
      else
        lsn++;
    }

    // commit the first txn, abort the second
    vec.push_back( { lsn++, txnid(txn[0]), Journal::kEntryTypeTxnCommit } );
    REQUIRE(0 == ups_txn_commit(txn[0], 0));
    REQUIRE(0 == ups_txn_abort(txn[1], 0));

    jp.flush_buffers();

    backup();
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    restore();
    require_open(); // no transactions - no recovery
    jp = JournalProxy(new Journal(lenv()));
    jp.require_close(true)
      .require_open()
      .require_entries(vec)
      .require_close(true);
    delete jp.journal;
    jp.journal = nullptr;
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);

    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    jp = JournalProxy(lenv());
    jp.require_empty();

    for (uint32_t i = 0; i < 100; i++) {
      DbProxy dbp(db);
      if (i & 1)
        dbp.require_find(i, record, UPS_KEY_NOT_FOUND);
      else
        dbp.require_find(i, record);
    }
  }

  void recoverEraseTest() {
    // create a transaction with many keys that are inserted, mostly
    // duplicates
    std::vector<JournalEntry> vec;
    JournalProxy jp(lenv());
    uint64_t lsn = 2;

    TxnProxy tp(env);
    vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnBegin } );

    // with many keys
    std::vector<uint8_t> record;
    DbProxy dbp(db);
    for (int i = 0; i < 100; i++) {
      dbp.require_insert_duplicate(tp.txn, (uint32_t)(i % 10), record);
      vec.push_back( { lsn++, tp.id(), 1, Journal::kEntryTypeInsert,
                             nullptr, nullptr, UPS_DUPLICATE } );
    }

    // now delete them, then commit the transaction
    for (uint32_t i = 0; i < 10; i++) {
      dbp.require_erase(tp.txn, i);
      vec.push_back( { lsn++, tp.id(), 1, Journal::kEntryTypeErase,
                      nullptr, 0 } );
    }
    vec.push_back( { lsn++, tp.id(), Journal::kEntryTypeTxnCommit } );
    tp.commit();

    jp.flush_buffers();

    // verify the journal entries
    backup();
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    restore();
    require_open(); // no transactions - no recovery
    jp = JournalProxy(new Journal(lenv()));
    jp.require_close(true)
      .require_open()
      .require_entries(vec)
      .require_close(true);
    delete jp.journal;
    jp.journal = nullptr;
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);

    // recover; the database must be empty!
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    jp = JournalProxy(lenv());
    jp.require_empty();
    dbp = DbProxy(db);
    dbp.require_key_count(0);
  }

  void recoverAfterChangesetTest() {
#ifndef WIN32
    ups_txn_t *txn;
    std::vector<uint8_t> kvec = {'k', 'e', 'y', '\0'};

    // do not immediately flush the changeset after a commit
    close();
    require_create(UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS,
                    nullptr, UPS_ENABLE_DUPLICATES, nullptr);

    int i, j = 0;
    for (i = 0; i < 64; i++) {
      TxnProxy tp(env, nullptr, true);
      DbProxy dbp(db);
      dbp.require_insert_duplicate(tp.txn, kvec, (uint32_t)i);
    }

    // backup the files, then restore
    backup();
    close(UPS_AUTO_CLEANUP);
    restore();

    // Open the environment and recover, then verify that the database
    // is complete
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      REQUIRE(key.size == kvec.size());
      REQUIRE(0 == ::strcmp((const char *)kvec.data(), (const char *)key.data));
      REQUIRE(rec.size == sizeof(j));
      REQUIRE(0 == ::memcmp(&j, rec.data, sizeof(j)));
      j++;
    }
    REQUIRE(i == j);
    REQUIRE(0 == ups_cursor_close(cursor));
#endif
  }

  void recoverAfterChangesetAndCommitTest() {
#ifndef WIN32
    ups_txn_t *txn;
    std::vector<uint8_t> kvec1 = {'k', 'e', 'y', '\0'};
    std::vector<uint8_t> kvec2 = {'k', 'e', 'z', '\0'};
    std::vector<uint8_t> rvec  = {'r', 'e', 'c', '\0'};

    // do not immediately flush the changeset after a commit
    close();
    require_create(UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS,
                    nullptr, UPS_ENABLE_DUPLICATES, nullptr);

    int i, j = 0;
    for (i = 0; i < 64; i++) {
      TxnProxy tp(env, nullptr, true);
      DbProxy dbp(db);
      dbp.require_insert_duplicate(tp.txn, kvec1, (uint32_t)i);
    }

    // changeset was flushed, now add another commit
    {
      TxnProxy tp(env, nullptr, true);
      DbProxy dbp(db);
      dbp.require_insert_duplicate(tp.txn, kvec2, rvec);
      i++;
    }

    backup();
    close(UPS_AUTO_CLEANUP);
    restore();

    // open the environment, verify the database
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      if (j < 64) {
        REQUIRE(key.size == kvec1.size());
        REQUIRE(0 == ::strcmp((const char *)kvec1.data(),
                                (const char *)key.data));
        REQUIRE(rec.size == sizeof(uint32_t));
        REQUIRE(0 == ::memcmp(&j, rec.data, sizeof(j)));
      }
      else {
        REQUIRE(key.size == kvec2.size());
        REQUIRE(0 == ::strcmp((const char *)kvec2.data(),
                                (const char *)key.data));
        REQUIRE(rec.size == rvec.size());
        REQUIRE(0 == ::strcmp((const char *)rvec.data(),
                                (const char *)rec.data));
      }
      j++;
    }
    REQUIRE(i == j);
    REQUIRE(0 == ups_cursor_close(cursor));
#endif
  }

  void recoverAfterChangesetAndCommit2Test() {
#ifndef WIN32
    std::vector<uint8_t> kvec1 = {'k', 'e', 'y', '\0'};
    std::vector<uint8_t> kvec2 = {'k', 'e', 'z', '\0'};
    std::vector<uint8_t> rvec  = {'r', 'e', 'c', '\0'};

    // do not immediately flush the changeset after a commit
    close();
    require_create(UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS,
                    nullptr, UPS_ENABLE_DUPLICATES, nullptr);

    TxnProxy longtp(env);
    DbProxy dbp(db);
    JournalProxy jp(reset_journal());
    jp.journal->state.threshold = 5;

    int i, j = 0, limit = 100;
    for (i = 0; i < limit; i++) {
      TxnProxy tp(env, nullptr, true);
      dbp.require_insert_duplicate(tp.txn, kvec1, (uint32_t)i);
    }

    // now commit the previous transaction
    dbp.require_insert_duplicate(longtp.txn, kvec2, rvec);
    longtp.commit();
    i++;

    // perform recovery
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);

    // verify the database
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      if (j < limit) {
        REQUIRE(key.size == kvec1.size());
        REQUIRE(0 == ::strcmp((const char *)kvec1.data(),
                                (const char *)key.data));
        REQUIRE(rec.size == sizeof(uint32_t));
        REQUIRE(0 == ::memcmp(&j, rec.data, sizeof(j)));
      }
      else {
        REQUIRE(key.size == kvec2.size());
        REQUIRE(0 == ::strcmp((const char *)kvec2.data(),
                                (const char *)key.data));
        REQUIRE(rec.size == rvec.size());
        REQUIRE(0 == ::strcmp((const char *)rvec.data(),
                                (const char *)rec.data));
      }
      j++;
    }
    REQUIRE(i == j);
    REQUIRE(0 == ups_cursor_close(cursor));
#endif
  }

  void recoverDuplicatesTest() {
#ifndef WIN32
    std::vector<uint8_t> kvec = {'k', 'e', 'y', '\0'};

    TxnProxy longtp(env);
    DbProxy dbp(db);
    JournalProxy jp(reset_journal());

    uint32_t i, limit = 5;
    for (i = 0; i < limit; i++) {
      TxnProxy tp(env, nullptr, true);

      ups_key_t key = ups_make_key(kvec.data(), 4);
      ups_record_t rec = ups_make_record(&i, sizeof(i));
      ups_cursor_t *cursor;
      REQUIRE(0 == ups_cursor_create(&cursor, db, tp.txn, 0));
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec,
                              UPS_DUPLICATE_INSERT_FIRST));
      REQUIRE(0 == ups_cursor_close(cursor));
    }

    // We now have 5 committed transactions logged to the journal, but
    // not flushed to the btree (because |longtp| was still active).
    // Backup the journal files.
    backup();
    longtp.abort();
    close(UPS_AUTO_CLEANUP);

    // Restore the journal and perform recovery
    restore();
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);

    // verify the database
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      i--;
      REQUIRE(key.size == kvec.size());
      REQUIRE(0 == ::strcmp((const char *)kvec.data(), (const char *)key.data));
      REQUIRE(rec.size == sizeof(uint32_t));
      REQUIRE(0 == ::memcmp(&i, rec.data, sizeof(i)));
    }
    REQUIRE(i == 0);
    REQUIRE(0 == ups_cursor_close(cursor));
#endif
  }

  void recoverFromRecoveryTest() {
#ifndef WIN32
    std::vector<uint8_t> kvec1 = {'k', 'e', 'y', '\0'};

    // do not immediately flush the changeset after a commit
    close();
    require_create(UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS,
                    nullptr, UPS_ENABLE_DUPLICATES, nullptr);

    // need a second database
    ups_db_t *db2;
    REQUIRE(0 == ups_env_create_db(env, &db2, 2,
                            UPS_ENABLE_DUPLICATE_KEYS, 0));

    // add 5 commits
    int i, j = 0;
    for (i = 0; i < 5; i++) {
      TxnProxy tp(env, nullptr, true);
      DbProxy dbp(db);
      dbp.require_insert_duplicate(tp.txn, kvec1, (uint32_t)i);
    }

    // changeset was flushed, now add another commit in the other database,
    // to make sure that it affects a different page
    i = 0;
    {
      TxnProxy tp(env, nullptr, true);
      DbProxy dbp(db2);
      dbp.require_insert_duplicate(tp.txn, kvec1, (uint32_t)i);
    }

    // perform recovery
    backup();
    close(UPS_AUTO_CLEANUP);
    restore();

    // make sure that recovery will fail
    ErrorInducer::activate(true);
    ErrorInducer::add(ErrorInducer::kChangesetFlush, 2);

    // open the environment, perform recovery
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, nullptr,
                    UPS_INTERNAL_ERROR);

    // disable error inducer, try again
    ErrorInducer::activate(false);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
    REQUIRE(0 == ups_env_open_db(env, &db2, 2, 0, 0));

    // now verify that the database is complete
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key = {0};
    ups_record_t rec = {0};
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      REQUIRE(key.size == kvec1.size());
      REQUIRE(0 == ::strcmp((const char *)kvec1.data(),
                              (const char *)key.data));
      REQUIRE(rec.size == sizeof(uint32_t));
      REQUIRE(0 == ::memcmp(&j, rec.data, sizeof(j)));
      j++;
    }
    REQUIRE(j == 5);
    REQUIRE(0 == ups_cursor_close(cursor));

    REQUIRE(0 == ups_cursor_create(&cursor, db2, 0, 0));
    j = 0;
    while (ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT) == 0) {
      REQUIRE(0 == ::strcmp((const char *)kvec1.data(),
                              (const char *)key.data));
      REQUIRE(0 == ::memcmp(&j, rec.data, sizeof(j)));
      REQUIRE(rec.size == sizeof(j));
      j++;
    }
    REQUIRE(j == 1);
    REQUIRE(0 == ups_cursor_close(cursor));
#endif
  }

  void switchThresholdTest() {
    ups_parameter_t params[] = {
        { UPS_PARAM_JOURNAL_SWITCH_THRESHOLD, 33 },
        { 0, 0 }
    };

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, params, 0, 0);

    // verify threshold through ups_env_get_parameters
    require_parameter(params[0].name, params[0].value);

    // verify threshold in the Journal object
    Journal *j = lenv()->journal.get();
    j->state.threshold = 5;

    // open w/o parameter
    close(UPS_AUTO_CLEANUP);
    require_open(UPS_ENABLE_TRANSACTIONS);
    require_parameter(params[0].name, 0);

    // open w/ parameter
    close(UPS_AUTO_CLEANUP);
    params[0].value = 44;
    require_open(UPS_ENABLE_TRANSACTIONS, params);
    require_parameter(params[0].name, params[0].value);
  }

  void issue45Test() {
    // create a transaction with one insert
    TxnProxy tp(env);
    DbProxy dbp(db);
    std::vector<uint8_t> rvec = {'a', 'a', 'a', 'a', 'a', '\0'};
    dbp.require_insert(tp.txn, 5u, rvec);

    // reopen and recover. issue 45 causes a segfault
    tp.abort();
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
  }

  void issue71Test() {
    DbProxy dbp(db);
    std::vector<uint8_t> rvec = {'a', 'a', 'a', 'a'};
    for (uint32_t i = 0; i < 80; i++) {
      dbp.require_insert(i, rvec);
    }

    // close the environment
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);

    // verify the journal file sizes
    require_file_size("test.db.jrn0", 33664);
    require_file_size("test.db.jrn1", 51168);
  }

  void recoverWithCrc32Test() {
    std::vector<uint8_t> record;
    close();
    uint32_t flags = UPS_ENABLE_TRANSACTIONS
                      | UPS_ENABLE_CRC32
                      | UPS_ENABLE_FSYNC;
    require_create(flags);

    TxnProxy tp(env);
    DbProxy dbp(db);
    dbp.require_insert(tp.txn, 1u, record);
    dbp.require_insert(tp.txn, 2u, record);
    dbp.require_insert(tp.txn, 3u, record);
    tp.commit();

    // close without clearing the journal file; this will trigger the
    // recovery when the environment is opened again
    close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG);

    // now open the Environment w/o UPS_AUTO_RECOVERY. This will fail
    // with error UPS_NEED_RECOVERY. 
    require_open(UPS_ENABLE_TRANSACTIONS, nullptr, UPS_NEED_RECOVERY);
    require_open(UPS_ENABLE_TRANSACTIONS | UPS_ENABLE_CRC32, nullptr,
                    UPS_NEED_RECOVERY);
    require_open(flags, nullptr, UPS_NEED_RECOVERY);

    // now open *with* recovery; this must succeed
    require_open(flags | UPS_AUTO_RECOVERY);

    // verify that the inserted keys exist
    dbp = DbProxy(db);
    dbp.require_find(1u, record);
    dbp.require_find(2u, record);
    dbp.require_find(3u, record);

    // also make sure that the correct flags are set
    require_flags(UPS_ENABLE_TRANSACTIONS, true);
    require_flags(UPS_ENABLE_CRC32, true);
    require_flags(UPS_ENABLE_FSYNC, true);
  }
};

TEST_CASE("Journal/createClose", "")
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

TEST_CASE("Journal/appendErase", "")
{
  JournalFixture f;
  f.appendEraseTest();
}

TEST_CASE("Journal/appendClear", "")
{
  JournalFixture f;
  f.appendClearTest();
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

TEST_CASE("Journal/recoverDuplicatesTest", "")
{
  JournalFixture f;
  f.recoverDuplicatesTest();
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

TEST_CASE("Journal/issue71Test", "")
{
  JournalFixture f;
  f.issue71Test();
}

TEST_CASE("Journal/recoverWithCrc32Test", "")
{
  JournalFixture f;
  f.recoverWithCrc32Test();
}

} // namespace upscaledb

