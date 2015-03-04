/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * Routines for the journal - writing, reading, recovering
 *
 * The journal is a facility for storing logical and physical redo-information.
 *
 * The logical information describes the database operation (i.e. insert/erase),
 * the physical information describes the modified pages.
 *
 * "Undo" information is not required because aborted Transactions are never
 * written to disk. The journal only can "redo" operations.
 *
 * The journal is organized in two files. If one of the files grows too large
 * then all new Transactions are stored in the other file
 * ("Log file switching"). When all Transactions from file #0 are committed,
 * and file #1 exceeds a limit, then the files are switched back again.
 *
 * For writing, files are buffered. The buffers are flushed when they
 * exceed a certain threshold, when a Transaction is committed or a Changeset
 * was written. In case of a commit or a changeset there will also be an
 * fsync, if HAM_ENABLE_FSYNC is enabled.
 *
 * The physical information is a collection of pages which are modified in
 * one or more database operations (i.e. ham_db_erase). This collection is
 * called a "changeset" and implemented in changeset.h/.cc. As soon as the
 * operation is finished, the changeset is flushed: if the changeset contains
 * just a single page, then this operation is atomic and is NOT logged.
 * Otherwise the whole changeset is appended to the journal, and afterwards
 * the database file is modified.
 *
 * For recovery to work, each page stores the lsn of its last modification.
 *
 * When recovering, the Journal first extracts the newest/latest entry.
 * If this entry is a changeset then the changeset is reapplied, because
 * we assume that there was a crash immediately AFTER the changeset was
 * written, but BEFORE the database file was modified. (The changeset is
 * idempotent; if the database file was successfully modified then the
 * changes are re-applied; this is not a problem.)
 *
 * Afterwards, hamsterdb uses the lsn's to figure out whether an update
 * was already applied or not. If the journal's last entry is a changeset then
 * this changeset's lsn marks the beginning of the sequence. Otherwise the lsn
 * is fetched from the journal file headers. All journal entries with an lsn
 * *older* than this start-lsn will be skipped, all others are re-applied.
 *
 * In this phase all changesets are skipped because the newest changeset was
 * already applied, and we know that all older changesets
 * have already been written successfully to the database file.
 *
 * @exception_safe: basic
 * @thread_safe: no
 */

#ifndef HAM_JOURNAL_H
#define HAM_JOURNAL_H

#include "0root/root.h"

#include <map>
#include <cstdio>
#include <string>

#include "ham/hamsterdb_int.h" // for metrics

#include "1base/dynamic_array.h"
#include "1os/file.h"
#include "1errorinducer/errorinducer.h"
#include "2page/page_collection.h"
#include "3journal/journal_entries.h"
#include "3journal/journal_state.h"
#include "3journal/journal_test.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
class Page;
class Database;
class Transaction;
class LocalEnvironment;
class LocalTransaction;
class LocalTransactionManager;

#include "1base/packstart.h"

//
// The Journal object
//
class Journal
{
  public:
    enum {
      // marks the start of a new transaction
      kEntryTypeTxnBegin   = 1,

      // marks the end of an aborted transaction
      kEntryTypeTxnAbort   = 2,

      // marks the end of an committed transaction
      kEntryTypeTxnCommit  = 3,

      // marks an insert operation
      kEntryTypeInsert     = 4,

      // marks an erase operation
      kEntryTypeErase      = 5,

      // marks a whole changeset operation (writes modified pages)
      kEntryTypeChangeset  = 6
    };

    //
    // An "iterator" structure for traversing the journal files
    //
    struct Iterator {
      Iterator()
        : fdidx(0), fdstart(0), offset(0) {
      }

      // selects the file descriptor [0..1]
      int fdidx;

      // which file descriptor did we start with? [0..1]
      int fdstart;

      // the offset in the file of the NEXT entry
      uint64_t offset;
    };

    // Constructor
    Journal(LocalEnvironment *env);

    // Creates a new journal
    void create();

    // Opens an existing journal
    void open();

    // Returns true if the journal is empty
    bool is_empty() {
      if (!m_state.files[0].is_open() && !m_state.files[1].is_open())
        return (true);

      for (int i = 0; i < 2; i++) {
        uint64_t size = m_state.files[i].get_file_size();
        if (size > 0)
          return (false);
      }

      return (true);
    }

    // Appends a journal entry for ham_txn_begin/kEntryTypeTxnBegin
    void append_txn_begin(LocalTransaction *txn, const char *name,
                    uint64_t lsn);

    // Appends a journal entry for ham_txn_abort/kEntryTypeTxnAbort
    void append_txn_abort(LocalTransaction *txn, uint64_t lsn);

    // Appends a journal entry for ham_txn_commit/kEntryTypeTxnCommit
    void append_txn_commit(LocalTransaction *txn, uint64_t lsn);

    // Appends a journal entry for ham_insert/kEntryTypeInsert
    void append_insert(Database *db, LocalTransaction *txn,
                    ham_key_t *key, ham_record_t *record, uint32_t flags,
                    uint64_t lsn);

    // Appends a journal entry for ham_erase/kEntryTypeErase
    void append_erase(Database *db, LocalTransaction *txn,
                    ham_key_t *key, int duplicate_index, uint32_t flags,
                    uint64_t lsn);

    // Appends a journal entry for a whole changeset/kEntryTypeChangeset
    void append_changeset(const Page **pages, int num_pages, uint64_t lsn);

    // Adjusts the transaction counters; called whenever |txn| is flushed.
    void transaction_flushed(LocalTransaction *txn);

    // Empties the journal, removes all entries
    void clear() {
      for (int i = 0; i < 2; i++)
        clear_file(i);
    }

    // Closes the journal, frees all allocated resources
    void close(bool noclear = false);

    // Performs the recovery! All committed Transactions will be re-applied,
    // all others are automatically aborted
    void recover(LocalTransactionManager *txn_manager);

    // Fills the metrics
    void fill_metrics(ham_env_metrics_t *metrics) {
      metrics->journal_bytes_flushed = m_state.count_bytes_flushed;
    }

  private:
    friend struct JournalFixture;

    // Returns a pointer to database. If the database was not yet opened then
    // it is opened implicitly.
    Database *get_db(uint16_t dbname);

    // Returns a pointer to a Transaction object.
    Transaction *get_txn(LocalTransactionManager *txn_manager, uint64_t txn_id);

    // Closes all databases.
    void close_all_databases();

    // Aborts all transactions which are still active.
    void abort_uncommitted_txns(LocalTransactionManager *txn_manager);

    // Helper function which adds a single page from the changeset to
    // the Journal; returns the page size (or compressed size, if compression
    // was enabled)
    uint32_t append_changeset_page(const Page *page, uint32_t page_size);

    // Recovers (re-applies) the physical changelog; returns the lsn of the
    // Changelog
    uint64_t recover_changeset();

    // Scans a file for the newest changeset. Returns the lsn of this
    // changeset, and the position (offset) in the file
    uint64_t scan_for_newest_changeset(File *file, uint64_t *position);

    // Recovers the logical journal
    void recover_journal(Context *context,
                    LocalTransactionManager *txn_manager, uint64_t start_lsn);

    // Switches the log file if necessary; returns the new log descriptor in the
    // transaction
    int switch_files_maybe();

    // returns the path of the journal file
    std::string get_path(int i);

    // Sequentially returns the next journal entry, starting with
    // the oldest entry.
    //
    // |iter| must be initialized with zeroes for the first call.
    // |auxbuffer| returns the auxiliary data of the entry and is either
    // a structure of type PJournalEntryInsert or PJournalEntryErase.
    //
    // Returns an empty entry (lsn is zero) after the last element.
    void get_entry(Iterator *iter, PJournalEntry *entry,
                    ByteArray *auxbuffer);

    // Appends an entry to the journal
    void append_entry(int idx,
                const uint8_t *ptr1 = 0, size_t ptr1_size = 0,
                const uint8_t *ptr2 = 0, size_t ptr2_size = 0,
                const uint8_t *ptr3 = 0, size_t ptr3_size = 0,
                const uint8_t *ptr4 = 0, size_t ptr4_size = 0,
                const uint8_t *ptr5 = 0, size_t ptr5_size = 0) {
      if (ptr1_size)
        m_state.buffer[idx].append(ptr1, ptr1_size);
      if (ptr2_size)
        m_state.buffer[idx].append(ptr2, ptr2_size);
      if (ptr3_size)
        m_state.buffer[idx].append(ptr3, ptr3_size);
      if (ptr4_size)
        m_state.buffer[idx].append(ptr4, ptr4_size);
      if (ptr5_size)
        m_state.buffer[idx].append(ptr5, ptr5_size);
    }

    // flush buffer if size limit is exceeded
    void maybe_flush_buffer(int idx) {
      if (m_state.buffer[idx].get_size() >= JournalState::kBufferLimit)
        flush_buffer(idx);
    }

    // Flushes a buffer to disk
    void flush_buffer(int idx, bool fsync = false) {
      if (m_state.buffer[idx].get_size() > 0) {
        // error inducer? then write only a part of the buffer and return
        if (ErrorInducer::is_active()
              && ErrorInducer::get_instance()->induce(ErrorInducer::kChangesetFlush)) {
          m_state.files[idx].write(m_state.buffer[idx].get_ptr(),
                  m_state.buffer[idx].get_size() - 5);
          throw Exception(HAM_INTERNAL_ERROR);
        }

        m_state.files[idx].write(m_state.buffer[idx].get_ptr(),
                        m_state.buffer[idx].get_size());
        m_state.count_bytes_flushed += m_state.buffer[idx].get_size();

        m_state.buffer[idx].clear();
        if (fsync)
          m_state.files[idx].flush();
      }
    }

    // Clears a single file
    void clear_file(int idx);

    // Returns the test object
    JournalTest test();

  private:
    // The mutable state
    JournalState m_state;
};

#include "1base/packstop.h"

} // namespace hamsterdb

#endif /* HAM_JOURNAL_H */
