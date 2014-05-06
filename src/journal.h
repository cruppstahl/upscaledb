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

/*
 * Routines for the journal - writing, reading, recovering
 *
 * The journal is a log for storing logical and physical redo-information.
 *
 * The logical information describe the database operation (i.e. insert/erase),
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
 * a single database operation (i.e. ham_db_erase). This collection is
 * called a "changeset" and implemented in changeset.h/.cc. As soon as the
 * operation is finished, the changeset is flushed: if the changeset contains
 * just a single page, then this operation is atomic and is NOT logged.
 * Otherwise the whole changeset is appended to the journal, and afterwards
 * the database file is modified.
 *
 * For recovery to work, each page stores the lsn of its last modification.
 * In addition, the journal also stores the last lsn - in the header structure
 * of each file, but also in each entry. 
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
 */

#ifndef HAM_JOURNAL_H__
#define HAM_JOURNAL_H__

#include <cstdio>
#include <string>

#include <ham/hamsterdb_int.h> // for metrics

#include "os.h"
#include "util.h"
#include "journal_entries.h"

namespace hamsterdb {

class Page;
class Database;
class LocalEnvironment;
class LocalTransaction;

#include "packstart.h"

//
// The Journal object
//
class Journal
{
    enum {
      // switch log file after |kSwitchTxnThreshold| transactions
      kSwitchTxnThreshold = 16,

      // flush buffers if this limit is exceeded
      kBufferLimit = 1024 * 1024, // 1 mb

      // magic for a journal file
      kHeaderMagic = ('h' << 24) | ('j' << 16) | ('o' << 8) | '2',

      // magic for a journal trailer
      kTrailerMagic = ('h' << 24) | ('t' << 16) | ('r' << 8) | '1'
    };

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
    // The header structure of a journal file
    //
    HAM_PACK_0 struct HAM_PACK_1 PJournalHeader {
      PJournalHeader()
        : magic(kHeaderMagic), _reserved(0), lsn(0) {
      }

      // the magic
      ham_u32_t magic;

      // reserved, for padding
      ham_u32_t _reserved;

      // the last used lsn
      ham_u64_t lsn;
    } HAM_PACK_2;

    //
    // The trailer of each journal entry
    //
    HAM_PACK_0 struct HAM_PACK_1 PJournalTrailer {
      PJournalTrailer()
        : magic(kTrailerMagic) {
      }

      // the magic
      ham_u32_t magic;

      // the entry type
      ham_u32_t type;

      // the full size of the entry
      ham_u32_t full_size;
    } HAM_PACK_2;

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
      ham_u64_t offset;
    };

    // Constructor
    Journal(LocalEnvironment *env);

    // Creates a new journal
    void create();

    // Opens an existing journal
    void open();

    // Returns true if the journal is empty
    bool is_empty() {
      ham_u64_t size;

      if (!m_files[0].is_open() && !m_files[1].is_open())
        return (true);

      for (int i = 0; i < 2; i++) {
        size = m_files[i].get_file_size();
        if (size && size != sizeof(PJournalHeader))
          return (false);
      }

      return (true);
    }

    // Appends a journal entry for ham_txn_begin/kEntryTypeTxnBegin
    // TODO |env| parameter is not necessary
    void append_txn_begin(LocalTransaction *txn, LocalEnvironment *env,
                const char *name, ham_u64_t lsn);

    // Appends a journal entry for ham_txn_abort/kEntryTypeTxnAbort
    void append_txn_abort(LocalTransaction *txn, ham_u64_t lsn);

    // Appends a journal entry for ham_txn_commit/kEntryTypeTxnCommit
    void append_txn_commit(LocalTransaction *txn, ham_u64_t lsn);

    // Appends a journal entry for ham_insert/kEntryTypeInsert
    void append_insert(Database *db, LocalTransaction *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags,
                ham_u64_t lsn);

    // Appends a journal entry for ham_erase/kEntryTypeErase
    void append_erase(Database *db, LocalTransaction *txn,
                ham_key_t *key, ham_u32_t dupe, ham_u32_t flags, ham_u64_t lsn);

    // Appends a journal entry for a whole changeset/kEntryTypeChangeset
    void append_changeset(Page **bucket1, ham_u32_t bucket1_size,
                    Page **bucket2, ham_u32_t bucket2_size,
                    Page **bucket3, ham_u32_t bucket3_size,
                    Page **bucket4, ham_u32_t bucket4_size,
                    ham_u32_t lsn);

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
    void recover();

    // Returns the next lsn
    ham_u64_t get_incremented_lsn() {
      return (m_lsn++);
    }

    // Fills the metrics
    void get_metrics(ham_env_metrics_t *metrics) {
      metrics->journal_bytes_flushed = m_count_bytes_flushed;
    }

    // Returns the previous lsn; only for testing!
    // TODO really required? JournalFixture is a friend!
    ham_u64_t test_get_lsn() {
      return (m_lsn);
    }

  private:
    friend struct JournalFixture;

    // Helper function which adds a single page from the changeset to
    // the Journal; returns the page size (or compressed size, if compression
    // was enabled)
    ham_u32_t append_changeset_page(Page *page, ham_u32_t page_size);

    // Recovers (re-applies) the physical changelog; returns the lsn of the
    // Changelog
    ham_u64_t recover_changeset();

    // Recovers the logical journal
    void recover_journal(ham_u64_t start_lsn);

    // Switches the log file if necessary; sets the new log descriptor in the
    // transaction
    void switch_files_maybe(LocalTransaction *txn);

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
                const void *ptr1 = 0, ham_u32_t ptr1_size = 0,
                const void *ptr2 = 0, ham_u32_t ptr2_size = 0,
                const void *ptr3 = 0, ham_u32_t ptr3_size = 0,
                const void *ptr4 = 0, ham_u32_t ptr4_size = 0,
                const void *ptr5 = 0, ham_u32_t ptr5_size = 0) {
      if (ptr1_size)
        m_buffer[idx].append(ptr1, ptr1_size);
      if (ptr2_size)
        m_buffer[idx].append(ptr2, ptr2_size);
      if (ptr3_size)
        m_buffer[idx].append(ptr3, ptr3_size);
      if (ptr4_size)
        m_buffer[idx].append(ptr4, ptr4_size);
      if (ptr5_size)
        m_buffer[idx].append(ptr5, ptr5_size);
    }

    // flush buffer if size limit is exceeded
    void maybe_flush_buffer(int idx) {
      if (m_buffer[idx].get_size() >= kBufferLimit)
        flush_buffer(idx);
    }

    // Flushes a buffer to disk
    void flush_buffer(int idx, bool fsync = false) {
      if (m_buffer[idx].get_size() > 0) {
        m_files[idx].write(m_buffer[idx].get_ptr(), m_buffer[idx].get_size());
        m_count_bytes_flushed += m_buffer[idx].get_size();

        m_buffer[idx].clear();
        if (fsync)
          m_files[idx].flush();
      }
    }

    // Clears a single file
    void clear_file(int idx);

    // References the Environment this journal file is for
    LocalEnvironment *m_env;

    // The index of the file descriptor we are currently writing to (0 or 1)
    ham_u32_t m_current_fd;

    // The two file descriptors
    File m_files[2];

    // Buffers for writing data to the files
    ByteArray m_buffer[2];

    // For counting all open transactions in the files
    ham_u32_t m_open_txn[2];

    // For counting all closed transactions in the files
    ham_u32_t m_closed_txn[2];

    // The last used lsn
    ham_u64_t m_lsn;

    // The lsn of the previous checkpoint
    ham_u64_t m_last_cp_lsn;

    // When having more than these Transactions in one file, we
    // swap the files
    ham_u32_t m_threshold;

    // Set to false to disable logging; used during recovery
    bool m_disable_logging;

    // Counting the flushed bytes (for ham_env_get_metrics)
    ham_u64_t m_count_bytes_flushed;
};

#include "packstop.h"

} // namespace hamsterdb

#endif /* HAM_JOURNAL_H__ */
