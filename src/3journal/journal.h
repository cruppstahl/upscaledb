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

/*
 * Routines for the journal - writing, reading, recovering
 *
 * The journal is a facility for storing logical and physical redo-information.
 *
 * The logical information describes the database operation (i.e. insert/erase),
 * the physical information describes the modified pages.
 *
 * "Undo" information is not required because aborted Txns are never
 * written to disk. The journal only can "redo" operations.
 *
 * The journal is organized in two files. If one of the files grows too large
 * then all new Txns are stored in the other file
 * ("Log file switching"). When all Txns from file #0 are committed,
 * and file #1 exceeds a limit, then the files are switched back again.
 *
 * For writing, files are buffered. The buffers are flushed when they
 * exceed a certain threshold, when a Txn is committed or a Changeset
 * was written. In case of a commit or a changeset there will also be an
 * fsync, if UPS_ENABLE_FSYNC is enabled.
 *
 * The physical information is a collection of pages which are modified in
 * one or more database operations (i.e. ups_db_erase). This collection is
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
 * Afterwards, upscaledb uses the lsn's to figure out whether an update
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

#ifndef UPS_JOURNAL_H
#define UPS_JOURNAL_H

#include "0root/root.h"

#include <vector>
#include <cstdio>
#include <string>

#include "ups/upscaledb_int.h" // for metrics

#include "1base/dynamic_array.h"
#include "1base/scoped_ptr.h"
#include "1os/file.h"
#include "1errorinducer/errorinducer.h"
#include "2page/page_collection.h"
#include "2compressor/compressor.h"
#include "3journal/journal_entries.h"
#include "3journal/journal_state.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
class Page;
struct Db;
struct Txn;
struct LocalEnv;
struct LocalTxn;
struct LocalTxnManager;

#include "1base/packstart.h"

//
// The Journal object
//
struct Journal
{
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
  Journal(LocalEnv *env);

  // Creates a new journal
  void create();

  // Opens an existing journal
  void open();

  // Returns true if the journal is empty
  bool is_empty() const {
    if (!state.files[0].is_open() && !state.files[1].is_open())
      return true;

    for (int i = 0; i < 2; i++) {
      uint64_t size = state.files[i].file_size();
      if (size > 0)
        return false;
    }

    return true;
  }

  // Appends a journal entry for ups_txn_begin/kEntryTypeTxnBegin
  void append_txn_begin(LocalTxn *txn, const char *name,
                  uint64_t lsn);

  // Appends a journal entry for ups_txn_commit/kEntryTypeTxnCommit
  void append_txn_commit(LocalTxn *txn, uint64_t lsn);

  // Appends a journal entry for ups_insert/kEntryTypeInsert
  void append_insert(Db *db, LocalTxn *txn,
                  ups_key_t *key, ups_record_t *record, uint32_t flags,
                  uint64_t lsn);

  // Appends a journal entry for ups_erase/kEntryTypeErase
  void append_erase(Db *db, LocalTxn *txn,
                  ups_key_t *key, int duplicate_index, uint32_t flags,
                  uint64_t lsn);

  // Appends a journal entry for a whole changeset/kEntryTypeChangeset
  // Returns the current file descriptor, which is the parameter for
  // on_changeset_flush()
  int append_changeset(std::vector<Page *> &pages, uint64_t last_blob_page,
                  uint64_t lsn);

  // Empties the journal, removes all entries
  void clear();

  // Closes the journal, frees all allocated resources
  void close(bool noclear = false);

  // Performs the recovery! All committed Txns will be re-applied,
  // all others are automatically aborted
  void recover(LocalTxnManager *txn_manager);

  // Fills the metrics
  void fill_metrics(ups_env_metrics_t *metrics) {
    metrics->journal_bytes_flushed = state.count_bytes_flushed;
    metrics->journal_bytes_before_compression
            = state.count_bytes_before_compression;
    metrics->journal_bytes_after_compression
            = state.count_bytes_after_compression;
  }

  // Flushes all buffers to disk. Used for testing.
  void test_flush_buffers();

  // Reads an entry from the journal. Used for testing.
  void test_read_entry(Journal::Iterator *iter, PJournalEntry *entry,
                ByteArray *auxbuffer);

  JournalState state;
};

#include "1base/packstop.h"

} // namespace upscaledb

#endif /* UPS_JOURNAL_H */
