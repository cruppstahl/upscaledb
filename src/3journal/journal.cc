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

#include "0root/root.h"

#include <string.h>
#ifndef WIN32
#  include <libgen.h>
#endif

#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1os/os.h"
#include "2device/device.h"
#include "2compressor/compressor_factory.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"
#include "4context/context.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

enum {
  // switch log file after |kSwitchTxnThreshold| transactions
  kSwitchTxnThreshold = 32,

  // flush buffers if this limit is exceeded
  kBufferLimit = 1024 * 1024, // 1 mb
};

static inline void
clear_file(JournalState &state, int idx)
{
  if (state.files[idx].is_open()) {
    state.files[idx].truncate(0);

    // after truncate, the file pointer is far beyond the new end of file;
    // reset the file pointer, or the next write will resize the file to
    // the original size
    state.files[idx].seek(0, File::kSeekSet);
  }
}

static inline std::string
log_file_path(JournalState &state, int i)
{
  std::string path;

  if (state.env->config.log_filename.empty()) {
    path = state.env->config.filename;
  }
  else {
    path = state.env->config.log_filename;
#ifdef UPS_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(state.env->config.filename.c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)state.env->config.filename.c_str());
#endif
  }
  if (i == 0)
    path += ".jrn0";
  else if (i == 1)
    path += ".jrn1";
  else
    assert(!"invalid index");
  return (path);
}

static inline void
flush_buffer(JournalState &state, int idx, bool fsync = false)
{
  if (likely(state.buffer.size() > 0)) {
    state.files[idx].write(state.buffer.data(), state.buffer.size());
    state.count_bytes_flushed += state.buffer.size();

    state.buffer.clear();
    if (unlikely(fsync))
      state.files[idx].flush();
  }
}

// Sequentially returns the next journal entry, starting with
// the oldest entry.
//
// |iter| must be initialized with zeroes for the first call.
// |auxbuffer| returns the auxiliary data of the entry and is either
// a structure of type PJournalEntryInsert or PJournalEntryErase.
//
// Returns an empty entry (lsn is zero) after the last element.
static inline void
read_entry(JournalState &state, Journal::Iterator *iter, PJournalEntry *entry,
                ByteArray *auxbuffer)
{
  auxbuffer->clear();

  // if iter->offset is 0, then the iterator was created from scratch
  // and we start reading from the first (oldest) entry.
  //
  // The oldest of the two logfiles is always the "other" one (the one
  // NOT in current_fd).
  if (iter->offset == 0) {
    iter->fdstart = iter->fdidx =
                        state.current_fd == 0
                            ? 1
                            : 0;
  }

  // get the size of the journal file
  uint64_t filesize = state.files[iter->fdidx].file_size();

  // reached EOF? then either skip to the next file or we're done
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = 0;
      filesize = state.files[iter->fdidx].file_size();
    }
    else {
      entry->lsn = 0;
      return;
    }
  }

  // second file is also empty? then return
  if (filesize == iter->offset) {
    entry->lsn = 0;
    return;
  }

  // now try to read the next entry
  try {
    state.files[iter->fdidx].pread(iter->offset, entry, sizeof(*entry));

    iter->offset += sizeof(*entry);

    // read auxiliary data if it's available
    if (entry->followup_size) {
      auxbuffer->resize((uint32_t)entry->followup_size);

      state.files[iter->fdidx].pread(iter->offset, auxbuffer->data(),
                      (size_t)entry->followup_size);
      iter->offset += entry->followup_size;
    }
  }
  catch (Exception &) {
    ups_trace(("failed to read journal entry, aborting recovery"));
    entry->lsn = 0; // this triggers the end of recovery
  }
}


// Appends an entry to the journal
static inline void
append_entry(JournalState &state, int idx,
            const uint8_t *ptr1 = 0, size_t ptr1_size = 0,
            const uint8_t *ptr2 = 0, size_t ptr2_size = 0,
            const uint8_t *ptr3 = 0, size_t ptr3_size = 0,
            const uint8_t *ptr4 = 0, size_t ptr4_size = 0,
            const uint8_t *ptr5 = 0, size_t ptr5_size = 0)
{
  if (ptr1_size)
    state.buffer.append(ptr1, ptr1_size);
  if (ptr2_size)
    state.buffer.append(ptr2, ptr2_size);
  if (ptr3_size)
    state.buffer.append(ptr3, ptr3_size);
  if (ptr4_size)
    state.buffer.append(ptr4, ptr4_size);
  if (ptr5_size)
    state.buffer.append(ptr5, ptr5_size);
}

// Switches the log file if necessary; returns the new log descriptor in the
// transaction
static inline int
switch_files_maybe(JournalState &state)
{
  int other = state.current_fd ? 0 : 1;

  // determine the journal file which is used for this transaction 
  // if the "current" file is not yet full, continue to write to this file
  //
  // otherwise delete the other file and use the other file as the current file
  if (unlikely(state.num_transactions > state.threshold)) {
    clear_file(state, other);
    state.current_fd = other;
    state.num_transactions = 0;
  }

  return state.current_fd;
}

// Returns a pointer to database. If the database was not yet opened then
// it is opened implicitly.
static inline Db *
get_db(JournalState &state, uint16_t dbname)
{
  // first check if the database is already open
  JournalState::DatabaseMap::iterator it = state.database_map.find(dbname);
  if (it != state.database_map.end())
    return it->second;

  // not found - open it
  DbConfig config;
  config.db_name = dbname;
  Db *db = state.env->open_db(config, 0);
  state.database_map[dbname] = db;
  return db;
}

// Returns a pointer to a Txn object.
static inline Txn *
get_txn(JournalState &state, LocalTxnManager *txn_manager,
                uint64_t txn_id)
{
  Txn *txn = txn_manager->oldest_txn();
  while (txn) {
    if (txn->id == txn_id)
      return txn;
    txn = txn->next();
  }

  return 0;
}

// Closes all databases.
static inline void
close_all_databases(JournalState &state)
{
  JournalState::DatabaseMap::iterator it = state.database_map.begin();
  while (it != state.database_map.end()) {
    JournalState::DatabaseMap::iterator it2 = it;
    it++;

    ups_status_t st = ups_db_close((ups_db_t *)it2->second, UPS_DONT_LOCK);
    if (st) {
      ups_log(("ups_db_close() failed w/ error %d (%s)", st, ups_strerror(st)));
      throw Exception(st);
    }
  }
  state.database_map.clear();
}

// Aborts all transactions which are still active.
static inline void
abort_uncommitted_txns(JournalState &state, LocalTxnManager *txn_manager)
{
  Txn *txn = txn_manager->oldest_txn();

  while (txn) {
    if (!txn->is_committed())
      txn->abort();
    txn = txn->next();
  }
}

// Helper function which adds a single page from the changeset to
// the Journal; returns the page size (or compressed size, if compression
// was enabled)
static inline uint32_t
append_changeset_page(JournalState &state, Page *page, uint32_t page_size)
{
  PJournalEntryPageHeader header(page->address());

  if (state.compressor.get()) {
    state.count_bytes_before_compression += page_size;
    header.compressed_size = state.compressor->compress((uint8_t *)page->data(),
                    page_size);
    append_entry(state, state.current_fd, (uint8_t *)&header, sizeof(header),
                    state.compressor->arena.data(),
                    header.compressed_size);
    state.count_bytes_after_compression += header.compressed_size;
    return header.compressed_size + sizeof(header);
  }

  append_entry(state, state.current_fd, (uint8_t *)&header, sizeof(header),
                (uint8_t *)page->data(), page_size);
  return page_size + sizeof(header);
}

// Scans a file for the oldest changeset. Returns the lsn of this
// changeset.
static inline uint64_t
scan_for_oldest_changeset(JournalState &state, File *file)
{
  Journal::Iterator it;
  PJournalEntry entry;
  ByteArray buffer;

  // get the next entry
  try {
    uint64_t filesize = file->file_size();

    while (it.offset < filesize) {
      file->pread(it.offset, &entry, sizeof(entry));

      if (entry.lsn == 0)
        break;

      if (entry.type == Journal::kEntryTypeChangeset) {
        return entry.lsn;
      }

      // increment the offset
      it.offset += sizeof(entry) + entry.followup_size;
    }
  }
  catch (Exception &ex) {
    ups_log(("exception (error %d) while reading journal", ex.code));
  }

  return 0;
}

// Redo all Changesets of a log file, in chronological order
// Returns the highest lsn of the last changeset applied
static inline uint64_t
redo_all_changesets(JournalState &state, int fdidx)
{
  Journal::Iterator it;
  PJournalEntry entry;
  ByteArray buffer;
  uint64_t max_lsn = 0;

  // for each entry...
  try {
    uint64_t log_file_size = state.files[fdidx].file_size();

    while (it.offset < log_file_size) {
      state.files[fdidx].pread(it.offset, &entry, sizeof(entry));

      // Skip all log entries which are NOT from a changeset
      if (entry.type != Journal::kEntryTypeChangeset) {
        it.offset += sizeof(entry) + entry.followup_size;
        continue;
      }

      max_lsn = entry.lsn;

      it.offset += sizeof(entry);

      // Read the Changeset header
      PJournalEntryChangeset changeset;
      state.files[fdidx].pread(it.offset, &changeset, sizeof(changeset));
      it.offset += sizeof(changeset);

      uint32_t page_size = state.env->config.page_size_bytes;
      ByteArray arena(page_size);
      ByteArray tmp;

      uint64_t file_size = state.env->device->file_size();

      state.env->page_manager->set_last_blob_page_id(changeset.last_blob_page);

      // for each page in this changeset...
      for (uint32_t i = 0; i < changeset.num_pages; i++) {
        PJournalEntryPageHeader page_header;
        state.files[fdidx].pread(it.offset, &page_header,
                        sizeof(page_header));
        it.offset += sizeof(page_header);
        if (page_header.compressed_size > 0) {
          tmp.resize(page_size);
          state.files[fdidx].pread(it.offset, tmp.data(),
                        page_header.compressed_size);
          it.offset += page_header.compressed_size;
          state.compressor->decompress(tmp.data(),
                        page_header.compressed_size, page_size, &arena);
        }
        else {
          state.files[fdidx].pread(it.offset, arena.data(), page_size);
          it.offset += page_size;
        }

        Page *page;

        // now write the page to disk
        if (page_header.address == file_size) {
          file_size += page_size;

          page = new Page(state.env->device.get());
          page->alloc(0);
        }
        else if (page_header.address > file_size) {
          file_size = (size_t)page_header.address + page_size;
          state.env->device->truncate(file_size);

          page = new Page(state.env->device.get());
          page->fetch(page_header.address);
        }
        else {
          if (page_header.address == 0)
            page = state.env->header->header_page;
          else
            page = new Page(state.env->device.get());
          page->fetch(page_header.address);
        }
        assert(page->address() == page_header.address);

        // overwrite the page data
        ::memcpy(page->data(), arena.data(), page_size);

        // flush the modified page to disk
        page->set_dirty(true);
        page->flush();

        if (page_header.address != 0)
          delete page;
      }
    }
  }
  catch (Exception &) {
    ups_trace(("Exception when applying changeset"));
    // propagate error
    throw;
  }

  return max_lsn;
}

// Recovers (re-applies) the physical changelog; returns the lsn of the
// Changelog
static inline uint64_t
recover_changeset(JournalState &state)
{
  // scan through both files, look for the file with the oldest changeset.
  uint64_t lsn1 = scan_for_oldest_changeset(state, &state.files[0]);
  uint64_t lsn2 = scan_for_oldest_changeset(state, &state.files[1]);

  // both files are empty or do not contain a changeset?
  if (lsn1 == 0 && lsn2 == 0)
    return 0;

  // now redo all changesets chronologically
  state.current_fd = lsn1 < lsn2 ? 0 : 1;

  uint64_t max_lsn1 = redo_all_changesets(state, state.current_fd);
  uint64_t max_lsn2 = redo_all_changesets(state, state.current_fd == 0 ? 1 : 0);

  // return the lsn of the newest changeset
  return std::max(max_lsn1, max_lsn2);
}

// Recovers the logical journal
static inline void
recover_journal(JournalState &state, Context *context,
                LocalTxnManager *txn_manager, uint64_t start_lsn)
{
  ups_status_t st = 0;
  Journal::Iterator it;
  ByteArray buffer;

  /* recovering the journal is rather simple - we iterate over the
   * files and re-apply EVERY operation (incl. txn_begin and txn_abort),
   * that was not yet flushed with a Changeset.
   *
   * Basically we iterate over both log files and skip everything with
   * a sequence number (lsn) smaller the one of the last Changeset.
   *
   * When done then auto-abort all transactions that were not yet
   * committed.
   */

  // make sure that there are no pending transactions - start with
  // a clean state!
  assert(txn_manager->oldest_txn() == 0);
  assert(ISSET(state.env->flags(), UPS_ENABLE_TRANSACTIONS));

  // do not append to the journal during recovery
  state.disable_logging = true;

  do {
    PJournalEntry entry;

    // get the next entry
    read_entry(state, &it, &entry, &buffer);

    // reached end of logfile?
    if (!entry.lsn)
      break;

    // re-apply this operation
    switch (entry.type) {
      case Journal::kEntryTypeTxnBegin: {
        Txn *txn = 0;
        st = ups_txn_begin((ups_txn_t **)&txn, (ups_env_t *)state.env, 
                (const char *)buffer.data(), 0, UPS_DONT_LOCK);
        // on success: patch the txn ID
        if (st == 0) {
          txn->id = entry.txn_id;
          txn_manager->set_txn_id(entry.txn_id);
        }
        break;
      }
      case Journal::kEntryTypeTxnAbort: {
        Txn *txn = get_txn(state, txn_manager, entry.txn_id);
        st = ups_txn_abort((ups_txn_t *)txn, UPS_DONT_LOCK);
        break;
      }
      case Journal::kEntryTypeTxnCommit: {
        Txn *txn = get_txn(state, txn_manager, entry.txn_id);
        st = ups_txn_commit((ups_txn_t *)txn, UPS_DONT_LOCK);
        break;
      }
      case Journal::kEntryTypeInsert: {
        PJournalEntryInsert *ins = (PJournalEntryInsert *)buffer.data();
        Txn *txn = 0;
        Db *db;
        ups_key_t key = {0};
        ups_record_t record = {0};
        if (!ins) {
          st = UPS_IO_ERROR;
          goto bail;
        }

        // do not insert if the key was already flushed to disk
        if (entry.lsn <= start_lsn)
          continue;

        uint8_t *payload = ins->key_data();

        // extract the key - it can be compressed or uncompressed
        ByteArray keyarena;
        if (ins->compressed_key_size != 0) {
          state.compressor->decompress(payload, ins->compressed_key_size,
                          ins->key_size);
          keyarena.append(state.compressor->arena.data(), ins->key_size);
          key.data = keyarena.data();
          payload += ins->compressed_key_size;
        }
        else {
          key.data = payload;
          payload += ins->key_size;
        }
        key.size = ins->key_size;
        // extract the record - it can be compressed or uncompressed
        ByteArray recarena;
        if (ins->compressed_record_size != 0) {
          state.compressor->decompress(payload, ins->compressed_record_size,
                          ins->record_size);
          recarena.append(state.compressor->arena.data(), ins->record_size);
          record.data = recarena.data();
          payload += ins->compressed_record_size;
        }
        else {
          record.data = payload;
          payload += ins->record_size;
        }
        record.size = ins->record_size;
        if (entry.txn_id)
          txn = get_txn(state, txn_manager, entry.txn_id);
        db = get_db(state, entry.dbname);

        // always use a cursor; otherwise flags like UPS_DUPLICATE_INSERT_FIRST
        // will cause errors
        ups_cursor_t *cursor;
        st = ups_cursor_create(&cursor, (ups_db_t *)db, (ups_txn_t *)txn, 0);
        if (unlikely(st))
          break;
        st = ups_cursor_insert(cursor, &key, &record,
                        ins->insert_flags | UPS_DONT_LOCK);
        ups_cursor_close(cursor);
        if (st == UPS_DUPLICATE_KEY) // ok if key already exists
          st = 0;
        break;
      }
      case Journal::kEntryTypeErase: {
        PJournalEntryErase *e = (PJournalEntryErase *)buffer.data();
        Txn *txn = 0;
        Db *db;
        ups_key_t key = {0};
        if (!e) {
          st = UPS_IO_ERROR;
          goto bail;
        }

        // do not erase if the key was already erased from disk
        if (entry.lsn <= start_lsn)
          continue;

        if (entry.txn_id)
          txn = get_txn(state, txn_manager, entry.txn_id);
        db = get_db(state, entry.dbname);
        key.data = e->key_data();
        if (e->compressed_key_size != 0) {
          state.compressor->decompress(e->key_data(), e->compressed_key_size,
                          e->key_size);
          key.data = state.compressor->arena.data();
        }
        else
          key.data = e->key_data();
        key.size = e->key_size;
        st = ups_db_erase((ups_db_t *)db, (ups_txn_t *)txn, &key,
                        e->erase_flags | UPS_DONT_LOCK);
        // key might have already been erased when the changeset
        // was flushed
        if (st == UPS_KEY_NOT_FOUND)
          st = 0;
        break;
      }
      case Journal::kEntryTypeChangeset: {
        // skip this; the changeset was already applied
        break;
      }
      default:
        ups_log(("invalid journal entry type or journal is corrupt"));
        st = UPS_IO_ERROR;
      }

      if (st)
        goto bail;
  } while (1);

bail:
  // all transactions which are not yet committed will be aborted
  abort_uncommitted_txns(state, txn_manager);

  // also close and delete all open databases - they were created in get_db()
  close_all_databases(state);

  // flush all committed transactions
  if (st == 0)
    st = state.env->flush(UPS_FLUSH_COMMITTED_TRANSACTIONS);

  // re-enable the logging
  state.disable_logging = false;

  if (st)
    throw Exception(st);
}


JournalState::JournalState(LocalEnv *env_)
  : env(env_), current_fd(0), num_transactions(0),
    threshold(env_->config.journal_switch_threshold),
    disable_logging(false), count_bytes_flushed(0),
    count_bytes_before_compression(0), count_bytes_after_compression(0)
{
  if (threshold == 0)
    threshold = kSwitchTxnThreshold;
}

Journal::Journal(LocalEnv *env)
  : state(env)
{
  int algo = env->config.journal_compressor;
  if (algo)
    state.compressor.reset(CompressorFactory::create(algo));
}

void
Journal::create()
{
  // create the two files
  for (int i = 0; i < 2; i++) {
    std::string path = log_file_path(state, i);
    state.files[i].create(path.c_str(), 0644);
  }
}

void
Journal::open()
{
  // open the two files
  try {
    std::string path = log_file_path(state, 0);
    state.files[0].open(path.c_str(), false);
    path = log_file_path(state, 1);
    state.files[1].open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    state.files[1].close();
    state.files[0].close();
    throw ex;
  }
}

void
Journal::append_txn_begin(LocalTxn *txn, const char *name, uint64_t lsn)
{
  if (unlikely(state.disable_logging))
    return;

  assert(NOTSET(txn->flags, UPS_TXN_TEMPORARY));

  PJournalEntry entry;
  entry.txn_id = txn->id;
  entry.type = Journal::kEntryTypeTxnBegin;
  entry.lsn = lsn;
  if (name)
    entry.followup_size = ::strlen(name) + 1;

  int cur = txn->log_descriptor = switch_files_maybe(state);

  if (unlikely(txn->name.size()))
    append_entry(state, cur, (uint8_t *)&entry, (uint32_t)sizeof(entry),
                (uint8_t *)txn->name.c_str(), (uint32_t)txn->name.size() + 1);
  else
    append_entry(state, cur, (uint8_t *)&entry, (uint32_t)sizeof(entry));

  state.num_transactions++;
}

void
Journal::append_txn_commit(LocalTxn *txn, uint64_t lsn)
{
  if (unlikely(state.disable_logging))
    return;

  assert(NOTSET(txn->flags, UPS_TXN_TEMPORARY));

  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->id;
  entry.type = Journal::kEntryTypeTxnCommit;

  append_entry(state, txn->log_descriptor, (uint8_t *)&entry, sizeof(entry));

  // flush after commit
  flush_buffer(state, state.current_fd,
                  ISSET(state.env->flags(), UPS_ENABLE_FSYNC));
}

void
Journal::append_insert(Db *db, LocalTxn *txn,
                ups_key_t *key, ups_record_t *record, uint32_t flags,
                uint64_t lsn)
{
  if (unlikely(state.disable_logging))
    return;

  flags &= ~(UPS_HINT_PREPEND | UPS_HINT_APPEND);

  PJournalEntry entry;

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = Journal::kEntryTypeInsert;
  // the followup_size will be filled in later when we know whether
  // compression is used
  entry.followup_size = sizeof(PJournalEntryInsert) - 1;

  int idx;
  if (ISSET(txn->flags, UPS_TXN_TEMPORARY)) {
    entry.txn_id = 0;
    idx = switch_files_maybe(state);
    state.num_transactions++;
  }
  else {
    entry.txn_id = txn->id;
    idx = txn->log_descriptor;
  }

  PJournalEntryInsert insert;
  insert.key_size = key->size;
  insert.record_size = record->size;
  insert.insert_flags = flags;

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = state.buffer.size();

  // write the header information
  append_entry(state, idx, (uint8_t *)&entry, sizeof(entry),
              (uint8_t *)&insert, sizeof(PJournalEntryInsert) - 1);

  // try to compress the payload; if the compressed result is smaller than
  // the original (uncompressed) payload then use it
  const void *key_data = key->data;
  uint32_t key_size = key->size;
  if (state.compressor.get()) {
    state.count_bytes_before_compression += key_size;
    uint32_t len = state.compressor->compress((uint8_t *)key->data, key->size);
    if (len < key->size) {
      key_size = len;
      key_data = state.compressor->arena.data();
      insert.compressed_key_size = len;
    }
    state.count_bytes_after_compression += key_size;
  }
  append_entry(state, idx, (uint8_t *)key_data, key_size);
  entry.followup_size += key_size;

  // and now the same for the record data
  const void *record_data = record->data;
  uint32_t record_size = record->size;
  if (state.compressor.get()) {
    state.count_bytes_before_compression += record_size;
    uint32_t len = state.compressor->compress((uint8_t *)record->data,
                    record_size);
    if (len < record_size) {
      record_size = len;
      record_data = state.compressor->arena.data();
      insert.compressed_record_size = len;
    }
    state.count_bytes_after_compression += record_size;
  }
  append_entry(state, idx, (uint8_t *)record_data, record_size);
  entry.followup_size += record_size;

  // now overwrite the patched entry
  state.buffer.overwrite(entry_position,
                  (uint8_t *)&entry, sizeof(entry));
  state.buffer.overwrite(entry_position + sizeof(entry),
                  (uint8_t *)&insert, sizeof(PJournalEntryInsert) - 1);

  if (ISSET(txn->flags, UPS_TXN_TEMPORARY))
    flush_buffer(state, state.current_fd,
                    ISSET(state.env->flags(), UPS_ENABLE_FSYNC));
}

void
Journal::append_erase(Db *db, LocalTxn *txn, ups_key_t *key,
                int duplicate_index, uint32_t flags, uint64_t lsn)
{
  if (unlikely(state.disable_logging))
    return;

  PJournalEntry entry;
  PJournalEntryErase erase;
  const void *payload_data = key->data;
  uint32_t payload_size = key->size;

  // try to compress the payload; if the compressed result is smaller than
  // the original (uncompressed) payload then use it
  if (state.compressor.get()) {
    state.count_bytes_before_compression += payload_size;
    uint32_t len = state.compressor->compress((uint8_t *)key->data, key->size);
    if (len < key->size) {
      payload_data = state.compressor->arena.data();
      payload_size = len;
      erase.compressed_key_size = len;
    }
    state.count_bytes_after_compression += payload_size;
  }

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = Journal::kEntryTypeErase;
  entry.followup_size = sizeof(PJournalEntryErase) + payload_size - 1;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = duplicate_index;

  int idx;
  if (ISSET(txn->flags, UPS_TXN_TEMPORARY)) {
    entry.txn_id = 0;
    idx = switch_files_maybe(state);
    state.num_transactions++;
  }
  else {
    entry.txn_id = txn->id;
    idx = txn->log_descriptor;
  }

  // append the entry to the logfile
  append_entry(state, idx, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&erase, sizeof(PJournalEntryErase) - 1,
                (uint8_t *)payload_data, payload_size);

  if (ISSET(txn->flags, UPS_TXN_TEMPORARY))
    flush_buffer(state, state.current_fd,
                    ISSET(state.env->flags(), UPS_ENABLE_FSYNC));
}

int
Journal::append_changeset(std::vector<Page *> &pages,
                uint64_t last_blob_page, uint64_t lsn)
{
  assert(pages.size() > 0);

  if (unlikely(state.disable_logging))
    return -1;

  PJournalEntry entry;
  PJournalEntryChangeset changeset;
  
  entry.lsn = lsn;
  entry.dbname = 0;
  entry.txn_id = 0;
  entry.type = Journal::kEntryTypeChangeset;
  // followup_size is incomplete - the actual page sizes are added later
  entry.followup_size = sizeof(PJournalEntryChangeset);
  changeset.num_pages = pages.size();
  changeset.last_blob_page = last_blob_page;

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = state.buffer.size();

  // write the data to the file
  append_entry(state, state.current_fd, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&changeset, sizeof(PJournalEntryChangeset));

  size_t page_size = state.env->config.page_size_bytes;
  for (std::vector<Page *>::iterator it = pages.begin();
                  it != pages.end();
                  ++it) {
    entry.followup_size += append_changeset_page(state, *it, page_size);
  }

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and patch in the followup-size
  state.buffer.overwrite(entry_position, (uint8_t *)&entry, sizeof(entry));

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and flush the file
  flush_buffer(state, state.current_fd,
                  ISSET(state.env->flags(), UPS_ENABLE_FSYNC));

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  return state.current_fd;
}

void
Journal::close(bool noclear)
{
  // the noclear flag is set during testing, for checking whether the files
  // contain the correct data. Flush the buffers, otherwise the tests will
  // fail because data is missing
  if (unlikely(noclear))
    flush_buffer(state, 0);

  if (likely(!noclear))
    clear();

  for (int i = 0; i < 2; i++)
    state.files[i].close();

  state.buffer.clear();
}

void
Journal::recover(LocalTxnManager *txn_manager)
{
  Context context(state.env, 0, 0);

  // first redo the changesets
  uint64_t start_lsn = recover_changeset(state);

  // load the state of the PageManager; the PageManager state is loaded AFTER
  // physical recovery because its page might have been restored in
  // recover_changeset()
  uint64_t page_manager_blobid = state.env->header->page_manager_blobid();
  if (page_manager_blobid != 0)
    state.env->page_manager->initialize(page_manager_blobid);

  // then start the normal recovery
  if (ISSET(state.env->flags(), UPS_ENABLE_TRANSACTIONS))
    recover_journal(state, &context, txn_manager, start_lsn);

  // clear the journal files
  clear();
}

void
Journal::clear()
{
  for (int i = 0; i < 2; i++)
    clear_file(state, i);
}

void
Journal::test_flush_buffers()
{
  flush_buffer(state, 0);
  flush_buffer(state, 1);
}

void
Journal::test_read_entry(Journal::Iterator *iter, PJournalEntry *entry,
                ByteArray *auxbuffer)
{
  return upscaledb::read_entry(state, iter, entry, auxbuffer);
}

} // namespace upscaledb
