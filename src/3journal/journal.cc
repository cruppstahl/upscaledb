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

#include "0root/root.h"

#include <string.h>
#ifndef WIN32
#  include <libgen.h>
#endif

#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1eventlog/eventlog.h"
#include "1os/os.h"
#include "2device/device.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"
#include "4context/context.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

Journal::Journal(LocalEnvironment *env)
  : m_state(env)
{
}

void
Journal::create()
{
  // create the two files
  for (int i = 0; i < 2; i++) {
    std::string path = get_path(i);
    m_state.files[i].create(path.c_str(), 0644);
  }
}

void
Journal::open()
{
  // open the two files
  try {
    std::string path = get_path(0);
    m_state.files[0].open(path.c_str(), false);
    path = get_path(1);
    m_state.files[1].open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    m_state.files[1].close();
    m_state.files[0].close();
    throw ex;
  }
}

int
Journal::switch_files_maybe()
{
  int other = m_state.current_fd ? 0 : 1;

  // determine the journal file which is used for this transaction 
  // if the "current" file is not yet full, continue to write to this file
  if (m_state.open_txn[m_state.current_fd]
                  + m_state.closed_txn[m_state.current_fd]
              < m_state.threshold)
    return (m_state.current_fd);

  // If the other file does no longer have open Transactions then
  // delete the other file and use the other file as the current file
  if (m_state.open_txn[other] == 0) {
    clear_file(other);
    m_state.current_fd = other;
    // fall through
  }

  // Otherwise just continue using the current file
  return (m_state.current_fd);
}

void
Journal::append_txn_begin(LocalTransaction *txn, const char *name, uint64_t lsn)
{
  if (m_state.disable_logging)
    return;

  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);

  PJournalEntry entry;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnBegin;
  entry.lsn = lsn;
  if (name)
    entry.followup_size = strlen(name) + 1;

  txn->set_log_desc(switch_files_maybe());

  int cur = txn->get_log_desc();

  if (txn->get_name().size())
    append_entry(cur, (uint8_t *)&entry, (uint32_t)sizeof(entry),
                (uint8_t *)txn->get_name().c_str(),
                (uint32_t)txn->get_name().size() + 1);
  else
    append_entry(cur, (uint8_t *)&entry, (uint32_t)sizeof(entry));
  maybe_flush_buffer(cur);

  m_state.open_txn[cur]++;

  // store the fp-index in the journal structure; it's needed for
  // journal_append_checkpoint() to quickly find out which file is
  // the newest
  m_state.current_fd = cur;

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.txn_begin", "%u, %u, %u", (uint32_t)txn->get_id(),
              (uint32_t)lsn, (uint32_t)cur));
}

void
Journal::append_txn_abort(LocalTransaction *txn, uint64_t lsn)
{
  if (m_state.disable_logging)
    return;

  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);

  int idx;
  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnAbort;

  // update the transaction counters of this logfile
  idx = txn->get_log_desc();
  m_state.open_txn[idx]--;
  m_state.closed_txn[idx]++;

  append_entry(idx, (uint8_t *)&entry, sizeof(entry));
  maybe_flush_buffer(idx);
  // no need for fsync - incomplete transactions will be aborted anyway

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.txn_abort", "%u, %u", (uint32_t)txn->get_id(),
              (uint32_t)lsn));
}

void
Journal::append_txn_commit(LocalTransaction *txn, uint64_t lsn)
{
  if (m_state.disable_logging)
    return;

  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);

  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnCommit;

  // do not yet update the transaction counters of this logfile; just
  // because the txn was committed does not mean that it will be flushed
  // immediately. The counters will be modified in transaction_flushed().
  int idx = txn->get_log_desc();

  append_entry(idx, (uint8_t *)&entry, sizeof(entry));

  // and flush the file
  flush_buffer(idx, m_state.env->get_flags() & HAM_ENABLE_FSYNC);

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.txn_commit", "%u, %u", (uint32_t)txn->get_id(),
              (uint32_t)lsn));
}

void
Journal::append_insert(Database *db, LocalTransaction *txn,
                ham_key_t *key, ham_record_t *record, uint32_t flags,
                uint64_t lsn)
{
  if (m_state.disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryInsert insert;
  uint32_t size = sizeof(PJournalEntryInsert)
                        + key->size
                        + (flags & HAM_PARTIAL
                            ? record->partial_size
                            : record->size)
                        - 1;

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = kEntryTypeInsert;
  entry.followup_size = size;

  int idx;
  if (txn->get_flags() & HAM_TXN_TEMPORARY) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    m_state.closed_txn[idx]++;
  }
  else {
    entry.txn_id = txn->get_id();
    idx = txn->get_log_desc();
  }

  insert.key_size = key->size;
  insert.record_size = record->size;
  insert.record_partial_size = record->partial_size;
  insert.record_partial_offset = record->partial_offset;
  insert.insert_flags = flags;

  // append the entry to the logfile
  append_entry(idx, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&insert, sizeof(PJournalEntryInsert) - 1,
                (uint8_t *)key->data, key->size,
                (uint8_t *)record->data, (flags & HAM_PARTIAL
                                ? record->partial_size
                                : record->size));

  // now flush the file
  if (txn->get_flags() & HAM_TXN_TEMPORARY)
    flush_buffer(idx, m_state.env->get_flags() & HAM_ENABLE_FSYNC);
  else
    maybe_flush_buffer(idx);

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.insert", "%u, %u, %s, %u, 0x%x, %u",
              (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0,
              key ? EventLog::escape(key->data, key->size) : "",
              (uint32_t)record->size,
              (uint32_t)flags,
              (uint32_t)lsn));
}

void
Journal::append_erase(Database *db, LocalTransaction *txn, ham_key_t *key,
                int duplicate_index, uint32_t flags, uint64_t lsn)
{
  if (m_state.disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryErase erase;
  uint32_t size = sizeof(PJournalEntryErase) + key->size - 1;

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = kEntryTypeErase;
  entry.followup_size = size;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = duplicate_index;

  int idx;
  if (txn->get_flags() & HAM_TXN_TEMPORARY) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    m_state.closed_txn[idx]++;
  }
  else {
    entry.txn_id = txn->get_id();
    idx = txn->get_log_desc();
  }

  // append the entry to the logfile
  append_entry(idx, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&erase, sizeof(PJournalEntryErase) - 1,
                (uint8_t *)key->data, key->size);

  // now flush the file
  if (txn->get_flags() & HAM_TXN_TEMPORARY)
    flush_buffer(idx, m_state.env->get_flags() & HAM_ENABLE_FSYNC);
  else
    maybe_flush_buffer(idx);

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.erase", "%u, %u, %s, 0x%x, %u",
              (uint32_t)db->name(),
              txn ? (uint32_t)txn->get_id() : 0,
              key ? EventLog::escape(key->data, key->size) : "",
              (uint32_t)flags,
              (uint32_t)lsn));
}

int
Journal::append_changeset(std::vector<Page::PersistedData *> &pages,
                uint64_t lsn)
{
  ham_assert(pages.size() > 0);

  if (m_state.disable_logging)
    return (-1);

  (void)switch_files_maybe();

  PJournalEntry entry;
  PJournalEntryChangeset changeset;
  
  entry.lsn = lsn;
  entry.dbname = 0;
  entry.txn_id = 0;
  entry.type = kEntryTypeChangeset;
  // followup_size is incomplete - the actual page sizes are added later
  entry.followup_size = sizeof(PJournalEntryChangeset);
  changeset.num_pages = pages.size();

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = m_state.buffer[m_state.current_fd].get_size();

  // write the data to the file
  append_entry(m_state.current_fd, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&changeset, sizeof(PJournalEntryChangeset));

  size_t page_size = m_state.env->config().page_size_bytes;
  for (std::vector<Page::PersistedData *>::iterator it = pages.begin();
                  it != pages.end();
                  ++it) {
    entry.followup_size += append_changeset_page(*it, page_size);
  }

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and patch in the followup-size
  m_state.buffer[m_state.current_fd].overwrite(entry_position,
          (uint8_t *)&entry, sizeof(entry));

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and flush the file
  flush_buffer(m_state.current_fd, m_state.env->get_flags() & HAM_ENABLE_FSYNC);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // if recovery is enabled (w/o transactions) then simulate a "commit" to
  // make sure that the log files are switched properly. Here, the
  // counter for "opened transactions" is incremented. It will be decremented
  // by the worker thread as soon as the dirty pages are flushed to disk.
  m_state.open_txn[m_state.current_fd]++;

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.changeset", "%u, %u", pages.size(), (uint32_t)lsn));

  return (m_state.current_fd);
}

uint32_t
Journal::append_changeset_page(const Page::PersistedData *page_data,
                uint32_t page_size)
{
  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.changeset_page", "%u", (uint32_t)page_data->address));
  PJournalEntryPageHeader header(page_data->address);

  append_entry(m_state.current_fd, (uint8_t *)&header, sizeof(header),
                page_data->raw_data->payload, page_size);
  return (page_size + sizeof(header));
}

void
Journal::changeset_flushed(int fd_index)
{
  m_state.closed_txn[fd_index]++;
}

void
Journal::transaction_flushed(LocalTransaction *txn)
{
  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);
  if (m_state.disable_logging) // ignore this call during recovery
    return;

  int idx = txn->get_log_desc();
  ham_assert(m_state.open_txn[idx] > 0);
  m_state.open_txn[idx]--;
  m_state.closed_txn[idx]++;
}

void
Journal::get_entry(Iterator *iter, PJournalEntry *entry, ByteArray *auxbuffer)
{
  uint64_t filesize;

  auxbuffer->clear();

  // if iter->offset is 0, then the iterator was created from scratch
  // and we start reading from the first (oldest) entry.
  //
  // The oldest of the two logfiles is always the "other" one (the one
  // NOT in current_fd).
  if (iter->offset == 0) {
    iter->fdstart = iter->fdidx =
                        m_state.current_fd == 0
                            ? 1
                            : 0;
  }

  // get the size of the journal file
  filesize = m_state.files[iter->fdidx].get_file_size();

  // reached EOF? then either skip to the next file or we're done
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = 0;
      filesize = m_state.files[iter->fdidx].get_file_size();
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
    m_state.files[iter->fdidx].pread(iter->offset, entry, sizeof(*entry));

    iter->offset += sizeof(*entry);

    // read auxiliary data if it's available
    if (entry->followup_size) {
      auxbuffer->resize((uint32_t)entry->followup_size);

      m_state.files[iter->fdidx].pread(iter->offset, auxbuffer->get_ptr(),
                      (size_t)entry->followup_size);
      iter->offset += entry->followup_size;
    }
  }
  catch (Exception &) {
    ham_trace(("failed to read journal entry, aborting recovery"));
    entry->lsn = 0; // this triggers the end of recovery
  }
}

void
Journal::close(bool noclear)
{
  int i;

  // the noclear flag is set during testing, for checking whether the files
  // contain the correct data. Flush the buffers, otherwise the tests will
  // fail because data is missing
  if (noclear) {
    flush_buffer(0);
    flush_buffer(1);
  }

  if (!noclear)
    clear();

  for (i = 0; i < 2; i++) {
    m_state.files[i].close();
    m_state.buffer[i].clear();
  }
}

Database *
Journal::get_db(uint16_t dbname)
{
  // first check if the Database is already open
  JournalState::DatabaseMap::iterator it = m_state.database_map.find(dbname);
  if (it != m_state.database_map.end())
    return (it->second);

  // not found - open it
  Database *db = 0;
  DatabaseConfiguration config;
  config.db_name = dbname;
  ham_status_t st = m_state.env->open_db(&db, config, 0);
  if (st)
    throw Exception(st);
  m_state.database_map[dbname] = db;
  return (db);
}

Transaction *
Journal::get_txn(LocalTransactionManager *txn_manager, uint64_t txn_id)
{
  Transaction *txn = txn_manager->get_oldest_txn();
  while (txn) {
    if (txn->get_id() == txn_id)
      return (txn);
    txn = txn->get_next();
  }

  return (0);
}

void
Journal::close_all_databases()
{
  ham_status_t st = 0;

  JournalState::DatabaseMap::iterator it = m_state.database_map.begin();
  while (it != m_state.database_map.end()) {
    JournalState::DatabaseMap::iterator it2 = it; it++;
    st = ham_db_close((ham_db_t *)it2->second, HAM_DONT_LOCK);
    if (st) {
      ham_log(("ham_db_close() failed w/ error %d (%s)", st, ham_strerror(st)));
      throw Exception(st);
    }
  }
  m_state.database_map.clear();
}

void
Journal::abort_uncommitted_txns(LocalTransactionManager *txn_manager)
{
  Transaction *txn = txn_manager->get_oldest_txn();

  while (txn) {
    if (!txn->is_committed())
      txn->abort();
    txn = txn->get_next();
  }
}

void
Journal::recover(LocalTransactionManager *txn_manager)
{
  Context context(m_state.env, 0, 0);

  // first redo the changesets
  uint64_t start_lsn = recover_changeset();

  // load the state of the PageManager; the PageManager state is loaded AFTER
  // physical recovery because its page might have been restored in
  // recover_changeset()
  uint64_t page_manager_blobid = m_state.env->header()->page_manager_blobid();
  if (page_manager_blobid != 0)
    m_state.env->page_manager()->initialize(page_manager_blobid);

  // then start the normal recovery
  if (m_state.env->get_flags() & HAM_ENABLE_TRANSACTIONS)
    recover_journal(&context, txn_manager, start_lsn);

  // clear the journal files
  clear();
}

uint64_t 
Journal::scan_for_oldest_changeset(File *file)
{
  Iterator it;
  PJournalEntry entry;
  ByteArray buffer;

  // get the next entry
  try {
    uint64_t filesize = file->get_file_size();

    while (it.offset < filesize) {
      file->pread(it.offset, &entry, sizeof(entry));

      if (entry.lsn == 0)
        break;

      if (entry.type == kEntryTypeChangeset) {
        return (entry.lsn);
      }

      // increment the offset
      it.offset += sizeof(entry) + entry.followup_size;
    }
  }
  catch (Exception &ex) {
    ham_log(("exception (error %d) while reading journal", ex.code));
  }

  return (0);
}

uint64_t 
Journal::recover_changeset()
{
  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.recover_changeset", ""));

  // scan through both files, look for the file with the oldest changeset.
  uint64_t lsn1 = scan_for_oldest_changeset(&m_state.files[0]);
  uint64_t lsn2 = scan_for_oldest_changeset(&m_state.files[1]);

  // both files are empty or do not contain a changeset?
  if (lsn1 == 0 && lsn2 == 0)
    return (0);

  // now redo all changesets chronologically
  m_state.current_fd = lsn1 < lsn2 ? 0 : 1;

  uint64_t max_lsn1 = redo_all_changesets(m_state.current_fd);
  uint64_t max_lsn2 = redo_all_changesets(m_state.current_fd == 0 ? 1 : 0);

  // return the lsn of the newest changeset
  return (std::max(max_lsn1, max_lsn2));
}

uint64_t
Journal::redo_all_changesets(int fdidx)
{
  Iterator it;
  PJournalEntry entry;
  ByteArray buffer;
  uint64_t max_lsn = 0;

  // for each entry...
  try {
    uint64_t log_file_size = m_state.files[fdidx].get_file_size();

    while (it.offset < log_file_size) {
      m_state.files[fdidx].pread(it.offset, &entry, sizeof(entry));

      // Skip all log entries which are NOT from a changeset
      if (entry.type != kEntryTypeChangeset) {
        it.offset += sizeof(entry) + entry.followup_size;
        continue;
      }

      max_lsn = entry.lsn;

      it.offset += sizeof(entry);

      // Read the Changeset header
      PJournalEntryChangeset changeset;
      m_state.files[fdidx].pread(it.offset, &changeset, sizeof(changeset));
      it.offset += sizeof(changeset);

      EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
                  "j.redo_changeset", "%u", changeset.num_pages));

      uint32_t page_size = m_state.env->config().page_size_bytes;
      ByteArray arena(page_size);

      uint64_t file_size = m_state.env->device()->file_size();

      // for each page in this changeset...
      for (uint32_t i = 0; i < changeset.num_pages; i++) {
        PJournalEntryPageHeader page_header;
        m_state.files[fdidx].pread(it.offset, &page_header,
                        sizeof(page_header));
        it.offset += sizeof(page_header);
        m_state.files[fdidx].pread(it.offset, arena.get_ptr(), page_size);
        it.offset += page_size;

        Page *page;

        EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
                    "j.redo_changeset_page", "%u",
                    (uint32_t)page_header.address));

        // now write the page to disk
        if (page_header.address == file_size) {
          file_size += page_size;

          page = new Page(m_state.env->device());
          page->alloc(0);
        }
        else if (page_header.address > file_size) {
          file_size = (size_t)page_header.address + page_size;
          m_state.env->device()->truncate(file_size);

          page = new Page(m_state.env->device());
          page->fetch(page_header.address);
        }
        else {
          page = new Page(m_state.env->device());
          page->fetch(page_header.address);
        }
        ham_assert(page->get_address() == page_header.address);

        // overwrite the page data
        ::memcpy(page->get_data(), arena.get_ptr(), page_size);

        // flush the modified page to disk
        page->set_dirty(true);
        Page::flush(m_state.env->device(), page->get_persisted_data());

        delete page;
      }
    }
  }
  catch (Exception &) {
    ham_trace(("Exception when applying changeset; skipping changeset"));
    // fall through
  }

  return (max_lsn);
}

void
Journal::recover_journal(Context *context,
                LocalTransactionManager *txn_manager, uint64_t start_lsn)
{
  ham_status_t st = 0;
  Iterator it;
  ByteArray buffer;

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.recover_journal", ""));

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
  ham_assert(txn_manager->get_oldest_txn() == 0);
  ham_assert(m_state.env->get_flags() & HAM_ENABLE_TRANSACTIONS);
  ham_assert(m_state.env->get_flags() & HAM_ENABLE_RECOVERY);

  // do not append to the journal during recovery
  m_state.disable_logging = true;

  do {
    PJournalEntry entry;

    // get the next entry
    get_entry(&it, &entry, &buffer);

    // reached end of logfile?
    if (!entry.lsn)
      break;

    // re-apply this operation
    switch (entry.type) {
      case kEntryTypeTxnBegin: {
        Transaction *txn = 0;
        st = ham_txn_begin((ham_txn_t **)&txn, (ham_env_t *)m_state.env, 
                (const char *)buffer.get_ptr(), 0, HAM_DONT_LOCK);
        // on success: patch the txn ID
        if (st == 0) {
          txn->set_id(entry.txn_id);
          txn_manager->set_txn_id(entry.txn_id);
        }
        break;
      }
      case kEntryTypeTxnAbort: {
        Transaction *txn = get_txn(txn_manager, entry.txn_id);
        st = ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeTxnCommit: {
        Transaction *txn = get_txn(txn_manager, entry.txn_id);
        st = ham_txn_commit((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeInsert: {
        PJournalEntryInsert *ins = (PJournalEntryInsert *)buffer.get_ptr();
        Transaction *txn = 0;
        Database *db;
        ham_key_t key = {0};
        ham_record_t record = {0};
        if (!ins) {
          st = HAM_IO_ERROR;
          goto bail;
        }

        // do not insert if the key was already flushed to disk
        if (entry.lsn <= start_lsn)
          continue;

        key.data = ins->get_key_data();
        key.size = ins->key_size;
        record.data = ins->get_record_data();
        record.size = ins->record_size;
        record.partial_size = ins->record_partial_size;
        record.partial_offset = ins->record_partial_offset;
        if (entry.txn_id)
          txn = get_txn(txn_manager, entry.txn_id);
        db = get_db(entry.dbname);
        st = ham_db_insert((ham_db_t *)db, (ham_txn_t *)txn, 
                    &key, &record, ins->insert_flags | HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeErase: {
        PJournalEntryErase *e = (PJournalEntryErase *)buffer.get_ptr();
        Transaction *txn = 0;
        Database *db;
        ham_key_t key = {0};
        if (!e) {
          st = HAM_IO_ERROR;
          goto bail;
        }

        // do not erase if the key was already erased from disk
        if (entry.lsn <= start_lsn)
          continue;

        if (entry.txn_id)
          txn = get_txn(txn_manager, entry.txn_id);
        db = get_db(entry.dbname);
        key.data = e->get_key_data();
        key.size = e->key_size;
        st = ham_db_erase((ham_db_t *)db, (ham_txn_t *)txn, &key,
                      e->erase_flags | HAM_DONT_LOCK);
        // key might have already been erased when the changeset
        // was flushed
        if (st == HAM_KEY_NOT_FOUND)
          st = 0;
        break;
      }
      case kEntryTypeChangeset: {
        // skip this; the changeset was already applied
        break;
      }
      default:
        ham_log(("invalid journal entry type or journal is corrupt"));
        st = HAM_IO_ERROR;
      }

      if (st)
        goto bail;
  } while (1);

bail:
  // all transactions which are not yet committed will be aborted
  abort_uncommitted_txns(txn_manager);

  // also close and delete all open databases - they were created in get_db()
  close_all_databases();

  // flush all committed transactions
  if (st == 0)
    st = m_state.env->flush(HAM_FLUSH_COMMITTED_TRANSACTIONS);

  // re-enable the logging
  m_state.disable_logging = false;

  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.recover_journal_result", "%d", st));

  if (st)
    throw Exception(st);
}

void
Journal::clear_file(int idx)
{
  EVENTLOG_APPEND((m_state.env->config().filename.c_str(),
              "j.clear_file", "%d", idx));

  if (m_state.files[idx].is_open()) {
    m_state.files[idx].truncate(0);

    // after truncate, the file pointer is far beyond the new end of file;
    // reset the file pointer, or the next write will resize the file to
    // the original size
    m_state.files[idx].seek(0, File::kSeekSet);
  }

  // clear the transaction counters
  m_state.open_txn[idx] = 0;
  m_state.closed_txn[idx] = 0;

  // also clear the buffer with the outstanding data
  m_state.buffer[idx].clear();
}

std::string
Journal::get_path(int i)
{
  std::string path;

  if (m_state.env->config().log_filename.empty()) {
    path = m_state.env->config().filename;
  }
  else {
    path = m_state.env->config().log_filename;
#ifdef WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(m_state.env->config().filename.c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)m_state.env->config().filename.c_str());
#endif
  }
  if (i == 0)
    path += ".jrn0";
  else if (i == 1)
    path += ".jrn1";
  else
    ham_assert(!"invalid index");
  return (path);
}

JournalTest
Journal::test()
{
  return (JournalTest(&m_state));
}

JournalState::JournalState(LocalEnvironment *env)
  : env(env), current_fd(0), threshold(env->config().journal_switch_threshold),
    disable_logging(false), count_bytes_flushed(0),
    count_bytes_before_compression(0), count_bytes_after_compression(0)
{
  if (threshold == 0)
    threshold = kSwitchTxnThreshold;

  open_txn[0] = 0;
  open_txn[1] = 0;
  closed_txn[0] = 0;
  closed_txn[1] = 0;
}

} // namespace hamsterdb
