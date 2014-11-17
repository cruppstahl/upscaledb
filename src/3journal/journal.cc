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

#include "0root/root.h"

#include <string.h>
#ifndef HAM_OS_WIN32
#  include <libgen.h>
#endif

#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1os/os.h"
#include "2device/device.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

Journal::Journal(LocalEnvironment *env)
  : m_env(env), m_current_fd(0), m_lsn(1), m_last_cp_lsn(0),
    m_threshold(kSwitchTxnThreshold), m_disable_logging(false),
    m_count_bytes_flushed(0)
{
  m_open_txn[0] = 0;
  m_open_txn[1] = 0;
  m_closed_txn[0] = 0;
  m_closed_txn[1] = 0;
}

void
Journal::create()
{
  // create the two files
  for (int i = 0; i < 2; i++) {
    std::string path = get_path(i);
    m_files[i].create(path.c_str(), 0644);
  }
}

void
Journal::open()
{
  // open the two files
  try {
    std::string path = get_path(0);
    m_files[0].open(path.c_str(), false);
    path = get_path(1);
    m_files[1].open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    m_files[1].close();
    m_files[0].close();
    throw ex;
  }
}

int
Journal::switch_files_maybe()
{
  int other = m_current_fd ? 0 : 1;

  // determine the journal file which is used for this transaction 
  // if the "current" file is not yet full, continue to write to this file
  if (m_open_txn[m_current_fd] + m_closed_txn[m_current_fd] < m_threshold)
    return (m_current_fd);

  // If the other file does no longer have open Transactions then
  // delete the other file and use the other file as the current file
  if (m_open_txn[other] == 0) {
    clear_file(other);
    m_current_fd = other;
    // fall through
  }

  // Otherwise just continue using the current file
  return (m_current_fd);
}

void
Journal::append_txn_begin(LocalTransaction *txn, const char *name,
                uint64_t lsn)
{
  if (m_disable_logging)
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

  m_open_txn[cur]++;

  // store the fp-index in the journal structure; it's needed for
  // journal_append_checkpoint() to quickly find out which file is
  // the newest
  m_current_fd = cur;
}

void
Journal::append_txn_abort(LocalTransaction *txn, uint64_t lsn)
{
  if (m_disable_logging)
    return;

  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);

  int idx;
  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnAbort;

  // update the transaction counters of this logfile
  idx = txn->get_log_desc();
  m_open_txn[idx]--;
  m_closed_txn[idx]++;

  append_entry(idx, (uint8_t *)&entry, sizeof(entry));
  maybe_flush_buffer(idx);
  // no need for fsync - incomplete transactions will be aborted anyway
}

void
Journal::append_txn_commit(LocalTransaction *txn, uint64_t lsn)
{
  if (m_disable_logging)
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
  flush_buffer(idx, m_env->get_flags() & HAM_ENABLE_FSYNC);
}

void
Journal::append_insert(Database *db, LocalTransaction *txn,
                ham_key_t *key, ham_record_t *record, uint32_t flags,
                uint64_t lsn)
{
  if (m_disable_logging)
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
  entry.dbname = db->get_name();
  entry.type = kEntryTypeInsert;
  entry.followup_size = size;

  int idx;
  if (txn->get_flags() & HAM_TXN_TEMPORARY) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    m_closed_txn[idx]++;
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
  maybe_flush_buffer(idx);
}

void
Journal::append_erase(Database *db, LocalTransaction *txn, ham_key_t *key,
                int duplicate_index, uint32_t flags, uint64_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryErase erase;
  uint32_t size = sizeof(PJournalEntryErase) + key->size - 1;

  entry.lsn = lsn;
  entry.dbname = db->get_name();
  entry.type = kEntryTypeErase;
  entry.followup_size = size;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = duplicate_index;

  int idx;
  if (txn->get_flags() & HAM_TXN_TEMPORARY) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    m_closed_txn[idx]++;
  }
  else {
    entry.txn_id = txn->get_id();
    idx = txn->get_log_desc();
  }

  // append the entry to the logfile
  append_entry(idx, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&erase, sizeof(PJournalEntryErase) - 1,
                (uint8_t *)key->data, key->size);
  maybe_flush_buffer(idx);
}

void
Journal::append_changeset(Page **bucket1, uint32_t bucket1_size,
                    Page **bucket2, uint32_t bucket2_size,
                    Page **bucket3, uint32_t bucket3_size,
                    Page **bucket4, uint32_t bucket4_size,
                    uint64_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryChangeset changeset;
  
  entry.lsn = lsn;
  entry.dbname = 0;
  entry.txn_id = 0;
  entry.type = kEntryTypeChangeset;
  // followup_size is incomplete - the actual page sizes are added later
  entry.followup_size = sizeof(PJournalEntryChangeset);
  changeset.num_pages = bucket1_size + bucket2_size
                            + bucket3_size + bucket4_size;

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = m_buffer[m_current_fd].get_size();

  // write the data to the file
  append_entry(m_current_fd, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&changeset, sizeof(PJournalEntryChangeset));

  size_t page_size = m_env->get_page_size();
  for (uint32_t i = 0; i < bucket1_size; i++)
    entry.followup_size += append_changeset_page(bucket1[i], page_size);
  for (uint32_t i = 0; i < bucket2_size; i++)
    entry.followup_size += append_changeset_page(bucket2[i], page_size);
  for (uint32_t i = 0; i < bucket3_size; i++)
    entry.followup_size += append_changeset_page(bucket3[i], page_size);
  for (uint32_t i = 0; i < bucket4_size; i++)
    entry.followup_size += append_changeset_page(bucket4[i], page_size);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and patch in the followup-size
  m_buffer[m_current_fd].overwrite(entry_position,
          (uint8_t *)&entry, sizeof(entry));

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and flush the file
  flush_buffer(m_current_fd, m_env->get_flags() & HAM_ENABLE_FSYNC);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // if recovery is enabled (w/o transactions) then simulate a "commit" to
  // make sure that the log files are switched properly
  m_closed_txn[m_current_fd]++;
  (void)switch_files_maybe();
}

uint32_t
Journal::append_changeset_page(Page *page, uint32_t page_size)
{
  PJournalEntryPageHeader header(page->get_address());

  append_entry(m_current_fd, (uint8_t *)&header, sizeof(header),
                page->get_raw_payload(), page_size);
  return (page_size + sizeof(header));
}

void
Journal::transaction_flushed(LocalTransaction *txn)
{
  ham_assert((txn->get_flags() & HAM_TXN_TEMPORARY) == 0);
  if (m_disable_logging) // ignore this call during recovery
    return;

  int idx = txn->get_log_desc();
  ham_assert(m_open_txn[idx] > 0);
  m_open_txn[idx]--;
  m_closed_txn[idx]++;
}

void
Journal::get_entry(Iterator *iter, PJournalEntry *entry, ByteArray *auxbuffer)
{
  size_t filesize;

  auxbuffer->clear();

  // if iter->offset is 0, then the iterator was created from scratch
  // and we start reading from the first (oldest) entry.
  //
  // The oldest of the two logfiles is always the "other" one (the one
  // NOT in current_fd).
  if (iter->offset == 0) {
    iter->fdstart = iter->fdidx =
                        m_current_fd == 0
                            ? 1
                            : 0;
  }

  // get the size of the journal file
  filesize = m_files[iter->fdidx].get_file_size();

  // reached EOF? then either skip to the next file or we're done
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = 0;
      filesize = m_files[iter->fdidx].get_file_size();
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
    m_files[iter->fdidx].pread(iter->offset, entry, sizeof(*entry));

    iter->offset += sizeof(*entry);

    // read auxiliary data if it's available
    if (entry->followup_size) {
      auxbuffer->resize((uint32_t)entry->followup_size);

      m_files[iter->fdidx].pread(iter->offset, auxbuffer->get_ptr(),
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
    m_files[i].close();
    m_buffer[i].clear();
  }
}

static Database *
recover_get_db(Environment *env, uint16_t dbname)
{
  // first check if the Database is already open
  Environment::DatabaseMap::iterator it = env->get_database_map().find(dbname);
  if (it != env->get_database_map().end())
    return (it->second);

  // not found - open it
  Database *db = 0;
  DatabaseConfiguration config;
  config.db_name = dbname;
  env->open_db(&db, config, 0);
  return (db);
}

static Transaction *
recover_get_txn(Environment *env, uint64_t txn_id)
{
  Transaction *txn = env->get_txn_manager()->get_oldest_txn();
  while (txn) {
    if (txn->get_id() == txn_id)
      return (txn);
    txn = txn->get_next();
  }

  return (0);
}

static void
__close_all_databases(LocalEnvironment *env)
{
  ham_status_t st = 0;

  Environment::DatabaseMap::iterator it = env->get_database_map().begin();
  while (it != env->get_database_map().end()) {
    Environment::DatabaseMap::iterator it2 = it; it++;
    st = ham_db_close((ham_db_t *)it2->second, HAM_DONT_LOCK);
    if (st) {
      if (env->get_flags() & HAM_ENABLE_RECOVERY)
        env->get_changeset().clear();
      ham_log(("ham_db_close() failed w/ error %d (%s)", st, ham_strerror(st)));
      throw Exception(st);
    }
  }
}

static void
__abort_uncommitted_txns(Environment *env)
{
  ham_status_t st;
  Transaction *newer, *txn = env->get_txn_manager()->get_oldest_txn();

  while (txn) {
    newer = txn->get_next();
    if (!txn->is_committed()) {
      st = ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
      if (st)
        throw Exception(st);
    }
    txn = newer;
  }
}

void
Journal::recover()
{
  // first re-apply the last changeset
  uint64_t start_lsn = recover_changeset();
  if (start_lsn > m_lsn)
    m_lsn = start_lsn;

  // load the state of the PageManager; the PageManager state is loaded AFTER
  // physical recovery because its page might have been restored in
  // recover_changeset()
  uint64_t page_manager_blobid
          = m_env->get_header()->get_page_manager_blobid();
  if (page_manager_blobid != 0) {
    m_env->get_page_manager()->load_state(page_manager_blobid);
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().clear();
  }

  // then start the normal recovery
  if (m_env->get_flags() & HAM_ENABLE_TRANSACTIONS)
    recover_journal(start_lsn);
}

uint64_t 
Journal::scan_for_newest_changeset(File *file, uint64_t *position)
{
  Iterator it;
  PJournalEntry entry;
  ByteArray buffer;
  uint64_t result = 0;

  // get the next entry
  try {
    uint64_t filesize = file->get_file_size();

    while (it.offset < filesize) {
      file->pread(it.offset, &entry, sizeof(entry));

      if (entry.lsn == 0)
        break;

      if (entry.type == kEntryTypeChangeset) {
        *position = it.offset;
        result = entry.lsn;
      }

      // increment the offset
      it.offset += sizeof(entry);
      if (entry.followup_size)
        it.offset += entry.followup_size;
    }
  }
  catch (Exception &ex) {
    ham_log(("exception (error %d) while reading journal", ex.code));
  }

  return (result);
}

uint64_t 
Journal::recover_changeset()
{
  // scan through both files, look for the file with the newest changeset
  uint64_t position0, position1, position;
  uint64_t lsn1 = scan_for_newest_changeset(&m_files[0], &position0);
  uint64_t lsn2 = scan_for_newest_changeset(&m_files[1], &position1);

  // both files are empty or do not contain a changeset?
  if (lsn1 == 0 && lsn2 == 0)
    return (0);

  // re-apply the newest changeset
  m_current_fd = lsn1 > lsn2 ? 0 : 1;
  position = lsn1 > lsn2 ? position0 : position1;

  PJournalEntry entry;
  uint64_t start_lsn = 0;

  try {
    m_files[m_current_fd].pread(position, &entry, sizeof(entry));
    position += sizeof(entry);
    ham_assert(entry.type == kEntryTypeChangeset);

    // Read the Changeset header
    PJournalEntryChangeset changeset;
    m_files[m_current_fd].pread(position, &changeset, sizeof(changeset));
    position += sizeof(changeset);

    uint32_t page_size = m_env->get_page_size();
    ByteArray arena(page_size);

    size_t file_size = m_env->get_device()->get_file_size();

    // for each page in this changeset...
    for (uint32_t i = 0; i < changeset.num_pages; i++) {
      PJournalEntryPageHeader page_header;
      m_files[m_current_fd].pread(position, &page_header, sizeof(page_header));
      position += sizeof(page_header);
      m_files[m_current_fd].pread(position, arena.get_ptr(), page_size);
      position += page_size;

      Page *page;

      // now write the page to disk
      if (page_header.address == file_size) {
        file_size += page_size;

        page = new Page(m_env->get_device());
        page->allocate(0);
      }
      else if (page_header.address > file_size) {
        file_size = (size_t)page_header.address + page_size;
        m_env->get_device()->truncate(file_size);

        page = new Page(m_env->get_device());
        page->fetch(page_header.address);
      }
      else {
        page = new Page(m_env->get_device());
        page->fetch(page_header.address);
      }

      // only overwrite the page data if the page's last modification
      // is OLDER than the changeset!
      bool skip = false;
      if (page->is_without_header() == false) {
        if (page->get_lsn() > entry.lsn) {
          skip = true;
          start_lsn = page->get_lsn();
        }
      }

      if (!skip) {
        // overwrite the page data
        memcpy(page->get_data(), arena.get_ptr(), page_size);

        ham_assert(page->get_address() == page_header.address);

        // flush the modified page to disk
        page->set_dirty(true);
        m_env->get_page_manager()->flush_page(page);
      }

      delete page;
    }
  }
  catch (Exception &) {
    ham_trace(("Exception when applying changeset; skipping changeset"));
    // fall through
  }

  return (std::max(start_lsn, entry.lsn));
}

void
Journal::recover_journal(uint64_t start_lsn)
{
  ham_status_t st = 0;
  Iterator it;
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
  ham_assert(m_env->get_txn_manager()->get_oldest_txn() == 0);
  ham_assert(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS);
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  // do not append to the journal during recovery
  m_disable_logging = true;

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
        st = ham_txn_begin((ham_txn_t **)&txn, (ham_env_t *)m_env, 
                (const char *)buffer.get_ptr(), 0, HAM_DONT_LOCK);
        // on success: patch the txn ID
        if (st == 0) {
          txn->set_id(entry.txn_id);
          LocalTransactionManager *ltm
                  = (LocalTransactionManager *)m_env->get_txn_manager();
          ltm->set_txn_id(entry.txn_id);
        }
        break;
      }
      case kEntryTypeTxnAbort: {
        Transaction *txn = recover_get_txn(m_env, entry.txn_id);
        st = ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeTxnCommit: {
        Transaction *txn = recover_get_txn(m_env, entry.txn_id);
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
          txn = recover_get_txn(m_env, entry.txn_id);
        db = recover_get_db(m_env, entry.dbname);
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
          txn = recover_get_txn(m_env, entry.txn_id);
        db = recover_get_db(m_env, entry.dbname);
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

      if (m_lsn < entry.lsn)
        m_lsn = entry.lsn;
  } while (1);

bail:
  // all transactions which are not yet committed will be aborted
  (void)__abort_uncommitted_txns(m_env);

  // also close and delete all open databases - they were created in
  // recover_get_db()
  (void)__close_all_databases(m_env);

  // flush all committed transactions
  if (st == 0)
    m_env->get_txn_manager()->flush_committed_txns();

  // re-enable the logging
  m_disable_logging = false;

  if (st)
    throw Exception(st);

  // clear the journal files
  clear();
}

void
Journal::clear_file(int idx)
{
  if (m_files[idx].is_open()) {
    m_files[idx].truncate(0);

    // after truncate, the file pointer is far beyond the new end of file;
    // reset the file pointer, or the next write will resize the file to
    // the original size
    m_files[idx].seek(0, File::kSeekSet);
  }

  // clear the transaction counters
  m_open_txn[idx] = 0;
  m_closed_txn[idx] = 0;

  // also clear the buffer with the outstanding data
  m_buffer[idx].clear();
}

std::string
Journal::get_path(int i)
{
  std::string path;

  if (m_env->get_config().log_filename.empty()) {
    path = m_env->get_config().filename;
  }
  else {
    path = m_env->get_config().log_filename;
#ifdef HAM_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(m_env->get_config().filename.c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)m_env->get_config().filename.c_str());
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

} // namespace hamsterdb
