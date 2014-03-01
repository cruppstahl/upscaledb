/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#ifndef HAM_OS_WIN32
#  include <libgen.h>
#endif

#include "db.h"
#include "device.h"
#include "env_local.h"
#include "error.h"
#include "mem.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "journal.h"
#include "page_manager.h"

namespace hamsterdb {

void
Journal::create()
{
  int i;
  PJournalHeader header;

  // create the two files
  for (i = 0; i < 2; i++) {
    std::string path = get_path(i);
    m_fd[i] = os_create(path.c_str(), 0, 0644);

    // and write the magic
    os_write(m_fd[i], &header, sizeof(header));
  }
}

void
Journal::open()
{
  int i;
  PJournalHeader header;
  PJournalEntry entry;
  PJournalTrailer trailer;
  ham_u64_t lsn[2];
  ham_status_t st1 = 0, st2 = 0;

  m_current_fd = 0;

  // open the two files; if the files do not exist then create them
  std::string path = get_path(0);
  try {
    m_fd[0] = os_open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    st1 = ex.code;
  }
  path = get_path(1);
  try {
    m_fd[1] = os_open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    st2 = ex.code;
  }

  if (st1 == st2 && st2 == HAM_FILE_NOT_FOUND)
    throw Exception(HAM_FILE_NOT_FOUND);

  if (st1 || st2) {
    (void)close();
    throw Exception(st1 ? st1 : st2);
  }

  // now read the header structures of both files; the file with the larger
  // lsn is "newer"
  for (i = 0; i < 2; i++) {
    // check the magic
    os_pread(m_fd[i], 0, &header, sizeof(header));

    if (header.magic != kHeaderMagic) {
      ham_trace(("journal has unknown magic or is corrupt"));
      (void)close();
      throw Exception(HAM_LOG_INV_FILE_HEADER);
    }

    // read the lsn from the header structure
    lsn[i] = header.lsn;
  }

  // the larger lsn will become the active file
  if (lsn[0] < lsn[1])
    m_current_fd = 1;

  m_lsn = std::max(lsn[0], lsn[1]);

  // now extract the highest lsn - this is where we will continue
  for (int i = 0; i < 2; i++) {
    // but make sure that the file is large enough!
    ham_u64_t size = os_get_file_size(m_fd[i]);

    if (size >= sizeof(entry)) {
      os_pread(m_fd[i], size - sizeof(PJournalTrailer),
                      &trailer, sizeof(trailer));

      // Verify the trailer magic; if it's invalid then skip this file
      // TODO fallback and fetch lsn from the beginning of the file??
      if (trailer.magic != kTrailerMagic) {
        ham_log(("Changeset magic is invalid, skipping"));
        continue;
      }

      os_pread(m_fd[i], size - trailer.full_size - sizeof(trailer),
                      &entry, sizeof(entry));
      ham_assert(entry.lsn != 0);

      // update the highest lsn
      //
      // also, if we have not yet figured out which file is "newer" then
      // use the file with the highest lsn as the "current" file
      if (m_lsn < entry.lsn) {
        m_lsn = entry.lsn;
        if (lsn[0] == lsn[1])
          m_current_fd = i;
      }
    }
  }
}

void
Journal::switch_files_maybe(Transaction *txn)
{
  int cur = m_current_fd;
  int other = cur ? 0 : 1;

  // determine the journal file which is used for this transaction 
  // if the "current" file is not yet full, continue to write to this file
  if (m_open_txn[cur] + m_closed_txn[cur] < m_threshold) {
    txn->set_log_desc(cur);
  }
  else if (m_open_txn[other] == 0) {
    // Otherwise, if the other file does no longer have open Transactions,
    // delete the other file and use the other file as the current file
    clear_file(other);
    cur = other;
    m_current_fd = cur;
    txn->set_log_desc(cur);
  }
}

void
Journal::append_txn_begin(Transaction *txn, LocalEnvironment *env, 
                const char *name, ham_u64_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnBegin;
  entry.lsn = lsn;
  if (name)
    entry.followup_size = strlen(name) + 1;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + entry.followup_size;

  switch_files_maybe(txn);

  int cur = txn->get_log_desc();

  if (txn->get_name().size())
    append_entry(cur, (void *)&entry, (ham_u32_t)sizeof(entry),
                (void *)txn->get_name().c_str(),
                (ham_u32_t)txn->get_name().size() + 1,
                (void *)&trailer, sizeof(trailer));
  else
    append_entry(cur, (void *)&entry, (ham_u32_t)sizeof(entry),
                (void *)&trailer, sizeof(trailer));

  m_open_txn[cur]++;

  // store the fp-index in the journal structure; it's needed for
  // journal_append_checkpoint() to quickly find out which file is
  // the newest
  m_current_fd = cur;
}

void
Journal::append_txn_abort(Transaction *txn, ham_u64_t lsn)
{
  if (m_disable_logging)
    return;

  int idx;
  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnAbort;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + entry.followup_size;

  // update the transaction counters of this logfile
  idx = txn->get_log_desc();
  m_open_txn[idx]--;
  m_closed_txn[idx]++;

  append_entry(idx, &entry, sizeof(entry), &trailer, sizeof(trailer));
  // no need for fsync - incomplete transactions will be aborted anyway
}

void
Journal::append_txn_commit(Transaction *txn, ham_u64_t lsn)
{
  if (m_disable_logging)
    return;

  int idx;
  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnCommit;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + entry.followup_size;

  // update the transaction counters of this logfile
  idx = txn->get_log_desc();
  m_open_txn[idx]--;
  m_closed_txn[idx]++;

  append_entry(idx, &entry, sizeof(entry), &trailer, sizeof(trailer));

  // and flush the file
  flush_buffer(idx, m_env->get_flags() & HAM_ENABLE_FSYNC);
}

void
Journal::append_insert(Database *db, Transaction *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags,
                ham_u64_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryInsert insert;
  ham_u32_t size = sizeof(PJournalEntryInsert) + key->size + record->size - 1;

  entry.lsn = lsn;
  entry.dbname = db->get_name();
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeInsert;
  entry.followup_size = size;

  insert.key_size = key->size;
  insert.record_size = record->size;
  insert.record_partial_size = record->partial_size;
  insert.record_partial_offset = record->partial_offset;
  insert.insert_flags = flags;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + sizeof(PJournalEntryInsert) - 1
                + key->size + record->size;

  // append the entry to the logfile
  append_entry(txn->get_log_desc(),
                &entry, sizeof(entry),
                &insert, sizeof(PJournalEntryInsert) - 1,
                key->data, key->size,
                record->data, record->size,
                &trailer, sizeof(trailer));
}

void
Journal::append_erase(Database *db, Transaction *txn, ham_key_t *key,
                ham_u32_t dupe, ham_u32_t flags, ham_u64_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryErase erase;
  ham_u32_t size = sizeof(PJournalEntryErase) + key->size - 1;

  entry.lsn = lsn;
  entry.dbname = db->get_name();
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeErase;
  entry.followup_size = size;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = dupe;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + sizeof(PJournalEntryErase) - 1
                + key->size;

  // append the entry to the logfile
  append_entry(txn->get_log_desc(),
                &entry, sizeof(entry),
                (PJournalEntry *)&erase, sizeof(PJournalEntryErase) - 1,
                key->data, key->size,
                &trailer, sizeof(trailer));
}

void
Journal::append_changeset(Page **bucket1, ham_u32_t bucket1_size,
                    Page **bucket2, ham_u32_t bucket2_size,
                    Page **bucket3, ham_u32_t bucket3_size,
                    Page **bucket4, ham_u32_t bucket4_size,
                    ham_u32_t lsn)
{
  if (m_disable_logging)
    return;

  PJournalEntry entry;
  PJournalEntryChangeset changeset;
  ham_u32_t page_size = m_env->get_page_size();
  ham_u32_t num_pages = bucket1_size + bucket2_size
          + bucket3_size + bucket4_size;
  ham_u32_t size = sizeof(PJournalEntryChangeset)
          + num_pages * (page_size + sizeof(PJournalEntryPageHeader));

  entry.lsn = lsn;
  entry.dbname = 0;
  entry.txn_id = 0;
  entry.type = kEntryTypeChangeset;
  entry.followup_size = size;
  changeset.num_pages = num_pages;

  PJournalTrailer trailer;
  trailer.type = entry.type;
  trailer.full_size = sizeof(entry) + entry.followup_size;

  // write the data to the file
  append_entry(m_current_fd, &entry, sizeof(entry),
                &changeset, sizeof(PJournalEntryChangeset));

  for (ham_u32_t i = 0; i < bucket1_size; i++) {
    PJournalEntryPageHeader header(bucket1[i]->get_address());
    append_entry(m_current_fd, &header, sizeof(header),
                    bucket1[i]->get_raw_payload(), page_size);
  }
  for (ham_u32_t i = 0; i < bucket2_size; i++) {
    PJournalEntryPageHeader header(bucket2[i]->get_address());
    append_entry(m_current_fd, &header, sizeof(header),
                    bucket2[i]->get_raw_payload(), page_size);
  }
  for (ham_u32_t i = 0; i < bucket3_size; i++) {
    PJournalEntryPageHeader header(bucket3[i]->get_address());
    append_entry(m_current_fd, &header, sizeof(header),
                    bucket3[i]->get_raw_payload(), page_size);
  }
  for (ham_u32_t i = 0; i < bucket4_size; i++) {
    PJournalEntryPageHeader header(bucket4[i]->get_address());
    append_entry(m_current_fd, &header, sizeof(header),
                    bucket4[i]->get_raw_payload(), page_size);
  }

  // finally append the trailer
  append_entry(m_current_fd, &trailer, sizeof(trailer));

  // and flush the file
  flush_buffer(m_current_fd, m_env->get_flags() & HAM_ENABLE_FSYNC);
}

void
Journal::get_entry(Iterator *iter, PJournalEntry *entry, ByteArray *auxbuffer)
{
  ham_u64_t filesize;

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
    iter->offset = sizeof(PJournalHeader);
  }

  // get the size of the journal file
  filesize = os_get_file_size(m_fd[iter->fdidx]);

  // reached EOF? then either skip to the next file or we're done
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = sizeof(PJournalHeader);
      filesize = os_get_file_size(m_fd[iter->fdidx]);
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
  os_pread(m_fd[iter->fdidx], iter->offset, entry, sizeof(*entry));

  iter->offset += sizeof(*entry);

  // read auxiliary data if it's available
  if (entry->followup_size) {
    auxbuffer->resize((ham_u32_t)entry->followup_size);

    os_pread(m_fd[iter->fdidx], iter->offset, auxbuffer->get_ptr(),
                    entry->followup_size);
    iter->offset += entry->followup_size;
  }

  // skip the trailer
  iter->offset += sizeof(PJournalTrailer);
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

  if (!noclear) {
    PJournalHeader header;

    clear();

    // update the header page of file 0 to store the lsn
    header.lsn = m_lsn;

    if (m_fd[0] != HAM_INVALID_FD)
      os_pwrite(m_fd[0], 0, &header, sizeof(header));
  }

  for (i = 0; i < 2; i++) {
    if (m_fd[i] != HAM_INVALID_FD) {
      os_close(m_fd[i]);
      m_fd[i] = HAM_INVALID_FD;
    }
    m_buffer[i].clear();
  }
}

static void
__recover_get_db(Environment *env, ham_u16_t dbname, Database **pdb)
{
  // first check if the Database is already open
  Environment::DatabaseMap::iterator it = env->get_database_map().find(dbname);
  if (it != env->get_database_map().end()) {
    *pdb = it->second;
    return;
  }

  // not found - open it
  env->open_db(pdb, dbname, 0, 0);
}

static void
recover_get_txn(Environment *env, ham_u64_t txn_id, Transaction **ptxn)
{
  Transaction *txn = env->get_oldest_txn();
  while (txn) {
    if (txn->get_id() == txn_id) {
      *ptxn = txn;
      return;
    }
    txn = txn->get_next();
  }

  *ptxn = 0;
}

static void
__close_all_databases(Environment *env)
{
  ham_status_t st = 0;

  Environment::DatabaseMap::iterator it = env->get_database_map().begin();
  while (it != env->get_database_map().end()) {
    Environment::DatabaseMap::iterator it2 = it; it++;
    st = ham_db_close((ham_db_t *)it2->second, HAM_DONT_LOCK);
    if (st) {
      ham_log(("ham_db_close() failed w/ error %d (%s)", st, ham_strerror(st)));
      throw Exception(st);
    }
  }
}

static void
__abort_uncommitted_txns(Environment *env)
{
  ham_status_t st;
  Transaction *newer, *txn = env->get_oldest_txn();

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
  ham_u64_t start_lsn = recover_changeset();

  // load the state of the PageManager; the PageManager state is loaded AFTER
  // physical recovery because its page might have been restored in
  // recover_changeset()
  ham_u64_t page_manager_blobid
          = m_env->get_header()->get_page_manager_blobid();
  if (page_manager_blobid != 0) {
    m_env->get_page_manager()->load_state(page_manager_blobid);
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().clear();
  }

  // then start the normal recovery
  recover_journal(start_lsn);
}

ham_u64_t 
Journal::recover_changeset()
{
  ham_u64_t log_size = os_get_file_size(m_fd[m_current_fd]);
  ham_u64_t file_size = m_env->get_device()->get_file_size();
  PJournalEntry entry;

  // seek to the position of the last journal entry
  if (log_size <= sizeof(PJournalEntry))
    return (0);
  
  PJournalTrailer trailer;
  os_pread(m_fd[m_current_fd], log_size - sizeof(PJournalTrailer),
                  &trailer, sizeof(trailer));

  // Verify the trailer magic; if it's invalid then skip the Changeset
  if (trailer.magic != kTrailerMagic) {
    ham_log(("Changeset magic is invalid, skipping"));
    return (0);
  }

  ham_u64_t position = log_size - trailer.full_size - sizeof(trailer);
  os_pread(m_fd[m_current_fd], position, &entry, sizeof(entry));
  position += sizeof(entry);

  // only continue if it was a changeset; otherwise return, and the journal
  // will be applied
  if (entry.type != kEntryTypeChangeset)
    return (0);

  // Read the Changeset header
  PJournalEntryChangeset changeset;
  os_pread(m_fd[m_current_fd], position, &changeset, sizeof(changeset));
  position += sizeof(changeset);

  ham_u32_t page_size = m_env->get_page_size();
  ByteArray arena(page_size);

  // for each page in this changeset...
  for (ham_u32_t i = 0; i < changeset.num_pages; i++) {
    PJournalEntryPageHeader page_header;
    os_pread(m_fd[m_current_fd], position, &page_header, sizeof(page_header));
    position += sizeof(page_header);
    os_pread(m_fd[m_current_fd], position, arena.get_ptr(), page_size);
    position += page_size;

    Page *page;

    // now write the page to disk
    if (page_header.address == file_size) {
      file_size += page_size;

      page = new Page(m_env);
      page->allocate(0);
    }
    else if (page_header.address > file_size) {
      file_size = page_header.address + page_size;
      m_env->get_device()->truncate(file_size);

      page = new Page(m_env);
      page->fetch(page_header.address);
    }
    else {
      page = new Page(m_env);
      page->fetch(page_header.address);
    }

    // overwrite the page data
    memcpy(page->get_data(), arena.get_ptr(), page_size);

    ham_assert(page->get_address() == page_header.address);

    // flush the modified page to disk
    page->set_dirty(true);
    m_env->get_page_manager()->flush_page(page);
    delete page;
  }
  return (entry.lsn);
}

void
Journal::recover_journal(ham_u64_t start_lsn)
{
  ham_status_t st = 0;
  Iterator it;
  ByteArray buffer;

  /* recovering the journal is rather simple - we iterate over the
   * files and re-apply EVERY operation (incl. txn_begin and txn_abort).
   *
   * in hamsterdb 1.x this routine just skipped all journal entries that were
   * already flushed to disk (i.e. everything with a lsn <= start_lsn
   * was ignored). However, if we also skip the txn_begin entries, then
   * some scenarios will fail:
   *
   *  --- time -------------------------->
   *  BEGIN,    INSERT,    COMMIT
   *  flush(1), flush(2), ^crash
   *
   * if the application crashes BEFORE the commit is flushed, then
   * start_lsn will be 2, and the txn_begin will be skipped. During recovery
   * we'd then end up in a situation where we want to commit a transaction
   * which was not created. Therefore start_lsn is ignored for txn_begin/
   * txn_commit/txn_abort, and only checked for insert/erase.
   *
   * when done then auto-abort all transactions that were not yet
   * committed
   */

  // make sure that there are no pending transactions - start with
  // a clean state!
  ham_assert(m_env->get_oldest_txn() == 0);
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
          m_env->set_txn_id(entry.txn_id);
        }
        break;
      }
      case kEntryTypeTxnAbort: {
        Transaction *txn = 0;
        recover_get_txn(m_env, entry.txn_id, &txn);
        st = ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeTxnCommit: {
        Transaction *txn = 0;
        recover_get_txn(m_env, entry.txn_id, &txn);
        st = ham_txn_commit((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeInsert: {
        PJournalEntryInsert *ins = (PJournalEntryInsert *)buffer.get_ptr();
        Transaction *txn;
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
        recover_get_txn(m_env, entry.txn_id, &txn);
        __recover_get_db(m_env, entry.dbname, &db);
        st = ham_db_insert((ham_db_t *)db, (ham_txn_t *)txn, 
                    &key, &record, ins->insert_flags | HAM_DONT_LOCK);
        break;
      }
      case kEntryTypeErase: {
        PJournalEntryErase *e = (PJournalEntryErase *)buffer.get_ptr();
        Transaction *txn;
        Database *db;
        ham_key_t key = {0};
        if (!e) {
          st = HAM_IO_ERROR;
          goto bail;
        }

        // do not erase if the key was already erased from disk
        if (entry.lsn <= start_lsn)
          continue;

        recover_get_txn(m_env, entry.txn_id, &txn);
        __recover_get_db(m_env, entry.dbname, &db);
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

      m_lsn = entry.lsn;
  } while (1);

bail:
  // all transactions which are not yet committed will be aborted
  (void)__abort_uncommitted_txns(m_env);

  // also close and delete all open databases - they were created in
  // __recover_get_db()
  (void)__close_all_databases(m_env);

  // re-enable the logging
  m_disable_logging = false;

  if (st)
    throw Exception(st);

  /* clear the journal files */
  clear();
}

void
Journal::clear_file(int idx)
{
  if (m_fd[idx] != HAM_INVALID_FD) {
    os_truncate(m_fd[idx], sizeof(PJournalHeader));

    // after truncate, the file pointer is far beyond the new end of file;
    // reset the file pointer, or the next write will resize the file to
    // the original size
    os_seek(m_fd[idx], sizeof(PJournalHeader), HAM_OS_SEEK_SET);
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

  if (m_env->get_log_directory().empty()) {
    path = m_env->get_filename();
  }
  else {
    path = m_env->get_log_directory();
#ifdef HAM_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(m_env->get_filename().c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)m_env->get_filename().c_str());
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
