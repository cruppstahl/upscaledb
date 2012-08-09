/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
#include "env.h"
#include "error.h"
#include "mem.h"
#include "log.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "journal.h"

static ham_size_t 
__get_aligned_entry_size(ham_size_t s)
{
  s += 8 - 1;
  s -= (s % 8);
  return (s);
}

ham_status_t
Journal::create()
{
  int i;
  Header header;
  ham_status_t st;

  /* initialize the magic */
  memset(&header, 0, sizeof(header));
  header.magic = HEADER_MAGIC;

  /* create the two files */
  for (i = 0; i < 2; i++) {
    std::string path = get_path(i);
    st = os_create(path.c_str(), 0, 0644, &m_fd[i]);
    if (st) {
      (void)close();
      return (st);
    }

    /* and write the magic */
    st = os_write(m_fd[i], &header, sizeof(header));
    if (st) {
      (void)close();
      return (st);
    }
  }
  return (st);
}

ham_status_t
Journal::open()
{
  int i;
  Header header;
  JournalEntry entry;
  ham_u64_t lsn[2];
  ham_status_t st, st1, st2;

  memset(&header, 0, sizeof(header));
  m_current_fd = 0;

  /* open the two files; if the files do not exist then create them */
  std::string path = get_path(0);
  st1 = os_open(path.c_str(), 0, &m_fd[0]);
  path = get_path(1);
  st2 = os_open(path.c_str(), 0, &m_fd[1]);

  if (st1 == st2 && st2 == HAM_FILE_NOT_FOUND)
    return (st1);

  if (st1 || st2) {
    (void)close();
    return (st1 ? st1 : st2);
  }

  for (i = 0; i < 2; i++) {
    /* check the magic */
    st = os_pread(m_fd[i], 0, &header, sizeof(header));
    if (st) {
      (void)close();
      return (st);
    }
    if (header.magic != HEADER_MAGIC) {
      ham_trace(("journal has unknown magic or is corrupt"));
      (void)close();
      return (HAM_LOG_INV_FILE_HEADER);
    }

    /* read the lsn from the header structure */
    if (m_lsn < header.lsn)
      m_lsn = header.lsn;
  }

  /* However, we now just read the lsn from the header structure. if there
   * are any additional logfile entries then the lsn of those entries is
   * more up-to-date than the one in the header structure. */
  for (i = 0; i < 2; i++) {
    /* but make sure that the file is large enough! */
    ham_offset_t size;
    st = os_get_filesize(m_fd[i], &size);
    if (st) {
      (void)close();
      return (st);
    }

    if (size >= sizeof(entry)) {
      st = os_pread(m_fd[i], size - sizeof(JournalEntry),
                    &entry, sizeof(entry));
      if (st) {
        (void)close();
        return (st);
      }
      lsn[i] = entry.lsn;
    }
    else
      lsn[i] = 0;
  }

  /* The file with the higher lsn will become file[0] */
  if (lsn[1] > lsn[0]) {
    std::swap(m_fd[0], m_fd[1]);
    if (m_lsn < lsn[1])
      m_lsn = lsn[1];
  }
  else {
    if (m_lsn < lsn[0])
      m_lsn = lsn[0];
  }

  return (0);
}

ham_status_t
Journal::append_txn_begin(Transaction *txn, Environment *env, 
                const char *name, ham_u64_t lsn)
{
  ham_status_t st;
  JournalEntry entry;
  int cur = m_current_fd;
  int other = cur ? 0 : 1;

  entry.txn_id = txn_get_id(txn);
  entry.type = ENTRY_TYPE_TXN_BEGIN;
  entry.lsn = lsn;
  if (name)
    entry.followup_size = strlen(name) + 1;

  /* 
   * determine the journal file which is used for this transaction 
   *
   * if the "current" file is not yet full, continue to write to this file
   */
  if (m_open_txn[cur] + m_closed_txn[cur] < m_threshold) {
    txn_set_log_desc(txn, cur);
  }
  else if (m_open_txn[other] == 0) {
    /*
     * Otherwise, if the other file does no longer have open Transactions,
     * delete the other file and use the other file as the current file
     */
    st = clear_file(other);
    if (st)
      return (st);
    cur = other;
    m_current_fd = cur;
    txn_set_log_desc(txn, cur);
  }
  /*
   * Otherwise continue writing to the current file, till the other file
   * can be deleted safely
   */
  else
    txn_set_log_desc(txn, cur);

  if (txn_get_name(txn))
    st = append_entry(cur, (void *)&entry, (ham_size_t)sizeof(entry),
                    (void *)txn_get_name(txn),
                    (ham_size_t)strlen(txn_get_name(txn)) + 1);
  else
    st = append_entry(cur, (void *)&entry, (ham_size_t)sizeof(entry));
  if (st)
    return (st);
  m_open_txn[cur]++;

  /* store the fp-index in the journal structure; it's needed for
   * journal_append_checkpoint() to quickly find out which file is 
   * the newest */
  m_current_fd = cur;

  return (0);
}

ham_status_t
Journal::append_txn_abort(Transaction *txn, ham_u64_t lsn)
{
  int idx;
  ham_status_t st;
  JournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn_get_id(txn);
  entry.type = ENTRY_TYPE_TXN_ABORT;

  /* update the transaction counters of this logfile */
  idx = txn_get_log_desc(txn);
  m_open_txn[idx]--;
  m_closed_txn[idx]++;

  st = append_entry(idx, &entry, sizeof(entry));
  if (st)
    return (st);
  if (m_env->get_flags() & HAM_WRITE_THROUGH)
    return (os_flush(m_fd[idx]));
  return (0);
}

ham_status_t
Journal::append_txn_commit(Transaction *txn, ham_u64_t lsn)
{
  int idx;
  ham_status_t st;
  JournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn_get_id(txn);
  entry.type = ENTRY_TYPE_TXN_COMMIT;

  /* update the transaction counters of this logfile */
  idx = txn_get_log_desc(txn);
  m_open_txn[idx]--;
  m_closed_txn[idx]++;

  st = append_entry(idx, &entry, sizeof(entry));
  if (st)
    return (st);
  if (m_env->get_flags() & HAM_WRITE_THROUGH)
    return (os_flush(m_fd[idx]));
  return (0);
}

ham_status_t
Journal::append_insert(Database *db, Transaction *txn, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags, 
                ham_u64_t lsn)
{
  char padding[16] = {0};
  JournalEntry entry;
  JournalEntryInsert insert;
  ham_size_t size = sizeof(JournalEntryInsert) + key->size + record->size - 1;
  ham_size_t padding_size = __get_aligned_entry_size(size) - size;

  entry.lsn = lsn;
  entry.dbname = db->get_name();
  entry.txn_id = txn_get_id(txn);
  entry.type = ENTRY_TYPE_INSERT;
  entry.followup_size = size + padding_size;

  insert.key_size = key->size;
  insert.record_size = record->size;
  insert.record_partial_size = record->partial_size;
  insert.record_partial_offset = record->partial_offset;
  insert.insert_flags = flags;

  /* append the entry to the logfile */
  return (append_entry(txn_get_log_desc(txn), 
                &entry, sizeof(entry),
                &insert, sizeof(JournalEntryInsert) - 1,
                key->data, key->size,
                record->data, record->size,
                padding, padding_size));
}

ham_status_t
Journal::append_erase(Database *db, Transaction *txn, ham_key_t *key, 
                ham_u32_t dupe, ham_u32_t flags, ham_u64_t lsn)
{
  char padding[16] = {0};
  JournalEntry entry;
  JournalEntryErase erase;
  ham_size_t size = sizeof(JournalEntryErase) + key->size - 1;
  ham_size_t padding_size = __get_aligned_entry_size(size) - size;

  entry.lsn = lsn;
  entry.dbname = db->get_name();
  entry.txn_id = txn_get_id(txn);
  entry.type = ENTRY_TYPE_ERASE;
  entry.followup_size = size + padding_size;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = dupe;

  /* append the entry to the logfile */
  return (append_entry(txn_get_log_desc(txn), 
                &entry, sizeof(entry),
                (JournalEntry *)&erase, sizeof(JournalEntryErase) - 1,
                key->data, key->size,
                padding, padding_size));
}

ham_status_t
Journal::get_entry(Iterator *iter, JournalEntry *entry, void **aux)
{
  ham_status_t st;
  ham_offset_t filesize;

  *aux = 0;

  /* if iter->offset is 0, then the iterator was created from scratch
   * and we start reading from the first (oldest) entry.
   *
   * The oldest of the two logfiles is always the "other" one (the one
   * NOT in current_fd).
   */
  if (iter->offset == 0) {
    iter->fdstart = iter->fdidx=
                  m_current_fd == 0
                      ? 1
                      : 0;
    iter->offset = sizeof(Header);
  }

  /* get the size of the journal file */
  st = os_get_filesize(m_fd[iter->fdidx], &filesize);
  if (st)
    return (st);

  /* reached EOF? then either skip to the next file or we're done */
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = sizeof(Header);
      st = os_get_filesize(m_fd[iter->fdidx], &filesize);
      if (st)
        return (st);
    }
    else {
      entry->lsn = 0;
      return (0);
    }
  }

  /* second file is also empty? then return */
  if (filesize == iter->offset) {
    entry->lsn = 0;
    return (0);
  }

  /* now try to read the next entry */
  st = os_pread(m_fd[iter->fdidx], iter->offset, entry, sizeof(*entry));
  if (st)
    return (st);

  iter->offset += sizeof(*entry);

  /* read auxiliary data if it's available */
  if (entry->followup_size) {
    *aux = allocate((ham_size_t)entry->followup_size);
    if (!*aux)
      return (HAM_OUT_OF_MEMORY);

    st = os_pread(m_fd[iter->fdidx], iter->offset, *aux, entry->followup_size);
    if (st) {
      alloc_free(*aux);
      *aux = 0;
      return (st);
    }

    iter->offset += entry->followup_size;
  }

  return (0);
}

ham_status_t
Journal::close(bool noclear)
{
  int i;
  ham_status_t st = 0;

  if (!noclear) {
    Header header;

    (void)clear();

    /* update the header page of file 0 to store the lsn */
    header.magic = HEADER_MAGIC;
    header.lsn = m_lsn;

    if (m_fd[0] != HAM_INVALID_FD)
        st = os_pwrite(m_fd[0], 0, &header, sizeof(header));
  }

  for (i = 0; i < 2; i++) {
    if (m_fd[i] != HAM_INVALID_FD) {
      (void)os_close(m_fd[i]);
      m_fd[i] = HAM_INVALID_FD;
    }
  }

  return (st);
}

static ham_status_t
__recover_get_db(Environment *env, ham_u16_t dbname, Database **pdb)
{
  ham_status_t st;

  /* first check if the Database is already open */
  Database *db = env->get_databases();
  while (db) {
    ham_u16_t name = index_get_dbname(env->get_indexdata_ptr(
                            db->get_indexdata_offset()));
    if (dbname == name) {
      *pdb = db;
      return (0);
    }
    db = db->get_next();
  }

  /* not found - open it */
  st = ham_new((ham_db_t **)&db);
  if (st)
    return (st);

  st = ham_env_open_db((ham_env_t *)env, (ham_db_t *)db, dbname, 
                    HAM_DONT_LOCK, 0);
  if (st)
    return (st);

  *pdb = db;
  return (0);
}

static ham_status_t
__recover_get_txn(Environment *env, ham_u64_t txn_id, Transaction **ptxn)
{
  Transaction *txn = env->get_oldest_txn();
  while (txn) {
    if (txn_get_id(txn) == txn_id) {
      *ptxn = txn;
      return (0);
    }
    txn = txn_get_newer(txn);
  }

  ham_assert(!"shouldn't be here");
  *ptxn = 0;
  return (HAM_INTERNAL_ERROR);
}

static ham_status_t
__close_all_databases(Environment *env)
{
  ham_status_t st = 0;
  Database *db;

  while ((db = env->get_databases())) {
    st = ham_close((ham_db_t *)db, HAM_DONT_LOCK);
    if (st) {
      ham_log(("ham_close() failed w/ error %d (%s)", st, ham_strerror(st)));
      return (st);
    }

    delete db;
  }

  return (0);
}

static ham_status_t
__abort_uncommitted_txns(Environment *env)
{
  ham_status_t st;
  Transaction *newer, *txn = env->get_oldest_txn();

  while (txn) {
    newer = txn_get_newer(txn);
    if (!(txn_get_flags(txn) & TXN_STATE_COMMITTED)) {
      st = ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
      if (st)
        return (st);
    }
    txn = newer;
  }

  return (0);
}

ham_status_t
Journal::recover()
{
  ham_status_t st = 0;
  ham_u64_t start_lsn = m_env->get_log()->get_lsn();
  Iterator it;
  void *aux = 0;

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

  /* make sure that there are no pending transactions - start with 
   * a clean state! */
  ham_assert(m_env->get_oldest_txn() == 0);
  ham_assert(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS);
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  /* officially disable recovery - otherwise while recovering we log
   * more stuff */
  m_env->set_flags(m_env->get_flags() & ~HAM_ENABLE_RECOVERY);

  do {
    JournalEntry entry;

    if (aux) {
      alloc_free(aux);
      aux = 0;
    }

    /* get the next entry */
    st = get_entry(&it, &entry, (void **)&aux);
    if (st)
      goto bail;

    /* reached end of logfile? */
    if (!entry.lsn)
      break;

      /* re-apply this operation */
      switch (entry.type) {
      case ENTRY_TYPE_TXN_BEGIN: {
        Transaction *txn;
        st = ham_txn_begin((ham_txn_t **)&txn, (ham_env_t *)m_env, 
                (const char *)aux, 0, HAM_DONT_LOCK);
        /* on success: patch the txn ID */
        if (st == 0) {
            txn_set_id(txn, entry.txn_id);
            m_env->set_txn_id(entry.txn_id);
        }
        break;
      }
      case ENTRY_TYPE_TXN_ABORT: {
        Transaction *txn;
        st=__recover_get_txn(m_env, entry.txn_id, &txn);
        if (st)
            break;
        st=ham_txn_abort((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case ENTRY_TYPE_TXN_COMMIT: {
        Transaction *txn;
        st = __recover_get_txn(m_env, entry.txn_id, &txn);
        if (st)
          break;
        st = ham_txn_commit((ham_txn_t *)txn, HAM_DONT_LOCK);
        break;
      }
      case ENTRY_TYPE_INSERT: {
        JournalEntryInsert *ins = (JournalEntryInsert *)aux;
        Transaction *txn;
        Database *db;
        ham_key_t key = {0};
        ham_record_t record = {0};
        if (!ins) {
          st = HAM_IO_ERROR;
          goto bail;
        }

        /* do not insert if the key was already flushed to disk */
        if (entry.lsn <= start_lsn)
          continue;

        key.data = ins->get_key_data();
        key.size = ins->key_size;
        record.data = ins->get_record_data();
        record.size = ins->record_size;
        record.partial_size = ins->record_partial_size;
        record.partial_offset = ins->record_partial_offset;
        st = __recover_get_txn(m_env, entry.txn_id, &txn);
        if (st)
          break;
        st = __recover_get_db(m_env, entry.dbname, &db);
        if (st)
          break;
        st = ham_insert((ham_db_t *)db, (ham_txn_t *)txn, &key, &record,
                        ins->insert_flags | HAM_DONT_LOCK);
        break;
      }
      case ENTRY_TYPE_ERASE: {
        JournalEntryErase *e = (JournalEntryErase *)aux;
        Transaction *txn;
        Database *db;
        ham_key_t key = {0};
        if (!e) {
          st = HAM_IO_ERROR;
          goto bail;
        }

        /* do not erase if the key was already erased from disk */
        if (entry.lsn <= start_lsn)
          continue;

        st = __recover_get_txn(m_env, entry.txn_id, &txn);
        if (st)
          break;
        st = __recover_get_db(m_env, entry.dbname, &db);
        if (st)
          break;
        key.data = e->get_key_data();
        key.size = e->key_size;
        st = ham_erase((ham_db_t *)db, (ham_txn_t *)txn, &key, 
                      e->erase_flags|HAM_DONT_LOCK);
        // key might have already been erased when the changeset
        // was flushed
        if (st == HAM_KEY_NOT_FOUND)
          st = 0;
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
  if (aux) {
      alloc_free(aux);
      aux = 0;
  }

  /* all transactions which are not yet committed will be aborted */
  (void)__abort_uncommitted_txns(m_env);

  /* also close and delete all open databases - they were created in
   * __recover_get_db() */
  (void)__close_all_databases(m_env);

  /* restore original flags */
  m_env->set_flags(m_env->get_flags() | HAM_ENABLE_RECOVERY);

  if (st)
    return (st);

  /* clear the journal files */
  st = clear();
  if (st) {
    ham_log(("unable to clear journal; please manually delete the "
              "journal files before re-opening the Database"));
    return (st);
  }

  return (0);
}

ham_status_t
Journal::clear_file(int idx)
{
  ham_status_t st;

  st = os_truncate(m_fd[idx], sizeof(Header));
  if (st)
      return (st);

  /* after truncate, the file pointer is far beyond the new end of file;
   * reset the file pointer, or the next write will resize the file to
   * the original size */
  st = os_seek(m_fd[idx], sizeof(Header), HAM_OS_SEEK_SET);
  if (st)
    return (st);

  /* clear the transaction counters */
  m_open_txn[idx] = 0;
  m_closed_txn[idx] = 0;

  return (0);
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
