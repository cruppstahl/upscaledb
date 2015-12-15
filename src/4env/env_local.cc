/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "2compressor/compressor_factory.h"
#include "2device/device_factory.h"
#include "3btree/btree_index.h"
#include "3btree/btree_stats.h"
#include "3blob_manager/blob_manager_factory.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db_local.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"
#include "4cursor/cursor.h"
#include "4context/context.h"
#include "4txn/txn_cursor.h"
#include "4uqi/parser.h"
#include "4uqi/statements.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

namespace upscaledb {

LocalEnvironment::LocalEnvironment(EnvironmentConfiguration &config)
  : Environment(config)
{
}

ups_status_t
LocalEnvironment::select_range(const char *query, Cursor *begin,
                            const Cursor *end, Result **result)
{
  ups_status_t st;

  // Parse the string into a SelectStatement object
  SelectStatement stmt;
  st = Parser::parse_select(query, stmt);
  if (st)
    return (st);

  // load (or open) the database
  bool is_opened = false;
  LocalDatabase *db;
  st = get_or_open_database(stmt.dbid, &db, &is_opened);
  if (st)
    return (st);

  // if Cursors are passed: check if they belong to this database
  if (begin && begin->db()->name() != stmt.dbid) {
    ups_log(("cursor 'begin' uses wrong database"));
    return (UPS_INV_PARAMETER);
  }
  if (end && end->db()->name() != stmt.dbid) {
    ups_log(("cursor 'begin' uses wrong database"));
    return (UPS_INV_PARAMETER);
  }

  // optimization: if duplicates are disabled then the query is always
  // non-distinct
  if (!(db->get_flags() & UPS_ENABLE_DUPLICATE_KEYS))
    stmt.distinct = true;

  // The Database object will do the remaining work
  st = db->select_range(&stmt, (LocalCursor *)begin,
                    (LocalCursor *)end, result);

  // Don't leak the database handle if it was opened above
  if (is_opened)
    (void)ups_db_close((ups_db_t *)db, UPS_DONT_LOCK);

  return (st);
}

ups_status_t
LocalEnvironment::get_or_open_database(uint16_t dbname, LocalDatabase **pdb,
                        bool *is_opened)
{
  LocalDatabase *db;

  *is_opened = false;
  *pdb = 0;

  DatabaseMap::iterator it = m_database_map.find(dbname);
  if (it == m_database_map.end()) {
    DatabaseConfiguration config(dbname);
    ups_status_t st = do_open_db((Database **)&db, config, 0);
    if (st != 0) {
      (void)ups_db_close((ups_db_t *)db, UPS_DONT_LOCK);
      delete db;
      return (st);
    }
    m_database_map[dbname] = db;
    *is_opened = true;
    *pdb = db;
  }
  else
    *pdb = (LocalDatabase *)it->second;

  return (0);
}

void
LocalEnvironment::recover(uint32_t flags)
{
  Context context(this);

  ups_status_t st = 0;
  m_journal.reset(new Journal(this));

  ups_assert(get_flags() & UPS_ENABLE_TRANSACTIONS);

  try {
    m_journal->open();
  }
  catch (Exception &ex) {
    if (ex.code == UPS_FILE_NOT_FOUND) {
      m_journal->create();
      return;
    }
  }

  /* success - check if we need recovery */
  if (!m_journal->is_empty()) {
    if (flags & UPS_AUTO_RECOVERY) {
      m_journal->recover((LocalTransactionManager *)m_txn_manager.get());
    }
    else {
      st = UPS_NEED_RECOVERY;
    }
  }

  /* in case of errors: close log and journal, but do not delete the files */
  if (st) {
    m_journal->close(true);
    throw Exception(st);
  }

  /* reset the page manager */
  m_page_manager->reset(&context);
}

PBtreeHeader *
LocalEnvironment::btree_header(int i)
{
  PBtreeHeader *d = (PBtreeHeader *)
        (m_header->header_page()->get_payload() + sizeof(PEnvironmentHeader));
  return (d + i);
}

LocalEnvironmentTest
LocalEnvironment::test()
{
  return (LocalEnvironmentTest(this));
}

ups_status_t
LocalEnvironment::do_create()
{
  if (m_config.flags & UPS_IN_MEMORY)
    m_config.flags |= UPS_DISABLE_RECLAIM_INTERNAL;

  /* initialize the device if it does not yet exist */
  m_device.reset(DeviceFactory::create(m_config));
  if (m_config.flags & UPS_ENABLE_TRANSACTIONS)
    m_txn_manager.reset(new LocalTransactionManager(this));

  /* create the file */
  m_device->create();

  /* allocate the header page */
  Page *page = new Page(m_device.get());
  page->alloc(Page::kTypeHeader, m_config.page_size_bytes);
  ::memset(page->get_data(), 0, m_config.page_size_bytes);
  page->set_type(Page::kTypeHeader);
  page->set_dirty(true);

  m_header.reset(new EnvironmentHeader(page));

  /* initialize the header */
  m_header->set_magic('H', 'A', 'M', '\0');
  m_header->set_version(UPS_VERSION_MAJ, UPS_VERSION_MIN, UPS_VERSION_REV,
          UPS_FILE_VERSION);
  m_header->set_page_size(m_config.page_size_bytes);
  m_header->set_max_databases(m_config.max_databases);

  /* load page manager after setting up the blobmanager and the device! */
  m_page_manager.reset(new PageManager(this));

  /* the blob manager needs a device and an initialized page manager */
  m_blob_manager.reset(BlobManagerFactory::create(this, m_config.flags));

  /* create a logfile and a journal (if requested) */
  if ((get_flags() & UPS_ENABLE_TRANSACTIONS)
      && (!(get_flags() & UPS_DISABLE_RECOVERY))) {
    m_journal.reset(new Journal(this));
    m_journal->create();
  }

  /* Now that the header was created we can finally store the compression
   * information */
  if (m_config.journal_compressor)
    m_header->set_journal_compression(m_config.journal_compressor);

  /* flush the header page - this will write through disk if logging is
   * enabled */
  if (m_journal.get())
    Page::flush(m_device.get(), m_header->header_page()->get_persisted_data());

  return (0);
}

ups_status_t
LocalEnvironment::do_open()
{
  ups_status_t st = 0;

  Context context(this);

  /* Initialize the device if it does not yet exist. The page size will
   * be filled in later (at this point in time, it's still unknown) */
  m_device.reset(DeviceFactory::create(m_config));

  /* open the file */
  m_device->open();

  if (m_config.flags & UPS_ENABLE_TRANSACTIONS)
    m_txn_manager.reset(new LocalTransactionManager(this));

  /*
   * read the database header
   *
   * !!!
   * now this is an ugly problem - the database header spans one page, but
   * what's the size of this page? chances are good that it's the default
   * page-size, but we really can't be sure.
   *
   * read 512 byte and extract the "real" page size, then read
   * the real page.
   */
  {
    Page *page = 0;
    uint8_t hdrbuf[512];

    /*
     * in here, we're going to set up a faked headerpage for the
     * duration of this call; BE VERY CAREFUL: we MUST clean up
     * at the end of this section or we'll be in BIG trouble!
     */
    Page fakepage(m_device.get());
    fakepage.set_data((PPageData *)hdrbuf);

    /* create the configuration object */
    m_header.reset(new EnvironmentHeader(&fakepage));

    /*
     * now fetch the header data we need to get an estimate of what
     * the database is made of really.
     */
    m_device->read(0, hdrbuf, sizeof(hdrbuf));

    m_config.page_size_bytes = m_header->page_size();

    /** check the file magic */
    if (!m_header->verify_magic('H', 'A', 'M', '\0')) {
      ups_log(("invalid file type"));
      st =  UPS_INV_FILE_HEADER;
      goto fail_with_fake_cleansing;
    }

    /* Check the database version; everything with a different file version
     * is incompatible.
     *
     * The msb was set to distinguish the PRO version. It is ignored here to
     * remain compatible with PRO. This can be removed when the
     * UPS_FILE_VERSION is incremented (current value: 4).
     */
    if ((m_header->version(3) & ~0x80) != UPS_FILE_VERSION) {
      ups_log(("invalid file version"));
      st = UPS_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }
    else if (m_header->version(0) == 1 &&
      m_header->version(1) == 0 &&
      m_header->version(2) <= 9) {
      ups_log(("invalid file version; < 1.0.9 is not supported"));
      st = UPS_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }

    st = 0;

fail_with_fake_cleansing:

    /* undo the headerpage fake first! */
    fakepage.set_data(0);
    m_header.reset(0);

    /* exit when an error was signaled */
    if (st) {
      if (m_device->is_open())
        m_device->close();
      return (st);
    }

    /* now read the "real" header page and store it in the Environment */
    page = new Page(m_device.get());
    page->fetch(0);
    m_header.reset(new EnvironmentHeader(page));
  }

  /* Now that the header page was fetched we can retrieve the compression
   * information */
  m_config.journal_compressor = m_header->journal_compression();

  /* load page manager after setting up the blobmanager and the device! */
  m_page_manager.reset(new PageManager(this));

  /* the blob manager needs a device and an initialized page manager */
  m_blob_manager.reset(BlobManagerFactory::create(this, m_config.flags));

  /* check if recovery is required */
  if (get_flags() & UPS_ENABLE_TRANSACTIONS)
    recover(m_config.flags);

  /* load the state of the PageManager */
  if (m_header->page_manager_blobid() != 0)
    m_page_manager->initialize(m_header->page_manager_blobid());

  return (0);
}

ups_status_t
LocalEnvironment::do_get_database_names(uint16_t *names, uint32_t *count)
{
  uint16_t name;
  uint32_t i = 0;
  uint32_t max_names = 0;

  max_names = *count;
  *count = 0;

  /* copy each database name to the array */
  ups_assert(m_header->max_databases() > 0);
  for (i = 0; i < m_header->max_databases(); i++) {
    name = btree_header(i)->dbname;
    if (name == 0)
      continue;

    if (*count >= max_names)
      return (UPS_LIMITS_REACHED);

    names[(*count)++] = name;
  }

  return 0;
}

ups_status_t
LocalEnvironment::do_get_parameters(ups_parameter_t *param)
{
  ups_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case UPS_PARAM_CACHE_SIZE:
        p->value = m_config.cache_size_bytes;
        break;
      case UPS_PARAM_PAGE_SIZE:
        p->value = m_config.page_size_bytes;
        break;
      case UPS_PARAM_MAX_DATABASES:
        p->value = m_header->max_databases();
        break;
      case UPS_PARAM_FLAGS:
        p->value = get_flags();
        break;
      case UPS_PARAM_FILEMODE:
        p->value = m_config.file_mode;
        break;
      case UPS_PARAM_FILENAME:
        if (m_config.filename.size())
          p->value = (uint64_t)(PTR_TO_U64(m_config.filename.c_str()));
        else
          p->value = 0;
        break;
      case UPS_PARAM_LOG_DIRECTORY:
        if (m_config.log_filename.size())
          p->value = (uint64_t)(PTR_TO_U64(m_config.log_filename.c_str()));
        else
          p->value = 0;
        break;
      case UPS_PARAM_JOURNAL_SWITCH_THRESHOLD:
        p->value = m_config.journal_switch_threshold;
        break;
      case UPS_PARAM_JOURNAL_COMPRESSION:
        p->value = m_config.journal_compressor;
        break;
      case UPS_PARAM_POSIX_FADVISE:
        p->value = m_config.posix_advice;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)p->name));
        return (UPS_INV_PARAMETER);
      }
    }
  }

  return (0);
}

ups_status_t
LocalEnvironment::do_flush(uint32_t flags)
{
  Context context(this, 0, 0);

  /* flush all committed transactions */
  if (m_txn_manager)
    m_txn_manager->flush_committed_txns(&context);

  if (flags & UPS_FLUSH_COMMITTED_TRANSACTIONS || get_flags() & UPS_IN_MEMORY)
    return (0);

  /* Flush all open pages to disk. This operation is blocking. */
  m_page_manager->flush_all_pages();

  /* Flush the device - this can trigger a fsync() if enabled */
  m_device->flush();

  return (0);
}

ups_status_t
LocalEnvironment::do_create_db(Database **pdb, DatabaseConfiguration &config,
                const ups_parameter_t *param)
{
  if (get_flags() & UPS_READ_ONLY) {
    ups_trace(("cannot create database in a read-only environment"));
    return (UPS_WRITE_PROTECTED);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case UPS_PARAM_RECORD_COMPRESSION:
          if (!CompressorFactory::is_available(param->value)) {
            ups_trace(("unknown algorithm for record compression"));
            return (UPS_INV_PARAMETER);
          }
          config.record_compressor = (int)param->value;
          break;
        case UPS_PARAM_KEY_COMPRESSION:
          if (!CompressorFactory::is_available(param->value)) {
            ups_trace(("unknown algorithm for key compression"));
            return (UPS_INV_PARAMETER);
          }
          config.key_compressor = (int)param->value;
          break;
        case UPS_PARAM_KEY_TYPE:
          config.key_type = (uint16_t)param->value;
          break;
        case UPS_PARAM_KEY_SIZE:
          if (param->value != 0) {
            if (param->value > 0xffff) {
              ups_trace(("invalid key size %u - must be < 0xffff"));
              return (UPS_INV_KEY_SIZE);
            }
            if (config.flags & UPS_RECORD_NUMBER32) {
              if (param->value > 0 && param->value != sizeof(uint32_t)) {
                ups_trace(("invalid key size %u - must be 4 for "
                           "UPS_RECORD_NUMBER32 databases",
                           (unsigned)param->value));
                return (UPS_INV_KEY_SIZE);
              }
            }
            if (config.flags & UPS_RECORD_NUMBER64) {
              if (param->value > 0 && param->value != sizeof(uint64_t)) {
                ups_trace(("invalid key size %u - must be 8 for "
                           "UPS_RECORD_NUMBER64 databases",
                           (unsigned)param->value));
                return (UPS_INV_KEY_SIZE);
              }
            }
            config.key_size = (uint16_t)param->value;
          }
          break;
        case UPS_PARAM_RECORD_TYPE:
          config.record_type = (uint16_t)param->value;
          break;
        case UPS_PARAM_RECORD_SIZE:
          config.record_size = (uint32_t)param->value;
          break;
        case UPS_PARAM_CUSTOM_COMPARE_NAME:
          config.compare_name = reinterpret_cast<const char *>(param->value);
          break;
        default:
          ups_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          return (UPS_INV_PARAMETER);
      }
    }
  }

  if (config.flags & UPS_RECORD_NUMBER32) {
    if (config.key_type == UPS_TYPE_UINT8
        || config.key_type == UPS_TYPE_UINT16
        || config.key_type == UPS_TYPE_UINT64
        || config.key_type == UPS_TYPE_REAL32
        || config.key_type == UPS_TYPE_REAL64) {
      ups_trace(("UPS_RECORD_NUMBER32 not allowed in combination with "
                      "fixed length type"));
      return (UPS_INV_PARAMETER);
    }
    config.key_type = UPS_TYPE_UINT32;
  }
  else if (config.flags & UPS_RECORD_NUMBER64) {
    if (config.key_type == UPS_TYPE_UINT8
        || config.key_type == UPS_TYPE_UINT16
        || config.key_type == UPS_TYPE_UINT32
        || config.key_type == UPS_TYPE_REAL32
        || config.key_type == UPS_TYPE_REAL64) {
      ups_trace(("UPS_RECORD_NUMBER64 not allowed in combination with "
                      "fixed length type"));
      return (UPS_INV_PARAMETER);
    }
    config.key_type = UPS_TYPE_UINT64;
  }

  // uint32 compression is only allowed for uint32-keys
  if (config.key_compressor == UPS_COMPRESSOR_UINT32_VARBYTE
      || config.key_compressor == UPS_COMPRESSOR_UINT32_FOR
      || config.key_compressor == UPS_COMPRESSOR_UINT32_SIMDFOR
      || config.key_compressor == UPS_COMPRESSOR_UINT32_SIMDCOMP
      || config.key_compressor == UPS_COMPRESSOR_UINT32_GROUPVARINT
      || config.key_compressor == UPS_COMPRESSOR_UINT32_STREAMVBYTE
      || config.key_compressor == UPS_COMPRESSOR_UINT32_MASKEDVBYTE) {
    if (config.key_type != UPS_TYPE_UINT32) {
      ups_trace(("Uint32 compression only allowed for uint32 keys "
                 "(UPS_TYPE_UINT32)"));
      return (UPS_INV_PARAMETER);
    }
    if (m_config.page_size_bytes != 16 * 1024) {
      ups_trace(("Uint32 compression only allowed for page size of 16k"));
      return (UPS_INV_PARAMETER);
    }
  }

  // all heavy-weight compressors are only allowed for
  // variable-length binary keys
  if (config.key_compressor == UPS_COMPRESSOR_LZF
        || config.key_compressor == UPS_COMPRESSOR_SNAPPY
        || config.key_compressor == UPS_COMPRESSOR_ZLIB) {
    if (config.key_type != UPS_TYPE_BINARY
          || config.key_size != UPS_KEY_SIZE_UNLIMITED) {
      ups_trace(("Key compression only allowed for unlimited binary keys "
                 "(UPS_TYPE_BINARY"));
      return (UPS_INV_PARAMETER);
    }
  }

  uint32_t mask = UPS_FORCE_RECORDS_INLINE
                    | UPS_FLUSH_WHEN_COMMITTED
                    | UPS_ENABLE_DUPLICATE_KEYS
                    | UPS_RECORD_NUMBER32
                    | UPS_RECORD_NUMBER64;
  if (config.flags & ~mask) {
    ups_trace(("invalid flags(s) 0x%x", config.flags & ~mask));
    return (UPS_INV_PARAMETER);
  }

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, config);

  Context context(this, 0, db);

  /* check if this database name is unique */
  uint16_t dbi;
  for (uint32_t i = 0; i < m_header->max_databases(); i++) {
    uint16_t name = btree_header(i)->dbname;
    if (!name)
      continue;
    if (name == config.db_name) {
      delete db;
      return (UPS_DATABASE_ALREADY_EXISTS);
    }
  }

  /* find a free slot in the PBtreeHeader array and store the name */
  for (dbi = 0; dbi < m_header->max_databases(); dbi++) {
    uint16_t name = btree_header(dbi)->dbname;
    if (!name) {
      btree_header(dbi)->dbname = config.db_name;
      break;
    }
  }
  if (dbi == m_header->max_databases()) {
    delete db;
    return (UPS_LIMITS_REACHED);
  }

  mark_header_page_dirty(&context);

  /* initialize the Database */
  ups_status_t st = db->create(&context, btree_header(dbi));
  if (st) {
    delete db;
    return (st);
  }

  /* force-flush the changeset */
  if (journal())
    context.changeset.flush(next_lsn());

  *pdb = db;
  return (0);
}

ups_status_t
LocalEnvironment::do_open_db(Database **pdb, DatabaseConfiguration &config,
                const ups_parameter_t *param)
{
  *pdb = 0;

  uint32_t mask = UPS_FORCE_RECORDS_INLINE
                    | UPS_FLUSH_WHEN_COMMITTED
                    | UPS_PARAM_JOURNAL_COMPRESSION
                    | UPS_READ_ONLY;
  if (config.flags & ~mask) {
    ups_trace(("invalid flags(s) 0x%x", config.flags & ~mask));
    return (UPS_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case UPS_PARAM_RECORD_COMPRESSION:
          ups_trace(("Record compression parameters are only allowed in "
                     "ups_env_create_db"));
          return (UPS_INV_PARAMETER);
        case UPS_PARAM_KEY_COMPRESSION:
          ups_trace(("Key compression parameters are only allowed in "
                     "ups_env_create_db"));
          return (UPS_INV_PARAMETER);
        default:
          ups_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          return (UPS_INV_PARAMETER);
      }
    }
  }

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, config);

  Context context(this, 0, db);

  ups_assert(0 != m_header->header_page());

  /* search for a database with this name */
  uint16_t dbi;
  for (dbi = 0; dbi < m_header->max_databases(); dbi++) {
    uint16_t name = btree_header(dbi)->dbname;
    if (!name)
      continue;
    if (config.db_name == name)
      break;
  }

  if (dbi == m_header->max_databases()) {
    delete db;
    return (UPS_DATABASE_NOT_FOUND);
  }

  /* open the database */
  ups_status_t st = db->open(&context, btree_header(dbi));
  if (st) {
    delete db;
    ups_trace(("Database could not be opened"));
    return (st);
  }

  *pdb = db;
  return (0);
}

ups_status_t
LocalEnvironment::do_rename_db(uint16_t oldname, uint16_t newname,
                uint32_t flags)
{
  Context context(this);

  /*
   * check if a database with the new name already exists; also search
   * for the database with the old name
   */
  uint16_t max = m_header->max_databases();
  uint16_t slot = max;
  ups_assert(max > 0);
  for (uint16_t dbi = 0; dbi < max; dbi++) {
    uint16_t name = btree_header(dbi)->dbname;
    if (name == newname)
      return (UPS_DATABASE_ALREADY_EXISTS);
    if (name == oldname)
      slot = dbi;
  }

  if (slot == max)
    return (UPS_DATABASE_NOT_FOUND);

  /* replace the database name with the new name */
  btree_header(slot)->dbname = newname;
  mark_header_page_dirty(&context);

  /* if the database with the old name is currently open: notify it */
  Environment::DatabaseMap::iterator it = m_database_map.find(oldname);
  if (it != m_database_map.end()) {
    Database *db = it->second;
    it->second->set_name(newname);
    m_database_map.erase(oldname);
    m_database_map.insert(DatabaseMap::value_type(newname, db));
  }

  return (0);
}

ups_status_t
LocalEnvironment::do_erase_db(uint16_t name, uint32_t flags)
{
  /* check if this database is still open */
  if (m_database_map.find(name) != m_database_map.end())
    return (UPS_DATABASE_ALREADY_OPEN);

  /*
   * if it's an in-memory environment then it's enough to purge the
   * database from the environment header
   */
  if (get_flags() & UPS_IN_MEMORY) {
    for (uint16_t dbi = 0; dbi < m_header->max_databases(); dbi++) {
      PBtreeHeader *desc = btree_header(dbi);
      if (name == desc->dbname) {
        desc->dbname = 0;
        return (0);
      }
    }
    return (UPS_DATABASE_NOT_FOUND);
  }

  /* temporarily load the database */
  LocalDatabase *db;
  DatabaseConfiguration config;
  config.db_name = name;
  ups_status_t st = do_open_db((Database **)&db, config, 0);
  if (st)
    return (st);

  Context context(this, 0, db);

  /*
   * delete all blobs and extended keys, also from the cache and
   * the extkey-cache
   *
   * also delete all pages and move them to the freelist; if they're
   * cached, delete them from the cache
   */
  st = db->drop(&context);
  if (st)
    return (st);

  /* now set database name to 0 and set the header page to dirty */
  for (uint16_t dbi = 0; dbi < m_header->max_databases(); dbi++) {
    PBtreeHeader *desc = btree_header(dbi);
    if (name == desc->dbname) {
      desc->dbname = 0;
      break;
    }
  }

  mark_header_page_dirty(&context);
  context.changeset.clear();

  (void)ups_db_close((ups_db_t *)db, UPS_DONT_LOCK);

  return (0);
}

Transaction *
LocalEnvironment::do_txn_begin(const char *name, uint32_t flags)
{
  Transaction *txn = new LocalTransaction(this, name, flags);
  m_txn_manager->begin(txn);
  return (txn);
}

ups_status_t
LocalEnvironment::do_txn_commit(Transaction *txn, uint32_t flags)
{
  return (m_txn_manager->commit(txn, flags));
}

ups_status_t
LocalEnvironment::do_txn_abort(Transaction *txn, uint32_t flags)
{
  return (m_txn_manager->abort(txn, flags));
}

ups_status_t
LocalEnvironment::do_close(uint32_t flags)
{
  Context context(this);

  /* flush all committed transactions */
  if (m_txn_manager)
    m_txn_manager->flush_committed_txns(&context);

  /* flush all pages and the freelist, reduce the file size */
  if (m_page_manager)
    m_page_manager->close(&context);

  /* close the header page */
  if (m_header && m_header->header_page()) {
    Page *page = m_header->header_page();
    if (page->get_data())
      m_device->free_page(page);
    delete page;
    m_header.reset();
  }

  /* close the device */
  if (m_device) {
    if (m_device->is_open()) {
      if (!(get_flags() & UPS_READ_ONLY))
        m_device->flush();
      m_device->close();
    }
  }

  /* close the log and the journal */
  if (m_journal)
    m_journal->close(!!(flags & UPS_DONT_CLEAR_LOG));

  return (0);
}

void
LocalEnvironment::do_fill_metrics(ups_env_metrics_t *metrics) const
{
  // PageManager metrics (incl. cache and freelist)
  m_page_manager->fill_metrics(metrics);
  // the BlobManagers
  m_blob_manager->fill_metrics(metrics);
  // the Journal (if available)
  if (m_journal)
    m_journal->fill_metrics(metrics);
  // the (first) database
  if (!m_database_map.empty()) {
    LocalDatabase *db = (LocalDatabase *)m_database_map.begin()->second;
    db->fill_metrics(metrics);
  }
  // and of the btrees
  BtreeIndex::fill_metrics(metrics);
  // SIMD support enabled?
  metrics->simd_lane_width = os_get_simd_lane_width();
}

void
LocalEnvironmentTest::set_journal(Journal *journal)
{
    m_env->m_journal.reset(journal);
}

} // namespace upscaledb
