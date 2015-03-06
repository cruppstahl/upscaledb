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

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "2device/device_factory.h"
#include "3btree/btree_index.h"
#include "3btree/btree_stats.h"
#include "3blob_manager/blob_manager_factory.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4txn/txn.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"
#include "4cursor/cursor.h"
#include "4context/context.h"
#include "4txn/txn_cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

namespace hamsterdb {

LocalEnvironment::LocalEnvironment(EnvironmentConfiguration &config)
  : Environment(config)
{
}

void
LocalEnvironment::recover(uint32_t flags)
{
  Context context(this);

  ham_status_t st = 0;
  m_journal.reset(new Journal(this));

  ham_assert(get_flags() & HAM_ENABLE_RECOVERY);

  try {
    m_journal->open();
  }
  catch (Exception &ex) {
    if (ex.code == HAM_FILE_NOT_FOUND) {
      m_journal->create();
      return;
    }
  }

  /* success - check if we need recovery */
  if (!m_journal->is_empty()) {
    if (flags & HAM_AUTO_RECOVERY) {
      m_journal->recover((LocalTransactionManager *)m_txn_manager.get());
    }
    else {
      st = HAM_NEED_RECOVERY;
      goto bail;
    }
  }

bail:
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
        (m_header->get_header_page()->get_payload()
            + sizeof(PEnvironmentHeader));
  return (d + i);
}

LocalEnvironmentTest
LocalEnvironment::test()
{
  return (LocalEnvironmentTest(this));
}

ham_status_t
LocalEnvironment::do_create()
{
  if (m_config.flags & HAM_IN_MEMORY)
    m_config.flags |= HAM_DISABLE_RECLAIM_INTERNAL;

  /* initialize the device if it does not yet exist */
  m_blob_manager.reset(BlobManagerFactory::create(this, m_config.flags));
  m_device.reset(DeviceFactory::create(m_config));
  if (m_config.flags & HAM_ENABLE_TRANSACTIONS)
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
  m_header->set_version(HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV,
          HAM_FILE_VERSION);
  m_header->set_page_size(m_config.page_size_bytes);
  m_header->set_max_databases(m_config.max_databases);

  /* load page manager after setting up the blobmanager and the device! */
  m_page_manager.reset(new PageManager(this));

  /* create a logfile and a journal (if requested) */
  if (get_flags() & HAM_ENABLE_RECOVERY) {
    m_journal.reset(new Journal(this));
    m_journal->create();
  }

  /* flush the header page - this will write through disk if logging is
   * enabled */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    m_header->get_header_page()->flush();

  return (0);
}

ham_status_t
LocalEnvironment::do_open()
{
  ham_status_t st = 0;

  Context context(this);

  /* Initialize the device if it does not yet exist. The page size will
   * be filled in later (at this point in time, it's still unknown) */
  m_blob_manager.reset(BlobManagerFactory::create(this, m_config.flags));
  m_device.reset(DeviceFactory::create(m_config));

  /* open the file */
  m_device->open();

  if (m_config.flags & HAM_ENABLE_TRANSACTIONS)
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
      ham_log(("invalid file type"));
      st =  HAM_INV_FILE_HEADER;
      goto fail_with_fake_cleansing;
    }

    /* check the database version; everything with a different file version
     * is incompatible */
    if (m_header->get_version(3) != HAM_FILE_VERSION) {
      ham_log(("invalid file version"));
      st = HAM_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }
    else if (m_header->get_version(0) == 1 &&
      m_header->get_version(1) == 0 &&
      m_header->get_version(2) <= 9) {
      ham_log(("invalid file version; < 1.0.9 is not supported"));
      st = HAM_INV_FILE_VERSION;
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

  /* load page manager after setting up the blobmanager and the device! */
  m_page_manager.reset(new PageManager(this));

  /* check if recovery is required */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    recover(m_config.flags);

  /* load the state of the PageManager */
  if (m_header->get_page_manager_blobid() != 0)
    m_page_manager->initialize(m_header->get_page_manager_blobid());

  return (0);
}

ham_status_t
LocalEnvironment::do_get_database_names(uint16_t *names, uint32_t *count)
{
  uint16_t name;
  uint32_t i = 0;
  uint32_t max_names = 0;

  max_names = *count;
  *count = 0;

  /* copy each database name to the array */
  ham_assert(m_header->get_max_databases() > 0);
  for (i = 0; i < m_header->get_max_databases(); i++) {
    name = btree_header(i)->get_dbname();
    if (name == 0)
      continue;

    if (*count >= max_names)
      return (HAM_LIMITS_REACHED);

    names[(*count)++] = name;
  }

  return 0;
}

ham_status_t
LocalEnvironment::do_get_parameters(ham_parameter_t *param)
{
  ham_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case HAM_PARAM_CACHE_SIZE:
        p->value = m_config.cache_size_bytes;
        break;
      case HAM_PARAM_PAGE_SIZE:
        p->value = m_config.page_size_bytes;
        break;
      case HAM_PARAM_MAX_DATABASES:
        p->value = m_header->get_max_databases();
        break;
      case HAM_PARAM_FLAGS:
        p->value = get_flags();
        break;
      case HAM_PARAM_FILEMODE:
        p->value = m_config.file_mode;
        break;
      case HAM_PARAM_FILENAME:
        if (m_config.filename.size())
          p->value = (uint64_t)(PTR_TO_U64(m_config.filename.c_str()));
        else
          p->value = 0;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        if (m_config.log_filename.size())
          p->value = (uint64_t)(PTR_TO_U64(m_config.log_filename.c_str()));
        else
          p->value = 0;
        break;
      case HAM_PARAM_JOURNAL_SWITCH_THRESHOLD:
        p->value = m_config.journal_switch_threshold;
        break;
      case HAM_PARAM_JOURNAL_COMPRESSION:
        p->value = 0;
        break;
      case HAM_PARAM_POSIX_FADVISE:
        p->value = m_config.posix_advice;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)p->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  return (0);
}

ham_status_t
LocalEnvironment::do_flush(uint32_t flags)
{
  Context context(this, 0, 0);

  /* flush all committed transactions */
  if (m_txn_manager)
    m_txn_manager->flush_committed_txns(&context);

  if (flags & HAM_FLUSH_COMMITTED_TRANSACTIONS || get_flags() & HAM_IN_MEMORY)
    return (0);

  /* flush the header page */
  m_header->get_header_page()->flush();

  /* flush all open pages to disk */
  m_page_manager->flush(false);

  /* flush the device - this usually causes a fsync() */
  m_device->flush();
  return (0);
}

ham_status_t
LocalEnvironment::do_create_db(Database **pdb, DatabaseConfiguration &config,
                const ham_parameter_t *param)
{
  if (get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot create database in a read-only environment"));
    return (HAM_WRITE_PROTECTED);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case HAM_PARAM_RECORD_COMPRESSION:
          ham_trace(("Record compression is only available in hamsterdb pro"));
          return (HAM_NOT_IMPLEMENTED);
        case HAM_PARAM_KEY_COMPRESSION:
          ham_trace(("Key compression is only available in hamsterdb pro"));
          return (HAM_NOT_IMPLEMENTED);
        case HAM_PARAM_KEY_TYPE:
          config.key_type = (uint16_t)param->value;
          break;
        case HAM_PARAM_KEY_SIZE:
          if (param->value != 0) {
            if (param->value > 0xffff) {
              ham_trace(("invalid key size %u - must be < 0xffff"));
              return (HAM_INV_KEY_SIZE);
            }
            if (config.flags & HAM_RECORD_NUMBER32) {
              if (param->value > 0 && param->value != sizeof(uint32_t)) {
                ham_trace(("invalid key size %u - must be 4 for "
                           "HAM_RECORD_NUMBER32 databases",
                           (unsigned)param->value));
                return (HAM_INV_KEY_SIZE);
              }
            }
            if (config.flags & HAM_RECORD_NUMBER64) {
              if (param->value > 0 && param->value != sizeof(uint64_t)) {
                ham_trace(("invalid key size %u - must be 8 for "
                           "HAM_RECORD_NUMBER64 databases",
                           (unsigned)param->value));
                return (HAM_INV_KEY_SIZE);
              }
            }
            config.key_size = (uint16_t)param->value;
          }
          break;
        case HAM_PARAM_RECORD_SIZE:
          config.record_size = (uint32_t)param->value;
          break;
        default:
          ham_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          return (HAM_INV_PARAMETER);
      }
    }
  }

  if (config.flags & HAM_RECORD_NUMBER32) {
    if (config.key_type == HAM_TYPE_UINT8
        || config.key_type == HAM_TYPE_UINT16
        || config.key_type == HAM_TYPE_UINT64
        || config.key_type == HAM_TYPE_REAL32
        || config.key_type == HAM_TYPE_REAL64) {
      ham_trace(("HAM_RECORD_NUMBER32 not allowed in combination with "
                      "fixed length type"));
      return (HAM_INV_PARAMETER);
    }
    config.key_type = HAM_TYPE_UINT32;
  }
  else if (config.flags & HAM_RECORD_NUMBER64) {
    if (config.key_type == HAM_TYPE_UINT8
        || config.key_type == HAM_TYPE_UINT16
        || config.key_type == HAM_TYPE_UINT32
        || config.key_type == HAM_TYPE_REAL32
        || config.key_type == HAM_TYPE_REAL64) {
      ham_trace(("HAM_RECORD_NUMBER64 not allowed in combination with "
                      "fixed length type"));
      return (HAM_INV_PARAMETER);
    }
    config.key_type = HAM_TYPE_UINT64;
  }

  uint32_t mask = HAM_FORCE_RECORDS_INLINE
                    | HAM_FLUSH_WHEN_COMMITTED
                    | HAM_ENABLE_DUPLICATE_KEYS
                    | HAM_RECORD_NUMBER32
                    | HAM_RECORD_NUMBER64;
  if (config.flags & ~mask) {
    ham_trace(("invalid flags(s) 0x%x", config.flags & ~mask));
    return (HAM_INV_PARAMETER);
  }

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, config);

  Context context(this, 0, db);

  /* check if this database name is unique */
  uint16_t dbi;
  for (uint32_t i = 0; i < m_header->get_max_databases(); i++) {
    uint16_t name = btree_header(i)->get_dbname();
    if (!name)
      continue;
    if (name == config.db_name) {
      delete db;
      return (HAM_DATABASE_ALREADY_EXISTS);
    }
  }

  /* find a free slot in the PBtreeHeader array and store the name */
  for (dbi = 0; dbi < m_header->get_max_databases(); dbi++) {
    uint16_t name = btree_header(dbi)->get_dbname();
    if (!name) {
      btree_header(dbi)->set_dbname(config.db_name);
      break;
    }
  }
  if (dbi == m_header->get_max_databases()) {
    delete db;
    return (HAM_LIMITS_REACHED);
  }

  mark_header_page_dirty(&context);

  /* initialize the Database */
  ham_status_t st = db->create(&context, btree_header(dbi));
  if (st) {
    delete db;
    return (st);
  }

  /* force-flush the changeset */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    context.changeset.flush(next_lsn());

  *pdb = db;
  return (0);
}

ham_status_t
LocalEnvironment::do_open_db(Database **pdb, DatabaseConfiguration &config,
                const ham_parameter_t *param)
{
  *pdb = 0;

  uint32_t mask = HAM_FORCE_RECORDS_INLINE
                    | HAM_FLUSH_WHEN_COMMITTED
                    | HAM_READ_ONLY;
  if (config.flags & ~mask) {
    ham_trace(("invalid flags(s) 0x%x", config.flags & ~mask));
    return (HAM_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case HAM_PARAM_RECORD_COMPRESSION:
          ham_trace(("Record compression is only available in hamsterdb pro"));
          return (HAM_NOT_IMPLEMENTED);
        case HAM_PARAM_KEY_COMPRESSION:
          ham_trace(("Key compression is only available in hamsterdb pro"));
          return (HAM_NOT_IMPLEMENTED);
        default:
          ham_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          return (HAM_INV_PARAMETER);
      }
    }
  }

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, config);

  Context context(this, 0, db);

  ham_assert(0 != m_header->get_header_page());

  /* search for a database with this name */
  uint16_t dbi;
  for (dbi = 0; dbi < m_header->get_max_databases(); dbi++) {
    uint16_t name = btree_header(dbi)->get_dbname();
    if (!name)
      continue;
    if (config.db_name == name)
      break;
  }

  if (dbi == m_header->get_max_databases()) {
    delete db;
    return (HAM_DATABASE_NOT_FOUND);
  }

  /* open the database */
  ham_status_t st = db->open(&context, btree_header(dbi));
  if (st) {
    delete db;
    ham_trace(("Database could not be opened"));
    return (st);
  }

  *pdb = db;
  return (0);
}

ham_status_t
LocalEnvironment::do_rename_db(uint16_t oldname, uint16_t newname,
                uint32_t flags)
{
  Context context(this);

  /*
   * check if a database with the new name already exists; also search
   * for the database with the old name
   */
  uint16_t max = m_header->get_max_databases();
  uint16_t slot = max;
  ham_assert(max > 0);
  for (uint16_t dbi = 0; dbi < max; dbi++) {
    uint16_t name = btree_header(dbi)->get_dbname();
    if (name == newname)
      return (HAM_DATABASE_ALREADY_EXISTS);
    if (name == oldname)
      slot = dbi;
  }

  if (slot == max)
    return (HAM_DATABASE_NOT_FOUND);

  /* replace the database name with the new name */
  btree_header(slot)->set_dbname(newname);
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

ham_status_t
LocalEnvironment::do_erase_db(uint16_t name, uint32_t flags)
{
  /* check if this database is still open */
  if (m_database_map.find(name) != m_database_map.end())
    return (HAM_DATABASE_ALREADY_OPEN);

  /*
   * if it's an in-memory environment then it's enough to purge the
   * database from the environment header
   */
  if (get_flags() & HAM_IN_MEMORY) {
    for (uint16_t dbi = 0; dbi < m_header->get_max_databases(); dbi++) {
      PBtreeHeader *desc = btree_header(dbi);
      if (name == desc->get_dbname()) {
        desc->set_dbname(0);
        return (0);
      }
    }
    return (HAM_DATABASE_NOT_FOUND);
  }

  /* temporarily load the database */
  LocalDatabase *db;
  DatabaseConfiguration config;
  config.db_name = name;
  ham_status_t st = do_open_db((Database **)&db, config, 0);
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
  for (uint16_t dbi = 0; dbi < m_header->get_max_databases(); dbi++) {
    PBtreeHeader *desc = btree_header(dbi);
    if (name == desc->get_dbname()) {
      desc->set_dbname(0);
      break;
    }
  }

  mark_header_page_dirty(&context);
  context.changeset.clear();

  (void)ham_db_close((ham_db_t *)db, HAM_DONT_LOCK);

  return (0);
}

Transaction *
LocalEnvironment::do_txn_begin(const char *name, uint32_t flags)
{
  Transaction *txn = new LocalTransaction(this, name, flags);
  m_txn_manager->begin(txn);
  return (txn);
}

ham_status_t
LocalEnvironment::do_txn_commit(Transaction *txn, uint32_t flags)
{
  return (m_txn_manager->commit(txn, flags));
}

ham_status_t
LocalEnvironment::do_txn_abort(Transaction *txn, uint32_t flags)
{
  return (m_txn_manager->abort(txn, flags));
}

ham_status_t
LocalEnvironment::do_close(uint32_t flags)
{
  Context context(this);

  /* flush all committed transactions */
  if (m_txn_manager)
    m_txn_manager->flush_committed_txns(&context);

  /* flush all pages and the freelist, reduce the file size */
  if (m_page_manager)
    m_page_manager->close(&context);

  /* if we're not in read-only mode, and not an in-memory-database,
   * and the dirty-flag is true: flush the page-header to disk */
  if (m_header && m_header->get_header_page() && !(get_flags() & HAM_IN_MEMORY)
        && m_device.get() && m_device.get()->is_open()
        && (!(get_flags() & HAM_READ_ONLY))) {
    m_header->get_header_page()->flush();
  }

  /* close the header page */
  if (m_header && m_header->get_header_page()) {
    Page *page = m_header->get_header_page();
    if (page->get_data())
      m_device->free_page(page);
    delete page;
    m_header.reset();
  }

  /* close the device */
  if (m_device) {
    if (m_device->is_open()) {
      if (!(get_flags() & HAM_READ_ONLY))
        m_device->flush();
      m_device->close();
    }
  }

  /* close the log and the journal */
  if (m_journal)
    m_journal->close(!!(flags & HAM_DONT_CLEAR_LOG));

  return (0);
}

void
LocalEnvironment::do_fill_metrics(ham_env_metrics_t *metrics) const
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

} // namespace hamsterdb
