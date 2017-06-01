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

static inline void
recover(LocalEnv *env, uint32_t flags)
{
  assert(ISSET(env->flags(), UPS_ENABLE_TRANSACTIONS));

  Context context(env);
  env->journal.reset(new Journal(env));

  try {
    env->journal->open();
  }
  catch (Exception &ex) {
    if (ex.code == UPS_FILE_NOT_FOUND) {
      env->journal->create();
      return;
    }
  }

  /* success - check if we need recovery */
  if (!env->journal->is_empty()) {
    if (ISSET(flags, UPS_AUTO_RECOVERY)) {
      env->journal->recover((LocalTxnManager *)env->txn_manager.get());
    }
    else {
      /* otherwise close log and journal, but do not delete the files */
      env->journal->close(true);
      throw Exception(UPS_NEED_RECOVERY);
    }
  }

  /* reset the page manager */
  env->page_manager->reset(&context);
}

static inline PBtreeHeader *
btree_header(EnvHeader *header, int i)
{
  PBtreeHeader *base = (PBtreeHeader *)
        (header->header_page->payload() + sizeof(PEnvironmentHeader));
  return base + i;
}

static inline LocalDb *
get_or_open_database(LocalEnv *env, uint16_t dbname, bool *is_opened)
{
  LocalDb *db;

  LocalEnv::DatabaseMap::iterator it = env->_database_map.find(dbname);
  if (it == env->_database_map.end()) {
    DbConfig config(dbname);
    db = (LocalDb *)env->do_open_db(config, 0);
    env->_database_map[dbname] = db;
    *is_opened = true;
    return db;
  }

  *is_opened = false;
  return (LocalDb *)it->second;
}

// Sets the dirty-flag of the header page and adds the header page
// to the Changeset (if recovery is enabled)
static inline void
mark_header_page_dirty(LocalEnv *env, Context *context)
{
  Page *page = env->header->header_page;
  page->set_dirty(true);
  if (env->journal)
    context->changeset.put(page);
}

ups_status_t
LocalEnv::create()
{
  if (ISSET(config.flags, UPS_IN_MEMORY))
    config.flags |= UPS_DISABLE_RECLAIM_INTERNAL;

  /* initialize the device if it does not yet exist */
  device.reset(DeviceFactory::create(config));
  if (ISSET(config.flags, UPS_ENABLE_TRANSACTIONS))
    txn_manager.reset(new LocalTxnManager(this));

  /* create the file */
  device->create();

  /* allocate the header page */
  Page *page = new Page(device.get());
  page->alloc(Page::kTypeHeader, config.page_size_bytes);
  ::memset(page->data(), 0, config.page_size_bytes);
  page->set_type(Page::kTypeHeader);
  page->set_dirty(true);

  header.reset(new EnvHeader(page));

  /* initialize the header */
  header->set_magic('H', 'A', 'M', '\0');
  header->set_version(UPS_VERSION_MAJ, UPS_VERSION_MIN, UPS_VERSION_REV,
          UPS_FILE_VERSION);
  header->set_page_size(config.page_size_bytes);
  header->set_max_databases(config.max_databases);

  /* load page manager after setting up the blobmanager and the device! */
  page_manager.reset(new PageManager(this));

  /* the blob manager needs a device and an initialized page manager */
  blob_manager.reset(BlobManagerFactory::create(this, config.flags));

  /* create a logfile and a journal (if requested) */
  if (ISSET(flags(), UPS_ENABLE_TRANSACTIONS)
        && NOTSET(flags(), UPS_DISABLE_RECOVERY)) {
    journal.reset(new Journal(this));
    journal->create();
  }

  /* Now that the header was created we can finally store the compression
   * information */
  if (config.journal_compressor)
    header->set_journal_compression(config.journal_compressor);

  /* flush the header page - this will write through disk if logging is
   * enabled */
  if (journal.get())
    header->header_page->flush();

  return 0;
}

ups_status_t
LocalEnv::open()
{
  ups_status_t st = 0;

  Context context(this);

  /* Initialize the device if it does not yet exist. The page size will
   * be filled in later (at this point in time, it's still unknown) */
  device.reset(DeviceFactory::create(config));

  /* open the file */
  device->open();

  if (ISSET(config.flags, UPS_ENABLE_TRANSACTIONS))
    txn_manager.reset(new LocalTxnManager(this));

  /*
   * read the database header
   *
   * !!!
   * now this is an ugly problem - the database header spans one page, but
   * what's the size of this page? chances are good that it's the default
   * size, but we really can't be sure.
   *
   * read 512 byte and extract the "real" page size, then read the real page.
   */
  {
    Page *page = 0;
    uint8_t hdrbuf[512];

    // fetch the header data we need to get an estimate of what
    // the database is made of really.
    device->read(0, hdrbuf, sizeof(hdrbuf));

    // set up a faked headerpage for the duration of this call
    Page fakepage(device.get());

    /* 
     * TODO TODO TODO
     * separate this into a function.
     * 1. if file size >= default page size (16kb):
     *   - read page
     *   - check the page size
     *     - if page size == 16kb: return page as root page
     *     - otherwise goto 3)
     * 2. else peek file size in file
     * 3. read root page
     * 4. remove Page::set_data()
     */
    fakepage.set_data((PPageData *)hdrbuf);

    /* create the configuration object */
    header.reset(new EnvHeader(&fakepage));

    config.page_size_bytes = header->page_size();

    /** check the file magic */
    if (unlikely(!header->verify_magic('H', 'A', 'M', '\0'))) {
      ups_log(("invalid file type"));
      st =  UPS_INV_FILE_HEADER;
      goto fail_with_fake_cleansing;
    }

    // Check the database version; everything with a different file version
    // is incompatible.
    if (header->version(3) != UPS_FILE_VERSION) {
      ups_log(("invalid file version"));
      st = UPS_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }

    st = 0;

fail_with_fake_cleansing:

    /* undo the headerpage fake first! */
    fakepage.set_data(0);
    header.reset(0);

    /* exit when an error was signaled */
    if (unlikely(st)) {
      if (device->is_open())
        device->close();
      return st;
    }

    /* now read the "real" header page and store it in the Environment */
    page = new Page(device.get());
    page->fetch(0);
    header.reset(new EnvHeader(page));
  }

  /* Now that the header page was fetched we can retrieve the compression
   * information */
  config.journal_compressor = header->journal_compression();

  /* load page manager after setting up the blobmanager and the device! */
  page_manager.reset(new PageManager(this));

  /* the blob manager needs a device and an initialized page manager */
  blob_manager.reset(BlobManagerFactory::create(this, config.flags));

  /* check if recovery is required */
  if (ISSET(flags(), UPS_ENABLE_TRANSACTIONS))
    recover(this, config.flags);

  /* load the state of the PageManager */
  if (header->page_manager_blobid() != 0)
    page_manager->initialize(header->page_manager_blobid());

  return 0;
}

std::vector<uint16_t>
LocalEnv::get_database_names()
{
  assert(header->max_databases() > 0);

  std::vector<uint16_t> vec;

  /* copy each database name to the array */
  for (uint32_t i = 0; i < header->max_databases(); i++) {
    uint16_t name = btree_header(header.get(), i)->dbname;
    if (unlikely(name == 0))
      continue;
    vec.push_back(name);
  }

  return vec;
}

ups_status_t
LocalEnv::get_parameters(ups_parameter_t *param)
{
  ups_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case UPS_PARAM_CACHE_SIZE:
        p->value = config.cache_size_bytes;
        break;
      case UPS_PARAM_PAGE_SIZE:
        p->value = config.page_size_bytes;
        break;
      case UPS_PARAM_MAX_DATABASES:
        p->value = header->max_databases();
        break;
      case UPS_PARAM_FLAGS:
        p->value = flags();
        break;
      case UPS_PARAM_FILEMODE:
        p->value = config.file_mode;
        break;
      case UPS_PARAM_FILENAME:
        if (config.filename.size())
          p->value = (uint64_t)(config.filename.c_str());
        else
          p->value = 0;
        break;
      case UPS_PARAM_LOG_DIRECTORY:
        if (config.log_filename.size())
          p->value = (uint64_t)(config.log_filename.c_str());
        else
          p->value = 0;
        break;
      case UPS_PARAM_JOURNAL_SWITCH_THRESHOLD:
        p->value = config.journal_switch_threshold;
        break;
      case UPS_PARAM_JOURNAL_COMPRESSION:
        p->value = config.journal_compressor;
        break;
      case UPS_PARAM_POSIX_FADVISE:
        p->value = config.posix_advice;
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
LocalEnv::flush(uint32_t flags)
{
  Context context(this, 0, 0);

  /* flush all committed transactions */
  if (likely(txn_manager.get() != 0))
    txn_manager->flush_committed_txns(&context);

  if (ISSET(flags, UPS_FLUSH_COMMITTED_TRANSACTIONS)
         || ISSET(this->flags(), UPS_IN_MEMORY))
    return 0;

  /* Flush all open pages to disk. This operation is blocking. */
  page_manager->flush_all_pages();

  /* Flush the device - this can trigger a fsync() if enabled */
  device->flush();

  return 0;
}

ups_status_t
LocalEnv::select_range(const char *query, Cursor *begin,
                            const Cursor *end, Result **result)
{
  // Parse the string into a SelectStatement object
  SelectStatement stmt;
  ups_status_t st = Parser::parse_select(query, stmt);
  if (unlikely(st))
    return st;

  // load (or open) the database
  bool is_opened = false;
  LocalDb *db = get_or_open_database(this, stmt.dbid, &is_opened);

  // if Cursors are passed: check if they belong to this database
  if (begin && begin->db->name() != stmt.dbid) {
    ups_log(("cursor 'begin' uses wrong database"));
    return UPS_INV_PARAMETER;
  }
  if (end && end->db->name() != stmt.dbid) {
    ups_log(("cursor 'begin' uses wrong database"));
    return UPS_INV_PARAMETER;
  }

  // optimization: if duplicates are disabled then the query is always
  // non-distinct
  if (NOTSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    stmt.distinct = true;

  // The Database object will do the remaining work
  st = db->select_range(&stmt, (LocalCursor *)begin,
                    (LocalCursor *)end, result);

  // Don't leak the database handle if it was opened above
  if (is_opened)
    (void)ups_db_close((ups_db_t *)db, UPS_DONT_LOCK);

  return st;
}

Db *
LocalEnv::do_create_db(DbConfig &dbconfig, const ups_parameter_t *param)
{
  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case UPS_PARAM_RECORD_COMPRESSION:
          if (unlikely(!CompressorFactory::is_available(param->value))) {
            ups_trace(("unknown algorithm for record compression"));
            throw Exception(UPS_INV_PARAMETER);
          }
          dbconfig.record_compressor = (int)param->value;
          break;
        case UPS_PARAM_KEY_COMPRESSION:
          if (unlikely(!CompressorFactory::is_available(param->value))) {
            ups_trace(("unknown algorithm for key compression"));
            throw Exception(UPS_INV_PARAMETER);
          }
          dbconfig.key_compressor = (int)param->value;
          break;
        case UPS_PARAM_KEY_TYPE:
          dbconfig.key_type = (uint16_t)param->value;
          break;
        case UPS_PARAM_KEY_SIZE:
          if (param->value != 0) {
            if (unlikely(param->value > 0xffff)) {
              ups_trace(("invalid key size %u - must be < 0xffff"));
              throw Exception(UPS_INV_KEY_SIZE);
            }
            if (ISSET(dbconfig.flags, UPS_RECORD_NUMBER32)) {
              if (param->value > 0 && param->value != sizeof(uint32_t)) {
                ups_trace(("invalid key size %u - must be 4 for "
                           "UPS_RECORD_NUMBER32 databases",
                           (unsigned)param->value));
                throw Exception(UPS_INV_KEY_SIZE);
              }
            }
            if (ISSET(dbconfig.flags, UPS_RECORD_NUMBER64)) {
              if (param->value > 0 && param->value != sizeof(uint64_t)) {
                ups_trace(("invalid key size %u - must be 8 for "
                           "UPS_RECORD_NUMBER64 databases",
                           (unsigned)param->value));
                throw Exception(UPS_INV_KEY_SIZE);
              }
            }
            dbconfig.key_size = (uint16_t)param->value;
          }
          break;
        case UPS_PARAM_RECORD_TYPE:
          dbconfig.record_type = (uint16_t)param->value;
          break;
        case UPS_PARAM_RECORD_SIZE:
          dbconfig.record_size = (uint32_t)param->value;
          break;
        case UPS_PARAM_CUSTOM_COMPARE_NAME:
          dbconfig.compare_name = reinterpret_cast<const char *>(param->value);
          break;
        default:
          ups_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          throw Exception(UPS_INV_PARAMETER);
      }
    }
  }

  if (ISSET(dbconfig.flags, UPS_RECORD_NUMBER32)) {
    if (dbconfig.key_type == UPS_TYPE_UINT8
        || dbconfig.key_type == UPS_TYPE_UINT16
        || dbconfig.key_type == UPS_TYPE_UINT64
        || dbconfig.key_type == UPS_TYPE_REAL32
        || dbconfig.key_type == UPS_TYPE_REAL64) {
      ups_trace(("UPS_RECORD_NUMBER32 not allowed in combination with "
                      "fixed length type"));
      throw Exception(UPS_INV_PARAMETER);
    }
    dbconfig.key_type = UPS_TYPE_UINT32;
  }
  else if (ISSET(dbconfig.flags, UPS_RECORD_NUMBER64)) {
    if (dbconfig.key_type == UPS_TYPE_UINT8
        || dbconfig.key_type == UPS_TYPE_UINT16
        || dbconfig.key_type == UPS_TYPE_UINT32
        || dbconfig.key_type == UPS_TYPE_REAL32
        || dbconfig.key_type == UPS_TYPE_REAL64) {
      ups_trace(("UPS_RECORD_NUMBER64 not allowed in combination with "
                      "fixed length type"));
      throw Exception(UPS_INV_PARAMETER);
    }
    dbconfig.key_type = UPS_TYPE_UINT64;
  }

  // the CUSTOM type is not allowed for records
  if (dbconfig.record_type == UPS_TYPE_CUSTOM) {
    ups_trace(("invalid record type UPS_TYPE_CUSTOM - use UPS_TYPE_BINARY "
                  "instead"));
    throw Exception(UPS_INV_PARAMETER);
  }

  // uint32 compression is only allowed for uint32-keys
  if (dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_VARBYTE
      || dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_FOR
      || dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_SIMDFOR
      || dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_SIMDCOMP
      || dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_GROUPVARINT
      || dbconfig.key_compressor == UPS_COMPRESSOR_UINT32_STREAMVBYTE) {
    if (unlikely(dbconfig.key_type != UPS_TYPE_UINT32)) {
      ups_trace(("Uint32 compression only allowed for uint32 keys "
                 "(UPS_TYPE_UINT32)"));
      throw Exception(UPS_INV_PARAMETER);
    }
    if (unlikely(config.page_size_bytes != 16 * 1024)) {
      ups_trace(("Uint32 compression only allowed for page size of 16k"));
      throw Exception(UPS_INV_PARAMETER);
    }
  }

  // all heavy-weight compressors are only allowed for
  // variable-length binary keys
  if (dbconfig.key_compressor == UPS_COMPRESSOR_LZF
        || dbconfig.key_compressor == UPS_COMPRESSOR_SNAPPY
        || dbconfig.key_compressor == UPS_COMPRESSOR_ZLIB) {
    if (unlikely(dbconfig.key_type != UPS_TYPE_BINARY
          || dbconfig.key_size != UPS_KEY_SIZE_UNLIMITED)) {
      ups_trace(("Key compression only allowed for unlimited binary keys "
                 "(UPS_TYPE_BINARY"));
      throw Exception(UPS_INV_PARAMETER);
    }
  }

  uint32_t mask = UPS_FORCE_RECORDS_INLINE
                    | UPS_ENABLE_DUPLICATE_KEYS
                    | UPS_IGNORE_MISSING_CALLBACK
                    | UPS_RECORD_NUMBER32
                    | UPS_RECORD_NUMBER64;
  if (unlikely(dbconfig.flags & ~mask)) {
    ups_trace(("invalid flags(s) 0x%x", dbconfig.flags & ~mask));
    throw Exception(UPS_INV_PARAMETER);
  }

  /* create a new Database object */
  LocalDb *db = new LocalDb(this, dbconfig);

  Context context(this, 0, db);

  /* check if this database name is unique */
  uint16_t dbi;
  for (uint32_t i = 0; i < header->max_databases(); i++) {
    uint16_t name = btree_header(header.get(), i)->dbname;
    if (unlikely(name == dbconfig.db_name)) {
      delete db;
      throw Exception(UPS_DATABASE_ALREADY_EXISTS);
    }
  }

  /* find a free slot in the PBtreeHeader array and store the name */
  for (dbi = 0; dbi < header->max_databases(); dbi++) {
    uint16_t name = btree_header(header.get(), dbi)->dbname;
    if (!name) {
      btree_header(header.get(), dbi)->dbname = dbconfig.db_name;
      break;
    }
  }
  if (unlikely(dbi == header->max_databases())) {
    delete db;
    throw Exception(UPS_LIMITS_REACHED);
  }

  mark_header_page_dirty(this, &context);

  /* initialize the Db */
  ups_status_t st = db->create(&context, btree_header(header.get(), dbi));
  if (unlikely(st)) {
    delete db;
    throw Exception(st);
  }

  /* force-flush the changeset */
  if (journal)
    context.changeset.flush(lsn_manager.next());

  return db;
}

Db *
LocalEnv::do_open_db(DbConfig &dbconfig, const ups_parameter_t *param)
{
  uint32_t mask = UPS_FORCE_RECORDS_INLINE
                    | UPS_PARAM_JOURNAL_COMPRESSION
                    | UPS_IGNORE_MISSING_CALLBACK
                    | UPS_READ_ONLY;
  if (unlikely(dbconfig.flags & ~mask)) {
    ups_trace(("invalid flag(s) 0x%x", dbconfig.flags & ~mask));
    throw Exception(UPS_INV_PARAMETER);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case UPS_PARAM_RECORD_COMPRESSION:
          ups_trace(("Record compression parameters are only allowed in "
                     "ups_env_create_db"));
          throw Exception(UPS_INV_PARAMETER);
        case UPS_PARAM_KEY_COMPRESSION:
          ups_trace(("Key compression parameters are only allowed in "
                     "ups_env_create_db"));
          throw Exception(UPS_INV_PARAMETER);
        default:
          ups_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          throw Exception(UPS_INV_PARAMETER);
      }
    }
  }

  /* create a new Database object */
  LocalDb *db = new LocalDb(this, dbconfig);

  Context context(this, 0, db);

  assert(0 != header->header_page);

  /* search for a database with this name */
  uint16_t dbi;
  for (dbi = 0; dbi < header->max_databases(); dbi++) {
    uint16_t name = btree_header(header.get(), dbi)->dbname;
    if (dbconfig.db_name == name)
      break;
  }

  if (unlikely(dbi == header->max_databases())) {
    delete db;
    throw Exception(UPS_DATABASE_NOT_FOUND);
  }

  /* open the database */
  ups_status_t st = db->open(&context, btree_header(header.get(), dbi));
  if (unlikely(st)) {
    delete db;
    ups_trace(("Database could not be opened"));
    throw Exception(st);
  }

  return db;
}

ups_status_t
LocalEnv::rename_db(uint16_t oldname, uint16_t newname, uint32_t flags)
{
  Context context(this);

  /*
   * check if a database with the new name already exists; also search
   * for the database with the old name
   */
  uint16_t max = header->max_databases();
  uint16_t slot = max;
  assert(max > 0);
  for (uint16_t dbi = 0; dbi < max; dbi++) {
    uint16_t name = btree_header(header.get(), dbi)->dbname;
    if (unlikely(name == newname))
      return UPS_DATABASE_ALREADY_EXISTS;
    if (name == oldname)
      slot = dbi;
  }

  if (unlikely(slot == max))
    return UPS_DATABASE_NOT_FOUND;

  /* replace the database name with the new name */
  btree_header(header.get(), slot)->dbname = newname;
  mark_header_page_dirty(this, &context);

  /* if the database with the old name is currently open: notify it */
  Env::DatabaseMap::iterator it = _database_map.find(oldname);
  if (unlikely(it != _database_map.end())) {
    Db *db = it->second;
    it->second->set_name(newname);
    _database_map.erase(oldname);
    _database_map.insert(DatabaseMap::value_type(newname, db));
  }

  return 0;
}

ups_status_t
LocalEnv::erase_db(uint16_t name, uint32_t flags)
{
  /* check if this database is still open */
  if (unlikely(_database_map.find(name) != _database_map.end()))
    return UPS_DATABASE_ALREADY_OPEN;

  /*
   * if it's an in-memory environment then it's enough to purge the
   * database from the environment header
   */
  if (ISSET(this->flags(), UPS_IN_MEMORY)) {
    for (uint16_t dbi = 0; dbi < header->max_databases(); dbi++) {
      PBtreeHeader *desc = btree_header(header.get(), dbi);
      if (name == desc->dbname) {
        desc->dbname = 0;
        return 0;
      }
    }
    return UPS_DATABASE_NOT_FOUND;
  }

  /* temporarily load the database */
  DbConfig dbconfig;
  dbconfig.db_name = name;
  LocalDb *db = (LocalDb *)do_open_db(dbconfig, 0);

  Context context(this, 0, db);

  /*
   * delete all blobs and extended keys, also from the cache and
   * the extkey-cache
   *
   * also delete all pages and move them to the freelist; if they're
   * cached, delete them from the cache
   */
  ups_status_t st = db->drop(&context);
  if (unlikely(st))
    return st;

  /* now set database name to 0 and set the header page to dirty */
  for (uint16_t dbi = 0; dbi < header->max_databases(); dbi++) {
    PBtreeHeader *desc = btree_header(header.get(), dbi);
    if (name == desc->dbname) {
      desc->dbname = 0;
      break;
    }
  }

  mark_header_page_dirty(this, &context);
  context.changeset.clear();

  (void)ups_db_close((ups_db_t *)db, UPS_DONT_LOCK);

  return 0;
}

Txn *
LocalEnv::txn_begin(const char *name, uint32_t flags)
{
  Txn *txn = new LocalTxn(this, name, flags);
  txn_manager->begin(txn);
  return txn;
}

ups_status_t
LocalEnv::txn_commit(Txn *txn, uint32_t)
{
  return txn_manager->commit(txn);
}

ups_status_t
LocalEnv::txn_abort(Txn *txn, uint32_t)
{
  return txn_manager->abort(txn);
}

ups_status_t
LocalEnv::do_close(uint32_t flags)
{
  Context context(this);

  /* flush all committed transactions */
  if (likely(txn_manager.get() != 0))
    txn_manager->flush_committed_txns(&context);

  /* flush all pages and the freelist, reduce the file size */
  if (likely(page_manager.get() != 0))
    page_manager->close(&context);

  /* close the header page */
  if (likely(header && header->header_page)) {
    Page *page = header->header_page;
    if (likely(page->data() != 0))
      device->free_page(page);
    delete page;
    header.reset();
  }

  /* close the device */
  if (likely(device && device->is_open())) {
    if (NOTSET(this->flags(), UPS_READ_ONLY))
      device->flush();
    device->close();
  }

  /* close the log and the journal */
  if (journal)
    journal->close(ISSET(flags, UPS_DONT_CLEAR_LOG));

  return 0;
}

void
LocalEnv::fill_metrics(ups_env_metrics_t *metrics)
{
  // PageManager metrics (incl. cache and freelist)
  page_manager->fill_metrics(metrics);
  // the BlobManagers
  blob_manager->fill_metrics(metrics);
  // the Journal (if available)
  if (journal)
    journal->fill_metrics(metrics);
  // the (first) database
  if (!_database_map.empty()) {
    LocalDb *db = (LocalDb *)_database_map.begin()->second;
    db->fill_metrics(metrics);
  }
  // and of the btrees
  BtreeIndex::fill_metrics(metrics);
}

} // namespace upscaledb
