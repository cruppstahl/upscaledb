/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include "db.h"
#include "env.h"
#include "btree_stats.h"
#include "device.h"
#include "version.h"
#include "serial.h"
#include "txn.h"
#include "device.h"
#include "btree.h"
#include "mem.h"
#include "freelist.h"
#include "extkeys.h"
#include "cache.h"
#include "log.h"
#include "journal.h"
#include "btree_key.h"
#include "os.h"
#include "blob.h"
#include "txn_cursor.h"
#include "cursor.h"
#include "btree_cursor.h"

using namespace hamsterdb;

namespace hamsterdb {

typedef struct free_cb_context_t
{
  Database *db;
  ham_bool_t is_leaf;
} free_cb_context_t;

Environment::Environment()
  : m_file_mode(0644), m_txn_id(0), m_context(0), m_device(0), m_cache(0),
  m_alloc(0), m_hdrpage(0), m_oldest_txn(0), m_newest_txn(0), m_log(0),
  m_journal(0), m_freelist(0), m_flags(0), m_pagesize(0),
  m_cachesize(0), m_max_databases_cached(0), m_blob_manager(this),
  m_duplicate_manager(this)
{
  memset(&m_perf_data, 0, sizeof(m_perf_data));

  set_allocator(Allocator::create());
}

Environment::~Environment()
{
  /* close the header page */
  if (get_device() && get_header_page()) {
    Page *page = get_header_page();
    if (page->get_pers())
      get_device()->free_page(page);
    delete page;
    set_header_page(0);
  }

  /* close the freelist */
  if (get_freelist()) {
    delete get_freelist();
    set_freelist(0);
  }

  /* close the cache */
  if (get_cache()) {
    delete get_cache();
    set_cache(0);
  }

  /* close the device if it still exists */
  if (get_device()) {
    Device *device = get_device();
    if (device->is_open()) {
      (void)device->flush();
      (void)device->close();
    }
    delete device;
    set_device(0);
  }

  /* close the allocator */
  if (get_allocator()) {
    delete get_allocator();
    set_allocator(0);
  }

}

BtreeDescriptor *
Environment::get_descriptor(int i)
{
  BtreeDescriptor *d = (BtreeDescriptor *)
        (get_header_page()->get_payload() + sizeof(env_header_t));
  return (d + i);
}

FreelistPayload *
Environment::get_freelist_payload()
{
  return ((FreelistPayload *)(get_header_page()->get_payload() +
            SIZEOF_FULL_HEADER(this)));
}

void
Environment::append_txn(Transaction *txn)
{
  if (!get_newest_txn()) {
    ham_assert(get_oldest_txn() == 0);
    set_oldest_txn(txn);
    set_newest_txn(txn);
  }
  else {
    txn->set_older(get_newest_txn());
    get_newest_txn()->set_newer(txn);
    set_newest_txn(txn);
    /* if there's no oldest txn (this means: all txn's but the
     * current one were already flushed) then set this txn as
     * the oldest txn */
    if (!get_oldest_txn())
      set_oldest_txn(txn);
  }
}

void
Environment::remove_txn(Transaction *txn)
{
  if (get_newest_txn() == txn)
    set_newest_txn(txn->get_older());

  if (get_oldest_txn() == txn) {
    Transaction *n = txn->get_newer();
    set_oldest_txn(n);
    if (n)
      n->set_older(0);
  }
  else {
    ham_assert(!"not yet implemented");
  }
}

ham_status_t
Environment::flush_committed_txns()
{
  Transaction *oldest;
  ham_u64_t last_id = 0;

  /* always get the oldest transaction; if it was committed: flush
   * it; if it was aborted: discard it; otherwise return */
  while ((oldest = get_oldest_txn())) {
    if (oldest->is_committed()) {
      if (last_id)
        ham_assert(last_id != oldest->get_id());
      last_id = oldest->get_id();
      ham_status_t st = flush_txn(oldest);
      if (st)
        return (st);
    }
    else if (oldest->is_aborted()) {
      ; /* nop */
    }
    else
      break;

    /* now remove the txn from the linked list */
    remove_txn(oldest);

    /* and free the whole memory */
    delete oldest;
  }

  /* clear the changeset; if the loop above was not entered or the
   * transaction was empty then it may still contain pages */
  get_changeset().clear();

  return (0);
}

ham_status_t
Environment::get_incremented_lsn(ham_u64_t *lsn)
{
  Journal *j = get_journal();
  if (j) {
    *lsn = j->get_incremented_lsn();
    if (*lsn == 0) {
      ham_log(("journal limits reached (lsn overflow) - please reorg"));
      return (HAM_LIMITS_REACHED);
    }
    return (0);
  }

  // otherwise return a dummy value
  *lsn = 1;
  return (0);
}

static ham_status_t
purge_callback(Page *page)
{
  ham_status_t st = page->uncouple_all_cursors();
  if (st)
    return (st);

  st = page->flush();
  if (st)
    return (st);

  page->free();
  delete page;
  return (0);
}

static bool
flush_all_pages_callback(Page *page, Database *db, ham_u32_t flags)
{
  (void)db;
  (void)page->flush();

  /*
   * if the page is deleted, uncouple all cursors, then
   * free the memory of the page, then remove from the cache
   */
  if (flags == 0) {
    (void)page->uncouple_all_cursors();
    (void)page->free();
    return (true);
  }

  return (false);
}

ham_status_t
Environment::purge_cache()
{
  Cache *cache = get_cache();

  /* in-memory-db: don't remove the pages or they would be lost */
  if (get_flags() & HAM_IN_MEMORY)
    return (0);

  return (cache->purge(purge_callback, (get_flags() & HAM_CACHE_STRICT) != 0));
}

ham_status_t
LocalEnvironment::flush_all_pages(bool nodelete)
{
  if (!get_cache())
    return (0);

  return (get_cache()->visit(flush_all_pages_callback, 0, nodelete ? 1 : 0));
}

ham_status_t
Environment::alloc_page(Page **page_ref, Database *db, ham_u32_t type,
            ham_u32_t flags)
{
  ham_status_t st;
  ham_u64_t tellpos = 0;
  Page *page = NULL;
  bool allocated_by_me = false;

  *page_ref = 0;
  ham_assert(0 == (flags & ~(PAGE_IGNORE_FREELIST | PAGE_CLEAR_WITH_ZERO)));

  /* first, we ask the freelist for a page */
  if (!(flags & PAGE_IGNORE_FREELIST) && get_freelist()) {
    st = get_freelist()->alloc_page(&tellpos, db);
    if (st)
      return (st);
    if (tellpos) {
      ham_assert(tellpos % get_pagesize() == 0);
      /* try to fetch the page from the cache */
      page = get_cache()->get_page(tellpos, 0);
      if (page)
        goto done;
      /* allocate a new page structure and read the page from disk */
      page = new Page(this, db);
      st = page->fetch(tellpos);
      if (st) {
        delete page;
        return (st);
      }
      goto done;
    }
  }

  if (!page) {
    page = new Page(this, db);
    allocated_by_me = true;
  }

  /* can we allocate a new page for the cache? */
  if (get_cache()->is_too_big()) {
    if (get_flags() & HAM_CACHE_STRICT) {
      if (allocated_by_me)
        delete page;
      return (HAM_CACHE_FULL);
    }
  }

  ham_assert(tellpos == 0);
  st = page->allocate();
  if (st)
    return (st);

done:
  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(type);
  page->set_dirty(true);
  page->set_db(db);

  /* clear the page with zeroes?  */
  if (flags & PAGE_CLEAR_WITH_ZERO)
    memset(page->get_pers(), 0, get_pagesize());

  /* an allocated page is always flushed if recovery is enabled */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    get_changeset().add_page(page);

  /* store the page in the cache */
  get_cache()->put_page(page);

  *page_ref = page;
  return (HAM_SUCCESS);
}

ham_status_t
Environment::fetch_page(Page **page_ref, Database *db,
            ham_u64_t address, bool only_from_cache)
{
  ham_status_t st;

  *page_ref = 0;

  /* fetch the page from the cache */
  Page *page = get_cache()->get_page(address, Cache::NOREMOVE);
  if (page) {
    *page_ref = page;
    ham_assert(page->get_pers());
    /* store the page in the changeset if recovery is enabled */
    if (get_flags() & HAM_ENABLE_RECOVERY)
      get_changeset().add_page(page);
    return (HAM_SUCCESS);
  }

  if (only_from_cache)
    return (HAM_SUCCESS);

  ham_assert(get_cache()->get_page(address) == 0);

  /* can we allocate a new page for the cache? */
  if (get_cache()->is_too_big()) {
    if (get_flags() & HAM_CACHE_STRICT)
      return (HAM_CACHE_FULL);
  }

  page = new Page(this, db);
  st = page->fetch(address);
  if (st) {
    delete page;
    return (st);
  }

  ham_assert(page->get_pers());

  /* store the page in the cache */
  get_cache()->put_page(page);

  /* store the page in the changeset */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    get_changeset().add_page(page);

  *page_ref = page;
  return (HAM_SUCCESS);
}

ham_status_t
Environment::flush_txn(Transaction *txn)
{
  ham_status_t st = 0;
  TransactionOperation *op = txn->get_oldest_op();
  TransactionCursor *cursor = 0;

  while (op) {
    TransactionNode *node = op->get_node();
    BtreeIndex *be = node->get_db()->get_btree();

    if (op->get_flags() & TransactionOperation::TXN_OP_FLUSHED)
      goto next_op;

    /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
    if (get_flags() & HAM_ENABLE_RECOVERY)
      ham_assert(get_changeset().is_empty());
#endif

    /*
     * depending on the type of the operation: actually perform the
     * operation on the btree
     *
     * if the txn-op has a cursor attached, then all (txn)cursors
     * which are coupled to this op have to be uncoupled, and their
     * parent (btree) cursor must be coupled to the btree item instead.
     */
    if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
        || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)
        || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)) {
      ham_u32_t additional_flag = 
        (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)
          ? HAM_DUPLICATE
          : HAM_OVERWRITE;
      if (!op->get_cursors()) {
        st = be->insert(txn, node->get_key(), op->get_record(),
                    op->get_orig_flags() | additional_flag);
      }
      else {
        TransactionCursor *tc2, *tc1 = op->get_cursors();
        Cursor *c2, *c1 = tc1->get_parent();
        /* pick the first cursor, get the parent/btree cursor and
         * insert the key/record pair in the btree. The btree cursor
         * then will be coupled to this item. */
        st = c1->get_btree_cursor()->insert(
                    node->get_key(), op->get_record(),
                    op->get_orig_flags() | additional_flag);
        if (!st) {
          /* uncouple the cursor from the txn-op, and remove it */
          op->remove_cursor(tc1);
          c1->couple_to_btree();
          c1->set_to_nil(Cursor::CURSOR_TXN);

          /* all other (btree) cursors need to be coupled to the same
           * item as the first one. */
          while ((tc2 = op->get_cursors())) {
            op->remove_cursor(tc2);
            c2 = tc2->get_parent();
            c2->get_btree_cursor()->couple_to_other(c1->get_btree_cursor());
            c2->couple_to_btree();
            c2->set_to_nil(Cursor::CURSOR_TXN);
          }
        }
      }
    }
    else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE) {
      if (op->get_referenced_dupe()) {
        st = be->erase_duplicate(txn, node->get_key(),
                    op->get_referenced_dupe(), op->get_flags());
      }
      else {
        st = be->erase(txn, node->get_key(), op->get_flags());
      }
      if (st == HAM_KEY_NOT_FOUND)
        st = 0;
    }

    if (st) {
      ham_trace(("failed to flush op: %d (%s)", (int)st, ham_strerror(st)));
      get_changeset().clear();
      return (st);
    }

    /* now flush the changeset to disk */
    if (get_flags() & HAM_ENABLE_RECOVERY) {
      get_changeset().add_page(get_header_page());
      st = get_changeset().flush(op->get_lsn());
      if (st) {
        ham_trace(("failed to flush op: %d (%s)", (int)st, ham_strerror(st)));
        get_changeset().clear();
        return (st);
      }
    }

    /*
     * this op is about to be flushed!
     *
     * as a consequence, all (txn)cursors which are coupled to this op
     * have to be uncoupled, as their parent (btree) cursor was
     * already coupled to the btree item instead
     */
    op->set_flags(TransactionOperation::TXN_OP_FLUSHED);
next_op:
    while ((cursor = op->get_cursors())) {
      Cursor *pc = cursor->get_parent();
      ham_assert(pc->get_txn_cursor() == cursor);
      pc->couple_to_btree();
      pc->set_to_nil(Cursor::CURSOR_TXN);
    }

    /* continue with the next operation of this txn */
    op = op->get_next_in_txn();
  }

  return (0);
}

/*
 * callback function for freeing blobs of an in-memory-database, implemented
 * in db.cc
 * TODO why is this external?
 */
extern ham_status_t
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context);


ham_status_t
LocalEnvironment::create(const char *filename, ham_u32_t flags,
        ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
        ham_u16_t maxdbs)
{
  ham_status_t st = 0;

  ham_assert(!get_header_page());

  set_flags(flags);
  if (filename)
    set_filename(filename);
  set_file_mode(mode);
  set_pagesize(pagesize);
  set_cachesize(cachesize);
  set_max_databases_cached(maxdbs);

  /* initialize the device if it does not yet exist */
  Device *device;
  if (flags&HAM_IN_MEMORY)
    device = new InMemoryDevice(this, flags);
  else
    device = new DiskDevice(this, flags);
  device->set_pagesize(get_pagesize());
  set_device(device);

  /* now make sure the pagesize is a multiple of
   * DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes */
  ham_assert(0 == (get_pagesize() % DB_PAGESIZE_MIN_REQD_ALIGNMENT));

  /* create the file */
  st = device->create(filename, flags, mode);
  if (st) {
    delete device;
    set_device(0);
    return (st);
  }

  /* allocate the header page */
  {
    Page *page = new Page(this);
    /* manually set the device pointer */
    page->set_device(device);
    st = page->allocate();
    if (st) {
      delete page;
      return (st);
    }
    memset(page->get_pers(), 0, get_pagesize());
    page->set_type(Page::TYPE_HEADER);
    set_header_page(page);

    /* initialize the header */
    set_magic('H', 'A', 'M', '\0');
    set_version(HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV, 0);
    set_serialno(HAM_SERIALNO);
    set_persistent_pagesize(get_pagesize());
    set_max_databases(get_max_databases_cached());
    ham_assert(get_max_databases() > 0);

    page->set_dirty(true);
  }

  /* create the freelist */
  if (!(get_flags() & HAM_IN_MEMORY))
    set_freelist(new Freelist(this));

  /* create a logfile and a journal (if requested) */
  if (get_flags() & HAM_ENABLE_RECOVERY) {
    hamsterdb::Log *log = new hamsterdb::Log(this);
    st = log->create();
    if (st) {
      delete log;
      return (st);
    }
    set_log(log);

    Journal *journal = new Journal(this);
    st = journal->create();
    if (st) {
      delete journal;
      return (st);
    }
    set_journal(journal);
  }

  /* initialize the cache */
  set_cache(new Cache(this, get_cachesize()));

  /* flush the header page - this will write through disk if logging is
   * enabled */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    return (get_header_page()->flush());
  return (0);
}

ham_status_t
LocalEnvironment::open(const char *filename, ham_u32_t flags,
        ham_size_t cachesize)
{
  ham_status_t st;

  /* initialize the device if it does not yet exist */
  Device *device;
  if (flags & HAM_IN_MEMORY)
    device = new InMemoryDevice(this, flags);
  else
    device = new DiskDevice(this, flags);
  set_device(device);
  if (filename)
    set_filename(filename);
  set_cachesize(cachesize);
  set_flags(flags);

  /* open the file */
  st = device->open(filename, flags);
  if (st) {
    delete device;
    set_device(0);
    return (st);
  }

  /*
   * read the database header
   *
   * !!!
   * now this is an ugly problem - the database header is one page, but
   * what's the size of this page? chances are good that it's the default
   * page-size, but we really can't be sure.
   *
   * read 512 byte and extract the "real" page size, then read
   * the real page. (but i really don't like this)
   */
  {
    Page *page = 0;
    env_header_t *hdr;
    ham_u8_t hdrbuf[512];
    Page fakepage(this);

    /*
     * in here, we're going to set up a faked headerpage for the
     * duration of this call; BE VERY CAREFUL: we MUST clean up
     * at the end of this section or we'll be in BIG trouble!
     */
    fakepage.set_pers((PageData *)hdrbuf);
    set_header_page(&fakepage);

    /*
     * now fetch the header data we need to get an estimate of what
     * the database is made of really.
     *
     * Because we 'faked' a headerpage setup right here, we can now use
     * the regular hamster macros to obtain data from the file
     * header -- pre v1.1.0 code used specially modified copies of
     * those macros here, but with the advent of dual-version database
     * format support here this was getting hairier and hairier.
     * So we now fake it all the way instead.
     */
    st = device->read(0, hdrbuf, sizeof(hdrbuf));
    if (st)
      goto fail_with_fake_cleansing;

    hdr = get_header();

    set_pagesize(get_persistent_pagesize());
    device->set_pagesize(get_persistent_pagesize());

    /** check the file magic */
    if (!compare_magic('H', 'A', 'M', '\0')) {
      ham_log(("invalid file type"));
      st  =  HAM_INV_FILE_HEADER;
      goto fail_with_fake_cleansing;
    }

    /* check the database version; everything > 1.0.9 is ok */
    if (envheader_get_version(hdr, 0) > HAM_VERSION_MAJ ||
        (envheader_get_version(hdr, 0) == HAM_VERSION_MAJ
          && envheader_get_version(hdr, 1) > HAM_VERSION_MIN)) {
      ham_log(("invalid file version"));
      st = HAM_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }
    else if (envheader_get_version(hdr, 0) == 1 &&
      envheader_get_version(hdr, 1) == 0 &&
      envheader_get_version(hdr, 2) <= 9) {
      ham_log(("invalid file version; < 1.0.9 is not supported"));
      st = HAM_INV_FILE_VERSION;
      goto fail_with_fake_cleansing;
    }

    st = 0;

fail_with_fake_cleansing:

    /* undo the headerpage fake first! */
    fakepage.set_pers(0);
    set_header_page(0);

    /* exit when an error was signaled */
    if (st) {
      delete device;
      set_device(0);
      return (st);
    }

    /* now read the "real" header page and store it in the Environment */
    page = new Page(this);
    page->set_device(device);
    st = page->fetch(0);
    if (st) {
      delete page;
      return (st);
    }
    set_header_page(page);
  }

  /*
   * initialize the cache; the cache is needed during recovery, therefore
   * we have to create the cache BEFORE we attempt to recover
   */
  set_cache(new Cache(this, get_cachesize()));

  /* create the freelist */
  if (!(get_flags() & HAM_IN_MEMORY) && !(get_flags() & HAM_READ_ONLY))
    set_freelist(new Freelist(this));

  /*
   * open the logfile and check if we need recovery. first open the
   * (physical) log and re-apply it. afterwards to the same with the
   * (logical) journal.
   */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    st = recover(flags);

  return (st);
}

ham_status_t
LocalEnvironment::rename_db(ham_u16_t oldname, ham_u16_t newname,
    ham_u32_t flags)
{
  ham_u16_t dbi;
  ham_u16_t slot;

  /*
   * check if a database with the new name already exists; also search
   * for the database with the old name
   */
  slot = get_max_databases();
  ham_assert(get_max_databases() > 0);
  for (dbi = 0; dbi<get_max_databases(); dbi++) {
    ham_u16_t name = get_descriptor(dbi)->get_dbname();
    if (name == newname)
      return (HAM_DATABASE_ALREADY_EXISTS);
    if (name == oldname)
      slot = dbi;
  }

  if (slot == get_max_databases())
    return (HAM_DATABASE_NOT_FOUND);

  /* replace the database name with the new name */
  get_descriptor(slot)->set_dbname(newname);
  set_dirty(true);

  /* if the database with the old name is currently open: notify it */
  Environment::DatabaseMap::iterator it = get_database_map().find(oldname);
  if (it != get_database_map().end()) {
    Database *db = it->second;
    it->second->set_name(newname);
    get_database_map().erase(oldname);
    get_database_map().insert(DatabaseMap::value_type(newname, db));
  }

  /* flush the header page if logging is enabled */
  if (get_flags() & HAM_ENABLE_RECOVERY)
    return (get_header_page()->flush());

  return (0);
}

ham_status_t
LocalEnvironment::erase_db(ham_u16_t name, ham_u32_t flags)
{
  Database *db;
  ham_status_t st;
  free_cb_context_t context;
  BtreeIndex *be;

  /* check if this database is still open */
  if (get_database_map().find(name) != get_database_map().end())
    return (HAM_DATABASE_ALREADY_OPEN);

  /*
   * if it's an in-memory environment: no need to go on, if the
   * database was closed, it does no longer exist
   */
  if (get_flags() & HAM_IN_MEMORY)
    return (HAM_DATABASE_NOT_FOUND);

  /* temporarily load the database */
  st = open_db(&db, name, 0, 0);
  if (st)
    return (st);

  /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
  if (get_flags() & HAM_ENABLE_RECOVERY) {
    ham_assert(get_changeset().is_empty());
    ham_assert(get_log()->is_empty());
  }
#endif

  /*
   * delete all blobs and extended keys, also from the cache and
   * the extkey-cache
   *
   * also delete all pages and move them to the freelist; if they're
   * cached, delete them from the cache
   */
  context.db = db;
  be = db->get_btree();

  st = be->enumerate(__free_inmemory_blobs_cb, &context);
  if (st) {
    (void)ham_db_close((ham_db_t *)db, HAM_DONT_LOCK);
    return (st);
  }

  /* if logging is enabled: flush the changeset and the header page */
  if (st == 0 && get_flags() & HAM_ENABLE_RECOVERY) {
    ham_u64_t lsn;
    get_changeset().add_page(get_header_page());
    st = get_incremented_lsn(&lsn);
    if (st == 0)
      st = get_changeset().flush(lsn);
  }

  ham_u32_t descriptor = db->get_btree()->get_descriptor_index();

  /* clean up and return */
  (void)ham_db_close((ham_db_t *)db, HAM_DONT_LOCK);

  /* now set database name to 0 and set the header page to dirty */
  get_descriptor(descriptor)->set_dbname(0);
  get_header_page()->set_dirty(true);

  return (0);
}

ham_status_t
LocalEnvironment::get_database_names(ham_u16_t *names, ham_size_t *count)
{
  ham_u16_t name;
  ham_size_t i = 0;
  ham_size_t max_names = 0;

  max_names = *count;
  *count = 0;

  /* copy each database name to the array */
  ham_assert(get_max_databases() > 0);
  for (i = 0; i<get_max_databases(); i++) {
    name = get_descriptor(i)->get_dbname();
    if (name == 0)
      continue;

    if (*count >= max_names)
      return (HAM_LIMITS_REACHED);

    names[(*count)++] = name;
  }

  return 0;
}

ham_status_t
LocalEnvironment::close(ham_u32_t flags)
{
  ham_status_t st;

  /* close all databases */
  Environment::DatabaseMap::iterator it = get_database_map().begin();
  while (it != get_database_map().end()) {
    Environment::DatabaseMap::iterator it2 = it; it++;
    Database *db = it2->second;
    if (flags & HAM_AUTO_CLEANUP)
      st = ham_db_close((ham_db_t *)db, flags | HAM_DONT_LOCK);
    else
      st = db->close(flags);
    if (st)
      return (st);
  }

  /* flush all committed transactions */
  st = flush_committed_txns();
  if (st)
    return (st);

  /* flush the freelist */
  if (get_freelist()) {
    delete get_freelist();
    set_freelist(0);
  }

  /*
   * if we're not in read-only mode, and not an in-memory-database,
   * and the dirty-flag is true: flush the page-header to disk
   */
  if (get_header_page()
      && !(get_flags()&HAM_IN_MEMORY)
      && get_device()
      && get_device()->is_open()
      && (!(get_flags()&HAM_READ_ONLY))) {
    get_header_page()->flush();
  }

  Device *device = get_device();

  /* close the header page */
  if (get_header_page()) {
    Page *page = get_header_page();
    ham_assert(device);
    if (page->get_pers())
      device->free_page(page);
    delete page;
    set_header_page(0);
  }

  /* flush all pages, get rid of the cache */
  if (get_cache()) {
    (void)flush_all_pages(false);
    delete get_cache();
    set_cache(0);
  }

  /* close the device */
  if (device) {
    if (device->is_open()) {
      if (!(get_flags() & HAM_READ_ONLY))
        device->flush();
      device->close();
    }
    delete device;
    set_device(0);
  }

  /* close the log and the journal */
  if (get_log()) {
    Log *log = get_log();
    log->close(!!(flags & HAM_DONT_CLEAR_LOG));
    delete log;
    set_log(0);
  }
  if (get_journal()) {
    Journal *journal = get_journal();
    journal->close(!!(flags & HAM_DONT_CLEAR_LOG));
    delete journal;
    set_journal(0);
  }

  return 0;
}

ham_status_t
LocalEnvironment::get_parameters(ham_parameter_t *param)
{
  ham_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case HAM_PARAM_CACHESIZE:
        p->value = get_cache()->get_capacity();
        break;
      case HAM_PARAM_PAGESIZE:
        p->value = get_pagesize();
        break;
      case HAM_PARAM_MAX_DATABASES:
        p->value = get_max_databases();
        break;
      case HAM_PARAM_FLAGS:
        p->value = get_flags();
        break;
      case HAM_PARAM_FILEMODE:
        p->value = get_file_mode();
        break;
      case HAM_PARAM_FILENAME:
        if (get_filename().size())
          p->value = (ham_u64_t)(PTR_TO_U64(get_filename().c_str()));
        else
          p->value = 0;
        break;
      case HAM_PARAM_LOG_DIRECTORY:
        if (get_log_directory().size())
          p->value = (ham_u64_t)(PTR_TO_U64(get_log_directory().c_str()));
        else
          p->value = 0;
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
LocalEnvironment::flush(ham_u32_t flags)
{
  ham_status_t st;
  Device *device = get_device();

  (void)flags;

  /* never flush an in-memory-database */
  if (get_flags() & HAM_IN_MEMORY)
    return (0);

  /* flush all committed transactions */
  st = flush_committed_txns();
  if (st)
    return (st);

  /* update the header page, if necessary */
  if (is_dirty()) {
    st = get_header_page()->flush();
    if (st)
      return st;
  }

  /* flush all open pages to disk */
  st = flush_all_pages(true);
  if (st)
    return st;

  /* flush the device - this usually causes a fsync() */
  st = device->flush();
  if (st)
    return st;

  return (HAM_SUCCESS);
}

ham_status_t
LocalEnvironment::create_db(Database **pdb, ham_u16_t dbname,
    ham_u32_t flags, const ham_parameter_t *param)
{
  ham_u16_t keysize = 0;
  ham_u16_t dbi;
  std::string logdir;

  *pdb = 0;

  if (flags & HAM_RECORD_NUMBER)
    keysize = sizeof(ham_u64_t);
  else
    keysize = (ham_u16_t)(DB_CHUNKSIZE - (BtreeKey::ms_sizeof_overhead));

  if (get_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot create database in an read-only environment"));
    return (HAM_WRITE_PROTECTED);
  }

  if (param) {
    for (; param->name; param++) {
      switch (param->name) {
        case HAM_PARAM_KEYSIZE:
          keysize = (ham_u16_t)param->value;
          if (flags & HAM_RECORD_NUMBER) {
            if (keysize > 0 && keysize < sizeof(ham_u64_t)) {
              ham_trace(("invalid keysize %u - must be 8 for HAM_RECORD_NUMBER"
                         " databases", (unsigned)keysize));
              return (HAM_INV_KEYSIZE);
            }
          }
          break;
        default:
          ham_trace(("invalid parameter 0x%x (%d)", param->name, param->name));
          return (HAM_INV_PARAMETER);
      }
    }
  }

  ham_u32_t mask = HAM_DISABLE_VAR_KEYLEN
                    | HAM_ENABLE_DUPLICATES
                    | HAM_ENABLE_EXTENDED_KEYS
                    | HAM_RECORD_NUMBER;
  if (flags & ~mask) {
    ham_trace(("invalid flags(s) 0x%x", flags & ~mask));
    return (HAM_INV_PARAMETER);
  }

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, dbname, flags);

  /* check if this database name is unique */
  ham_assert(get_max_databases() > 0);
  for (ham_size_t i = 0; i < get_max_databases(); i++) {
    ham_u16_t name = get_descriptor(i)->get_dbname();
    if (!name)
      continue;
    if (name == dbname || dbname == HAM_FIRST_DATABASE_NAME) {
      delete db;
      return (HAM_DATABASE_ALREADY_EXISTS);
    }
  }

  /* find a free slot in the BtreeDescriptor array and store the name */
  ham_assert(get_max_databases() > 0);
  for (dbi = 0; dbi < get_max_databases(); dbi++) {
    ham_u16_t name = get_descriptor(dbi)->get_dbname();
    if (!name) {
      get_descriptor(dbi)->set_dbname(dbname);
      break;
    }
  }
  if (dbi == get_max_databases()) {
    delete db;
    return (HAM_LIMITS_REACHED);
  }

  /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
  if (get_flags() & HAM_ENABLE_RECOVERY) {
    ham_assert(get_changeset().is_empty());
    ham_assert(get_log()->is_empty());
  }
#endif

  /* initialize the Database */
  ham_status_t st = db->create(dbi, keysize);
  if (st) {
    delete db;
    return (st);
  }

  set_dirty(true);

  /* if logging is enabled: flush the changeset and the header page */
  if (st == 0 && get_flags() & HAM_ENABLE_RECOVERY) {
    ham_u64_t lsn;
    get_changeset().add_page(get_header_page());
    st = get_incremented_lsn(&lsn);
    if (st == 0)
      st = get_changeset().flush(lsn);
    if (st) {
      delete db;
      return (st);
    }
  }

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  get_database_map()[dbname] = db;

  *pdb = db;
  
  return (0);
}

ham_status_t
LocalEnvironment::open_db(Database **pdb, ham_u16_t dbname, ham_u32_t flags,
    const ham_parameter_t *param)
{
  ham_u16_t dbi;

  *pdb = 0;

  ham_u32_t mask = HAM_DISABLE_VAR_KEYLEN
                    | HAM_ENABLE_EXTENDED_KEYS
                    | HAM_READ_ONLY;
  if (flags & ~mask) {
    ham_trace(("invalid flags(s) 0x%x", flags & ~mask));
    return (HAM_INV_PARAMETER);
  }

  if (param && param->name != 0) {
    ham_trace(("invalid parameter"));
    return (HAM_INV_PARAMETER);
  }

  /* make sure that this database is not yet open */
  if (get_database_map().find(dbname) !=  get_database_map().end())
    return (HAM_DATABASE_ALREADY_OPEN);

  /* create a new Database object */
  LocalDatabase *db = new LocalDatabase(this, dbname, flags);

  ham_assert(get_allocator());
  ham_assert(get_device());
  ham_assert(0 != get_header_page());
  ham_assert(get_max_databases() > 0);

  /* search for a database with this name */
  for (dbi = 0; dbi < get_max_databases(); dbi++) {
    ham_u16_t name = get_descriptor(dbi)->get_dbname();
    if (!name)
      continue;
    if (dbname == HAM_FIRST_DATABASE_NAME || dbname == name)
      break;
  }

  if (dbi == get_max_databases()) {
    delete db;
    return (HAM_DATABASE_NOT_FOUND);
  }

  /* open the database */
  ham_status_t st = db->open(dbi);
  if (st) {
    delete db;
    ham_trace(("Database could not be opened"));
    return (st);
  }

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  get_database_map()[dbname] = db;

  *pdb = db;

  return (0);
}

ham_status_t
LocalEnvironment::txn_begin(Transaction **txn, const char *name,
    ham_u32_t flags)
{
  ham_status_t st = 0;

  *txn = new Transaction(this, name, flags);

  /* append journal entry */
  if (get_flags() & HAM_ENABLE_RECOVERY
      && get_flags() & HAM_ENABLE_TRANSACTIONS) {
    ham_u64_t lsn;
    st = get_incremented_lsn(&lsn);
    if (st == 0)
      st = get_journal()->append_txn_begin(*txn, this, name, lsn);
  }

  return (st);
}

ham_status_t
LocalEnvironment::txn_commit(Transaction *txn, ham_u32_t flags)
{
  ham_status_t st = 0;

  /* are cursors attached to this txn? if yes, fail */
  if (txn->get_cursor_refcount()) {
    ham_trace(("Transaction cannot be committed till all attached "
          "Cursors are closed"));
    return (HAM_CURSOR_STILL_OPEN);
  }

  /* append journal entry */
  if (get_flags() & HAM_ENABLE_RECOVERY
      && get_flags() & HAM_ENABLE_TRANSACTIONS) {
    ham_u64_t lsn;
    st = get_incremented_lsn(&lsn);
    if (st)
      return (st);
    st = get_journal()->append_txn_commit(txn, lsn);
    if (st)
      return (st);
  }

  return (txn->commit(flags));
}

ham_status_t
LocalEnvironment::txn_abort(Transaction *txn, ham_u32_t flags)
{
  ham_status_t st = 0;

  /* are cursors attached to this txn? if yes, fail */
  if (txn->get_cursor_refcount()) {
    ham_trace(("Transaction cannot be aborted till all attached "
          "Cursors are closed"));
    return (HAM_CURSOR_STILL_OPEN);
  }

  /* append journal entry */
  if (get_flags() & HAM_ENABLE_RECOVERY
      && get_flags() & HAM_ENABLE_TRANSACTIONS) {
    ham_u64_t lsn;
    st = get_incremented_lsn(&lsn);
    if (st)
      return (st);
    st = get_journal()->append_txn_abort(txn, lsn);
    if (st)
      return (st);
  }

  return (txn->abort(flags));
}

ham_status_t
LocalEnvironment::recover(ham_u32_t flags)
{
  ham_status_t st;
  Log *log = new Log(this);
  Journal *journal = new Journal(this);

  ham_assert(get_flags() & HAM_ENABLE_RECOVERY);

  /* open the log and the journal (if transactions are enabled) */
  st = log->open();
  set_log(log);
  if (st == HAM_FILE_NOT_FOUND)
    st = log->create();
  if (st)
    goto bail;
  if (get_flags() & HAM_ENABLE_TRANSACTIONS) {
    st = journal->open();
    set_journal(journal);
    if (st == HAM_FILE_NOT_FOUND)
      st = journal->create();
    if (st)
      goto bail;
  }

  /* success - check if we need recovery */
  if (!log->is_empty()) {
    if (flags & HAM_AUTO_RECOVERY) {
      st = log->recover();
      if (st)
        goto bail;
    }
    else {
      st = HAM_NEED_RECOVERY;
      goto bail;
    }
  }

  if (get_flags() & HAM_ENABLE_TRANSACTIONS) {
    if (!journal->is_empty()) {
      if (flags & HAM_AUTO_RECOVERY) {
        st = journal->recover();
        if (st)
          goto bail;
      }
      else {
        st = HAM_NEED_RECOVERY;
        goto bail;
      }
    }
  }

goto success;

bail:
  /* in case of errors: close log and journal, but do not delete the files */
  if (log) {
    log->close(true);
    delete log;
    set_log(0);
  }
  if (journal) {
    journal->close(true);
    delete journal;
    set_journal(0);
  }
  return (st);

success:
  /* done with recovering - if there's no log and/or no journal then
   * create them and store them in the environment */
  if (!log) {
    log = new Log(this);
    st = log->create();
    if (st)
      return (st);
  }
  set_log(log);

  if (get_flags() & HAM_ENABLE_TRANSACTIONS) {
    if (!journal) {
      journal = new Journal(this);
      st = journal->create();
      if (st)
        return (st);
    }
    set_journal(journal);
  }
  else if (journal)
    delete journal;

  return (0);
}

} // namespace hamsterdb
