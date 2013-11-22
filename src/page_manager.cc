/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include <string.h>

#include "page.h"
#include "cache.h"
#include "device.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

#include "page_manager.h"

namespace hamsterdb {

PageManager::PageManager(LocalEnvironment *env, ham_u32_t cachesize)
  : m_env(env), m_cache(0), m_freelist(0), m_page_count_fetched(0),
    m_page_count_flushed(0), m_page_count_index(0), m_page_count_blob(0),
    m_page_count_freelist(0)
{
  m_cache = new Cache(env, cachesize);
}

PageManager::~PageManager()
{
  if (m_freelist) {
    delete m_freelist;
    m_freelist = 0;
  }

  if (m_cache) {
    delete m_cache;
    m_cache = 0;
  }
}

void
PageManager::get_metrics(ham_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = m_page_count_fetched;
  metrics->page_count_flushed = m_page_count_flushed;
  metrics->page_count_type_index   = m_page_count_index;
  metrics->page_count_type_blob    = m_page_count_blob;
  metrics->page_count_type_freelist= m_page_count_freelist;

  if (m_cache)
    m_cache->get_metrics(metrics);

  if (m_freelist)
    m_freelist->get_metrics(metrics);
}

ham_status_t
PageManager::fetch_page(Page **ppage, LocalDatabase *db, ham_u64_t address,
                bool only_from_cache)
{
  ham_status_t st;

  *ppage = 0;

  /* fetch the page from the cache */
  Page *page = m_cache->get_page(address, Cache::NOREMOVE);
  if (page) {
    *ppage = page;
    ham_assert(page->get_data());
    /* store the page in the changeset if recovery is enabled */
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().add_page(page);
    return (0);
  }

  if (only_from_cache || m_env->get_flags() & HAM_IN_MEMORY)
    return (0);

  ham_assert(m_cache->get_page(address) == 0);

  /* can we allocate a new page for the cache? */
  if (m_cache->is_too_big()) {
    if (m_env->get_flags() & HAM_CACHE_STRICT)
      return (HAM_CACHE_FULL);
  }

  page = new Page(m_env, db);
  st = page->fetch(address);
  if (st) {
    delete page;
    return (st);
  }

  ham_assert(page->get_data());

  /* store the page in the cache */
  m_cache->put_page(page);

  /* store the page in the changeset */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  *ppage = page;

  m_page_count_fetched++;

  return (0);
}

ham_status_t
PageManager::alloc_page(Page **ppage, LocalDatabase *db, ham_u32_t page_type,
                ham_u32_t flags)
{
  ham_status_t st;
  ham_u64_t tellpos = 0;
  Page *page = 0;
  bool allocated_by_me = false;

  *ppage = 0;
  ham_assert(0 == (flags & ~(PageManager::kIgnoreFreelist
                                | PageManager::kClearWithZero)));

  /* first, we ask the freelist for a page */
  if (!(flags & PageManager::kIgnoreFreelist) && m_freelist) {
    if ((st = m_freelist->alloc_page(&tellpos)))
      return (st);
    if (tellpos > 0) {
      ham_assert(tellpos % m_env->get_pagesize() == 0);
      /* try to fetch the page from the cache */
      page = m_cache->get_page(tellpos, 0);
      if (page)
        goto done;
      /* allocate a new page structure and read the page from disk */
      page = new Page(m_env, db);
      st = page->fetch(tellpos);
      if (st) {
        delete page;
        return (st);
      }
      goto done;
    }
  }

  if (!page) {
    page = new Page(m_env, db);
    allocated_by_me = true;
  }

  /* can we allocate a new page for the cache? */
  if (m_cache->is_too_big()) {
    if (m_env->get_flags() & HAM_CACHE_STRICT) {
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
  /* clear the page with zeroes?  */
  if (flags & PageManager::kClearWithZero)
    memset(page->get_data(), 0, m_env->get_pagesize());

  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(page_type);
  page->set_dirty(true);
  page->set_db(db);

  /* an allocated page is always flushed if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  /* store the page in the cache */
  m_cache->put_page(page);

  switch (page_type) {
    case Page::kTypeBindex:
    case Page::kTypeBroot: {
      memset(page->get_payload(), 0, sizeof(PBtreeNode));
      m_page_count_index++;
      break;
    }
    case Page::kTypeFreelist:
      m_page_count_freelist++;
      break;
    case Page::kTypeBlob:
      m_page_count_blob++;
      break;
    default:
      break;
  }

  *ppage = page;
  return (0);
}

ham_status_t
PageManager::alloc_blob(Database *db, ham_u32_t size, ham_u64_t *address,
                        bool *allocated)
{
  *address = 0;
  *allocated = false;

  // first check the freelist
  if (m_freelist) {
    ham_status_t st = m_freelist->alloc_area(size, address);
    if (st)
      return (st);
    if (*address)
      return (0);
  }

  return (0);
}

static bool
flush_all_pages_callback(Page *page, Database *db, ham_u32_t flags)
{
  page->get_env()->get_page_manager()->flush_page(page);

  /*
   * if the page is deleted, uncouple all cursors, then
   * free the memory of the page, then remove from the cache
   */
  if (flags == 0) {
    (void)BtreeCursor::uncouple_all_cursors(page);
    return (true);
  }

  return (false);
}

ham_status_t
PageManager::flush_all_pages(bool nodelete)
{
  return (m_cache->visit(flush_all_pages_callback, 0, nodelete ? 1 : 0));
}

static ham_status_t
purge_callback(Page *page)
{
  ham_status_t st = BtreeCursor::uncouple_all_cursors(page);
  if (st)
    return (st);

  st = page->get_env()->get_page_manager()->flush_page(page);
  if (st)
    return (st);

  delete page;
  return (0);
}

ham_status_t
PageManager::purge_cache()
{
  /* in-memory-db: don't remove the pages or they would be lost */
  if (m_env->get_flags() & HAM_IN_MEMORY)
    return (0);

  return (m_cache->purge(purge_callback,
              (m_env->get_flags() & HAM_CACHE_STRICT) != 0));
}

ham_status_t
PageManager::reclaim_space()
{
  if (!m_freelist)
    return (0);

  ham_assert(!(m_env->get_flags() & HAM_DISABLE_RECLAIM_INTERNAL));

  ham_u32_t pagesize = m_env->get_pagesize();
  ham_u64_t filesize;
  ham_status_t st = m_env->get_device()->get_filesize(&filesize);
  if (st)
    return (st);

  ham_u64_t new_size = filesize;
  while (true) {
    if (!m_freelist->is_page_free(new_size - pagesize))
      break;
    new_size -= pagesize;
    st = m_freelist->truncate_page(new_size);
    if (st)
      return (st);
  }

  if (new_size == filesize)
    return (0);
  return (m_env->get_device()->truncate(new_size));
}

static bool
db_close_callback(Page *page, Database *db, ham_u32_t flags)
{
  LocalEnvironment *env = page->get_env();

  if (page->get_db() == db && page->get_address() != 0) {
    (void)env->get_page_manager()->flush_page(page);

    /*
     * TODO is this really necessary?? i don't think so
     */
    if (page->get_data() &&
        (!(page->get_flags() & Page::kNpersNoHeader)) &&
          (page->get_type() == Page::kTypeBroot ||
            page->get_type() == Page::kTypeBindex)) {
      (void)BtreeCursor::uncouple_all_cursors(page);
    }

    return (true);
  }

  return (false);
}

ham_status_t
PageManager::check_integrity()
{
  if (m_cache)
    return (m_cache->check_integrity());
  return (0);
}

ham_u64_t
PageManager::get_cache_capacity() const
{
  return (m_cache->get_capacity());
}

void
PageManager::close_database(Database *db)
{
  if (m_cache)
    (void)m_cache->visit(db_close_callback, db, 0);
}

ham_status_t
PageManager::add_to_freelist(Page *page)
{
  Freelist *f = get_freelist();

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  return (f ? f->free_page(page) : 0);
}

ham_status_t
PageManager::close()
{
  ham_status_t st = flush_all_pages();
  if (st)
    return (st);

  // reclaim unused disk space
  // if logging is enabled: also flush the changeset to write back the
  // modified freelist pages
  bool try_reclaim = m_env->get_flags() & HAM_DISABLE_RECLAIM_INTERNAL
                ? false
                : true;
#ifdef WIN32
  // Win32: it's not possible to truncate the file while there's an active
  // mapping, therefore only reclaim if memory mapped I/O is disabled
  if (!(m_env->get_flags() & HAM_DISABLE_MMAP))
    try_reclaim = false;
#endif

  if (try_reclaim) {
    st = reclaim_space();
    if (st)
      return (st);

    if (m_env->get_flags() & HAM_ENABLE_RECOVERY) {
      st = m_env->get_changeset().flush(m_env->get_incremented_lsn());
      if (st)
        return (st);
    }
  }

  // flush again; there were pages fetched during reclaim, and they have
  // to be released now
  st = flush_all_pages();
  if (st)
    return (st);

  return (st);
}

} // namespace hamsterdb

