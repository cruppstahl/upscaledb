/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "device.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

#include "page_manager.h"

namespace hamsterdb {

PageManager::PageManager(LocalEnvironment *env, ham_u32_t cache_size)
  : m_env(env), m_freelist(0), m_cache_size(cache_size), m_epoch(0),
    m_page_count_fetched(0), m_page_count_flushed(0), m_page_count_index(0),
    m_page_count_blob(0), m_page_count_freelist(0), m_cache_hits(0),
    m_cache_misses(0)
{
}

PageManager::~PageManager()
{
  if (m_freelist) {
    delete m_freelist;
    m_freelist = 0;
  }
}

void
PageManager::get_metrics(ham_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = m_page_count_fetched;
  metrics->page_count_flushed = m_page_count_flushed;
  metrics->page_count_type_index = m_page_count_index;
  metrics->page_count_type_blob = m_page_count_blob;
  metrics->page_count_type_freelist= m_page_count_freelist;
  metrics->cache_hits = m_cache_hits;
  metrics->cache_misses = m_cache_misses;

  if (m_freelist)
    m_freelist->get_metrics(metrics);
}

Page *
PageManager::fetch_page(LocalDatabase *db, ham_u64_t address,
                bool only_from_cache)
{
  /* fetch the page from our list */
  Page *page = fetch_page(address);
  if (page) {
    m_cache_hits++;

    ham_assert(page->get_data());
    /* store the page in the changeset if recovery is enabled */
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().add_page(page);
    return (page);
  }

  m_cache_misses++;

  if (only_from_cache || m_env->get_flags() & HAM_IN_MEMORY)
    return (0);

  page = new Page(m_env, db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  ham_assert(page->get_data());

  /* store the page in the list */
  store_page(page);

  /* store the page in the changeset */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  m_page_count_fetched++;

  return (page);
}

Page *
PageManager::alloc_page(LocalDatabase *db, ham_u32_t page_type, ham_u32_t flags)
{
  ham_u64_t freelist = 0;
  Page *page = 0;

  ham_assert(0 == (flags & ~(PageManager::kIgnoreFreelist
                                | PageManager::kClearWithZero)));

  /* first, we ask the freelist for a page */
  if (!(flags & PageManager::kIgnoreFreelist) && m_freelist) {
    /* check the internal list for a free page */
    for (PageMap::iterator it = m_page_map.begin();
                  it != m_page_map.end(); it++) {
      if (it->second.is_free) {
        it->second.is_free = false;
        page = it->second.page;
        goto done;
      }
    }

    freelist = m_freelist->alloc_page();
    if (freelist > 0) {
      ham_assert(freelist % m_env->get_page_size() == 0);
      /* try to fetch the page from the cache */
      page = fetch_page(freelist);
      if (page)
        goto done;
      /* allocate a new page structure and read the page from disk */
      page = new Page(m_env, db);
      page->fetch(freelist);
      goto done;
    }
  }

  if (!page)
    page = new Page(m_env, db);

  ham_assert(freelist == 0);
  page->allocate();

done:
  /* clear the page with zeroes?  */
  if (flags & PageManager::kClearWithZero)
    memset(page->get_data(), 0, m_env->get_page_size());

  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(page_type);
  page->set_dirty(true);
  page->set_db(db);

  /* an allocated page is always flushed if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  /* store the page in the cache */
  store_page(page);

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

  return (page);
}

ham_u64_t
PageManager::alloc_blob(Database *db, ham_u32_t size, bool *pallocated)
{
  ham_u64_t address = 0;

  if (pallocated)
    *pallocated = false;

  // first check the freelist
  if (m_freelist)
    address = m_freelist->alloc_area(size);

  return (address);
}

void
PageManager::flush_all_pages(bool nodelete)
{
  for (PageMap::iterator it = m_page_map.begin();
                  it != m_page_map.end(); it++) {
    flush_page(it->second.page);
    // if the page will be deleted then uncouple all cursors
    if (!nodelete) {
      BtreeCursor::uncouple_all_cursors(it->second.page);
      delete it->second.page;
    }
  }

  if (!nodelete)
    m_page_map.clear();
}

void
PageManager::purge_cache()
{
  /* in-memory-db: don't remove the pages or they would be lost */
  if (m_env->get_flags() & HAM_IN_MEMORY)
    return;

  /* calculate a limit of pages that we will flush */
  unsigned max_pages = m_page_map.size();

  if (max_pages == 0)
    max_pages = 1;
  /* but still we set an upper limit to avoid IO spikes */
  else if (max_pages > kPurgeLimit)
    max_pages = kPurgeLimit;

  unsigned i = 0;
  PageMap::iterator it = m_page_map.begin();
  while (it != m_page_map.end() && i < max_pages) {
    Page *page = it->second.page;
    /* pick the page if it's unused, (not in a changeset), NOT mapped and old
     * enough */
    if (page->get_flags() & Page::kNpersMalloc
            && !m_env->get_changeset().contains(page)
            && page->get_address() > 0
            && m_epoch - it->second.birthday > kPurgeThreshold) {
      flush_page(it->second.page);
      BtreeCursor::uncouple_all_cursors(it->second.page);
      delete it->second.page;
      m_page_map.erase(it++);
      i++;
    }
    else {
      ++it;
    }
  }
}

void
PageManager::close_database(Database *db)
{
  PageMap::iterator it = m_page_map.begin();
  while (it != m_page_map.end()) {
    Page *page = it->second.page;
    if (page->get_address() > 0 && page->get_db() == db) {
      flush_page(page);
      BtreeCursor::uncouple_all_cursors(page);
      delete page;
      m_page_map.erase(it++);
    }
    else {
      ++it;
    }
  }
}

void
PageManager::reclaim_space()
{
  if (!m_freelist)
    return;

  ham_assert(!(m_env->get_flags() & HAM_DISABLE_RECLAIM_INTERNAL));

  ham_u32_t page_size = m_env->get_page_size();
  ham_u64_t filesize = m_env->get_device()->get_filesize();

  // ignore subsequent errors - we're closing the database, and if
  // reclaiming fails then this is not a tragedy
  try {
    ham_u64_t new_size = filesize;
    while (true) {
      if (!m_freelist->is_page_free(new_size - page_size))
        break;
      new_size -= page_size;
      m_freelist->truncate_page(new_size);
    }
    if (new_size == filesize)
      return;
    m_env->get_device()->truncate(new_size);
  }
  catch (Exception &) {
  }
}

void
PageManager::check_integrity()
{
  // TODO
}

void
PageManager::add_to_freelist(Page *page)
{
  PageMap::iterator it = m_page_map.find(page->get_address());
  ham_assert(it != m_page_map.end());
  ham_assert(it->second.is_free == false);
  it->second.is_free = true;

  Freelist *f = get_freelist();

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  if (f)
    f->free_page(page);
}

void
PageManager::close()
{
  flush_all_pages();

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
    reclaim_space();

    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().flush(m_env->get_incremented_lsn());
  }

  // flush again; there were pages fetched during reclaim, and they have
  // to be released now
  flush_all_pages();
}

} // namespace hamsterdb

