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

#include "util.h"
#include "page.h"
#include "device.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

#include "page_manager.h"

namespace hamsterdb {

PageManager::PageManager(LocalEnvironment *env, ham_u64_t cache_size)
  : m_env(env), m_cache(env, cache_size), m_needs_flush(false),
    m_state_page(0), m_last_blob_page(0), m_page_count_fetched(0),
    m_page_count_flushed(0), m_page_count_index(0), m_page_count_blob(0),
    m_page_count_page_manager(0), m_freelist_hits(0), m_freelist_misses(0)
{
}

PageManager::~PageManager()
{
}

void
PageManager::load_state(ham_u64_t pageid)
{
  if (m_state_page)
    delete m_state_page;

  m_state_page = new Page(m_env, 0);
  m_state_page->fetch(pageid);

  m_free_pages.clear();

  Page *page = m_state_page;
  ham_u32_t page_size = m_env->get_page_size();

  while (1) {
    ham_assert(page->get_type() == Page::kTypePageManager);
    ham_u8_t *p = page->get_payload();

    // get the overflow address
    ham_u64_t overflow = ham_db2h64(*(ham_u64_t *)p);
    p += 8;

    // get the number of stored elements
    ham_u32_t counter = ham_db2h32(*(ham_u32_t *)p);
    p += 4;

    // now read all pages
    for (ham_u32_t i = 0; i < counter; i++) {
      // 4 bits page_counter, 4 bits for number of following bytes
      int page_counter = (*p & 0xf0) >> 4;
      int num_bytes = *p & 0x0f;
      ham_assert(page_counter > 0);
      ham_assert(num_bytes <= 8);
      p += 1;

      ham_u64_t id = decode(num_bytes, p);
      p += num_bytes;

      m_free_pages[id * page_size] = page_counter;
    }

    // load the overflow page
    if (overflow)
      page = fetch_page(0, overflow);
    else
      break;
  }
}

ham_u64_t
PageManager::store_state()
{
  // no modifications? then simply return the old blobid
  if (!m_needs_flush)
    return (m_state_page ? m_state_page->get_address() : 0);

  m_needs_flush = false;

  if (!m_state_page) {
    m_state_page = new Page(m_env, 0);
    m_state_page->allocate(Page::kTypePageManager);
    // reset the overflow pointer
    ham_u8_t *p = m_state_page->get_payload();
    *(ham_u64_t *)p = 0;
  }

  ham_u32_t page_size = m_env->get_page_size();

  /* store the page in the changeset if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(m_state_page);

  FreeMap::const_iterator it = m_free_pages.begin();
  Page *page = m_state_page;

  // make sure that the page is logged
  page->set_dirty(true);

  while (it != m_free_pages.end()) {
    // this is where we will store the data
    ham_u8_t *p = page->get_payload();
    p += 8;   // leave room for the pointer to the next page
    p += 4;   // leave room for the counter

    ham_u32_t counter = 0;

    while (it != m_free_pages.end()) {
      // 9 bytes is the maximum amount of storage that we will need for a
      // new entry; if it does not fit then break
      if ((p + 9) - page->get_payload() >= m_env->get_usable_page_size())
        break;

      // ... and check if the next entry (and the following) are directly
      // next to the current page
      ham_u32_t page_counter = 1;
      ham_u64_t base = it->first;
      ham_u64_t current = it->first;

      // move to the next entry
      it++;

      for (; it != m_free_pages.end() && page_counter < 16 - 1; it++) {
        if (it->first != current + page_size)
          break;
        current += page_size;
        page_counter++;
      }

      // now |base| is the start of a sequence of free pages, and the
      // sequence has |page_counter| pages
      //
      // This is encoded as
      // - 1 byte header
      //   - 4 bits for |page_counter|
      //   - 4 bits for the number of bytes following ("n")
      // - n byte page-id (div page_size)
      ham_assert(page_counter < 16);
      ham_assert(base % page_size == 0);
      int num_bytes = encode(p + 1, base / page_size);
      *p = (page_counter << 4) | num_bytes;
      p += 1 + num_bytes;

      counter++;
    }

    p = page->get_payload();
    ham_u64_t next_pageid = ham_db2h64(*(ham_u64_t *)p);
    *(ham_u64_t *)p = ham_h2db64(0);
    p += 8;  // overflow page

    // now store the counter
    *(ham_u32_t *)p = ham_h2db32(counter);
    p += 4;

    // are we done? if not then continue with the next page
    if (it != m_free_pages.end()) {
      // allocate (or fetch) an overflow page
      if (!next_pageid) {
        Page *new_page = alloc_page(0, Page::kTypePageManager, kIgnoreFreelist);
        // patch the overflow pointer in the old (current) page
        p = page->get_payload();
        *(ham_u64_t *)p = ham_h2db64(new_page->get_address());
        page = new_page;
        // reset the overflow pointer in the new page
        p = page->get_payload();
        *(ham_u64_t *)p = 0;
      }
      else
        page = fetch_page(0, next_pageid);

      // make sure that the page is logged
      page->set_dirty(true);
    }
  }

  return (m_state_page->get_address());
}

void
PageManager::get_metrics(ham_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = m_page_count_fetched;
  metrics->page_count_flushed = m_page_count_flushed;
  metrics->page_count_type_index = m_page_count_index;
  metrics->page_count_type_blob = m_page_count_blob;
  metrics->page_count_type_page_manager = m_page_count_page_manager;
  metrics->freelist_hits = m_freelist_hits;
  metrics->freelist_misses = m_freelist_misses;
  m_cache.get_metrics(metrics);
}

Page *
PageManager::fetch_page(LocalDatabase *db, ham_u64_t address,
                bool only_from_cache)
{
  Page *page = 0;

  /* fetch the page from the cache */
  page = m_cache.get_page(address);
  if (page) {
    ham_assert(page->get_data());
    /* store the page in the changeset if recovery is enabled */
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().add_page(page);
    return (page);
  }

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
  ham_u64_t address = 0;
  Page *page = 0;
  ham_u32_t page_size = m_env->get_page_size();

  /* first check the internal list for a free page */
  if ((flags & kIgnoreFreelist) == 0 && !m_free_pages.empty()) {
    FreeMap::iterator it = m_free_pages.begin();

    address = it->first;
    ham_assert(address % page_size == 0);
    /* remove the page from the freelist */
    m_free_pages.erase(it);
    m_needs_flush = true;

    m_freelist_hits++;

    /* try to fetch the page from the cache */
    page = fetch_page(address);
    if (page)
      goto done;
    /* allocate a new page structure and read the page from disk */
    page = new Page(m_env, db);
    page->fetch(address);
    goto done;
  }

  m_freelist_misses++;

  if (!page)
    page = new Page(m_env, db);

  page->allocate(page_type);

done:
  /* clear the page with zeroes?  */
  if (flags & PageManager::kClearWithZero)
    memset(page->get_data(), 0, page_size);

  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(page_type);
  page->set_dirty(true);
  page->set_db(db);

  /* an allocated page is always flushed if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  /* store the page in the cache */
  store_page(page, flags & kDisableStoreState);

  switch (page_type) {
    case Page::kTypeBindex:
    case Page::kTypeBroot: {
      memset(page->get_payload(), 0, sizeof(PBtreeNode));
      m_page_count_index++;
      break;
    }
    case Page::kTypePageManager:
      m_page_count_page_manager++;
      break;
    case Page::kTypeBlob:
      m_page_count_blob++;
      break;
    default:
      break;
  }

  return (page);
}

Page *
PageManager::alloc_multiple_blob_pages(LocalDatabase *db, int num_pages)
{
  // allocate only one page? then use the normal ::alloc_page() method
  if (num_pages == 1)
    return (alloc_page(db, Page::kTypeBlob));

  Page *page = 0;
  ham_u32_t page_size = m_env->get_page_size();

  // Now check the freelist
  if (!m_free_pages.empty()) {
    for (FreeMap::iterator it = m_free_pages.begin(); it != m_free_pages.end();
            it++) {
      if (it->second >= num_pages) {
        for (int i = 0; i < num_pages; i++) {
          if (i == 0) {
            page = fetch_page(db, it->first);
            page->set_type(Page::kTypeBlob);
          }
          else {
            Page *p = fetch_page(db, it->first + (i * page_size));
            p->set_type(Page::kTypeBlob);
            p->set_flags(p->get_flags() | Page::kNpersNoHeader);
          }
        }
        if (it->second > num_pages)
          m_free_pages[it->first + num_pages * page_size]
                = it->second - num_pages;
        m_free_pages.erase(it);
        return (page);
      }
    }
  }

  // Freelist lookup was not successful -> allocate new pages. Only the first
  // page is a regular page; all others do not have page headers.
  //
  // disable "store state": the PageManager otherwise could alloc overflow
  // pages in the middle of our blob sequence.
  ham_u32_t flags = kIgnoreFreelist | kDisableStoreState;
  for (int i = 0; i < num_pages; i++) {
    if (page == 0)
      page = alloc_page(db, Page::kTypeBlob, flags);
    else {
      Page *p = alloc_page(db, Page::kTypeBlob, flags);
      p->set_flags(p->get_flags() | Page::kNpersNoHeader);
    }
  }

  // now store the state
  maybe_store_state();

  return (page);
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

void
PageManager::flush_all_pages(bool nodelete)
{
  if (nodelete == false)
    m_last_blob_page = 0;
  m_cache.visit(flush_all_pages_callback, 0, nodelete ? 1 : 0);

  if (m_state_page)
    flush_page(m_state_page);
}

void
PageManager::purge_callback(Page *page, PageManager *pm)
{
  BtreeCursor::uncouple_all_cursors(page);
  page->get_env()->get_page_manager()->flush_page(page);

  if (pm->m_last_blob_page == page)
    pm->m_last_blob_page = 0;

  delete page;
}

void
PageManager::purge_cache()
{
  // in-memory-db: don't remove the pages or they would be lost
  if (m_env->get_flags() & HAM_IN_MEMORY || !m_cache.is_full())
    return;

  // Purge as many pages as possible to get memory usage down to the
  // cache's limit.
  //
  // By default this is capped to |kPurgeAtLeast| pages to avoid I/O spikes.
  // In benchmarks this has proven to be a good limit.
  ham_u32_t max_pages = m_cache.get_capacity() / m_env->get_page_size();
  if (max_pages == 0)
    max_pages = 1;
  ham_u32_t limit = m_cache.get_current_elements() - max_pages;
  if (limit < kPurgeAtLeast)
    limit = kPurgeAtLeast;
  m_cache.purge(purge_callback, this, limit);
}

static bool
db_close_callback(Page *page, Database *db, ham_u32_t flags)
{
  LocalEnvironment *env = page->get_env();

  if (page->get_db() == db && page->get_address() != 0) {
    env->get_page_manager()->flush_page(page);

    // TODO is this really necessary?? i don't think so
    if (page->get_data() &&
        (!(page->get_flags() & Page::kNpersNoHeader)) &&
          (page->get_type() == Page::kTypeBroot ||
            page->get_type() == Page::kTypeBindex)) {
      BtreeCursor::uncouple_all_cursors(page);
    }

    return (true);
  }

  return (false);
}

void
PageManager::close_database(Database *db)
{
  m_last_blob_page = 0;

  m_cache.visit(db_close_callback, db, 0);
}

void
PageManager::reclaim_space()
{
  m_last_blob_page = 0;
  ham_assert(!(m_env->get_flags() & HAM_DISABLE_RECLAIM_INTERNAL));
  bool do_truncate = false;
  ham_u64_t file_size = m_env->get_device()->get_file_size();
  ham_u32_t page_size = m_env->get_page_size();

  while (m_free_pages.size() > 1) {
    FreeMap::iterator fit = m_free_pages.find(file_size - page_size);
    if (fit != m_free_pages.end()) {
      Page *page = m_cache.get_page(fit->first);
      if (page) {
        m_cache.remove_page(page);
        delete page;
      }
      file_size -= page_size;
      do_truncate = true;
      m_free_pages.erase(fit);
      continue;
    }
    break;
  }

  if (do_truncate) {
    m_needs_flush = true;
    maybe_store_state(true);
    m_env->get_device()->truncate(file_size);
  }
}

void
PageManager::add_to_freelist(Page *page, int page_count)
{
  ham_assert(page_count > 0);

  m_needs_flush = true;
  m_free_pages[page->get_address()] = page_count;

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  // do not call maybe_store_state() - this change in the state is not
  // relevant for logging.
}

void
PageManager::close()
{
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

  // flush all dirty pages to disk
  flush_all_pages();

  delete m_state_page;
  m_state_page = 0;
  m_last_blob_page = 0;
}

int
PageManager::encode(ham_u8_t *p, ham_u64_t n)
{
  if (n <= 0xf) {
    *p = n;
    return (1);
  }
  if (n <= 0xff) {
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (2);
  }
  if (n <= 0xfff) {
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (3);
  }
  if (n <= 0xffff) {
    *(p + 3) = (n & 0xf000) >> 12;
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (4);
  }
  if (n <= 0xfffff) {
    *(p + 4) = (n & 0xf0000) >> 16;
    *(p + 3) = (n & 0xf000) >> 12;
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (5);
  }
  if (n <= 0xffffff) {
    *(p + 5) = (n & 0xf00000) >> 24;
    *(p + 4) = (n & 0xf0000) >> 16;
    *(p + 3) = (n & 0xf000) >> 12;
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (6);
  }
  if (n <= 0xfffffff) {
    *(p + 6) = (n & 0xf000000) >> 32;
    *(p + 5) = (n & 0xf00000) >> 24;
    *(p + 4) = (n & 0xf0000) >> 16;
    *(p + 3) = (n & 0xf000) >> 12;
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (7);
  }
  *(p + 7) = (n & 0xf0000000) >> 36;
  *(p + 6) = (n & 0xf000000) >> 32;
  *(p + 5) = (n & 0xf00000) >> 24;
  *(p + 4) = (n & 0xf0000) >> 16;
  *(p + 3) = (n & 0xf000) >> 12;
  *(p + 2) = (n & 0xf00) >> 8;
  *(p + 1) = (n & 0xf0) >> 4;
  *(p + 0) = n & 0xf;
  return (8);
}

ham_u64_t
PageManager::decode(int n, ham_u8_t *p)
{
  ham_u64_t ret = 0;

  for (int i = 0; i < n - 1; i++) {
    ret += *(p + (n - i - 1));
    ret <<= 4;
  }

  // last assignment is without *= 10
  return (ret + *p);
}

} // namespace hamsterdb

