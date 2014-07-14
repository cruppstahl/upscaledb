/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#include "0root/root.h"

#include <string.h>

#include "3rdparty/murmurhash3/MurmurHash3.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/byte_array.h"
#include "1base/pickle.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

PageManager::PageManager(LocalEnvironment *env, uint64_t cache_size)
  : m_env(env), m_cache(env, cache_size), m_needs_flush(false),
    m_state_page(0), m_last_blob_page(0), m_last_blob_page_id(0),
    m_page_count_fetched(0), m_page_count_flushed(0), m_page_count_index(0),
    m_page_count_blob(0), m_page_count_page_manager(0), m_cache_hits(0),
    m_cache_misses(0), m_freelist_hits(0), m_freelist_misses(0)
{
}

void
PageManager::load_state(uint64_t pageid)
{
  if (m_state_page)
    delete m_state_page;

  m_state_page = new Page(m_env->get_device());
  m_state_page->fetch(pageid);
  if (m_env->get_flags() & HAM_ENABLE_CRC32)
    verify_crc32(m_state_page);

  m_free_pages.clear();

  Page *page = m_state_page;
  uint32_t page_size = m_env->get_page_size();

  // the first page stores the page ID of the last blob
  m_last_blob_page_id = *(uint64_t *)page->get_payload();

  while (1) {
    ham_assert(page->get_type() == Page::kTypePageManager);
    uint8_t *p = page->get_payload();
    // skip m_last_blob_page_id?
    if (page == m_state_page)
      p += sizeof(uint64_t);

    // get the overflow address
    uint64_t overflow = *(uint64_t *)p;
    p += 8;

    // get the number of stored elements
    uint32_t counter = *(uint32_t *)p;
    p += 4;

    // now read all pages
    for (uint32_t i = 0; i < counter; i++) {
      // 4 bits page_counter, 4 bits for number of following bytes
      int page_counter = (*p & 0xf0) >> 4;
      int num_bytes = *p & 0x0f;
      ham_assert(page_counter > 0);
      ham_assert(num_bytes <= 8);
      p += 1;

      uint64_t id = Pickle::decode_u64(num_bytes, p);
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

uint64_t
PageManager::store_state()
{
  // no modifications? then simply return the old blobid
  if (!m_needs_flush)
    return (m_state_page ? m_state_page->get_address() : 0);

  m_needs_flush = false;

  // no freelist pages, no freelist state? then don't store anything
  if (!m_state_page && m_free_pages.empty())
    return (0);

  // otherwise allocate a new page, if required
  if (!m_state_page) {
    m_state_page = new Page(m_env->get_device());
    m_state_page->allocate(Page::kTypePageManager, Page::kInitializeWithZeroes);
  }

  uint32_t page_size = m_env->get_page_size();

  /* store the page in the changeset if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(m_state_page);

  Page *page = m_state_page;

  // make sure that the page is logged
  page->set_dirty(true);

  uint8_t *p = m_state_page->get_payload();

  // store page-ID of the last allocated blob
  *(uint64_t *)p = m_last_blob_page_id;
  p += sizeof(uint64_t);

  // reset the overflow pointer and the counter
  // TODO here we lose a whole chain of overflow pointers if there was such
  // a chain. We only save the first. That's not critical but also not nice.
  uint64_t next_pageid = *(uint64_t *)p;
  if (next_pageid) {
    m_free_pages[next_pageid] = 1;
    ham_assert(next_pageid % page_size == 0);
  }

  // No freelist entries? then we're done. Make sure that there's no
  // overflow pointer or other garbage in the page!
  if (m_free_pages.empty()) {
    *(uint64_t *)p = 0;
    p += sizeof(uint64_t);
    *(uint32_t *)p = 0;
    return (m_state_page->get_address());
  }

  FreeMap::const_iterator it = m_free_pages.begin();
  while (it != m_free_pages.end()) {
    // this is where we will store the data
    p = page->get_payload();
    // skip m_last_blob_page_id?
    if (page == m_state_page)
      p += sizeof(uint64_t);
    p += 8;   // leave room for the pointer to the next page
    p += 4;   // leave room for the counter

    uint32_t counter = 0;

    while (it != m_free_pages.end()) {
      // 9 bytes is the maximum amount of storage that we will need for a
      // new entry; if it does not fit then break
      if ((p + 9) - page->get_payload() >= (ptrdiff_t)m_env->get_usable_page_size())
        break;

      // ... and check if the next entry (and the following) are directly
      // next to the current page
      uint32_t page_counter = 1;
      uint64_t base = it->first;
      ham_assert(base % page_size == 0);
      uint64_t current = it->first;

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
      int num_bytes = Pickle::encode_u64(p + 1, base / page_size);
      *p = (page_counter << 4) | num_bytes;
      p += 1 + num_bytes;

      counter++;
    }

    p = page->get_payload();
    if (page == m_state_page) // skip m_last_blob_page_id?
      p += sizeof(uint64_t);
    uint64_t next_pageid = *(uint64_t *)p;
    *(uint64_t *)p = 0;
    p += 8;  // overflow page

    // now store the counter
    *(uint32_t *)p = counter;

    // are we done? if not then continue with the next page
    if (it != m_free_pages.end()) {
      // allocate (or fetch) an overflow page
      if (!next_pageid) {
        Page *new_page = alloc_page(0, Page::kTypePageManager, kIgnoreFreelist);
        // patch the overflow pointer in the old (current) page
        p = page->get_payload();
        if (page == m_state_page) // skip m_last_blob_page_id?
          p += sizeof(uint64_t);
        *(uint64_t *)p = new_page->get_address();

        // reset the overflow pointer in the new page
        page = new_page;
        p = page->get_payload();
        *(uint64_t *)p = 0;
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
PageManager::fetch_page(LocalDatabase *db, uint64_t address,
                uint32_t flags)
{
  Page *page = 0;

  /* fetch the page from the cache */
  page = m_cache.get_page(address);
  if (page) {
    ham_assert(page->get_data());
    if (flags & kNoHeader)
      page->set_without_header(true);
    /* store the page in the changeset if recovery is enabled */
    if (!(flags & kReadOnly) && m_env->get_flags() & HAM_ENABLE_RECOVERY)
      m_env->get_changeset().add_page(page);
    return (page);
  }

  if ((flags & kOnlyFromCache) || m_env->get_flags() & HAM_IN_MEMORY)
    return (0);

  page = new Page(m_env->get_device(), db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  ham_assert(page->get_data());

  if (flags & kNoHeader)
    page->set_without_header(true);
  // Pro: verify crc32
  else if (m_env->get_flags() & HAM_ENABLE_CRC32)
    verify_crc32(page);

  /* store the page in the list */
  store_page(page);

  /* store the page in the changeset */
  if (!(flags & kReadOnly) && m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  m_page_count_fetched++;

  return (page);
}

Page *
PageManager::alloc_page(LocalDatabase *db, uint32_t page_type, uint32_t flags)
{
  uint64_t address = 0;
  Page *page = 0;
  uint32_t page_size = m_env->get_page_size();
  bool allocated = false;

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
    page = new Page(m_env->get_device(), db);
    page->fetch(address);
    goto done;
  }

  m_freelist_misses++;

  try {
    if (!page) {
      allocated = true;
      page = new Page(m_env->get_device(), db);
    }

    page->allocate(page_type);
  }
  catch (Exception &ex) {
    if (allocated)
      delete page;
    throw ex;
  }

done:
  /* clear the page with zeroes?  */
  if (flags & PageManager::kClearWithZero)
    memset(page->get_data(), 0, page_size);

  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(page_type);
  page->set_dirty(true);
  page->set_db(db);
  page->set_crc32(0);

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  /* an allocated page is always flushed if recovery is enabled */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
    m_env->get_changeset().add_page(page);

  /* store the page in the cache */
  store_page(page, (flags & kDisableStoreState) != 0);

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
PageManager::alloc_multiple_blob_pages(LocalDatabase *db, size_t num_pages)
{
  // allocate only one page? then use the normal ::alloc_page() method
  if (num_pages == 1)
    return (alloc_page(db, Page::kTypeBlob));

  Page *page = 0;
  uint32_t page_size = m_env->get_page_size();

  // Now check the freelist
  if (!m_free_pages.empty()) {
    for (FreeMap::iterator it = m_free_pages.begin(); it != m_free_pages.end();
            it++) {
      if (it->second >= num_pages) {
        for (size_t i = 0; i < num_pages; i++) {
          if (i == 0) {
            page = fetch_page(db, it->first);
            page->set_type(Page::kTypeBlob);
            page->set_without_header(false);
          }
          else {
            Page *p = fetch_page(db, it->first + (i * page_size));
            p->set_type(Page::kTypeBlob);
            p->set_without_header(true);
          }
        }
        if (it->second > num_pages) {
          m_free_pages[it->first + num_pages * page_size]
                = it->second - num_pages;
        }
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
  uint32_t flags = kIgnoreFreelist | kDisableStoreState;
  for (size_t i = 0; i < num_pages; i++) {
    if (page == 0)
      page = alloc_page(db, Page::kTypeBlob, flags);
    else {
      Page *p = alloc_page(db, Page::kTypeBlob, flags);
      p->set_without_header(true);
    }
  }

  // now store the state
  maybe_store_state();

  return (page);
}

static bool
flush_all_pages_callback(Page *page, LocalEnvironment *env,
        LocalDatabase *db, uint32_t flags)
{
  env->get_page_manager()->flush_page(page);

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
PageManager::flush_page(Page *page)
{
  if (page->is_dirty()) {
    m_page_count_flushed++;

    // Pro: update crc32
    if ((m_env->get_flags() & HAM_ENABLE_CRC32)
        && likely(!page->is_without_header())) {
      uint32_t crc32;
      MurmurHash3_x86_32(page->get_payload(),
                      m_env->get_page_size() - (sizeof(PPageHeader) - 1),
                      (uint32_t)page->get_address(), &crc32);
      page->set_crc32(crc32);
    }

    page->flush();
  }
}

void
PageManager::flush_all_pages(bool nodelete)
{
  if (nodelete == false && m_last_blob_page) {
    m_last_blob_page_id = m_last_blob_page->get_address();
    m_last_blob_page = 0;
  }
  m_cache.visit(flush_all_pages_callback, m_env, 0, nodelete ? 1 : 0);

  if (m_state_page)
    flush_page(m_state_page);
}

void
PageManager::purge_callback(Page *page, PageManager *pm)
{
  BtreeCursor::uncouple_all_cursors(page);

  if (pm->m_last_blob_page == page) {
    pm->m_last_blob_page_id = pm->m_last_blob_page->get_address();
    pm->m_last_blob_page = 0;
  }

  pm->flush_page(page);
  delete page;
}

void
PageManager::purge_cache()
{
  // in-memory-db: don't remove the pages or they would be lost
  if (m_env->get_flags() & HAM_IN_MEMORY || !cache_is_full())
    return;

  // Purge as many pages as possible to get memory usage down to the
  // cache's limit.
  //
  // By default this is capped to |kPurgeAtLeast| pages to avoid I/O spikes.
  // In benchmarks this has proven to be a good limit.
  size_t max_pages = m_cache.get_capacity() / m_env->get_page_size();
  if (max_pages == 0)
    max_pages = 1;
  size_t limit = m_cache.get_current_elements() - max_pages;
  if (limit < kPurgeAtLeast)
    limit = kPurgeAtLeast;
  m_cache.purge(purge_callback, this, limit);
}

static bool
db_close_callback(Page *page, LocalEnvironment *env,
        LocalDatabase *db, uint32_t flags)
{
  if (page->get_db() == db && page->get_address() != 0) {
    env->get_page_manager()->flush_page(page);

    // TODO is this really necessary?? i don't think so
    if (page->get_data() &&
        !page->is_without_header() &&
          (page->get_type() == Page::kTypeBroot ||
            page->get_type() == Page::kTypeBindex)) {
      BtreeCursor::uncouple_all_cursors(page);
    }

    return (true);
  }

  return (false);
}

void
PageManager::close_database(LocalDatabase *db)
{
  if (m_last_blob_page) {
    m_last_blob_page_id = m_last_blob_page->get_address();
    m_last_blob_page = 0;
  }

  m_cache.visit(db_close_callback, m_env, db, 0);
}

void
PageManager::reclaim_space()
{
  if (m_last_blob_page) {
    m_last_blob_page_id = m_last_blob_page->get_address();
    m_last_blob_page = 0;
  }
  ham_assert(!(m_env->get_flags() & HAM_DISABLE_RECLAIM_INTERNAL));
  bool do_truncate = false;
  size_t file_size = m_env->get_device()->get_file_size();
  uint32_t page_size = m_env->get_page_size();

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
PageManager::add_to_freelist(Page *page, size_t page_count)
{
  ham_assert(page_count > 0);

  if (m_env->get_flags() & HAM_IN_MEMORY)
    return;

  m_needs_flush = true;
  m_free_pages[page->get_address()] = page_count;
  ham_assert(page->get_address() % m_env->get_page_size() == 0);

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

  // TODO this is just a hack b/c of a too complex cleanup logic in
  // the environment; will be removed in 2.1.9
  if ((m_env->get_flags() & HAM_ENABLE_RECOVERY)
        && (m_env->get_journal() == 0))
    try_reclaim = false;

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

void
PageManager::verify_crc32(Page *page)
{
  uint32_t crc32;
  MurmurHash3_x86_32(page->get_payload(),
                  m_env->get_page_size() - (sizeof(PPageHeader) - 1),
                  (uint32_t)page->get_address(), &crc32);
  if (crc32 != page->get_crc32()) {
    ham_trace(("crc32 mismatch in page %lu: 0x%lx != 0x%lx",
                    page->get_address(), crc32, page->get_crc32()));
    throw Exception(HAM_INTEGRITY_VIOLATED);
  }
}

} // namespace hamsterdb
