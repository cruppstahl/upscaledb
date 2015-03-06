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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "1base/pickle.h"
#include "2page/page.h"
#include "2device/device.h"
#include "2queue/queue.h"
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_worker.h"
#include "3page_manager/page_manager_test.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

enum {
  kPurgeAtLeast = 20
};

PageManagerState::PageManagerState(LocalEnvironment *env)
  : config(env->config()), header(env->header()),
    device(env->device()), lsn_manager(env->lsn_manager()),
    cache(env->config()), needs_flush(false), purge_cache_pending(false),
    state_page(0), last_blob_page(0), last_blob_page_id(0),
    page_count_fetched(0), page_count_index(0), page_count_blob(0),
    page_count_page_manager(0), cache_hits(0), cache_misses(0),
    freelist_hits(0), freelist_misses(0)
{
}

PageManager::PageManager(LocalEnvironment *env)
  : m_state(env)
{
  /* start the worker thread */
  m_worker.reset(new PageManagerWorker(&m_state.cache));
}

void
PageManager::initialize(uint64_t pageid)
{
  Context context(0, 0, 0);

  m_state.free_pages.clear();
  if (m_state.state_page)
    delete m_state.state_page;
  m_state.state_page = new Page(m_state.device);
  m_state.state_page->fetch(pageid);

  Page *page = m_state.state_page;
  uint32_t page_size = m_state.config.page_size_bytes;

  // the first page stores the page ID of the last blob
  m_state.last_blob_page_id = *(uint64_t *)page->get_payload();

  while (1) {
    ham_assert(page->get_type() == Page::kTypePageManager);
    uint8_t *p = page->get_payload();
    // skip m_state.last_blob_page_id?
    if (page == m_state.state_page)
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

      m_state.free_pages[id * page_size] = page_counter;
    }

    // load the overflow page
    if (overflow)
      page = fetch(&context, overflow, 0);
    else
      break;
  }
}

Page *
PageManager::fetch(Context *context, uint64_t address, uint32_t flags)
{
  /* fetch the page from the cache */
  Page *page;
  
  if (address == 0)
    page = m_state.header->get_header_page();
  else
    page = m_state.cache.get(address);

  if (page) {
    if (flags & PageManager::kNoHeader)
      page->set_without_header(true);
    return (safely_lock_page(context, page, true));
  }

  if ((flags & PageManager::kOnlyFromCache)
          || m_state.config.flags & HAM_IN_MEMORY)
    return (0);

  page = new Page(m_state.device, context->db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  ham_assert(page->get_data());

  /* store the page in the list */
  m_state.cache.put(page);

  /* write to disk (if necessary) */
  if (!(flags & PageManager::kDisableStoreState)
          && !(flags & PageManager::kReadOnly))
    maybe_store_state(context, false);

  if (flags & PageManager::kNoHeader)
    page->set_without_header(true);

  m_state.page_count_fetched++;
  return (safely_lock_page(context, page, false));
}

Page *
PageManager::alloc(Context *context, uint32_t page_type, uint32_t flags)
{
  uint64_t address = 0;
  Page *page = 0;
  uint32_t page_size = m_state.config.page_size_bytes;
  bool allocated = false;

  /* first check the internal list for a free page */
  if ((flags & PageManager::kIgnoreFreelist) == 0
          && !m_state.free_pages.empty()) {
    PageManagerState::FreeMap::iterator it = m_state.free_pages.begin();

    address = it->first;
    ham_assert(address % page_size == 0);
    /* remove the page from the freelist */
    m_state.free_pages.erase(it);
    m_state.needs_flush = true;

    m_state.freelist_hits++;

    /* try to fetch the page from the cache */
    page = m_state.cache.get(address);
    if (page)
      goto done;
    /* allocate a new page structure and read the page from disk */
    page = new Page(m_state.device, context->db);
    page->fetch(address);
    goto done;
  }

  m_state.freelist_misses++;

  try {
    if (!page) {
      allocated = true;
      page = new Page(m_state.device, context->db);
    }

    page->alloc(page_type);
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
  page->set_db(context->db);

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  /* store the page in the cache and the Changeset */
  m_state.cache.put(page);
  safely_lock_page(context, page, false);

  /* write to disk (if necessary) */
  if (!(flags & PageManager::kDisableStoreState)
          && !(flags & PageManager::kReadOnly))
    maybe_store_state(context, false);

  switch (page_type) {
    case Page::kTypeBindex:
    case Page::kTypeBroot: {
      memset(page->get_payload(), 0, sizeof(PBtreeNode));
      m_state.page_count_index++;
      break;
    }
    case Page::kTypePageManager:
      m_state.page_count_page_manager++;
      break;
    case Page::kTypeBlob:
      m_state.page_count_blob++;
      break;
    default:
      break;
  }

  return (page);
}

Page *
PageManager::alloc_multiple_blob_pages(Context *context, size_t num_pages)
{
  // allocate only one page? then use the normal ::alloc() method
  if (num_pages == 1)
    return (alloc(context, Page::kTypeBlob, 0));

  Page *page = 0;
  uint32_t page_size = m_state.config.page_size_bytes;

  // Now check the freelist
  if (!m_state.free_pages.empty()) {
    for (PageManagerState::FreeMap::iterator it = m_state.free_pages.begin();
            it != m_state.free_pages.end();
            it++) {
      if (it->second >= num_pages) {
        for (size_t i = 0; i < num_pages; i++) {
          if (i == 0) {
            page = fetch(context, it->first, 0);
            page->set_type(Page::kTypeBlob);
            page->set_without_header(false);
          }
          else {
            Page *p = fetch(context, it->first + (i * page_size), 0);
            p->set_type(Page::kTypeBlob);
            p->set_without_header(true);
          }
        }
        if (it->second > num_pages) {
          m_state.free_pages[it->first + num_pages * page_size]
                = it->second - num_pages;
        }
        m_state.free_pages.erase(it);
        return (page);
      }
    }
  }

  // Freelist lookup was not successful -> allocate new pages. Only the first
  // page is a regular page; all others do not have page headers.
  //
  // disable "store state": the PageManager otherwise could alloc overflow
  // pages in the middle of our blob sequence.
  uint32_t flags = PageManager::kIgnoreFreelist
                        | PageManager::kDisableStoreState;
  for (size_t i = 0; i < num_pages; i++) {
    if (page == 0)
      page = alloc(context, Page::kTypeBlob, flags);
    else {
      Page *p = alloc(context, Page::kTypeBlob, flags);
      p->set_without_header(true);
    }
  }

  // now store the state
  maybe_store_state(context, false);
  return (page);
}

void
PageManager::fill_metrics(ham_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = m_state.page_count_fetched;
  metrics->page_count_flushed = Page::ms_page_count_flushed;
  metrics->page_count_type_index = m_state.page_count_index;
  metrics->page_count_type_blob = m_state.page_count_blob;
  metrics->page_count_type_page_manager = m_state.page_count_page_manager;
  metrics->freelist_hits = m_state.freelist_hits;
  metrics->freelist_misses = m_state.freelist_misses;
  m_state.cache.fill_metrics(metrics);
}

struct FlushAllPagesPurger
{
  FlushAllPagesPurger(bool delete_pages)
    : delete_pages(delete_pages) {
  }

  bool operator()(Page *page) {
    ScopedSpinlock lock(page->mutex());
    page->flush();
    return (delete_pages);
  }

  bool delete_pages;
};

void
PageManager::flush(bool delete_pages)
{
  FlushAllPagesPurger purger(delete_pages);
  m_state.cache.purge_if(purger);

  if (m_state.state_page) {
    ScopedSpinlock lock(m_state.state_page->mutex());
    m_state.state_page->flush();
  }
}

// Returns true if the page can be purged: page must use allocated
// memory instead of an mmapped pointer; page must not be in use (= in
// a changeset) and not have cursors attached
struct PurgeProcessor
{
  PurgeProcessor(Page *last_blob_page, FlushPageMessage *message)
    : last_blob_page(last_blob_page), message(message) {
  }

  bool operator()(Page *page) {
    // the lock in here will be unlocked by the worker thread
    if (page == last_blob_page || !page->mutex().try_lock())
      return (false);
    message->list.push_back(page);
    return (true);
  }

  Page *last_blob_page;
  FlushPageMessage *message;
};

void
PageManager::purge_cache(Context *context)
{
  // do NOT purge the cache iff
  //   1. this is an in-memory Environment
  //   2. there's still a "purge cache" operation pending
  //   3. the cache is not full
  if (m_state.config.flags & HAM_IN_MEMORY
      || m_state.purge_cache_pending
      || !m_state.cache.is_cache_full())
    return;

  // Purge as many pages as possible to get memory usage down to the
  // cache's limit.
  FlushPageMessage *message = new FlushPageMessage();
  PurgeProcessor processor(m_state.last_blob_page, message);
  m_state.cache.purge(processor, m_state.last_blob_page);

  if (message->list.size())
    m_worker->add_to_queue(message);
  else
    delete message;
}

void
PageManager::reclaim_space(Context *context)
{
  if (m_state.last_blob_page) {
    m_state.last_blob_page_id = m_state.last_blob_page->get_address();
    m_state.last_blob_page = 0;
  }
  ham_assert(!(m_state.config.flags & HAM_DISABLE_RECLAIM_INTERNAL));

  bool do_truncate = false;
  size_t file_size = m_state.device->file_size();
  uint32_t page_size = m_state.config.page_size_bytes;

  while (m_state.free_pages.size() > 1) {
    PageManagerState::FreeMap::iterator fit =
                m_state.free_pages.find(file_size - page_size);
    if (fit != m_state.free_pages.end()) {
      Page *page = m_state.cache.get(fit->first);
      if (page) {
        m_state.cache.del(page);
        delete page;
      }
      file_size -= page_size;
      do_truncate = true;
      m_state.free_pages.erase(fit);
      continue;
    }
    break;
  }

  if (do_truncate) {
    m_state.needs_flush = true;
    maybe_store_state(context, true);
    m_state.device->truncate(file_size);
  }
}

struct DbClosePurger
{
  DbClosePurger(LocalDatabase *db)
    : m_db(db) {
  }

  bool operator()(Page *page) {
    if (page->get_db() == m_db && page->get_address() != 0) {
      ScopedSpinlock lock(page->mutex());
      ham_assert(page->cursor_list() == 0);
      page->flush();
      return (true);
    }
    return (false);
  }

  LocalDatabase *m_db;
};

void
PageManager::close_database(Context *context, LocalDatabase *db)
{
  if (m_state.last_blob_page) {
    m_state.last_blob_page_id = m_state.last_blob_page->get_address();
    m_state.last_blob_page = 0;
  }

  context->changeset.clear();

  DbClosePurger purger(db);
  m_state.cache.purge_if(purger);
}

void
PageManager::del(Context *context, Page *page, size_t page_count)
{
  ham_assert(page_count > 0);

  if (m_state.config.flags & HAM_IN_MEMORY)
    return;

  // remove all pages from the changeset, otherwise they won't be unlocked
  context->changeset.del(page);
  if (page_count > 1) {
    uint32_t page_size = m_state.config.page_size_bytes;
    for (size_t i = 1; i < page_count; i++) {
      Page *p = m_state.cache.get(page->get_address() + i * page_size);
      if (p && context->changeset.has(p))
        context->changeset.del(p);
    }
  }

  m_state.needs_flush = true;
  m_state.free_pages[page->get_address()] = page_count;
  ham_assert(page->get_address() % m_state.config.page_size_bytes == 0);

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  // do not call maybe_store_state() - this change in the m_state is not
  // relevant for logging.
}

void
PageManager::reset(Context *context)
{
  close(context);

  /* start the worker thread */
  m_worker.reset(new PageManagerWorker(&m_state.cache));
}

void
PageManager::close(Context *context)
{
  /* wait for the worker thread to stop */
  if (m_worker.get())
    m_worker->stop_and_join();

  // store the state of the PageManager
  if ((m_state.config.flags & HAM_IN_MEMORY) == 0
      && (m_state.config.flags & HAM_READ_ONLY) == 0) {
    maybe_store_state(context, true);
  }

  // reclaim unused disk space
  // if logging is enabled: also flush the changeset to write back the
  // modified freelist pages
  bool try_reclaim = m_state.config.flags & HAM_DISABLE_RECLAIM_INTERNAL
                ? false
                : true;

#ifdef WIN32
  // Win32: it's not possible to truncate the file while there's an active
  // mapping, therefore only reclaim if memory mapped I/O is disabled
  if (!(m_state.config.flags & HAM_DISABLE_MMAP))
    try_reclaim = false;
#endif

  if (try_reclaim) {
    reclaim_space(context);
  }

  // clear the Changeset because flush() will delete all Page pointers
  context->changeset.clear();

  // flush all dirty pages to disk, then delete them
  flush(true);

  delete m_state.state_page;
  m_state.state_page = 0;
  m_state.last_blob_page = 0;
}

Page *
PageManager::get_last_blob_page(Context *context)
{
  if (m_state.last_blob_page)
    return (safely_lock_page(context, m_state.last_blob_page, true));
  if (m_state.last_blob_page_id)
    return (fetch(context, m_state.last_blob_page_id, 0));
  return (0);
}

void 
PageManager::set_last_blob_page(Page *page)
{
  m_state.last_blob_page_id = 0;
  m_state.last_blob_page = page;
}

uint64_t
PageManager::store_state(Context *context)
{
  // no modifications? then simply return the old blobid
  if (!m_state.needs_flush)
    return (m_state.state_page ? m_state.state_page->get_address() : 0);

  m_state.needs_flush = false;

  // no freelist pages, no freelist state? then don't store anything
  if (!m_state.state_page && m_state.free_pages.empty())
    return (0);

  // otherwise allocate a new page, if required
  if (!m_state.state_page) {
    m_state.state_page = new Page(m_state.device);
    m_state.state_page->alloc(Page::kTypePageManager,
            Page::kInitializeWithZeroes);
  }

  // don't bother locking the state page
  context->changeset.put(m_state.state_page);

  uint32_t page_size = m_state.config.page_size_bytes;

  // make sure that the page is logged
  Page *page = m_state.state_page;
  page->set_dirty(true);

  uint8_t *p = page->get_payload();

  // store page-ID of the last allocated blob
  *(uint64_t *)p = m_state.last_blob_page_id;
  p += sizeof(uint64_t);

  // reset the overflow pointer and the counter
  // TODO here we lose a whole chain of overflow pointers if there was such
  // a chain. We only save the first. That's not critical but also not nice.
  uint64_t next_pageid = *(uint64_t *)p;
  if (next_pageid) {
    m_state.free_pages[next_pageid] = 1;
    ham_assert(next_pageid % page_size == 0);
  }

  // No freelist entries? then we're done. Make sure that there's no
  // overflow pointer or other garbage in the page!
  if (m_state.free_pages.empty()) {
    *(uint64_t *)p = 0;
    p += sizeof(uint64_t);
    *(uint32_t *)p = 0;
    return (m_state.state_page->get_address());
  }

  PageManagerState::FreeMap::const_iterator it = m_state.free_pages.begin();
  while (it != m_state.free_pages.end()) {
    // this is where we will store the data
    p = page->get_payload();
    // skip m_state.last_blob_page_id?
    if (page == m_state.state_page)
      p += sizeof(uint64_t);
    p += 8;   // leave room for the pointer to the next page
    p += 4;   // leave room for the counter

    uint32_t counter = 0;

    while (it != m_state.free_pages.end()) {
      // 9 bytes is the maximum amount of storage that we will need for a
      // new entry; if it does not fit then break
      if ((p + 9) - page->get_payload()
              >= (ptrdiff_t)(m_state.config.page_size_bytes
                                - Page::kSizeofPersistentHeader))
        break;

      // ... and check if the next entry (and the following) are directly
      // next to the current page
      uint32_t page_counter = 1;
      uint64_t base = it->first;
      ham_assert(base % page_size == 0);
      uint64_t current = it->first;

      // move to the next entry
      it++;

      for (; it != m_state.free_pages.end() && page_counter < 16 - 1; it++) {
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
    if (page == m_state.state_page) // skip m_state.last_blob_page_id?
      p += sizeof(uint64_t);
    uint64_t next_pageid = *(uint64_t *)p;
    *(uint64_t *)p = 0;
    p += 8;  // overflow page

    // now store the counter
    *(uint32_t *)p = counter;

    // are we done? if not then continue with the next page
    if (it != m_state.free_pages.end()) {
      // allocate (or fetch) an overflow page
      if (!next_pageid) {
        Page *new_page = alloc(context, Page::kTypePageManager,
                PageManager::kIgnoreFreelist);
        // patch the overflow pointer in the old (current) page
        p = page->get_payload();
        if (page == m_state.state_page) // skip m_state.last_blob_page_id?
          p += sizeof(uint64_t);
        *(uint64_t *)p = new_page->get_address();

        // reset the overflow pointer in the new page
        page = new_page;
        p = page->get_payload();
        *(uint64_t *)p = 0;
      }
      else
        page = fetch(context, next_pageid, 0);

      // make sure that the page is logged
      page->set_dirty(true);
    }
  }

  return (m_state.state_page->get_address());
}

void
PageManager::maybe_store_state(Context *context, bool force)
{
  if (force || (m_state.config.flags & HAM_ENABLE_RECOVERY)) {
    uint64_t new_blobid = store_state(context);
    if (new_blobid != m_state.header->get_page_manager_blobid()) {
      m_state.header->set_page_manager_blobid(new_blobid);
      // don't bother to lock the header page
      m_state.header->get_header_page()->set_dirty(true);
      context->changeset.put(m_state.header->get_header_page());
    }
  }
}

Page *
PageManager::safely_lock_page(Context *context, Page *page,
                bool allow_recursive_lock)
{
  context->changeset.put(page);

  ham_assert(page->mutex().try_lock() == false);

  // fetch contents again?
  if (!page->get_data()) {
    page->fetch(page->get_address());
  }

  return (page);
}

PageManagerTest
PageManager::test()
{
  return (PageManagerTest(this));
}

PageManagerTest::PageManagerTest(PageManager *page_manager)
  : m_sut(page_manager)
{
}

uint64_t
PageManagerTest::store_state()
{
  Context context(0, 0, 0);
  return (m_sut->store_state(&context));
}

void
PageManagerTest::remove_page(Page *page)
{
  m_sut->m_state.cache.del(page);
}

bool
PageManagerTest::is_page_free(uint64_t pageid)
{
  return (m_sut->m_state.free_pages.find(pageid)
                  != m_sut->m_state.free_pages.end());
}

Page *
PageManagerTest::fetch_page(uint64_t id)
{
  return (m_sut->m_state.cache.get(id));
}

void
PageManagerTest::store_page(Page *page)
{
  m_sut->m_state.cache.put(page);
}

bool
PageManagerTest::is_cache_full()
{
  return (m_sut->m_state.cache.is_cache_full());
}

PageManagerState *
PageManagerTest::state()
{
  return (&m_sut->m_state);
}

} // namespace hamsterdb
