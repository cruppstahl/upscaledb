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

#include "2worker/worker.h"

#include "0root/root.h"

#include <string.h>

#include "3rdparty/murmurhash3/MurmurHash3.h"
// Always verify that a file of level N does not include headers > N!
#include "1base/signal.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_test.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template <typename T>
struct Deleter {
  void operator()(T *p) {
    delete p;
  }
};

struct AsyncFlushMessage {
  AsyncFlushMessage(PageManager *page_manager_, Device *device_,
          Signal *signal_)
    : page_manager(page_manager_), device(device_), signal(signal_) {
    in_progress = false;
  }

  PageManager *page_manager;
  Device *device;
  Signal *signal;
  boost::atomic<bool> in_progress;
  std::vector<uint64_t> page_ids;
};

static void
async_flush_pages(AsyncFlushMessage *message)
{
  std::vector<uint64_t>::iterator it = message->page_ids.begin();
  for (it = message->page_ids.begin(); it != message->page_ids.end(); it++) {
    Page *page = message->page_manager->try_lock_purge_candidate(*it);
    if (!page)
      continue;
    assert(page->mutex().try_lock() == false);

    // flush dirty pages
    if (page->is_dirty()) {
      try {
        page->flush();
      }
      catch (Exception &) {
        page->mutex().unlock();
        if (message->signal)
          message->signal->notify();
        message->in_progress = false;
        throw; // TODO really?
      }
    }
    page->mutex().unlock();
  }
  if (message->in_progress)
    message->in_progress = false;
  if (message->signal)
    message->signal->notify();
}

PageManagerState::PageManagerState(LocalEnvironment *_env)
  : env(_env), config(_env->config()), header(_env->header()),
    device(_env->device()), lsn_manager(_env->lsn_manager()),
    cache(_env->config()), freelist(config), needs_flush(false),
    state_page(0), last_blob_page(0), last_blob_page_id(0),
    page_count_fetched(0), page_count_index(0), page_count_blob(0),
    page_count_page_manager(0), cache_hits(0), cache_misses(0), message(0)
{
}

PageManagerState::~PageManagerState()
{
  delete message;
  message = 0;
}

PageManager::PageManager(LocalEnvironment *env)
  : m_state(env)
{
  /* start the worker thread */
  m_worker = new WorkerPool(1);
}

PageManager::~PageManager()
{
  delete m_worker;
}

void
PageManager::initialize(uint64_t pageid)
{
  Context context(0, 0, 0);

  m_state.freelist.clear();

  if (m_state.state_page)
    delete m_state.state_page;
  m_state.state_page = new Page(m_state.device);
  m_state.state_page->fetch(pageid);
  if (m_state.config.flags & UPS_ENABLE_CRC32)
    verify_crc32(m_state.state_page);

  Page *page = m_state.state_page;

  // the first page stores the page ID of the last blob
  m_state.last_blob_page_id = *(uint64_t *)page->payload();

  while (1) {
    assert(page->type() == Page::kTypePageManager);
    uint8_t *p = page->payload();
    // skip m_state.last_blob_page_id?
    if (page == m_state.state_page)
      p += sizeof(uint64_t);

    // get the overflow address
    uint64_t overflow = *(uint64_t *)p;
    p += 8;

    m_state.freelist.decode_state(p);

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
  ScopedSpinlock lock(m_state.mutex);
  return (fetch_unlocked(context, address, flags));
}

Page *
PageManager::alloc(Context *context, uint32_t page_type, uint32_t flags)
{
  ScopedSpinlock lock(m_state.mutex);
  return (alloc_unlocked(context, page_type, flags));
}

Page *
PageManager::alloc_multiple_blob_pages(Context *context, size_t num_pages)
{
  ScopedSpinlock lock(m_state.mutex);

  // allocate only one page? then use the normal ::alloc() method
  if (num_pages == 1)
    return (alloc_unlocked(context, Page::kTypeBlob, 0));

  Page *page = 0;
  uint32_t page_size = m_state.config.page_size_bytes;

  // Now check the freelist
  uint64_t address = m_state.freelist.alloc(num_pages);
  if (address != 0) {
    for (size_t i = 0; i < num_pages; i++) {
      if (i == 0) {
        page = fetch_unlocked(context, address, 0);
        page->set_type(Page::kTypeBlob);
        page->set_without_header(false);
      }
      else {
        Page *p = fetch_unlocked(context, address + (i * page_size), 0);
        p->set_type(Page::kTypeBlob);
        p->set_without_header(true);
      }
    }
    return (page);
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
      page = alloc_unlocked(context, Page::kTypeBlob, flags);
    else {
      Page *p = alloc_unlocked(context, Page::kTypeBlob, flags);
      p->set_without_header(true);
    }
  }

  // now store the state
  maybe_store_state(context, false);
  return (page);
}

void
PageManager::fill_metrics(ups_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = m_state.page_count_fetched;
  metrics->page_count_flushed = Page::ms_page_count_flushed;
  metrics->page_count_type_index = m_state.page_count_index;
  metrics->page_count_type_blob = m_state.page_count_blob;
  metrics->page_count_type_page_manager = m_state.page_count_page_manager;
  metrics->freelist_hits = m_state.freelist.freelist_hits;
  metrics->freelist_misses = m_state.freelist.freelist_misses;
  m_state.cache.fill_metrics(metrics);
}

struct FlushAllPagesVisitor
{
  FlushAllPagesVisitor(AsyncFlushMessage *message_)
    : message(message_) {
  }

  bool operator()(Page *page) {
    if (page->is_dirty())
      message->page_ids.push_back(page->address());
    return (false);
  }

  AsyncFlushMessage *message;
};

void
PageManager::flush_all_pages()
{
  Signal signal;
  AsyncFlushMessage *message = new AsyncFlushMessage(this, m_state.device,
                                    &signal);

  FlushAllPagesVisitor visitor(message);

  {
    ScopedSpinlock lock(m_state.mutex);

    m_state.cache.purge_if(visitor);

    if (m_state.header->header_page()->is_dirty())
      message->page_ids.push_back(0);

    if (m_state.state_page && m_state.state_page->is_dirty())
      message->page_ids.push_back(m_state.state_page->address());
  }

  if (message->page_ids.size() > 0) {
    run_async(boost::bind(&async_flush_pages, message));
    signal.wait();
  }

  delete message;
}

void
PageManager::purge_cache(Context *context)
{
  ScopedSpinlock lock(m_state.mutex);

  // do NOT purge the cache iff
  //   1. this is an in-memory Environment
  //   2. there's still a "purge cache" operation pending
  //   3. the cache is not full
  if (m_state.config.flags & UPS_IN_MEMORY
      || (m_state.message && m_state.message->in_progress == true)
      || !m_state.cache.is_cache_full())
    return;

  if (!m_state.message)
    m_state.message = new AsyncFlushMessage(this, m_state.device, 0);

  m_state.message->page_ids.clear();
  m_state.garbage.clear();

  m_state.cache.purge_candidates(m_state.message->page_ids, m_state.garbage,
          m_state.last_blob_page);

  // don't bother if there are only few pages
  if (m_state.message->page_ids.size() > 10) {
    m_state.message->in_progress = true;
    run_async(boost::bind(&async_flush_pages, m_state.message));
  }

  for (std::vector<Page *>::iterator it = m_state.garbage.begin();
                  it != m_state.garbage.end();
                  it++) {
    Page *page = *it;
    if (page->mutex().try_lock()) {
      assert(page->cursor_list() == 0);
      m_state.cache.del(page);
      page->mutex().unlock();
      delete page;
    }
  }
}

void
PageManager::reclaim_space(Context *context)
{
  ScopedSpinlock lock(m_state.mutex);

  if (m_state.last_blob_page) {
    m_state.last_blob_page_id = m_state.last_blob_page->address();
    m_state.last_blob_page = 0;
  }
  assert(!(m_state.config.flags & UPS_DISABLE_RECLAIM_INTERNAL));

  bool do_truncate = false;
  uint32_t page_size = m_state.config.page_size_bytes;
  uint64_t file_size = m_state.device->file_size();
  uint64_t address = m_state.freelist.truncate(file_size);

  if (address < file_size) {
    for (uint64_t page_id = address;
            page_id <= file_size - page_size;
            page_id += page_size) {
      Page *page = m_state.cache.get(page_id);
      if (page) {
        m_state.cache.del(page);
        delete page;
      }
    }

    do_truncate = true;
    file_size = address;
  }

  if (do_truncate) {
    m_state.needs_flush = true;
    m_state.device->truncate(file_size);
    maybe_store_state(context, true);
  }
}

struct CloseDatabaseVisitor
{
  CloseDatabaseVisitor(LocalDatabase *db_, AsyncFlushMessage *message_)
    : db(db_), message(message_) {
  }

  bool operator()(Page *page) {
    if (page->db() == db && page->address() != 0) {
      message->page_ids.push_back(page->address());
      pages.push_back(page);
    }
    return (false);
  }

  LocalDatabase *db;
  std::vector<Page *> pages;
  AsyncFlushMessage *message;
};

void
PageManager::close_database(Context *context, LocalDatabase *db)
{
  Signal signal;
  AsyncFlushMessage *message = new AsyncFlushMessage(this, m_state.device,
                                    &signal);

  CloseDatabaseVisitor visitor(db, message);

  {
    ScopedSpinlock lock(m_state.mutex);

    if (m_state.last_blob_page) {
      m_state.last_blob_page_id = m_state.last_blob_page->address();
      m_state.last_blob_page = 0;
    }

    context->changeset.clear();
    m_state.cache.purge_if(visitor);

    if (m_state.header->header_page()->is_dirty())
      message->page_ids.push_back(0);
  }

  if (message->page_ids.size() > 0) {
    run_async(boost::bind(&async_flush_pages, message));
    signal.wait();
  }

  delete message;

  ScopedSpinlock lock(m_state.mutex);
  // now delete the pages
  for (std::vector<Page *>::iterator it = visitor.pages.begin();
          it != visitor.pages.end();
          it++) {
    m_state.cache.del(*it);
    // TODO Journal/recoverFromRecoveryTest fails because pages are still
    // locked; make sure that they're unlocked before they are deleted
    (*it)->mutex().try_lock();
    (*it)->mutex().unlock();
    delete *it;
  }
}

void
PageManager::del(Context *context, Page *page, size_t page_count)
{
  ScopedSpinlock lock(m_state.mutex);

  assert(page_count > 0);

  if (m_state.config.flags & UPS_IN_MEMORY)
    return;

  // remove the page(s) from the changeset
  context->changeset.del(page);
  if (page_count > 1) {
    uint32_t page_size = m_state.config.page_size_bytes;
    for (size_t i = 1; i < page_count; i++) {
      Page *p = m_state.cache.get(page->address() + i * page_size);
      if (p && context->changeset.has(p))
        context->changeset.del(p);
    }
  }

  m_state.needs_flush = true;
  m_state.freelist.put(page->address(), page_count);
  assert(page->address() % m_state.config.page_size_bytes == 0);

  if (page->node_proxy()) {
    delete page->node_proxy();
    page->set_node_proxy(0);
  }

  // do not call maybe_store_state() - this change in the m_state is not
  // relevant for logging.
}

void
PageManager::close(Context *context)
{
  // no need to lock the mutex; this method is called during shutdown

  // cut off unused space at the end of the file; this space is managed
  // by the device
  m_state.device->reclaim_space();

  // reclaim unused disk space
  // if logging is enabled: also flush the changeset to write back the
  // modified freelist pages
  bool try_reclaim = m_state.config.flags & UPS_DISABLE_RECLAIM_INTERNAL
                ? false
                : true;

#ifdef WIN32
  // Win32: it's not possible to truncate the file while there's an active
  // mapping, therefore only reclaim if memory mapped I/O is disabled
  if (!(m_state.config.flags & UPS_DISABLE_MMAP))
    try_reclaim = false;
#endif

  if (try_reclaim)
    reclaim_space(context);

  // store the state of the PageManager
  if ((m_state.config.flags & UPS_IN_MEMORY) == 0
      && (m_state.config.flags & UPS_READ_ONLY) == 0) {
    maybe_store_state(context, true);
  }

  // clear the Changeset because flush() will delete all Page pointers
  context->changeset.clear();

  // flush all dirty pages to disk, then delete them
  flush_all_pages();

  /* wait for the worker thread to stop */
  if (m_worker) {
	delete m_worker;
	m_worker = 0;
  }

  delete m_state.state_page;
  m_state.state_page = 0;
  m_state.last_blob_page = 0;
}

void
PageManager::reset(Context *context)
{
  close(context);

  /* start the worker thread */
  m_worker = new WorkerPool(1);
}

Page *
PageManager::get_last_blob_page(Context *context)
{
  ScopedSpinlock lock(m_state.mutex);

  if (m_state.last_blob_page)
    return (safely_lock_page(context, m_state.last_blob_page, true));
  if (m_state.last_blob_page_id)
    return (fetch_unlocked(context, m_state.last_blob_page_id, 0));
  return (0);
}

void 
PageManager::set_last_blob_page(Page *page)
{
  ScopedSpinlock lock(m_state.mutex);
  m_state.last_blob_page_id = page ? page->address() : 0;
  m_state.last_blob_page = page;
}

uint64_t
PageManager::last_blob_page_id()
{
  ScopedSpinlock lock(m_state.mutex);
  if (m_state.last_blob_page_id)
    return (m_state.last_blob_page_id);
  if (m_state.last_blob_page)
    return (m_state.last_blob_page->address());
  return (0);
}

void
PageManager::set_last_blob_page_id(uint64_t id)
{
  ScopedSpinlock lock(m_state.mutex);
  m_state.last_blob_page_id = id;
  m_state.last_blob_page = 0;
}

Page *
PageManager::fetch_unlocked(Context *context, uint64_t address, uint32_t flags)
{
  /* fetch the page from the cache */
  Page *page;
  
  if (address == 0)
    page = m_state.header->header_page();
  else if (m_state.state_page && address == m_state.state_page->address())
    page = m_state.state_page;
  else
    page = m_state.cache.get(address);

  if (page) {
    if (flags & PageManager::kNoHeader)
      page->set_without_header(true);
    return (safely_lock_page(context, page, true));
  }

  if ((flags & PageManager::kOnlyFromCache)
          || m_state.config.flags & UPS_IN_MEMORY)
    return (0);

  page = new Page(m_state.device, context->db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  assert(page->data());

  /* store the page in the list */
  m_state.cache.put(page);

  /* write state to disk (if necessary) */
  if (!(flags & PageManager::kDisableStoreState)
          && !(flags & PageManager::kReadOnly))
    maybe_store_state(context, false);

  if (flags & PageManager::kNoHeader)
    page->set_without_header(true);
  else if (m_state.config.flags & UPS_ENABLE_CRC32)
    verify_crc32(page);

  m_state.page_count_fetched++;
  return (safely_lock_page(context, page, false));
}

Page *
PageManager::alloc_unlocked(Context *context, uint32_t page_type,
                uint32_t flags)
{
  uint64_t address = 0;
  Page *page = 0;
  uint32_t page_size = m_state.config.page_size_bytes;
  bool allocated = false;

  /* first check the internal list for a free page */
  if ((flags & PageManager::kIgnoreFreelist) == 0) {
    address = m_state.freelist.alloc(1);

    if (address != 0) {
      assert(address % page_size == 0);
      m_state.needs_flush = true;

      /* try to fetch the page from the cache */
      page = m_state.cache.get(address);
      if (page)
        goto done;
      /* otherwise fetch the page from disk */
      page = new Page(m_state.device, context->db);
      page->fetch(address);
      goto done;
    }
  }

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
    ::memset(page->data(), 0, page_size);

  /* initialize the page; also set the 'dirty' flag to force logging */
  page->set_type(page_type);
  page->set_dirty(true);
  page->set_db(context->db);
  page->set_without_header(false);
  page->set_crc32(0);

  if (page->node_proxy()) {
    delete page->node_proxy();
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
      ::memset(page->payload(), 0, sizeof(PBtreeNode));
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

uint64_t
PageManager::store_state(Context *context)
{
  // no modifications? then simply return the old blobid
  if (!m_state.needs_flush)
    return (m_state.state_page ? m_state.state_page->address() : 0);

  m_state.needs_flush = false;

  // no freelist pages, no freelist state? then don't store anything
  if (!m_state.state_page && m_state.freelist.empty())
    return (0);

  // otherwise allocate a new page, if required
  if (!m_state.state_page) {
    m_state.state_page = new Page(m_state.device);
    m_state.state_page->alloc(Page::kTypePageManager,
            Page::kInitializeWithZeroes);
  }

  // don't bother locking the state page; it will never be accessed by
  // the worker thread because it's not stored in the cache
  context->changeset.put(m_state.state_page);

  m_state.state_page->set_dirty(true);

  Page *page = m_state.state_page;
  uint8_t *p = page->payload();

  // store page-ID of the last allocated blob
  *(uint64_t *)p = m_state.last_blob_page_id;
  p += sizeof(uint64_t);

  // reset the overflow pointer and the counter
  // TODO here we lose a whole chain of overflow pointers if there was such
  // a chain. We only save the first. That's not critical but also not nice.
  uint64_t next_pageid = *(uint64_t *)p;
  if (next_pageid) {
    m_state.freelist.put(next_pageid, 1);
    *(uint64_t *)p = 0;
  }

  // No freelist entries? then we're done. Make sure that there's no
  // overflow pointer or other garbage in the page!
  if (m_state.freelist.empty()) {
    p += sizeof(uint64_t);
    *(uint32_t *)p = 0;
    return (m_state.state_page->address());
  }

  std::pair<bool, Freelist::FreeMap::const_iterator> continuation;
  continuation.first = false;   // initialization
  continuation.second = m_state.freelist.free_pages.end();
  do {
    int offset = page == m_state.state_page
                      ? sizeof(uint64_t)
                      : 0;
    continuation = m_state.freelist.encode_state(continuation,
                            page->payload() + offset,
                            m_state.config.page_size_bytes
                                - Page::kSizeofPersistentHeader
                                - offset);

    if (continuation.first == false)
      break;

    // load the next page
    if (!next_pageid) {
      Page *new_page = alloc_unlocked(context, Page::kTypePageManager,
              PageManager::kIgnoreFreelist);
      // patch the overflow pointer in the old (current) page
      p = page->payload() + offset;
      *(uint64_t *)p = new_page->address();

      // reset the overflow pointer in the new page
      page = new_page;
      p = page->payload();
      *(uint64_t *)p = 0;
    }
    else {
      page = fetch_unlocked(context, next_pageid, 0);
      p = page->payload();
    }

    // make sure that the page is logged
    page->set_dirty(true);

  } while (true);

  return (m_state.state_page->address());
}

void
PageManager::maybe_store_state(Context *context, bool force)
{
  if (force || m_state.env->journal()) {
    uint64_t new_blobid = store_state(context);
    if (new_blobid != m_state.header->page_manager_blobid()) {
      m_state.header->set_page_manager_blobid(new_blobid);
      // don't bother to lock the header page
      m_state.header->header_page()->set_dirty(true);
      context->changeset.put(m_state.header->header_page());
    }
  }
}

Page *
PageManager::safely_lock_page(Context *context, Page *page,
                bool allow_recursive_lock)
{
  context->changeset.put(page);

  assert(page->mutex().try_lock() == false);

  return (page);
}

Page *
PageManager::try_lock_purge_candidate(uint64_t address)
{
  Page *page = 0;

  //ScopedSpinlock lock(m_state.mutex); -- deadlocks
  if (!m_state.mutex.try_lock())
    return (0);

  if (address == 0)
    page = m_state.header->header_page();
  else if (m_state.state_page && address == m_state.state_page->address())
    page = m_state.state_page;
  else
    page = m_state.cache.get(address);

  if (!page || !page->mutex().try_lock()) {
    m_state.mutex.unlock();
    return (0);
  }

  m_state.mutex.unlock();

  // !!
  // Do not purge pages with cursors, since Cursor::move will return pointers
  // directly into the page's data, and these pointers will be invalidated
  // as soon as the page is purged.
  //
  if (page->cursor_list() != 0) {
    page->mutex().unlock();
    return (0);
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
  return (m_sut->m_state.freelist.has(pageid));
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

void
PageManager::verify_crc32(Page *page)
{
  uint32_t crc32;
  MurmurHash3_x86_32(page->payload(),
                  page->persisted_data.size - (sizeof(PPageHeader) - 1),
                  (uint32_t)page->address(), &crc32);
  if (crc32 != page->crc32()) {
    ups_trace(("crc32 mismatch in page %lu: 0x%lx != 0x%lx",
                    page->address(), crc32, page->crc32()));
    throw Exception(UPS_INTEGRITY_VIOLATED);
  }
}

} // namespace upscaledb
