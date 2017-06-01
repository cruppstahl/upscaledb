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
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

static inline Page *
alloc_unlocked(PageManagerState *state, Context *context, uint32_t page_type,
                uint32_t flags);
static inline Page *
fetch_unlocked(PageManagerState *state, Context *context,
                uint64_t address, uint32_t flags);

template <typename T>
struct Deleter
{
  void operator()(T *p) {
    delete p;
  }
};

struct AsyncFlushMessage
{
  AsyncFlushMessage(PageManager *page_manager_, Device *device_,
          Signal *signal_)
    : page_manager(page_manager_), device(device_), signal(signal_),
      in_progress(false) {
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
  for (std::vector<uint64_t>::iterator it = message->page_ids.begin();
                  it != message->page_ids.end();
                  it++) {
    // skip page if it's already in use
    Page *page = message->page_manager->try_lock_purge_candidate(*it);
    if (!page)
      continue;
    assert(page->mutex().try_lock() == false);

    // flush page if it's dirty
    if (page->is_dirty()) {
      try {
        page->flush();
      }
      catch (Exception &) {
        // ignore page, fall through
      }
    }
    page->mutex().unlock();
  }
  if (message->in_progress)
    message->in_progress = false;
  if (message->signal)
    message->signal->notify();
}

static inline void
verify_crc32(Page *page)
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

static inline Page *
add_to_changeset(Changeset *changeset, Page *page)
{
  changeset->put(page);
  assert(page->mutex().try_lock() == false);
  return page;
}

static inline uint64_t
store_state_impl(PageManagerState *state, Context *context)
{
  // no modifications? then simply return the old blobid
  if (!state->needs_flush)
    return state->state_page ? state->state_page->address() : 0;

  state->needs_flush = false;

  // no freelist pages, no freelist state? then don't store anything
  if (!state->state_page && state->freelist.empty())
    return 0;

  // otherwise allocate a new page, if required
  if (!state->state_page) {
    state->state_page = new Page(state->device);
    state->state_page->alloc(Page::kTypePageManager,
            Page::kInitializeWithZeroes);
  }

  // don't bother locking the state page; it will never be accessed by
  // the worker thread because it's not stored in the cache
  context->changeset.put(state->state_page);

  state->state_page->set_dirty(true);

  Page *page = state->state_page;
  uint8_t *p = page->payload();

  // store page-ID of the last allocated blob
  *(uint64_t *)p = state->last_blob_page_id;
  p += sizeof(uint64_t);

  // reset the overflow pointer and the counter
  // TODO here we lose a whole chain of overflow pointers if there was such
  // a chain. We only save the first. That's not critical but also not nice.
  uint64_t next_pageid = *(uint64_t *)p;
  if (next_pageid) {
    state->freelist.put(next_pageid, 1);
    *(uint64_t *)p = 0;
  }

  // No freelist entries? then we're done. Make sure that there's no
  // overflow pointer or other garbage in the page!
  if (state->freelist.empty()) {
    p += sizeof(uint64_t);
    *(uint32_t *)p = 0;
    return state->state_page->address();
  }

  std::pair<bool, Freelist::FreeMap::const_iterator> continuation;
  continuation.first = false;   // initialization
  continuation.second = state->freelist.free_pages.end();
  do {
    int offset = page == state->state_page
                      ? sizeof(uint64_t)
                      : 0;
    continuation = state->freelist.encode_state(continuation,
                            page->payload() + offset,
                            state->config.page_size_bytes
                                - Page::kSizeofPersistentHeader
                                - offset);

    if (continuation.first == false)
      break;

    // load the next page
    if (!next_pageid) {
      Page *new_page = alloc_unlocked(state, context, Page::kTypePageManager,
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
      page = fetch_unlocked(state, context, next_pageid, 0);
      p = page->payload();
    }

    // make sure that the page is logged
    page->set_dirty(true);
  } while (true);

  return state->state_page->address();
}

static inline void
maybe_store_state(PageManagerState *state, Context *context, bool force)
{
  if (force || state->env->journal.get()) {
    uint64_t new_blobid = store_state_impl(state, context);
    if (new_blobid != state->header->page_manager_blobid()) {
      state->header->set_page_manager_blobid(new_blobid);
      // don't bother to lock the header page
      state->header->header_page->set_dirty(true);
      context->changeset.put(state->header->header_page);
    }
  }
}

static inline Page *
fetch_unlocked(PageManagerState *state, Context *context, uint64_t address,
                uint32_t flags)
{
  /* fetch the page from the cache */
  Page *page;
  
  if (address == 0)
    page = state->header->header_page;
  else if (state->state_page && address == state->state_page->address())
    page = state->state_page;
  else
    page = state->cache.get(address);

  if (page) {
    page->set_without_header(ISSET(flags, PageManager::kNoHeader));
    return add_to_changeset(&context->changeset, page);
  }

  if (ISSET(flags, PageManager::kOnlyFromCache)
          || ISSET(state->config.flags, UPS_IN_MEMORY))
    return 0;

  page = new Page(state->device, context->db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  assert(page->data());

  /* store the page in the list */
  state->cache.put(page);

  /* write state to disk (if necessary) */
  if (NOTSET(flags, PageManager::kDisableStoreState)
          && NOTSET(flags, PageManager::kReadOnly))
    maybe_store_state(state, context, false);

  /* only verify crc if the page has a header */
  page->set_without_header(ISSET(flags, PageManager::kNoHeader));
  if (!page->is_without_header()
          && ISSET(state->config.flags, UPS_ENABLE_CRC32))
    verify_crc32(page);

  state->page_count_fetched++;
  return add_to_changeset(&context->changeset, page);
}

static inline Page *
alloc_unlocked(PageManagerState *state, Context *context, uint32_t page_type,
                uint32_t flags)
{
  uint64_t address = 0;
  Page *page = 0;
  uint32_t page_size = state->config.page_size_bytes;
  bool allocated = false;

  /* first check the internal list for a free page */
  if (NOTSET(flags, PageManager::kIgnoreFreelist)) {
    address = state->freelist.alloc(1);

    if (address != 0) {
      assert(address % page_size == 0);
      state->needs_flush = true;

      /* try to fetch the page from the cache */
      page = state->cache.get(address);
      if (page)
        goto done;
      /* otherwise fetch the page from disk */
      page = new Page(state->device, context->db);
      page->fetch(address);
      goto done;
    }
  }

  try {
    if (!page) {
      allocated = true;
      page = new Page(state->device, context->db);
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
  if (ISSET(flags, PageManager::kClearWithZero))
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
  state->cache.put(page);
  add_to_changeset(&context->changeset, page);

  /* write to disk (if necessary) */
  if (NOTSET(flags, PageManager::kDisableStoreState)
          && NOTSET(flags, PageManager::kReadOnly))
    maybe_store_state(state, context, false);

  switch (page_type) {
    case Page::kTypeBindex:
    case Page::kTypeBroot: {
      ::memset(page->payload(), 0, sizeof(PBtreeNode));
      state->page_count_index++;
      break;
    }
    case Page::kTypePageManager:
      state->page_count_page_manager++;
      break;
    case Page::kTypeBlob:
      state->page_count_blob++;
      break;
    default:
      break;
  }

  return page;
}


PageManagerState::PageManagerState(LocalEnv *_env)
  : env(_env), config(_env->config), header(_env->header.get()),
    device(_env->device.get()), lsn_manager(&_env->lsn_manager),
    cache(_env->config), freelist(config), needs_flush(false),
    state_page(0), last_blob_page(0), last_blob_page_id(0),
    page_count_fetched(0), page_count_index(0), page_count_blob(0),
    page_count_page_manager(0), cache_hits(0), cache_misses(0), message(0),
    worker(new WorkerPool(1))
{
}

PageManagerState::~PageManagerState()
{
  delete message;
  message = 0;

  delete state_page;
  state_page = 0;
  last_blob_page = 0;
}

void
PageManager::initialize(uint64_t pageid)
{
  Context context(0, 0, 0);

  state->freelist.clear();

  if (state->state_page)
    delete state->state_page;
  state->state_page = new Page(state->device);
  state->state_page->fetch(pageid);
  if (ISSET(state->config.flags, UPS_ENABLE_CRC32))
    verify_crc32(state->state_page);

  Page *page = state->state_page;

  // the first page stores the page ID of the last blob
  state->last_blob_page_id = *(uint64_t *)page->payload();

  while (1) {
    assert(page->type() == Page::kTypePageManager);
    uint8_t *p = page->payload();
    // skip state->last_blob_page_id?
    if (page == state->state_page)
      p += sizeof(uint64_t);

    // get the overflow address
    uint64_t overflow = *(uint64_t *)p;
    p += 8;

    state->freelist.decode_state(p);

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
  ScopedSpinlock lock(state->mutex);
  return fetch_unlocked(state.get(), context, address, flags);
}

Page *
PageManager::alloc(Context *context, uint32_t page_type, uint32_t flags)
{
  ScopedSpinlock lock(state->mutex);
  return alloc_unlocked(state.get(), context, page_type, flags);
}

Page *
PageManager::alloc_multiple_blob_pages(Context *context, size_t num_pages)
{
  ScopedSpinlock lock(state->mutex);

  // allocate only one page? then use the normal ::alloc() method
  if (num_pages == 1)
    return alloc_unlocked(state.get(), context, Page::kTypeBlob, 0);

  Page *page = 0;
  uint32_t page_size = state->config.page_size_bytes;

  // Now check the freelist
  uint64_t address = state->freelist.alloc(num_pages);
  if (address != 0) {
    for (size_t i = 0; i < num_pages; i++) {
      if (i == 0) {
        page = fetch_unlocked(state.get(), context, address, 0);
        page->set_type(Page::kTypeBlob);
      }
      else {
        Page *p = fetch_unlocked(state.get(), context,
                        address + (i * page_size), PageManager::kNoHeader);
        p->set_type(Page::kTypeBlob);
      }
    }

    return page;
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
      page = alloc_unlocked(state.get(), context, Page::kTypeBlob, flags);
    else {
      Page *p = alloc_unlocked(state.get(), context, Page::kTypeBlob, flags);
      p->set_without_header(true);
    }
  }

  // now store the state
  maybe_store_state(state.get(), context, false);
  return page;
}

void
PageManager::fill_metrics(ups_env_metrics_t *metrics) const
{
  metrics->page_count_fetched = state->page_count_fetched;
  metrics->page_count_flushed = Page::ms_page_count_flushed;
  metrics->page_count_type_index = state->page_count_index;
  metrics->page_count_type_blob = state->page_count_blob;
  metrics->page_count_type_page_manager = state->page_count_page_manager;
  metrics->freelist_hits = state->freelist.freelist_hits;
  metrics->freelist_misses = state->freelist.freelist_misses;
  state->cache.fill_metrics(metrics);
}

struct FlushAllPagesVisitor
{
  FlushAllPagesVisitor(AsyncFlushMessage *message_)
    : message(message_) {
  }

  bool operator()(Page *page) {
    if (page->is_dirty())
      message->page_ids.push_back(page->address());
    return false;
  }

  AsyncFlushMessage *message;
};

void
PageManager::flush_all_pages()
{
  Signal signal;
  AsyncFlushMessage *message = new AsyncFlushMessage(this, state->device,
                                    &signal);

  FlushAllPagesVisitor visitor(message);

  {
    ScopedSpinlock lock(state->mutex);

    state->cache.purge_if(visitor);

    if (state->header->header_page->is_dirty())
      message->page_ids.push_back(0);

    if (state->state_page && state->state_page->is_dirty())
      message->page_ids.push_back(state->state_page->address());
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
  ScopedSpinlock lock(state->mutex);

  // do NOT purge the cache iff
  //   1. this is an in-memory Environment
  //   2. there's still a "purge cache" operation pending
  //   3. the cache is not full
  if (ISSET(state->config.flags, UPS_IN_MEMORY)
      || (state->message && state->message->in_progress == true)
      || !state->cache.is_cache_full())
    return;

  if (unlikely(!state->message))
    state->message = new AsyncFlushMessage(this, state->device, 0);

  state->message->page_ids.clear();
  state->garbage.clear();

  state->cache.purge_candidates(state->message->page_ids, state->garbage,
          state->last_blob_page);

  // don't bother if there are only few pages
  if (state->message->page_ids.size() > 10) {
    state->message->in_progress = true;
    run_async(boost::bind(&async_flush_pages, state->message));
  }

  for (std::vector<Page *>::iterator it = state->garbage.begin();
                  it != state->garbage.end();
                  it++) {
    Page *page = *it;
    if (likely(page->mutex().try_lock())) {
      assert(page->cursor_list.is_empty());
      state->cache.del(page);
      page->mutex().unlock();
      delete page;
    }
  }
}

void
PageManager::reclaim_space(Context *context)
{
  ScopedSpinlock lock(state->mutex);

  if (state->last_blob_page) {
    state->last_blob_page_id = state->last_blob_page->address();
    state->last_blob_page = 0;
  }
  assert(NOTSET(state->config.flags, UPS_DISABLE_RECLAIM_INTERNAL));

  bool do_truncate = false;
  uint32_t page_size = state->config.page_size_bytes;
  uint64_t file_size = state->device->file_size();
  uint64_t address = state->freelist.truncate(file_size);

  if (address < file_size) {
    for (uint64_t page_id = address;
            page_id <= file_size - page_size;
            page_id += page_size) {
      Page *page = state->cache.get(page_id);
      if (page) {
        state->cache.del(page);
        delete page;
      }
    }

    do_truncate = true;
    file_size = address;
  }

  if (do_truncate) {
    state->needs_flush = true;
    state->device->truncate(file_size);
    maybe_store_state(state.get(), context, true);
  }
}

struct CloseDatabaseVisitor
{
  CloseDatabaseVisitor(LocalDb *db_, AsyncFlushMessage *message_)
    : db(db_), message(message_) {
  }

  bool operator()(Page *page) {
    if (page->db() == db && page->address() != 0) {
      message->page_ids.push_back(page->address());
      pages.push_back(page);
    }
    return false;
  }

  LocalDb *db;
  std::vector<Page *> pages;
  AsyncFlushMessage *message;
};

void
PageManager::close_database(Context *context, LocalDb *db)
{
  Signal signal;
  AsyncFlushMessage *message = new AsyncFlushMessage(this, state->device,
                                    &signal);

  CloseDatabaseVisitor visitor(db, message);

  {
    ScopedSpinlock lock(state->mutex);

    if (state->last_blob_page) {
      state->last_blob_page_id = state->last_blob_page->address();
      state->last_blob_page = 0;
    }

    context->changeset.clear();
    state->cache.purge_if(visitor);

    if (state->header->header_page->is_dirty())
      message->page_ids.push_back(0);
  }

  if (message->page_ids.size() > 0) {
    run_async(boost::bind(&async_flush_pages, message));
    signal.wait();
  }

  delete message;

  ScopedSpinlock lock(state->mutex);
  // now delete the pages
  for (std::vector<Page *>::iterator it = visitor.pages.begin();
          it != visitor.pages.end();
          it++) {
    state->cache.del(*it);
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
  assert(page_count > 0);

  ScopedSpinlock lock(state->mutex);
  if (ISSET(state->config.flags, UPS_IN_MEMORY))
    return;

  // remove the page(s) from the changeset
  context->changeset.del(page);
  if (page_count > 1) {
    uint32_t page_size = state->config.page_size_bytes;
    for (size_t i = 1; i < page_count; i++) {
      Page *p = state->cache.get(page->address() + i * page_size);
      if (p && context->changeset.has(p))
        context->changeset.del(p);
    }
  }

  state->needs_flush = true;
  state->freelist.put(page->address(), page_count);
  assert(page->address() % state->config.page_size_bytes == 0);

  if (page->node_proxy()) {
    delete page->node_proxy();
    page->set_node_proxy(0);
  }

  // do not call maybe_store_state() - this change in the state is not
  // relevant for logging.
}

void
PageManager::close(Context *context)
{
  // no need to lock the mutex; this method is called during shutdown

  // cut off unused space at the end of the file; this space is managed
  // by the device
  state->device->reclaim_space();

  // reclaim unused disk space
  // if logging is enabled: also flush the changeset to write back the
  // modified freelist pages
  bool try_reclaim = NOTSET(state->config.flags, UPS_DISABLE_RECLAIM_INTERNAL);

#ifdef WIN32
  // Win32: it's not possible to truncate the file while there's an active
  // mapping, therefore only reclaim if memory mapped I/O is disabled
  if (NOTSET(state->config.flags, UPS_DISABLE_MMAP))
    try_reclaim = false;
#endif

  if (try_reclaim)
    reclaim_space(context);

  // store the state of the PageManager
  if (NOTSET(state->config.flags, UPS_IN_MEMORY)
        && NOTSET(state->config.flags, UPS_READ_ONLY))
    maybe_store_state(state.get(), context, true);

  // clear the Changeset because flush() will delete all Page pointers
  context->changeset.clear();

  // flush all dirty pages to disk, then delete them
  flush_all_pages();

  // join the worker thread
  state->worker.reset(0);
}

void
PageManager::reset(Context *context)
{
  close(context);
  state.reset(new PageManagerState(state->env));
}

Page *
PageManager::last_blob_page(Context *context)
{
  ScopedSpinlock lock(state->mutex);

  if (state->last_blob_page)
    return add_to_changeset(&context->changeset, state->last_blob_page);
  if (state->last_blob_page_id)
    return fetch_unlocked(state.get(), context, state->last_blob_page_id, 0);
  return 0;
}

void 
PageManager::set_last_blob_page(Page *page)
{
  ScopedSpinlock lock(state->mutex);
  state->last_blob_page_id = page ? page->address() : 0;
  state->last_blob_page = page;
}

uint64_t
PageManager::last_blob_page_id()
{
  ScopedSpinlock lock(state->mutex);
  if (state->last_blob_page_id)
    return state->last_blob_page_id;
  if (state->last_blob_page)
    return state->last_blob_page->address();
  return 0;
}

void
PageManager::set_last_blob_page_id(uint64_t id)
{
  ScopedSpinlock lock(state->mutex);
  state->last_blob_page_id = id;
  state->last_blob_page = 0;
}

Page *
PageManager::try_lock_purge_candidate(uint64_t address)
{
  Page *page = 0;

  // try to lock the PageManager; if this fails then return immediately
  ScopedTryLock<Spinlock> lock(state->mutex);
  if (!lock.is_locked())
    return 0;

  if (address == 0)
    page = state->header->header_page;
  else if (state->state_page && address == state->state_page->address())
    page = state->state_page;
  else
    page = state->cache.get(address);

  if (!page || !page->mutex().try_lock())
    return 0;

  // !!
  // Do not purge pages with cursors, since Cursor::move will return pointers
  // directly into the page's data, and these pointers will be invalidated
  // as soon as the page is purged.
  //
  if (!page->cursor_list.is_empty()) {
    page->mutex().unlock();
    return 0;
  }

  return page;
}

uint64_t
PageManager::test_store_state()
{
  Context context(0, 0, 0);
  return store_state_impl(state.get(), &context);
}

} // namespace upscaledb
