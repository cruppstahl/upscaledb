/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_test.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

static Page *
alloc_impl(PageManagerState &state, Context *context, uint32_t page_type,
                uint32_t flags);

static Page *
fetch_impl(PageManagerState &state, Context *context, uint64_t address,
                uint32_t flags);

enum {
  kPurgeAtLeast = 20
};

static uint64_t
store_state_impl(PageManagerState &state, Context *context)
{
  // no modifications? then simply return the old blobid
  if (!state.needs_flush)
    return (state.state_page ? state.state_page->get_address() : 0);

  state.needs_flush = false;

  // no freelist pages, no freelist state? then don't store anything
  if (!state.state_page && state.free_pages.empty())
    return (0);

  // otherwise allocate a new page, if required
  if (!state.state_page) {
    state.state_page = new Page(state.device);
    state.state_page->allocate(Page::kTypePageManager,
            Page::kInitializeWithZeroes);
  }

  context->changeset.put(state.state_page);

  uint32_t page_size = state.config.page_size_bytes;

  // make sure that the page is logged
  Page *page = state.state_page;
  page->set_dirty(true);

  uint8_t *p = page->get_payload();

  // store page-ID of the last allocated blob
  *(uint64_t *)p = state.last_blob_page_id;
  p += sizeof(uint64_t);

  // reset the overflow pointer and the counter
  // TODO here we lose a whole chain of overflow pointers if there was such
  // a chain. We only save the first. That's not critical but also not nice.
  uint64_t next_pageid = *(uint64_t *)p;
  if (next_pageid) {
    state.free_pages[next_pageid] = 1;
    ham_assert(next_pageid % page_size == 0);
  }

  // No freelist entries? then we're done. Make sure that there's no
  // overflow pointer or other garbage in the page!
  if (state.free_pages.empty()) {
    *(uint64_t *)p = 0;
    p += sizeof(uint64_t);
    *(uint32_t *)p = 0;
    return (state.state_page->get_address());
  }

  PageManagerState::FreeMap::const_iterator it = state.free_pages.begin();
  while (it != state.free_pages.end()) {
    // this is where we will store the data
    p = page->get_payload();
    // skip state.last_blob_page_id?
    if (page == state.state_page)
      p += sizeof(uint64_t);
    p += 8;   // leave room for the pointer to the next page
    p += 4;   // leave room for the counter

    uint32_t counter = 0;

    while (it != state.free_pages.end()) {
      // 9 bytes is the maximum amount of storage that we will need for a
      // new entry; if it does not fit then break
      if ((p + 9) - page->get_payload()
              >= (ptrdiff_t)(state.config.page_size_bytes
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

      for (; it != state.free_pages.end() && page_counter < 16 - 1; it++) {
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
    if (page == state.state_page) // skip state.last_blob_page_id?
      p += sizeof(uint64_t);
    uint64_t next_pageid = *(uint64_t *)p;
    *(uint64_t *)p = 0;
    p += 8;  // overflow page

    // now store the counter
    *(uint32_t *)p = counter;

    // are we done? if not then continue with the next page
    if (it != state.free_pages.end()) {
      // allocate (or fetch) an overflow page
      if (!next_pageid) {
        Page *new_page = alloc_impl(state, context, Page::kTypePageManager,
                PageManager::kIgnoreFreelist);
        // patch the overflow pointer in the old (current) page
        p = page->get_payload();
        if (page == state.state_page) // skip state.last_blob_page_id?
          p += sizeof(uint64_t);
        *(uint64_t *)p = new_page->get_address();

        // reset the overflow pointer in the new page
        page = new_page;
        p = page->get_payload();
        *(uint64_t *)p = 0;
      }
      else
        page = fetch_impl(state, context, next_pageid, 0);

      // make sure that the page is logged
      page->set_dirty(true);
    }
  }

  return (state.state_page->get_address());
}

/* if recovery is enabled then immediately write the modified blob */
static void
maybe_store_state(PageManagerState &state, Context *context, bool force)
{
  if (force || (state.config.flags & HAM_ENABLE_RECOVERY)) {
    uint64_t new_blobid = store_state_impl(state, context);
    if (new_blobid != state.header->get_page_manager_blobid()) {
      state.header->set_page_manager_blobid(new_blobid);
      state.header->get_header_page()->set_dirty(true);
      context->changeset.put(state.header->get_header_page());
    }
  }
}

/* returns true if the cache is full */
static bool
is_cache_full_impl(const PageManagerState &state)
{
  return (state.cache.allocated_elements() * state.config.page_size_bytes
                    > state.cache.capacity());
}

static Page *
fetch_impl(PageManagerState &state, Context *context, uint64_t address,
                uint32_t flags)
{
  /* fetch the page from the cache */
  Page *page = state.cache.get(address);
  if (page) {
    ham_assert(page->get_data());
    if (flags & PageManager::kNoHeader)
      page->set_without_header(true);
    context->changeset.put(page);
    return (page);
  }

  if ((flags & PageManager::kOnlyFromCache)
          || state.config.flags & HAM_IN_MEMORY)
    return (0);

  page = new Page(state.device, context->db);
  try {
    page->fetch(address);
  }
  catch (Exception &ex) {
    delete page;
    throw ex;
  }

  ham_assert(page->get_data());

  /* store the page in the list */
  state.cache.put(page);

  /* write to disk (if necessary) */
  if (!(flags & PageManager::kDisableStoreState)
          && !(flags & PageManager::kReadOnly))
    maybe_store_state(state, context, false);

  if (flags & PageManager::kNoHeader)
    page->set_without_header(true);

  context->changeset.put(page);
  state.page_count_fetched++;
  return (page);
}

static Page *
alloc_impl(PageManagerState &state, Context *context, uint32_t page_type,
                uint32_t flags)
{
  uint64_t address = 0;
  Page *page = 0;
  uint32_t page_size = state.config.page_size_bytes;
  bool allocated = false;

  /* first check the internal list for a free page */
  if ((flags & PageManager::kIgnoreFreelist) == 0
          && !state.free_pages.empty()) {
    PageManagerState::FreeMap::iterator it = state.free_pages.begin();

    address = it->first;
    ham_assert(address % page_size == 0);
    /* remove the page from the freelist */
    state.free_pages.erase(it);
    state.needs_flush = true;

    state.freelist_hits++;

    /* try to fetch the page from the cache */
    page = state.cache.get(address);
    if (page)
      goto done;
    /* allocate a new page structure and read the page from disk */
    page = new Page(state.device, context->db);
    page->fetch(address);
    goto done;
  }

  state.freelist_misses++;

  try {
    if (!page) {
      allocated = true;
      page = new Page(state.device, context->db);
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
  page->set_db(context->db);

  if (page->get_node_proxy()) {
    delete page->get_node_proxy();
    page->set_node_proxy(0);
  }

  context->changeset.put(page);

  /* store the page in the cache */
  state.cache.put(page);

  /* write to disk (if necessary) */
  if (!(flags & PageManager::kDisableStoreState)
          && !(flags & PageManager::kReadOnly))
    maybe_store_state(state, context, false);

  switch (page_type) {
    case Page::kTypeBindex:
    case Page::kTypeBroot: {
      memset(page->get_payload(), 0, sizeof(PBtreeNode));
      state.page_count_index++;
      break;
    }
    case Page::kTypePageManager:
      state.page_count_page_manager++;
      break;
    case Page::kTypeBlob:
      state.page_count_blob++;
      break;
    default:
      break;
  }

  return (page);
}

static Page *
alloc_multiple_blob_pages_impl(PageManagerState &state, Context *context,
                size_t num_pages)
{
  // allocate only one page? then use the normal ::alloc() method
  if (num_pages == 1)
    return (alloc_impl(state, context, Page::kTypeBlob, 0));

  Page *page = 0;
  uint32_t page_size = state.config.page_size_bytes;

  // Now check the freelist
  if (!state.free_pages.empty()) {
    for (PageManagerState::FreeMap::iterator it = state.free_pages.begin();
            it != state.free_pages.end();
            it++) {
      if (it->second >= num_pages) {
        for (size_t i = 0; i < num_pages; i++) {
          if (i == 0) {
            page = fetch_impl(state, context, it->first, 0);
            page->set_type(Page::kTypeBlob);
            page->set_without_header(false);
          }
          else {
            Page *p = fetch_impl(state, context, it->first + (i*page_size), 0);
            p->set_type(Page::kTypeBlob);
            p->set_without_header(true);
          }
        }
        if (it->second > num_pages) {
          state.free_pages[it->first + num_pages * page_size]
                = it->second - num_pages;
        }
        state.free_pages.erase(it);
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
      page = alloc_impl(state, context, Page::kTypeBlob, flags);
    else {
      Page *p = alloc_impl(state, context, Page::kTypeBlob, flags);
      p->set_without_header(true);
    }
  }

  // now store the state
  maybe_store_state(state, context, false);
  return (page);
}

struct FlushAllPagesPurger
{
  bool operator()(Page *page) {
    page->flush();
    return (false);
  }
};

static void
flush_impl(PageManagerState &state)
{
  FlushAllPagesPurger purger;
  state.cache.purge_if(purger);

  if (state.state_page)
    state.state_page->flush();
}

// callback for purging pages
struct Purger
{
  Purger(Context *context, PageManager *page_manager)
    : m_context(context), m_page_manager(page_manager) {
  }

  void operator()(Page *page) {
    BtreeCursor::uncouple_all_cursors(m_context, page);

    if (m_page_manager->m_state.last_blob_page == page) {
      m_page_manager->m_state.last_blob_page_id
              = m_page_manager->m_state.last_blob_page->get_address();
      m_page_manager->m_state.last_blob_page = 0;
    }

    page->flush();
    delete page;
  }

  Context *m_context;
  PageManager *m_page_manager;
};

struct DbClosePurger
{
  DbClosePurger(LocalDatabase *db)
    : m_db(db) {
  }

  bool operator()(Page *page) {
    if (page->get_db() == m_db && page->get_address() != 0) {
      ham_assert(page->get_cursor_list() == 0);
      page->flush();
      return (true);
    }
    return (false);
  }

  LocalDatabase *m_db;
};

PageManagerState::PageManagerState(LocalEnvironment *env)
  : config(env->get_config()), header(env->header()),
    device(env->device()), lsn_manager(env->lsn_manager()),
    cache(CacheState(config)), needs_flush(false), state_page(0),
    last_blob_page(0), last_blob_page_id(0), page_count_fetched(0),
    page_count_index(0), page_count_blob(0), page_count_page_manager(0),
    cache_hits(0), cache_misses(0), freelist_hits(0), freelist_misses(0)
{
}

PageManager::PageManager(const PageManagerState &state)
  : m_state(state)
{
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
      page = fetch_impl(m_state, &context, overflow, 0);
    else
      break;
  }
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

Page *
PageManager::fetch(Context *context, uint64_t address, uint32_t flags)
{
  return (fetch_impl(m_state, context, address, flags));
}

Page *
PageManager::alloc(Context *context, uint32_t page_type, uint32_t flags)
{
  return (alloc_impl(m_state, context, page_type, flags));
}

Page *
PageManager::alloc_multiple_blob_pages(Context *context, size_t num_pages)
{
  return (alloc_multiple_blob_pages_impl(m_state, context, num_pages));
}

void
PageManager::flush()
{
  flush_impl(m_state);
}

void
PageManager::purge_cache(Context *context)
{
  // in-memory-db: don't remove the pages or they would be lost
  if (m_state.config.flags & HAM_IN_MEMORY || !is_cache_full_impl(m_state))
    return;

  // Purge as many pages as possible to get memory usage down to the
  // cache's limit.
  Purger purger(context, this);
  m_state.cache.purge(context, purger);
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
    maybe_store_state(m_state, context, true);
    m_state.device->truncate(file_size);
  }
}

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
PageManager::del(Page *page, size_t page_count)
{
  ham_assert(page_count > 0);

  if (m_state.config.flags & HAM_IN_MEMORY)
    return;

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
PageManager::close(Context *context)
{
  // store the state of the PageManager
  if ((m_state.config.flags & HAM_IN_MEMORY) == 0
      && (m_state.config.flags & HAM_READ_ONLY) == 0) {
    maybe_store_state(m_state, context, true);
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

  // clear the Changeset because flush_impl() will delete all Page pointers
  context->changeset.clear();

  // flush all dirty pages to disk, then delete them
  flush();

  delete m_state.state_page;
  m_state.state_page = 0;
  m_state.last_blob_page = 0;
}

Page *
PageManager::get_last_blob_page(Context *context)
{
  if (m_state.last_blob_page)
    return (m_state.last_blob_page);
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

PageManagerTestGateway
PageManager::test()
{
  return (PageManagerTestGateway(&m_state));
}

PageManagerTestGateway::PageManagerTestGateway(PageManagerState *state)
  : m_state(state)
{
}

uint64_t
PageManagerTestGateway::store_state()
{
  Context context(0, 0, 0);
  return (store_state_impl(*m_state, &context));
}

void
PageManagerTestGateway::remove_page(Page *page)
{
  m_state->cache.del(page);
}

bool
PageManagerTestGateway::is_page_free(uint64_t pageid)
{
  return (m_state->free_pages.find(pageid) != m_state->free_pages.end());
}

Page *
PageManagerTestGateway::fetch_page(uint64_t id)
{
  return (m_state->cache.get(id));
}

void
PageManagerTestGateway::store_page(Page *page)
{
  m_state->cache.put(page);
}

bool
PageManagerTestGateway::is_cache_full()
{
  return (is_cache_full_impl(*m_state));
}

} // namespace hamsterdb
