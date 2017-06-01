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

/*
 * The PageManager allocates, fetches and frees pages. It manages the
 * list of all pages (free and not free), and maps their virtual ID to
 * their physical address in the file.
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef UPS_PAGE_MANAGER_STATE_H
#define UPS_PAGE_MANAGER_STATE_H

#include "0root/root.h"

#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!
#include "1base/spinlock.h"
#include "2config/env_config.h"
#include "3cache/cache.h"
#include "3page_manager/freelist.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Device;
struct EnvHeader;
struct LocalDb;
struct LocalEnv;
struct LsnManager;
struct AsyncFlushMessage;
struct WorkerPool;

/*
 * The internal state of the PageManager
 */
struct PageManagerState {
  // constructor
  PageManagerState(LocalEnv *env);

  // destructor
  ~PageManagerState();

  //  For serializing access 
  Spinlock mutex;

  // The Environment
  LocalEnv *env;

  // Copy of the Environment's configuration
  const EnvConfig config;

  // The Environment's header
  EnvHeader *header;

  // The Device
  Device *device;

  // The lsn manager
  LsnManager *lsn_manager;

  // The cache
  Cache cache;

  // The freelist
  Freelist freelist;

  // Whether |m_free_pages| must be flushed or not
  bool needs_flush;

  // Page with the persisted state data. If multiple pages are allocated
  // then these pages form a linked list, with |m_state_page| being the head
  Page *state_page;

  // Cached page where to add more blobs
  Page *last_blob_page;

  // Page where to add more blobs - if |m_last_blob_page| was flushed
  uint64_t last_blob_page_id;

  // tracks number of fetched pages
  uint64_t page_count_fetched;

  // tracks number of index pages
  uint64_t page_count_index;

  // tracks number of blob pages
  uint64_t page_count_blob;

  // tracks number of page manager pages
  uint64_t page_count_page_manager;

  // tracks number of cache hits
  uint64_t cache_hits;

  // tracks number of cache misses
  uint64_t cache_misses;

  // For sending information to the worker thread; cached to avoid memory
  // allocations
  AsyncFlushMessage *message;

  // For collecting unused pages; cached to avoid memory allocations
  std::vector<Page *> garbage;

  // The worker thread which flushes dirty pages
  ScopedPtr<WorkerPool> worker;
};

} // namespace upscaledb

#endif /* UPS_PAGE_MANAGER_STATE_H */
