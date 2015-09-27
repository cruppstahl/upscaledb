/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

class Device;
class EnvironmentHeader;
class LocalDatabase;
class LocalEnvironment;
class LsnManager;
struct AsyncFlushMessage;

/*
 * The internal state of the PageManager
 */
struct PageManagerState
{
  PageManagerState(LocalEnvironment *env);

  ~PageManagerState();

  //  For serializing access 
  Spinlock mutex;

  // Copy of the Environment's configuration
  const EnvironmentConfiguration config;

  // The Environment's header
  EnvironmentHeader *header;

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
};

} // namespace upscaledb

#endif /* UPS_PAGE_MANAGER_STATE_H */
