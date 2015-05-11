/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * The PageManager allocates, fetches and frees pages. It manages the
 * list of all pages (free and not free), and maps their virtual ID to
 * their physical address in the file.
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_PAGE_MANAGER_STATE_H
#define HAM_PAGE_MANAGER_STATE_H

#include "0root/root.h"

#include <map>
#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!
#include "2config/env_config.h"
#include "3cache/cache.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Device;
class EnvironmentHeader;
class LocalDatabase;
class LocalEnvironment;
class LsnManager;

/*
 * The internal state of the PageManager
 */
struct PageManagerState
{
  // The freelist maps page-id to number of free pages (usually 1)
  typedef std::map<uint64_t, size_t> FreeMap;

  PageManagerState(LocalEnvironment *env);

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

  // The map with free pages
  FreeMap free_pages;

  // Whether |m_free_pages| must be flushed or not
  bool needs_flush;

  // Whether a "purge cache" operation is pending
  boost::atomic<bool> purge_cache_pending;

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

  // number of successful freelist hits
  uint64_t freelist_hits;

  // number of freelist misses
  uint64_t freelist_misses;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_STATE_H */
