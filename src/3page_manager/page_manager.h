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

/*
 * The PageManager allocates, fetches and frees pages. It manages the
 * list of all pages (free and not free), and maps their virtual ID to
 * their physical address in the file.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_PAGE_MANAGER_H
#define HAM_PAGE_MANAGER_H

#include "0root/root.h"

#include <map>

// Always verify that a file of level N does not include headers > N!
#include "3page_manager/page_manager_state.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LocalDatabase;
class LocalEnvironment;

/*
 * The PageManager implementation
 */
struct PageManager
{
  // Flags for PageManager::alloc_page()
  enum {
    // flag for alloc_page(): Clear the full page with zeroes
    kClearWithZero     = 1,

    // flag for alloc_page(): Ignores the freelist
    kIgnoreFreelist    = 2,

    // flag for alloc_page(): Do not persist the PageManager state to disk
    kDisableStoreState = 4,

    // Flag for fetch_page(): only fetches from cache, not from disk
    kOnlyFromCache = 1,

    // Flag for fetch_page(): does not add page to the Changeset
    kReadOnly = 2,

    // Flag for fetch_page(): page is part of a multi-page blob, has no header
    kNoHeader = 4
  };

  // Loads the state from a blob
  // TODO make private?
  void load_state(uint64_t blobid);

  // Stores the state to a blob; returns the blobid
  // TODO make private?
  uint64_t store_state();

  // Fills in the current metrics for the PageManager, the Cache and the
  // Freelist
  void get_metrics(ham_env_metrics_t *metrics) const;

  // Fetches a page from disk
  //
  // @param db The Database which fetches this page
  // @param address The page's address
  // @param flags bitwise OR'd: kOnlyFromCache, kReadOnly
  Page *fetch_page(LocalDatabase *db, uint64_t address, uint32_t flags = 0);

  // Allocates a new page
  //
  // @param db The Database which allocates this page
  // @param page_type One of Page::TYPE_* in page.h
  // @param flags kClearWithZero
  Page *alloc_page(LocalDatabase *db, uint32_t page_type,
                  uint32_t flags = 0);

  // Allocates multiple adjacent pages
  //
  // Used by the BlobManager to store blobs that span multiple pages
  // Returns the first page in the list of pages
  Page *alloc_multiple_blob_pages(LocalDatabase *db, size_t num_pages);

  // Flushes a Page to disk
  // TODO required?
  void flush_page(Page *page);

  // Flush all pages, and clear the cache.
  //
  // Set |clear_cache| to true if you want the cache to be cleared
  // TODO can we get rid of clear_cache?
  void flush_all_pages(bool clear_cache = false);

  // Purges the cache if the cache limits are exceeded
  void purge_cache();

  // Reclaim file space; truncates unused file space at the end of the file.
  void reclaim_space();

  // Flushes and closes all pages of a database
  void close_database(LocalDatabase *db);

  // Adds a page (or many pages) to the freelist; will not do anything
  // if the Environment is in-memory.
  void add_to_freelist(Page *page, size_t page_count = 1);

  // Returns the Page pointer where we can add more blobs
  Page *get_last_blob_page(LocalDatabase *db);

  // Sets the Page pointer where we can add more blobs
  void set_last_blob_page(Page *page);

  // Closes the PageManager; flushes all dirty pages
  void close();

  // The constructor is not used directly. Use the PageManagerFactory instead
  PageManager(PageManagerState state);

  // The state
  PageManagerState m_state;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H */
