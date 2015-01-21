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
 * @exception_safe: basic
 * @thread_safe: no
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
  enum {
    // flag for alloc(): Clear the full page with zeroes
    kClearWithZero     = 1,

    // flag for alloc(): Ignores the freelist
    kIgnoreFreelist    = 2,

    // flag for alloc(): Do not persist the PageManager state to disk
    kDisableStoreState = 4,

    // Flag for fetch(): only fetches from cache, not from disk
    kOnlyFromCache = 1,

    // Flag for fetch(): does not add page to the Changeset
    kReadOnly = 2,

    // Flag for fetch(): page is part of a multi-page blob, has no header
    kNoHeader = 4
  };

  // Loads the state from a blob
  void initialize(uint64_t blobid);

  // Fills in the current metrics for the PageManager, the Cache and the
  // Freelist
  void fill_metrics(ham_env_metrics_t *metrics) const;

  // Fetches a page from disk. |flags| are bitwise OR'd: kOnlyFromCache,
  // kReadOnly, kNoHeader...
  Page *fetch(LocalDatabase *db, uint64_t address, uint32_t flags = 0);

  // Allocates a new page. |page_type| is one of Page::kType* in page.h.
  // |flags| are either 0 or kClearWithZero
  Page *alloc(LocalDatabase *db, uint32_t page_type, uint32_t flags = 0);

  // Allocates multiple adjacent pages.
  // Used by the BlobManager to store blobs that span multiple pages
  // Returns the first page in the list of pages
  Page *alloc_multiple_blob_pages(size_t num_pages);

  // Flushes all pages to disk
  void flush();

  // Purges the cache if the cache limits are exceeded
  void purge_cache();

  // Reclaim file space; truncates unused file space at the end of the file.
  void reclaim_space();

  // Flushes and closes all pages of a database
  void close_database(LocalDatabase *db);

  // Schedules one (or many sequential) pages for deletion and adds them
  // to the Freelist. Will not do anything if the Environment is in-memory.
  void del(Page *page, size_t page_count = 1);

  // Closes the PageManager; flushes all dirty pages
  void close();

  // Returns the Page pointer where we can add more blobs
  Page *get_last_blob_page();

  // Sets the Page pointer where we can add more blobs
  void set_last_blob_page(Page *page);

  // The constructor is not used directly. Use the PageManagerFactory instead
  PageManager(PageManagerState state);

  // The state
  PageManagerState m_state;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H */
