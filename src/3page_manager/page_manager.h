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
#include "1base/scoped_ptr.h"
#include "3page_manager/page_manager_state.h"
#include "3page_manager/page_manager_test.h"
#include "3page_manager/page_manager_worker.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
class LocalDatabase;
class LocalEnvironment;

class PageManager
{
  public:
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

    // Constructor
    PageManager(LocalEnvironment *env);

    // Loads the state from a blob
    void initialize(uint64_t blobid);

    // Fills in the current metrics for the PageManager, the Cache and the
    // Freelist
    void fill_metrics(ham_env_metrics_t *metrics) const;

    // Fetches a page from disk. |flags| are bitwise OR'd: kOnlyFromCache,
    // kReadOnly, kNoHeader...
    // The page is locked and stored in |context->changeset|.
    Page *fetch(Context *context, uint64_t address, uint32_t flags = 0);

    // Allocates a new page. |page_type| is one of Page::kType* in page.h.
    // |flags| are either 0 or kClearWithZero
    // The page is locked and stored in |context->changeset|.
    Page *alloc(Context *context, uint32_t page_type, uint32_t flags = 0);

    // Allocates multiple adjacent pages.
    // Used by the BlobManager to store blobs that span multiple pages
    // Returns the first page in the list of pages
    // The pages are locked and stored in |context->changeset|.
    Page *alloc_multiple_blob_pages(Context *context, size_t num_pages);

    // Flushes all pages to disk and deletes them if |delete_pages| is true
    void flush(bool delete_pages);

    // Asks the worker thread to purge the cache if the cache limits are
    // exceeded
    void purge_cache(Context *context);

    // Reclaim file space; truncates unused file space at the end of the file.
    void reclaim_space(Context *context);

    // Flushes and closes all pages of a database
    void close_database(Context *context, LocalDatabase *db);

    // Schedules one (or many sequential) pages for deletion and adds them
    // to the Freelist. Will not do anything if the Environment is in-memory.
    void del(Context *context, Page *page, size_t page_count = 1);

    // Resets the PageManager; calls clear(), then starts a new worker thread
    void reset(Context *context);

    // Closes the PageManager; flushes all dirty pages
    void close(Context *context);

    // Returns the Page pointer where we can add more blobs
    Page *get_last_blob_page(Context *context);

    // Sets the Page pointer where we can add more blobs
    void set_last_blob_page(Page *page);

    // Returns additional testing interfaces
    PageManagerTest test();

  private:
    friend struct Purger;
    friend class PageManagerTest;
    friend class PageManagerWorker;

    // Persists the PageManager's state in the file
    uint64_t store_state(Context *context);

    // Calls store_state() whenever it makes sense
    void maybe_store_state(Context *context, bool force);

    // Locks a page, fetches contents from disk if they were flushed in
    // the meantime
    Page *safely_lock_page(Context *context, Page *page,
                bool allow_recursive_lock);

    // The worker thread which flushes dirty pages
    ScopedPtr<PageManagerWorker> m_worker;

    // The state
    PageManagerState m_state;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H */
