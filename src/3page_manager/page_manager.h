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

    // Closes the PageManager; flushes all dirty pages
    void close(Context *context);

    // Calls close(), then re-initializes the PageManager; used to restart
    // the internal state after recovery was performed
    void reset(Context *context);

    // Returns the Page pointer where we can add more blobs
    Page *get_last_blob_page(Context *context);

    // Sets the Page pointer where we can add more blobs
    void set_last_blob_page(Page *page);

    // Fetches a page from the cache and locks it with try_lock(); returns
    // the page object, or NULL if try_lock failed. This method is used to
    // fetch purge candidates.
    Page *try_fetch(uint64_t page_id);

    // Returns additional testing interfaces
    PageManagerTest test();

  private:
    friend struct Purger;
    friend class PageManagerTest;
    friend class PageManagerWorker;

    // PRO: verifies the crc32 of a page
    void verify_crc32(Page *page);

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
