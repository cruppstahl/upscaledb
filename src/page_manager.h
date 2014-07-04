/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_PAGE_MANAGER_H__
#define HAM_PAGE_MANAGER_H__

#include <map>

#include "ham/hamsterdb_int.h"

#include "error.h"
#include "env_local.h"
#include "db_local.h"
#include "cache.h"

namespace hamsterdb {

//
// The PageManager allocates, fetches and frees pages. It manages the
// list of all pages (free and not free), and maps their virtual ID to
// their physical address in the file.
//
class PageManager {
    // The freelist maps page-id to number of free pages (usually 1)
    typedef std::map<ham_u64_t, size_t> FreeMap;

  public:
    // Flags for PageManager::alloc_page()
    enum {
      // flag for alloc_page(): Clear the full page with zeroes
      kClearWithZero     = 1,

      // flag for alloc_page(): Ignores the freelist
      kIgnoreFreelist    = 2,

      // flag for alloc_page(): Do not persist the PageManager state to disk
      kDisableStoreState = 4,

      // The amount of pages that are at least flushed in purge_cache()
      kPurgeAtLeast = 20,

      // Only pages above this age are purged
      kPurgeThreshold = 100,

      // Flag for fetch_page(): only fetches from cache, not from disk
      kOnlyFromCache = 1,

      // Flag for fetch_page(): does not add page to the Changeset
      kReadOnly = 2,

      // Flag for fetch_page(): page is part of a multi-page blob, has no header
      kNoHeader = 4
    };

    // Default constructor
    //
    // The cache size is specified in bytes!
    PageManager(LocalEnvironment *env, ham_u64_t cache_size);

    // Destructor
    ~PageManager();

    // Loads the state from a blob
    void load_state(ham_u64_t blobid);

    // Stores the state to a blob; returns the blobid
    ham_u64_t store_state();

    // Fills in the current metrics for the PageManager, the Cache and the
    // Freelist
    void get_metrics(ham_env_metrics_t *metrics) const;

    // Fetches a page from disk
    //
    // @param db The Database which fetches this page
    // @param address The page's address
    // @param flags bitwise OR'd: kOnlyFromCache, kReadOnly
    Page *fetch_page(LocalDatabase *db, ham_u64_t address, ham_u32_t flags = 0);

    // Allocates a new page
    //
    // @param db The Database which allocates this page
    // @param page_type One of Page::TYPE_* in page.h
    // @param flags kClearWithZero
    Page *alloc_page(LocalDatabase *db, ham_u32_t page_type,
                    ham_u32_t flags = 0);

    // Allocates multiple adjacent pages
    //
    // Used by the BlobManager to store blobs that span multiple pages
    // Returns the first page in the list of pages
    Page *alloc_multiple_blob_pages(LocalDatabase *, size_t num_pages);

    // Flushes a Page to disk
    void flush_page(Page *page);

    // Flush all pages, and clear the cache.
    //
    // Set |clear_cache| to true if you want the cache to be cleared
    void flush_all_pages(bool clear_cache = false);

    // Purges the cache if the cache limits are exceeded
    void purge_cache();

    // Reclaim file space; truncates unused file space at the end of the file.
    void reclaim_space();

    // Flushes all pages of a database
    void close_database(Database *db);

    // Returns the cache's capacity
    ham_u64_t get_cache_capacity() const {
      return (m_cache.get_capacity());
    }

    // Adds a page (or many pages) to the freelist; will not do anything
    // if the Environment is in-memory.
    void add_to_freelist(Page *page, size_t page_count = 1);

    // Returns the Page pointer where we can add more blobs
    Page *get_last_blob_page(LocalDatabase *db) {
      if (m_last_blob_page)
        return (m_last_blob_page);
      if (m_last_blob_page_id)
        return (fetch_page(db, m_last_blob_page_id));
      return (0);
    }

    // Sets the Page pointer where we can add more blobs
    void set_last_blob_page(Page *page) {
      m_last_blob_page_id = 0;
      m_last_blob_page = page;
    }

    // Closes the PageManager; flushes all dirty pages
    void close();

    // Removes a page from the list; only for testing.
    void test_remove_page(Page *page) {
      m_cache.remove_page(page);
    }

    // Returns true if a page is free. Ignores multi-pages; only for
    // testing and integrity checks
    bool is_page_free(ham_u64_t pageid) {
      FreeMap::iterator it = m_free_pages.find(pageid);
      return (it != m_free_pages.end());
    }

  private:
    friend struct BlobManagerFixture;
    friend struct PageManagerFixture;
    friend struct LogHighLevelFixture;

    // Fetches a page from the list
    Page *fetch_page(ham_u64_t id) {
      return (m_cache.get_page(id));
    }

    // Stores a page in the list
    void store_page(Page *page, bool dont_flush_state = false) {
      m_cache.put_page(page);

      /* write to disk (if necessary) */
      if (dont_flush_state == false)
        maybe_store_state();
    }

    /* returns true if the cache is full */
    bool cache_is_full() const {
      return (m_cache.is_full());
    }

    /* if recovery is enabled then immediately write the modified blob */
    void maybe_store_state(bool force = false) {
      if (force || (m_env->get_flags() & HAM_ENABLE_RECOVERY)) {
        ham_u64_t new_blobid = store_state();
        if (new_blobid != m_env->get_header()->get_page_manager_blobid()) {
          m_env->get_header()->set_page_manager_blobid(new_blobid);
          m_env->get_header()->get_header_page()->set_dirty(true);
          /* store the page in the changeset if recovery is enabled */
          if (m_env->get_flags() & HAM_ENABLE_RECOVERY)
            m_env->get_changeset().add_page(m_env->get_header()->get_header_page());
        }
      }
    }

    // Encodes |n| to |p|; returns the number of required bytes
    int encode(ham_u8_t *p, ham_u64_t n);

    // Decodes a number of |n| bytes stored in |p| and returns
    // the decoded number
    ham_u64_t decode(size_t n, ham_u8_t *p);

    // callback for purging pages
    static void purge_callback(Page *page, PageManager *pm);

    // verifies the crc32 checksum of a page; throws an exception if the
    // verification fails
    void verify_crc32(Page *page);

    // The current Environment handle
    LocalEnvironment *m_env;

    // The cache
    Cache m_cache;

    // The map with free pages
    FreeMap m_free_pages;

    // Whether |m_free_pages| must be flushed or not
    bool m_needs_flush;

    // Page with the persisted state data. If multiple pages are allocated
    // then these pages form a linked list, with |m_state_page| being the head
    Page *m_state_page;

    // Cached page where to add more blobs
    Page *m_last_blob_page;

    // Page where to add more blobs - if |m_last_blob_page| was flushed
    ham_u64_t m_last_blob_page_id;

    // tracks number of fetched pages
    ham_u64_t m_page_count_fetched;

    // tracks number of flushed pages
    ham_u64_t m_page_count_flushed;

    // tracks number of index pages
    ham_u64_t m_page_count_index;

    // tracks number of blob pages
    ham_u64_t m_page_count_blob;

    // tracks number of page manager pages
    ham_u64_t m_page_count_page_manager;

    // tracks number of cache hits
    ham_u64_t m_cache_hits;

    // tracks number of cache misses
    ham_u64_t m_cache_misses;

    // number of successful freelist hits
    ham_u64_t m_freelist_hits;

    // number of freelist misses
    ham_u64_t m_freelist_misses;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H__ */
