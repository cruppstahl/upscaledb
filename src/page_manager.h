/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_PAGE_MANAGER_H__
#define HAM_PAGE_MANAGER_H__

#include <map>

#include "ham/hamsterdb_int.h"

#include "error.h"
#include "freelist.h"
#include "env_local.h"
#include "db_local.h"

namespace hamsterdb {

//
// The PageManager allocates, fetches and frees pages. It manages the
// list of all pages (free and not free), and maps their virtual ID to
// their physical address in the file.
//
class PageManager {
  private:
    // The state of a page
    struct PageState {
      PageState(ham_u64_t _birthday = 0, Page *_page = 0)
        : birthday(_birthday), page(_page), is_free(false) {
      }

      ham_u64_t birthday;
      Page *page;
      bool is_free;
    };

    typedef std::map<ham_u64_t, PageState> PageMap;

  public:
    // Flags for PageManager::alloc_page()
    enum {
      // Do not use freelist when allocating the page
      kIgnoreFreelist =  8,

      // Clear the full page with zeroes
      kClearWithZero  = 16,

      // Limits the amount of pages that are flushed in purge_cache()
      kPurgeLimit = 20,

      // Only pages above this age are purged
      kPurgeThreshold = 100
    };

    // Default constructor
    //
    // The cache size is specified in bytes!
    PageManager(LocalEnvironment *env, ham_u32_t cache_size);

    // Destructor
    ~PageManager();

    // Fills in the current metrics for the PageManager, the Cache and the
    // Freelist
    void get_metrics(ham_env_metrics_t *metrics) const;

    // Fetches a page from disk
    //
    // @param db The Database which fetches this page
    // @param address The page's address
    // @param only_from_cache If false (and a cache miss) then the page
    //              is fetched from disk
    Page *fetch_page(LocalDatabase *db, ham_u64_t address,
                    bool only_from_cache = false);

    // Allocates a new page
    //
    // @param db The Database which allocates this page
    // @param page_type One of Page::TYPE_* in page.h
    // @param flags Either kIgnoreFreelist or kClearWithZero
    Page *alloc_page(LocalDatabase *db, ham_u32_t page_type, ham_u32_t flags);

    // Flushes a Page to disk
    void flush_page(Page *page) {
      if (page->is_dirty()) {
        m_page_count_flushed++;
        page->flush();
      }
    }

    // Allocates space for a blob, either by using the freelist or by
    // allocating free disk space at the end of the file
    //
    // @param db The Database (can be NULL)
    // @param size The requested size (in bytes)
    // @param allocated Will return whether the space is newly allocated
    // Returns the address of the space
    ham_u64_t alloc_blob(Database *db, ham_u32_t size, bool *allocated = 0);

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

    // Checks the integrity of the freelist and the cache
    void check_integrity();

    // Returns the cache's capacity
    ham_u64_t get_cache_capacity() const {
      return (m_cache_size);
    }

    // Adds a page to the freelist
    void add_to_freelist(Page *page);

    // Adds an area to the freelist; used for blobs, but make sure to add
    // sizeof(PBlobHeader) to the blob's payload size!
    void add_to_freelist(Database *db, ham_u64_t address, ham_u32_t size) {
      Freelist *f = get_freelist();
      if (f)
        f->free_area(address, size);
    }


    // Closes the PageManager; flushes all dirty pages
    void close();

    // Removes a page from the list; only for testing.
    void test_remove_page(Page *page) {
      PageMap::iterator it = m_page_map.find(page->get_address());
      ham_assert(it != m_page_map.end());
      m_page_map.erase(it);
    }

  private:
    friend struct BlobManagerFixture;
    friend struct CacheFixture;
    friend struct FreelistFixture;
    friend struct PageManagerFixture;

    // Returns the freelist; only for testing!
    Freelist *test_get_freelist() {
      return (get_freelist());
    }

    // Returns the (initialized) freelist pointer
    Freelist *get_freelist() {
      if (!m_freelist
          && !(m_env->get_flags() & HAM_IN_MEMORY)
          && !(m_env->get_flags() & HAM_READ_ONLY))
        m_freelist = new Freelist(m_env);
      return (m_freelist);
    }

    // Fetches a page from the list
    Page *fetch_page(ham_u64_t id) {
      m_epoch++;

      PageMap::iterator it = m_page_map.find(id);
      if (it != m_page_map.end())
        return (it->second.page);
      return (0);
    }

    // Stores a page in the list
    void store_page(Page *page) {
      ham_assert(m_page_map.find(page->get_address()) == m_page_map.end());
      PageState ps = PageState(m_epoch, page);
      m_page_map[page->get_address()] = ps;
    }

    // The current Environment handle
    LocalEnvironment *m_env;

    // the Freelist manages the free space in the file; can be NULL
    Freelist *m_freelist;

    // The list of pages currently in use
    PageMap m_page_map;

    // The cache size (in bytes)
    ham_u32_t m_cache_size;

    // the current epoch (a monotonic counter to calculate the "age" of
    // a cached page)
    ham_u64_t m_epoch;

    // tracks number of fetched pages
    ham_u64_t m_page_count_fetched;

    // tracks number of flushed pages
    ham_u64_t m_page_count_flushed;

    // tracks number of index pages
    ham_u64_t m_page_count_index;

    // tracks number of blob pages
    ham_u64_t m_page_count_blob;

    // tracks number of freelist pages
    ham_u64_t m_page_count_freelist;

    // tracks number of cache hits
    ham_u64_t m_cache_hits;

    // tracks number of cache misses
    ham_u64_t m_cache_misses;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H__ */
