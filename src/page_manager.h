/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include <string.h>
#include <vector>

#include "ham/hamsterdb_int.h"

#include "error.h"
#include "freelist.h"
#include "env.h"
#include "db_local.h"

namespace hamsterdb {

class Cache;

//
// The PageManager allocates, fetches and frees pages.
//
class PageManager {
  public:
    // Flags for PageManager::alloc_page()
    enum {
      // Do not use freelist when allocating the page
      kIgnoreFreelist =  8,

      // Clear the full page with zeroes
      kClearWithZero  = 16
    };

    // Default constructor
    //
    // @param env The Environment
    // @param cachesize The cache size, in bytes
    PageManager(Environment *env, ham_size_t cachesize);

    // Destructor
    ~PageManager();

    // Fills in the current metrics for the PageManager, the Cache and the
    // Freelist
    void get_metrics(ham_env_metrics_t *metrics) const;

    // Fetches a page from disk
    //
    // @param page Will point to the allocated page
    // @param db The Database which fetches this page
    // @param address The page's address
    // @param only_from_cache If false (and a cache miss) then the page
    //              is fetched from disk
    ham_status_t fetch_page(Page **page, LocalDatabase *db, ham_u64_t address,
                    bool only_from_cache = false);

    // Allocates a new page
    //
    // @param page Will point to the allocated page
    // @param db The Database which allocates this page
    // @param page_type One of Page::TYPE_* in page.h
    // @param flags Either kIgnoreFreelist or kClearWithZero
    ham_status_t alloc_page(Page **page, LocalDatabase *db, ham_u32_t page_type,
                    ham_u32_t flags);

    // Flushes a Page to disk
    ham_status_t flush_page(Page *page) {
      if (page->is_dirty()) {
        m_page_count_flushed++;
        return (page->flush());
      }
      return (0);
    }

    // Allocates space for a blob, either by using the freelist or by
    // allocating free disk space at the end of the file
    //
    // @param db The Database (can be NULL)
    // @param size The requested size (in bytes)
    // @param address Will return the address of the space
    // @param allocated Will return whether the space is newly allocated
    ham_status_t alloc_blob(Database *db, ham_size_t size, ham_u64_t *address,
                    bool *allocated);

    // Flush all pages, and clear the cache.
    //
    // Set |clear_cache| to true if you want the cache to be cleared
    ham_status_t flush_all_pages(bool clear_cache = false);

    // Purges the cache if the cache limits are exceeded
    ham_status_t purge_cache();

    // retrieves the freelist
    // this is public because it's required for testing */
    // TODO rename to test_
    Freelist *get_freelist(Database *db) {
      if (!m_freelist
          && !(m_env->get_flags() & HAM_IN_MEMORY)
          && !(m_env->get_flags() & HAM_READ_ONLY))
        m_freelist = new Freelist(m_env);
      return (m_freelist);
    }

    // flush all pages of a database (but not the header page,
    // it's still required and will be flushed below)
    void close_database(Database *db);

    // returns the Cache pointer; only for testing!
    Cache *test_get_cache() {
      return (m_cache);
    }

    // checks the integrity of the freelist and the cache
    ham_status_t check_integrity();

    // returns the cache's capacity
    ham_u64_t get_cache_capacity() const;

    // Adds a page to the freelist
    ham_status_t add_to_freelist(Page *page) {
      Freelist *f = get_freelist(page->get_db());
      if (f)
        return (f->free_page(page));
      return (0);
    }

    // Adds an area to the freelist; used for blobs, but make sure to add
    // sizeof(PBlobHeader) to the blob's payload size!
    ham_status_t add_to_freelist(Database *db, ham_u64_t address,
                    ham_size_t size) {
      Freelist *f = get_freelist(db);
      if (f)
        return (f->free_area(address, size));
      return (0);
    }

  private:
    // The current Environment handle
    Environment *m_env;

    // the Cache caches the database pages
    Cache *m_cache;

    // the Freelist manages the free space in the file; can be NULL
    Freelist *m_freelist;

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
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H__ */
