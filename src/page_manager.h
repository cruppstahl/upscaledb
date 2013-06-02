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

/**
 * @brief The PageManager allocates, fetches and frees pages.
 *
 */

#ifndef HAM_PAGE_MANAGER_H__
#define HAM_PAGE_MANAGER_H__

#include <string.h>
#include <vector>

#include "internal_fwd_decl.h"
#include "error.h"
#include "full_freelist.h"
#include "reduced_freelist.h"

namespace hamsterdb {

class Cache;

class PageManager {
  public:
    /** Flags for alloc_page */
    enum {
      /** Do not use freelist when allocating the page */
      IGNORE_FREELIST =  8,

      /** Clear the full page with zeroes */
      CLEAR_WITH_ZERO = 16
    };

    /**
     * Default constructor
     *
     * @param env The Environment
     * @param cachesize The cache size, in bytes
     */
    PageManager(Environment *env, ham_size_t cachesize);

    /** Destructor */
    ~PageManager();

    /**
     * Fetches a page from disk
     *
     * @param page Will point to the allocated page
     * @param db The Database which fetches this page
     * @param address The page's address
     * @param only_from_cache If false (and a cache miss) then the page
     *              is fetched from disk
     */
    ham_status_t fetch_page(Page **page, Database *db, ham_u64_t address,
                    bool only_from_cache = false);

    /**
     * Allocates a new page
     *
     * @param page Will point to the allocated page
     * @param db The Database which allocates this page
     * @param page_type One of Page::TYPE_* in page.h
     * @param flags Either IGNORE_FREELIST or CLEAR_WITH_ZERO
     */
    ham_status_t alloc_page(Page **page, Database *db, ham_u32_t page_type,
                    ham_u32_t flags);

    /**
     * Allocates space for a blob, either by using the freelist or by
     * allocating free disk space at the end of the file
     *
     * @param db The Database (can be NULL)
     * @param size The requested size (in bytes)
     * @param address Will return the address of the space
     * @param allocated Will return whether the space is newly allocated
     */
    ham_status_t alloc_blob(Database *db, ham_size_t size, ham_u64_t *address,
                    bool *allocated);

    /**
     * Flush all pages, and clear the cache.
     *
     * @param clear_cache Set to true if you do NOT want the cache to be cleared
     */
    ham_status_t flush_all_pages(bool clear_cache = false);

    /**
     * Purges the cache if the cache limits are exceeded
     */
    ham_status_t purge_cache();

    /**
     * Frees a page and moves it to the freelist
     *
     * @param page The page to free
     * TODO same as add_to_freelist()?
     * TODO what else should be done here?? also call Page::free?
     * TODO return void?
     */
    ham_status_t free_page(Page *page) {
      if (m_freelist)
        m_freelist->free_page(page);
      return (0);
    }

    /** Returns the cache pointer */
    Cache *get_cache() {
      return (m_cache);
    }

    /** Returns the Freelist (required for testing) */
    FullFreelist *get_freelist() {
      return (m_freelist);
    }

    /** 
     * flush all pages of a database (but not the header page,
     * it's still required and will be flushed below)
     */
    void close_database(Database *db);

    /** Adds a page to the freelist */
    void add_to_freelist(Page *page) {
      if (m_freelist)
        m_freelist->free_page(page);
    }

    /** Adds an area to the freelist; used for blobs, but make sure to add
     * sizeof(PBlobHeader) to the blob's payload size! */
    void add_to_freelist(ham_u64_t address, ham_size_t size) {
      if (m_freelist)
        m_freelist->free_area(address, size);
    }

  private:
    /** traverses a blob page, adds all free blobs to the freelist */
    void add_free_blobs_to_freelist(Page *page);

    Environment *m_env;

    /** the cache caches the database pages */
    Cache *m_cache;

    /** the Freelist manages the free space in the file */
    FullFreelist *m_freelist;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_MANAGER_H__ */
