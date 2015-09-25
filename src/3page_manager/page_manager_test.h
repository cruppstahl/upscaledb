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
 * A test gateway for the PageManager
 *
 * @exception_safe: no
 * @thread_safe: no
 */

#ifndef UPS_PAGE_MANAGER_TEST_H
#define UPS_PAGE_MANAGER_TEST_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3page_manager/page_manager_state.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Page;
class PageManager;

class PageManagerTest
{
  public:
    // Constructor
    PageManagerTest(PageManager *page_manager);

    // Stores the local PageManager state to disk; returns the blob id
    uint64_t store_state();

    // Removes a page from the list; only for testing.
    void remove_page(Page *page);

    // Returns true if a page is free. Ignores multi-pages; only for
    // testing and integrity checks
    bool is_page_free(uint64_t pageid);

    // Fetches a page from the cache
    Page *fetch_page(uint64_t id);

    // Stores a page in the cache
    void store_page(Page *page);

    // Returns true if the cache is full
    bool is_cache_full();

    // Returns the state
    PageManagerState *state();

  private:
    // Reference of the PageManager instance
    PageManager *m_sut;
};

} // namespace hamsterdb

#endif /* UPS_PAGE_MANAGER_TEST_H */
