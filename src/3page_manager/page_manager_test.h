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
 * A test gateway for the PageManager
 *
 * @exception_safe: no
 * @thread_safe: no
 */

#ifndef HAM_PAGE_MANAGER_TEST_H
#define HAM_PAGE_MANAGER_TEST_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3page_manager/page_manager_state.h"

#ifndef HAM_ROOT_H
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

#endif /* HAM_PAGE_MANAGER_TEST_H */
