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
