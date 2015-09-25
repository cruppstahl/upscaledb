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
 * A changeset collects all pages that are modified during a single
 * operation.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_CHANGESET_H
#define UPS_CHANGESET_H

#include "0root/root.h"

#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "2page/page_collection.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LocalEnvironment;

class Changeset
{
    struct UnlockPage
    {
      bool operator()(Page *page) {
    #ifdef UPS_ENABLE_HELGRIND
        page->mutex().try_lock();
    #endif
        page->mutex().unlock();
        return (true);
      }
    };

  public:
    Changeset(LocalEnvironment *env)
      : m_env(env), m_collection(Page::kListChangeset) {
    }

    /*
     * Returns a page from the changeset, or NULL if the page is not part
     * of the changeset
     */
    Page *get(uint64_t address) {
      return (m_collection.get(address));
    }

    /* Append a new page to the changeset. The page is locked. */
    void put(Page *page) {
      if (!has(page)) {
        page->mutex().lock();
      }
      m_collection.put(page);
    }

    /* Removes a page from the changeset. The page is unlocked. */
    void del(Page *page) {
      page->mutex().unlock();
      m_collection.del(page);
    }

    /* Check if the page is already part of the changeset */
    bool has(Page *page) const {
      return (m_collection.has(page));
    }

    /* Returns true if the changeset is empty */
    bool is_empty() const {
      return (m_collection.is_empty());
    }

    /* Removes all pages from the changeset. The pages are unlocked. */
    void clear() {
      UnlockPage unlocker;
      m_collection.for_each(unlocker);
      m_collection.clear();
    }

    /*
     * Flush all pages in the changeset - first write them to the log, then
     * write them to the disk.
     * On success: will clear the changeset and the journal
     */
    void flush(uint64_t lsn);

  private:
    /* The Environment */
    LocalEnvironment *m_env;

    /* The pages which were added to this Changeset */
    PageCollection m_collection;
};

} // namespace hamsterdb

#endif /* UPS_CHANGESET_H */
