/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
 * A changeset collects all pages that are modified during a single
 * operation.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_CHANGESET_H
#define HAM_CHANGESET_H

#include "0root/root.h"

#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "2page/page_collection.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LocalEnvironment;

/**
 * The changeset class
 */
class Changeset
{
    enum {
      kDummyLsn = 1
    };

  public:
    Changeset(LocalEnvironment *env)
      : m_env(env), m_collection(Page::kListChangeset) {
    }

    /* Returns true if the changeset is empty */
    bool is_empty() const {
      return (m_collection.is_empty());
    }

    /* Append a new page to the changeset */
    void add_page(Page *page);

    /*
     * Returns a page from the changeset.
     * Returns NULL if the page is not part of the changeset
     */
    Page *get_page(uint64_t pageid);

    /* Removes all pages from the changeset */
    void clear();

    /*
     * Flush all pages in the changeset - first write them to the log, then
     * write them to the disk.
     * On success: will clear the changeset and the journal
     */
    void flush(uint64_t lsn = kDummyLsn);

    /* Check if the page is already part of the changeset */
    bool contains(Page *page) const {
      return (m_collection.contains(page->get_address()));
    }

  private:
    friend struct ChangesetFixture;

    /* The Environment which created this Changeset */
    LocalEnvironment *m_env;

    /* The pages which were added to this Changeset */
    PageCollection m_collection;
};

} // namespace hamsterdb

#endif /* HAM_CHANGESET_H */
