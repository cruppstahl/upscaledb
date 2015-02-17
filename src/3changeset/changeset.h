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
#include "3changeset/changeset_state.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Changeset
{
  public:
    Changeset(const ChangesetState &state)
      : m_state(state) {
    }

    /*
     * Returns a page from the changeset, or NULL if the page is not part
     * of the changeset
     */
    Page *get(uint64_t address);

    /* Append a new page to the changeset. The page is locked. */
    void put(Page *page);

    /* Removes a page from the changeset. The page is unlocked. */
    void del(Page *page);

    /* Check if the page is already part of the changeset */
    bool has(Page *page) const;

    /* Returns true if the changeset is empty */
    bool is_empty() const;

    /* Removes all pages from the changeset. The pages are unlocked. */
    void clear();

    /*
     * Flush all pages in the changeset - first write them to the log, then
     * write them to the disk.
     * On success: will clear the changeset and the journal
     */
    void flush(uint64_t lsn);

  private:
    // The mutable state
    ChangesetState m_state;
};

} // namespace hamsterdb

#endif /* HAM_CHANGESET_H */
