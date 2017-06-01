/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#include "2config/env_config.h"
#include "2page/page.h"
#include "2page/page_collection.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct LocalEnv;

struct Changeset {
  Changeset(LocalEnv *env_)
  : env(env_) {
  }

  /*
   * Returns a page from the changeset, or NULL if the page is not part
   * of the changeset
   */
  Page *get(uint64_t address) {
    return collection.get(address);
  }

  /* Append a new page to the changeset. The page is locked. */
  void put(Page *page) {
    if (!has(page))
      page->mutex().lock();
    collection.put(page);
  }

  /* Removes a page from the changeset. The page is unlocked. */
  void del(Page *page) {
    page->mutex().unlock();
    collection.del(page);
  }

  /* Check if the page is already part of the changeset */
  bool has(Page *page) const {
    return collection.has(page);
  }

  /* Returns true if the changeset is empty */
  bool is_empty() const {
    return collection.is_empty();
  }

  /* Removes all pages from the changeset. The pages are unlocked. */
  void clear();

  /*
   * Flush all pages in the changeset - first write them to the log, then
   * write them to the disk.
   * On success: will clear the changeset and the journal
   */
  void flush(uint64_t lsn);

  /* The Environment */
  LocalEnv *env;

  /* The pages which were added to this Changeset */
  PageCollection<Page::kListChangeset> collection;
};

} // namespace upscaledb

#endif /* UPS_CHANGESET_H */
