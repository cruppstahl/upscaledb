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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"

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
    : m_env(env), m_head(0), m_blobs(0), m_blobs_size(0), m_blobs_capacity(0),
      m_page_manager(0), m_page_manager_size(0), m_page_manager_capacity(0),
      m_indices(0), m_indices_size(0), m_indices_capacity(0),
      m_others(0), m_others_size(0), m_others_capacity(0) {
    }

    ~Changeset() {
      if (m_blobs)
        ::free(m_blobs);
      if (m_page_manager)
        ::free(m_page_manager);
      if (m_indices)
        ::free(m_indices);
      if (m_others)
        ::free(m_others);
    }

    /** is the changeset empty? */
    bool is_empty() const {
      return (m_head == 0);
    }

    /** append a new page to the changeset */
    void add_page(Page *page);

    /**
     * get a page from the changeset
     * returns NULL if the page is not part of the changeset
     */
    Page *get_page(ham_u64_t pageid);

    /** removes all pages from the changeset */
    void clear();

    /**
     * flush all pages in the changeset - first write them to the log, then
     * write them to the disk
     *
     * on success: will clear the changeset and the journal
     */
    void flush(ham_u64_t lsn = kDummyLsn);

    /** check if the page is already part of the changeset */
    bool contains(Page *page) {
      return (page->is_in_list(m_head, Page::kListChangeset));
    }

  private:
    /** The Environment which created this Changeset */
    LocalEnvironment *m_env;

    /** the head of our linked list */
    Page *m_head;

    /** cached vectors for Changeset::flush(); using plain pointers
     * instead of std::vector because
     * - improved performance
     * - workaround for an MSVC 9 bug:
     *   http://social.msdn.microsoft.com/Forums/en-us/vcgeneral/thread/1bf2b062-150f-4f86-8081-d4d5dd0d1956
     */
    Page **m_blobs;
    size_t m_blobs_size;
    size_t m_blobs_capacity;

    Page **m_page_manager;
    size_t m_page_manager_size;
    size_t m_page_manager_capacity;

    Page **m_indices;
    size_t m_indices_size;
    size_t m_indices_capacity;

    Page **m_others;
    size_t m_others_size;
    size_t m_others_capacity;
};

} // namespace hamsterdb

#endif /* HAM_CHANGESET_H */
