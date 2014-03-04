/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief A changeset collects all pages that are modified during a single
 * operation.
 *
 */

#ifndef HAM_CHANGESET_H__
#define HAM_CHANGESET_H__

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "page.h"
#include "errorinducer.h"

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
      m_others(0), m_others_size(0), m_others_capacity(0), m_inducer(0) {
    }

    ~Changeset() {
      delete m_inducer;
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
    ham_u32_t m_blobs_size;
    ham_u32_t m_blobs_capacity;

    Page **m_page_manager;
    ham_u32_t m_page_manager_size;
    ham_u32_t m_page_manager_capacity;

    Page **m_indices;
    ham_u32_t m_indices_size;
    ham_u32_t m_indices_capacity;

    Page **m_others;
    ham_u32_t m_others_size;
    ham_u32_t m_others_capacity;

  public:
    /** an error inducer - required for testing */
    ErrorInducer *m_inducer;
};

} // namespace hamsterdb

#endif /* HAM_CHANGESET_H__ */
