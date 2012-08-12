/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "internal_fwd_decl.h"
#include "errorinducer.h"
#include "page.h"

namespace ham {

/**
 * The changeset class
 */
class Changeset 
{
  public:
    Changeset()
    : m_head(0), m_blobs(0), m_blobs_size(0), m_blobs_capacity(0),
      m_freelists(0), m_freelists_size(0), m_freelists_capacity(0), 
      m_indices(0), m_indices_size(0), m_indices_capacity(0), 
      m_others(0), m_others_size(0), m_others_capacity(0), m_inducer(0) {
    }

    ~Changeset() {
      if (m_inducer)
        delete m_inducer;
      if (m_blobs)
        ::free(m_blobs);
      if (m_freelists)
        ::free(m_freelists);
      if (m_indices)
        ::free(m_indices);
      if (m_others)
        ::free(m_others);
    }

    /** is the changeset empty? */
    bool is_empty() {
      return (m_head == 0);
    }

    /** append a new page to the changeset */
    void add_page(Page *page);

    /**
     * get a page from the changeset
     * returns NULL if the page is not part of the changeset
     */
    Page *get_page(ham_offset_t pageid);

    /** removes all pages from the changeset */
    void clear();

    /**
     * flush all pages in the changeset - first write them to the log, then 
     * write them to the disk
     *
     * on success: will clear the changeset and the log 
     */
    ham_status_t flush(ham_u64_t lsn);

    /** check if the page is already part of the changeset */
    bool contains(Page *page) {
      return (page->is_in_list(m_head, Page::LIST_CHANGESET));
    }

  private:
    /* write all pages in a bucket to the log file */
    ham_status_t log_bucket(Page **bucket, ham_size_t bucket_size,
                            ham_u64_t lsn, ham_size_t &page_count) ;

    /* the head of our linked list */
    Page *m_head;

    /* cached vectors for Changeset::flush(); using plain pointers
     * instead of std::vector because
     * - improved performance
     * - workaround for an MSVC 9 bug:
     *   http://social.msdn.microsoft.com/Forums/en-us/vcgeneral/thread/1bf2b062-150f-4f86-8081-d4d5dd0d1956
     */
    Page **m_blobs;
    ham_size_t m_blobs_size;
    ham_size_t m_blobs_capacity;

    Page **m_freelists;
    ham_size_t m_freelists_size;
    ham_size_t m_freelists_capacity;

    Page **m_indices;
    ham_size_t m_indices_size;
    ham_size_t m_indices_capacity;

    Page **m_others;
    ham_size_t m_others_size;
    ham_size_t m_others_capacity;

  public:
    /* an error inducer */
    ErrorInducer *m_inducer;
};

} // namespace ham

#endif /* HAM_CHANGESET_H__ */
