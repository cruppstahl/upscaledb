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

#include "internal_fwd_decl.h"
#include "errorinducer.h"

#include <vector>


/**
 * The changeset class
 */
class Changeset 
{
  public:
    Changeset()
    : m_head(0), m_inducer(0) {
    }

    ~Changeset() {
        if (m_inducer)
            delete m_inducer;
    }

    /** is the changeset empty? */
    bool is_empty() {
        return (m_head==0);
    }

    /** append a new page to the changeset */
    void add_page(ham_page_t *page);

    /**
     * get a page from the changeset
     * returns NULL if the page is not part of the changeset
     */
    ham_page_t *get_page(ham_offset_t pageid);

    /** removes all pages from the changeset */
    void clear();

    /**
     * flush all pages in the changeset - first write them to the log, then 
     * write them to the disk
     *
     * if header_is_index is true everything will be logged if the header page
     * is part of the changeset.
     *
     * on success: will clear the changeset and the log 
     */
    ham_status_t flush(ham_u64_t lsn, bool header_is_index=false);

    /** retrieve the head of the linked list */
    ham_page_t *get_head() {
        return (m_head);
    }

  private:
    typedef std::vector<ham_page_t *> bucket;

    /* write all pages in a bucket to the log file */
    ham_status_t log_bucket(bucket &b, ham_u64_t lsn, ham_size_t &page_count) ;

    /* the head of our linked list */
    ham_page_t *m_head;

    /* cached vectors for Changeset::flush() */
    std::vector<ham_page_t *> m_blobs;
    std::vector<ham_page_t *> m_freelists;
    std::vector<ham_page_t *> m_indices;
    std::vector<ham_page_t *> m_others;

  public:
    /* an error inducer */
    ErrorInducer *m_inducer;
};

#endif /* HAM_CHANGESET_H__ */
