/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief freelist structures, functions and macros
 *
 */

#ifndef HAM_FREELIST_H__
#define HAM_FREELIST_H__

namespace hamsterdb {

class Environment;
class Page;

/**
 * The freelist class structure
 */
class Freelist
{
  public:
    /** constructor */
    Freelist(Environment *env)
      : m_env(env) {
    }

    virtual ~Freelist() {
    }

    /** adds an unused page to the freelist */
    virtual ham_status_t free_page(Page *page) = 0;

    /** adds an unused area to the freelist */
    virtual ham_status_t free_area(ham_u64_t address, ham_size_t size) = 0;

    /** tries to allocate a page from the freelist */
    virtual ham_status_t alloc_page(ham_u64_t *paddress) = 0;

    /** tries to allocate a blob area from the freelist */
    virtual ham_status_t alloc_area(ham_size_t size, ham_u64_t *paddress) = 0;

    /** verifies integrity of the freelist */
    virtual ham_status_t check_integrity() {
      return (0);
    }

    /** returns the alignment for blobs */
    virtual int get_blob_alignment() const = 0;

  protected:
    /** pointer to the Environment */
    Environment *m_env;
};

} // namespace hamsterdb

#endif /* HAM_FREELIST_H__ */
