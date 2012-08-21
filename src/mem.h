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
 * @brief memory management routines
 *
 */

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#include <string.h>

namespace ham {

#if defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC)
#  undef alloc
#  undef free
#  undef realloc
#  undef calloc
#endif


/**
 * a memory allocator
 */
class Allocator
{
  public:
    /** a constructor */
    Allocator() {
    }

    /** a virtual destructor */
    virtual ~Allocator() {
    }

    /** allocate a chunk of memory */
    virtual void *alloc(ham_size_t size) = 0;

    /** release a chunk of memory */
    virtual void free(const void *ptr) = 0;

    /** re-allocate a chunk of memory */
    virtual void *realloc(const void *ptr, ham_size_t size) = 0;

    /** a calloc function */
    void *calloc(ham_size_t size) {
      void *p = alloc(size);
      if (p)
        memset(p, 0, size);
      return (p);
    }

    /**
     * a factory for creating the standard allocator (based on libc malloc
     * and free)
     */
    static Allocator *create();
};

} // namespace ham

#endif /* HAM_MEM_H__ */
