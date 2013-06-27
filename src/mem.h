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

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#include "config.h"

#include <stdlib.h>
#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#endif
#ifdef HAM_USE_TCMALLOC
#  include <google/tcmalloc.h>
#endif

#include "ham/hamsterdb.h"

struct ham_env_metrics_t;

namespace hamsterdb {

/*
 * The static Memory class provides memory management functions in a common
 * c++ namespace. The functions can allocate, reallocate and free memory
 * while tracking usage statistics.
 *
 * If tcmalloc is used then additional metrics will be available.
 *
 * This class only has static members and methods. It does not have a
 * constructor.
 */
class Memory {
  public:
    // allocates a byte array of |size| elements, casted into type |T *|;
    // returns null if out of memory.
    // usage:
    //
    //     char *p = Memory::allocate<char>(1024);
    //
    template<typename T>
    static T *allocate(size_t size) {
      ms_total_allocations++;
      ms_current_allocations++;

#ifdef HAM_USE_TCMALLOC
      return ((T *)::tc_malloc(size));
#else
      return ((T *)::malloc(size));
#endif
    }

    // allocation function; returns null if out of memory. initializes
    // the allocated memory with zeroes.
    // usage:
    //
    //     const char *p = Memory::callocate<const char>(50);
    //
    template<typename T>
    static T *callocate(size_t size) {
      ms_total_allocations++;
      ms_current_allocations++;

#ifdef HAM_USE_TCMALLOC
      return ((T *)::tc_calloc(1, size));
#else
      return ((T *)::calloc(1, size));
#endif
    }

    // re-allocation function; returns null if out of memory.
    // |ptr| can be null on first use.
    // usage:
    //
    //     p = Memory::reallocate_bytes<char>(p, 100);
    //
    template<typename T>
    static T *reallocate(T *ptr, size_t size) {
      if (ptr == 0) {
        ms_total_allocations++;
        ms_current_allocations++;
      }
#ifdef HAM_USE_TCMALLOC
      return ((T *)::tc_realloc(ptr, size));
#else
      return ((T *)::realloc(ptr, size));
#endif
    }

    // releases a memory block; can deal with NULL pointers.
    static void release(void *ptr) {
      if (ptr) {
        ms_current_allocations--;
#ifdef HAM_USE_TCMALLOC
        ::tc_free(ptr);
#else
        ::free(ptr);
#endif
      }
    }

    // updates and returns the collected metrics
    static void get_global_metrics(ham_env_metrics_t *metrics);

    // releases unused memory back to the operating system
    static void release_to_system();

  private:
    // peak memory usage
    static ham_u64_t ms_peak_memory;

    // total memory allocations
    static ham_u64_t ms_total_allocations;

    // currently active allocations
    static ham_u64_t ms_current_allocations;
};

} // namespace hamsterdb

#endif /* HAM_MEM_H__ */
