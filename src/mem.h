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
 * @brief memory management routines
 *
 */

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#include <stdlib.h>
#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#endif
#ifdef HAVE_GOOGLE_TCMALLOC_H
#  include <google/tcmalloc.h>
#endif

namespace hamsterdb {

/**
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
  enum {
    // Version tag for the pickled metrics
    kMetricsVersion = 1
  };

  public:
    // Memory usage metrics
    struct Metrics {
      // number of total allocations for the whole lifetime of the process
      ham_u64_t total_allocations;

      // currently active allocations
      ham_u64_t current_allocations;

      // current amount of memory allocated and tracked by the process
      // (excludes memory used by the kernel or not allocated with
      // malloc/free)
      ham_u64_t current_memory;

      // peak usage of memory
      ham_u64_t peak_memory;

      // the heap size of this process
      ham_u64_t heap_size;
    };

    // allocates a byte array of |size| elements, casted into type |T *|;
    // returns null if out of memory.
    // usage:
    //
    //     char *p = Memory::allocate<char>(1024);
    //
    template<typename T>
    static T *allocate(size_t size) {
      ms_metrics.total_allocations++;
      ms_metrics.current_allocations++;

#ifdef HAVE_GOOGLE_TCMALLOC_H
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
      ms_metrics.total_allocations++;
      ms_metrics.current_allocations++;

#ifdef HAVE_GOOGLE_TCMALLOC_H
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
#ifdef HAVE_GOOGLE_TCMALLOC_H
      return ((T *)::tc_realloc(ptr, size));
#else
      return ((T *)::realloc(ptr, size));
#endif
    }

    // releases a memory block; can deal with NULL pointers.
    static void release(void *ptr) {
      if (ptr) {
        ms_metrics.current_allocations--;
#ifdef HAVE_GOOGLE_TCMALLOC_H
        ::tc_free(ptr);
#else
        ::free(ptr);
#endif
      }
    }

    // updates and returns the collected metrics
    static void get_global_metrics(Metrics *metrics);

    // returns the virtual memory pagesize
    static size_t get_vm_pagesize();

    // releases unused memory back to the operating system
    static void release_to_system();

  private:
    // the collected metrics
    static Metrics ms_metrics;
};

} // namespace hamsterdb

#endif /* HAM_MEM_H__ */
