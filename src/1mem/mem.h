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
 * Memory handling
 */

#ifndef UPS_MEM_H
#define UPS_MEM_H

#include "0root/root.h"

#include <new>
#include <stdlib.h>
#ifdef UPS_USE_TCMALLOC
#  include <gperftools/tcmalloc.h>
#endif

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

struct ups_env_metrics_t;

namespace upscaledb {

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
struct Memory {
  // allocates |size| bytes, casted into type |T *|;
  // returns null if out of memory.
  // usage:
  //
  //     char *p = Memory::allocate<char>(1024);
  //
  template<typename T>
  static T *allocate(size_t size) {
    ms_total_allocations++;
    ms_current_allocations++;
#ifdef UPS_USE_TCMALLOC
    T *t = (T *)::tc_malloc(size);
#else
    T *t = (T *)::malloc(size);
#endif
    if (unlikely(!t))
      throw Exception(UPS_OUT_OF_MEMORY);
    return t;
  }

  // allocates |size| bytes; returns null if out of memory. initializes
  // the allocated memory with zeroes.
  // usage:
  //
  //     const char *p = Memory::callocate<const char>(50);
  //
  template<typename T>
  static T *callocate(size_t size) {
    ms_total_allocations++;
    ms_current_allocations++;

#ifdef UPS_USE_TCMALLOC
    T *t = (T *)::tc_calloc(1, size);
#else
    T *t = (T *)::calloc(1, size);
#endif
    if (unlikely(!t))
      throw Exception(UPS_OUT_OF_MEMORY);
    return t;
  }

  // re-allocates |ptr| for |size| bytes; returns null if out of memory.
  // |ptr| can be null on first use.
  // usage:
  //
  //     p = Memory::reallocate<char>(p, 100);
  //
  template<typename T>
  static T *reallocate(T *ptr, size_t size) {
    if (unlikely(ptr == 0)) {
      ms_total_allocations++;
      ms_current_allocations++;
    }
#ifdef UPS_USE_TCMALLOC
    T *t = (T *)::tc_realloc(ptr, size);
#else
    T *t = (T *)::realloc(ptr, size);
#endif
    if (unlikely(!t))
      throw Exception(UPS_OUT_OF_MEMORY);
    return t;
  }

  // releases a memory block; can deal with NULL pointers.
  static void release(void *ptr) {
    if (likely(ptr != 0)) {
      ms_current_allocations--;
#ifdef UPS_USE_TCMALLOC
      ::tc_free(ptr);
#else
      ::free(ptr);
#endif
    }
  }

  // updates and returns the collected metrics
  static void get_global_metrics(ups_env_metrics_t *metrics);

  // peak memory usage
  static uint64_t ms_peak_memory;

  // total memory allocations
  static uint64_t ms_total_allocations;

  // currently active allocations
  static uint64_t ms_current_allocations;
};

} // namespace upscaledb

#endif // UPS_MEM_H
