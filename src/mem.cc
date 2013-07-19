/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"
#include "ham/hamsterdb_int.h"

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#ifdef HAVE_GOOGLE_TCMALLOC_H
#  include <google/malloc_extension.h>
#endif

#include "mem.h"
#include "os.h"

namespace hamsterdb {

ham_u64_t Memory::ms_peak_memory;
ham_u64_t Memory::ms_total_allocations;
ham_u64_t Memory::ms_current_allocations;


void
Memory::get_global_metrics(ham_env_metrics_t *metrics)
{
#ifdef HAVE_GOOGLE_TCMALLOC_H
  size_t value = 0;
  MallocExtension::instance()->GetNumericProperty(
                  "generic.current_allocated_bytes", &value);
  metrics->mem_current_usage = value;
  if (ms_peak_memory < value)
    ms_peak_memory = metrics->mem_peak_usage = value;
  MallocExtension::instance()->GetNumericProperty(
                  "generic.heap_size", &value);
  metrics->mem_heap_size = value;
#endif

  metrics->mem_total_allocations = ms_total_allocations;
  metrics->mem_current_allocations = ms_current_allocations;
}

void 
Memory::release_to_system()
{
#ifdef HAVE_GOOGLE_TCMALLOC_H
  MallocExtension::instance()->ReleaseFreeMemory();
#  elif WIN32
  // TODO
#  elif __APPLE__
  // TODO
#  elif __GNUC__
  ::malloc_trim(os_get_granularity());
#endif
}

} // namespace hamsterdb
