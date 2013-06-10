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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <unistd.h> // sysconf
#ifdef HAVE_GOOGLE_TCMALLOC_H
#  include <google/malloc_extension.h>
#endif

#include "mem.h"

namespace hamsterdb {

Memory::Metrics Memory::ms_metrics;


void
Memory::get_global_metrics(Metrics *metrics)
{
#ifdef HAVE_GOOGLE_TCMALLOC_H
  size_t value = 0;
  MallocExtension::instance()->GetNumericProperty(
                  "generic.current_allocated_bytes", &value);
  ms_metrics.current_memory = value;
  if (ms_metrics.peak_memory < value)
    ms_metrics.peak_memory = value;
  MallocExtension::instance()->GetNumericProperty(
                  "generic.heap_size", &value);
  ms_metrics.heap_size = value;
#endif

  *metrics = ms_metrics;
}

size_t 
Memory::get_vm_pagesize()
{
  return ((size_t)::sysconf(_SC_PAGE_SIZE));
}

void 
Memory::release_to_system()
{
#ifdef HAVE_GOOGLE_TCMALLOC_H
  MallocExtension::instance()->ReleaseFreeMemory();
#else
  ::malloc_trim(get_vm_pagesize());
#endif
}

} // namespace hamsterdb
