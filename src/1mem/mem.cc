/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

#ifdef UPS_USE_TCMALLOC
#  include <google/tcmalloc.h>
#  include <google/malloc_extension.h>
#endif
#include <stdlib.h>

#include "ups/upscaledb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/file.h"
#include "1mem/mem.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

uint64_t Memory::ms_peak_memory;
uint64_t Memory::ms_total_allocations;
uint64_t Memory::ms_current_allocations;

void
Memory::get_global_metrics(ups_env_metrics_t *metrics)
{
#ifdef UPS_USE_TCMALLOC
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

} // namespace upscaledb
