/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#include "config.h"

#ifdef HAM_USE_TCMALLOC
#  include <google/tcmalloc.h>
#  include <google/malloc_extension.h>
#elif __GNUC__
#  include <malloc.h>
#endif

#include <ham/hamsterdb_int.h>

#include "mem.h"
#include "os.h"

namespace hamsterdb {

ham_u64_t Memory::ms_peak_memory;
ham_u64_t Memory::ms_total_allocations;
ham_u64_t Memory::ms_current_allocations;

void
Memory::get_global_metrics(ham_env_metrics_t *metrics)
{
#ifdef HAM_USE_TCMALLOC
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
#ifdef HAM_USE_TCMALLOC
  MallocExtension::instance()->ReleaseFreeMemory();
#elif WIN32
  // TODO
#elif __APPLE__
  // TODO
#elif __GNUC__
  ::malloc_trim(os_get_granularity());
#endif
}

} // namespace hamsterdb
