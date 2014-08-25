/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "0root/root.h"

#ifdef HAM_USE_TCMALLOC
#  include <google/tcmalloc.h>
#  include <google/malloc_extension.h>
#elif HAVE_MALLOC_H
#  include <malloc.h>
#endif

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "1mem/mem.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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
  ::malloc_trim(File::get_granularity());
#endif
}

} // namespace hamsterdb
