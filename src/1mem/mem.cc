/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "0root/root.h"

#ifdef HAM_USE_TCMALLOC
#  include <google/tcmalloc.h>
#  include <google/malloc_extension.h>
#endif
#include <stdlib.h>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/file.h"
#include "1mem/mem.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

uint64_t Memory::ms_peak_memory;
uint64_t Memory::ms_total_allocations;
uint64_t Memory::ms_current_allocations;

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

} // namespace hamsterdb
