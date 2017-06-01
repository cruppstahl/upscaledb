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

#ifndef UPS_BENCH_METRICS_H
#define UPS_BENCH_METRICS_H

#include <ups/upscaledb_int.h>
#include <boost/cstdint.hpp> // MSVC 2008 does not have stdint.h

struct Metrics {
  const char *name;
  uint64_t insert_ops; 
  uint64_t erase_ops; 
  uint64_t find_ops; 
  uint64_t txn_commit_ops; 
  uint64_t other_ops; 
  uint64_t insert_bytes; 
  uint64_t find_bytes; 
  double elapsed_wallclock_seconds;
  double insert_latency_min;
  double insert_latency_max;
  double insert_latency_total;
  double erase_latency_min;
  double erase_latency_max;
  double erase_latency_total;
  double find_latency_min;
  double find_latency_max;
  double find_latency_total;
  double txn_commit_latency_min;
  double txn_commit_latency_max;
  double txn_commit_latency_total;
  ups_env_metrics_t upscaledb_metrics;
};

#endif /* UPS_BENCH_METRICS_H */
