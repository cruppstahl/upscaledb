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

#ifndef UPS_BENCH_METRICS_H
#define UPS_BENCH_METRICS_H

#include <ups/upscaledb_int.h>
#include <boost/cstdint.hpp> // MSVC 2008 does not have stdint.h
using namespace boost;

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
  ups_env_metrics_t hamster_metrics;
};

#endif /* UPS_BENCH_METRICS_H */
