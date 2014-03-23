/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef METRICS_H__
#define METRICS_H__

#include <ham/hamsterdb_int.h>
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
  ham_env_metrics_t hamster_metrics;
};

#endif /* METRICS_H__ */
