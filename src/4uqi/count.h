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

#include "0root/root.h"

#include "1base/error.h"
#include "2config/db_config.h"
#include "4uqi/scanvisitor.h"
#include "4uqi/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct CountScanVisitor : public ScanVisitor
{
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  CountScanVisitor()
    : count(0) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size,
                  const void *record_data, uint32_t record_size) {
    count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, const void *record_array,
                  size_t length) {
    count += length;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, UPS_TYPE_UINT64);
    uqi_result_add_row(result, "COUNT", 6, &count, sizeof(count));
  }

  // The counter
  uint64_t count;
};

struct CountScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    assert(stmt->function.name == "count");
    assert(stmt->predicate.name == "");
    return new CountScanVisitor();
  }
};


template<typename PodType>
struct CountIfScanVisitor : public ScanVisitor
{
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  CountIfScanVisitor(const DbConfig *dbconf, SelectStatement *stmt)
    : count(0), plugin(dbconf, stmt) {
    key_size = dbconf->key_size;
    record_size = dbconf->record_size;
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (plugin.pred(key_data, key_size, record_data, record_size))
      count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    PodType *key_array = (PodType *)key_data;
    PodType *record_array = (PodType *)record_data;
    PodType *stream;

    if (ISSET(statement->function.flags, UQI_STREAM_KEY))
      stream = key_array;
    else
      stream = record_array;

    for (size_t i = 0; i < length; i++, stream++) {
      if (plugin.pred(&key_array[i], key_size, &record_array[i], record_size))
        count++;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, UPS_TYPE_UINT64);
    uqi_result_add_row(result, "COUNT", 6, &count, sizeof(count));
  }

  // The counter
  uint64_t count;

  // The predicate plugin
  PredicatePluginWrapper plugin;

  // The key size
  uint32_t key_size;

  // The record size
  uint32_t record_size;
};

struct CountIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    assert(stmt->function.name == "count");
    assert(stmt->predicate.name != "");

    // COUNT with predicate
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return new CountIfScanVisitor<uint8_t>(cfg, stmt);
      case UPS_TYPE_UINT16:
        return new CountIfScanVisitor<uint16_t>(cfg, stmt);
      case UPS_TYPE_UINT32:
        return new CountIfScanVisitor<uint32_t>(cfg, stmt);
      case UPS_TYPE_UINT64:
        return new CountIfScanVisitor<uint64_t>(cfg, stmt);
      case UPS_TYPE_REAL32:
        return new CountIfScanVisitor<float>(cfg, stmt);
      case UPS_TYPE_REAL64:
        return new CountIfScanVisitor<double>(cfg, stmt);
      default:
        return new CountIfScanVisitor<uint8_t>(cfg, stmt);
    }
  }
};

} // namespace upscaledb

