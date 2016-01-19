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

#include "1base/error.h"
#include "2config/db_config.h"
#include "3btree/btree_visitor.h"
#include "4uqi/plugins.h"
#include "4uqi/result.h"
#include "4uqi/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename PodType, typename AggType>
struct AverageScanVisitor : public ScanVisitor {
  AverageScanVisitor(SelectStatement *stmt, int result_type_)
    : ScanVisitor(stmt), sum(0), count(0), result_type(result_type_) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) {
    PodType *t;
    if (isset(statement->function.flags, UQI_STREAM_KEY))
      t = (PodType *)key_data;
    else
      t = (PodType *)record_data;

    sum += *t * duplicate_count;
    count += duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    PodType *data;
    if (isset(statement->function.flags, UQI_STREAM_KEY))
      data = (PodType *)key_data;
    else
      data = (PodType *)record_data;

    for (size_t i = 0; i < length; i++, data++)
      sum += *data;
    count += length;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    sum /= (AggType)count;

    uqi_result_add_row(result, UPS_TYPE_BINARY, "AVERAGE", 8,
                    result_type, &sum, sizeof(sum));
  }

  // The aggregated sum
  AggType sum;

  // The element counter
  uint64_t count;

  // The type of the result
  int result_type;
};

struct AverageScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.name == "average");
    ups_assert(stmt->predicate.name == "");

    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new AverageScanVisitor<uint8_t, uint64_t>(stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT16:
        return (new AverageScanVisitor<uint16_t, uint64_t>(stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT32:
        return (new AverageScanVisitor<uint32_t, uint64_t>(stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT64:
        return (new AverageScanVisitor<uint64_t, uint64_t>(stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_REAL32:
        return (new AverageScanVisitor<float, double>(stmt, UPS_TYPE_REAL64));
      case UPS_TYPE_REAL64:
        return (new AverageScanVisitor<double, double>(stmt, UPS_TYPE_REAL64));
      default:
        return (0);
    }
  }
};


template<typename PodType, typename AggType>
struct AverageIfScanVisitor : public ScanVisitor {
  AverageIfScanVisitor(const DatabaseConfiguration *dbconf,
                    SelectStatement *stmt, int result_type_)
    : ScanVisitor(stmt), sum(0), count(0), plugin(stmt->predicate_plg),
      state(0), result_type(result_type_) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, dbconf->key_type,
                            dbconf->key_size, dbconf->record_type,
                            dbconf->record_size, 0);
    key_size = dbconf->key_size;
    record_size = dbconf->record_size;
  }

  ~AverageIfScanVisitor() {
    // clean up the plugin's state
    if (plugin->cleanup) {
      plugin->cleanup(state);
      state = 0;
    }
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) {
    PodType *t;

    if (plugin->pred(state, key_data, key_size, record_data, record_size)) {
      if (isset(statement->function.flags, UQI_STREAM_KEY))
        t = (PodType *)key_data;
      else
        t = (PodType *)record_data;

      sum += *t * duplicate_count;
      count += duplicate_count;
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    PodType *key_array = (PodType *)key_data;
    PodType *record_array = (PodType *)record_data;
    PodType *stream;

    if (isset(statement->function.flags, UQI_STREAM_KEY))
      stream = key_array;
    else
      stream = record_array;

    for (size_t i = 0; i < length; i++, stream++) {
      if (plugin->pred(state, &key_array[i], key_size,
                    &record_array[i], record_size))
        sum += *stream;
        count++;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    sum /= (AggType)count;

    uqi_result_add_row(result, UPS_TYPE_BINARY, "AVERAGE", 8,
                    result_type, &sum, sizeof(sum));
  }

  // The aggreated sum
  AggType sum;

  // The element counter
  uint64_t count;

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;

  // The type of the result
  int result_type;

  // The key size
  uint32_t key_size;

  // The record size
  uint32_t record_size;
};

struct AverageIfScanVisitorFactory 
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.name == "average");
    ups_assert(stmt->predicate.name != "");

    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new AverageIfScanVisitor<uint8_t, uint64_t>(cfg,
                            stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT16:
        return (new AverageIfScanVisitor<uint16_t, uint64_t>(cfg,
                            stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT32:
        return (new AverageIfScanVisitor<uint32_t, uint64_t>(cfg,
                            stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT64:
        return (new AverageIfScanVisitor<uint64_t, uint64_t>(cfg,
                            stmt, UPS_TYPE_UINT64));
      case UPS_TYPE_REAL32:
        return (new AverageIfScanVisitor<float, double>(cfg,
                            stmt, UPS_TYPE_REAL64));
      case UPS_TYPE_REAL64:
        return (new AverageIfScanVisitor<double, double>(cfg,
                            stmt, UPS_TYPE_REAL64));
      default:
        return (0);
    }
  }
};

} // namespace upscaledb

