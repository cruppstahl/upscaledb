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
#include "4uqi/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct CountScanVisitor : public ScanVisitor {
  CountScanVisitor()
    : count(0) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size,
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) {
    count += duplicate_count;
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
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.name == "count");
    ups_assert(stmt->predicate.name == "");
    return (new CountScanVisitor());
  }
};


template<typename PodType>
struct CountIfScanVisitor : public ScanVisitor {
  CountIfScanVisitor(const DatabaseConfiguration *dbconf, SelectStatement *stmt)
    : count(0), plugin(stmt->predicate_plg), state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, dbconf->key_type,
                            dbconf->key_size, dbconf->record_type,
                            dbconf->record_size, 0);
    key_size = dbconf->key_size;
    record_size = dbconf->record_size;
  }

  ~CountIfScanVisitor() {
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
    if (plugin->pred(state, key_data, key_size, record_data, record_size))
      count += duplicate_count;
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
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;

  // The key size
  uint32_t key_size;

  // The record size
  uint32_t record_size;
};

struct CountIfScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.name == "count");
    ups_assert(stmt->predicate.name != "");

    // COUNT with predicate
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new CountIfScanVisitor<uint8_t>(cfg, stmt));
      case UPS_TYPE_UINT16:
        return (new CountIfScanVisitor<uint16_t>(cfg, stmt));
      case UPS_TYPE_UINT32:
        return (new CountIfScanVisitor<uint32_t>(cfg, stmt));
      case UPS_TYPE_UINT64:
        return (new CountIfScanVisitor<uint64_t>(cfg, stmt));
      case UPS_TYPE_REAL32:
        return (new CountIfScanVisitor<float>(cfg, stmt));
      case UPS_TYPE_REAL64:
        return (new CountIfScanVisitor<double>(cfg, stmt));
      default:
        return (new CountIfScanVisitor<uint8_t>(cfg, stmt));
    }
  }
};

} // namespace upscaledb

