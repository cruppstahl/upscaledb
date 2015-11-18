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

template<typename PodType, typename AggType>
struct SumScanVisitor : public ScanVisitor {
  SumScanVisitor(int result_type_)
    : sum(0), result_type(result_type_) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ups_assert(key_size == sizeof(PodType));
    PodType *t = (PodType *)key_data;
    sum += *t * duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t length) {
    PodType *data = (PodType *)key_array;
    for (size_t i = 0; i < length; i++, data++)
      sum += *data;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    result->type = result_type;
    result->u.result_u64 = sum;
  }

  // The aggregated sum
  AggType sum;

  // The type of the result
  int result_type;
};

struct SumScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.first == "sum");
    ups_assert(stmt->predicate.first == "");

    // COUNT with predicate
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new SumScanVisitor<uint8_t, uint64_t>(UPS_TYPE_UINT64));
      case UPS_TYPE_UINT16:
        return (new SumScanVisitor<uint16_t, uint64_t>(UPS_TYPE_UINT64));
      case UPS_TYPE_UINT32:
        return (new SumScanVisitor<uint32_t, uint64_t>(UPS_TYPE_UINT64));
      case UPS_TYPE_UINT64:
        return (new SumScanVisitor<uint64_t, uint64_t>(UPS_TYPE_UINT64));
      case UPS_TYPE_REAL32:
        return (new SumScanVisitor<float, double>(UPS_TYPE_REAL64));
      case UPS_TYPE_REAL64:
        return (new SumScanVisitor<double, double>(UPS_TYPE_REAL64));
      default:
        return (0);
    }
  }
};

template<typename PodType, typename AggType>
struct SumIfScanVisitor : public ScanVisitor {
  SumIfScanVisitor(const DatabaseConfiguration *dbconf, uqi_plugin_t *plugin_,
                        int result_type_)
    : sum(0), plugin(plugin_), state(0), result_type(result_type_) {
    if (plugin->init)
      state = plugin->init(dbconf->key_type, dbconf->key_size, 0);
  }

  ~SumIfScanVisitor() {
    // clean up the plugin's state
    if (plugin->cleanup) {
      plugin->cleanup(state);
      state = 0;
    }
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ups_assert(key_size == sizeof(PodType));
    if (plugin->pred(state, key_data, key_size)) {
      PodType *t = (PodType *)key_data;
      sum += *t * duplicate_count;
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, size_t length) {
    PodType *data = (PodType *)key_data;
    for (size_t i = 0; i < length; i++, data++) {
      if (plugin->pred(state, data, sizeof(PodType)))
        sum += *data;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    result->type = result_type;
    result->u.result_u64 = sum;
  }

  // The aggreated sum
  AggType sum;

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;

  // The type of the result
  int result_type;
};

struct SumIfScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    ups_assert(stmt->function.first == "sum");
    ups_assert(stmt->predicate.first != "");

    // COUNT with predicate
    uqi_plugin_t *plg = stmt->predicate_plg;
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new SumIfScanVisitor<uint8_t, uint64_t>(cfg,
                            plg, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT16:
        return (new SumIfScanVisitor<uint16_t, uint64_t>(cfg,
                            plg, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT32:
        return (new SumIfScanVisitor<uint32_t, uint64_t>(cfg,
                            plg, UPS_TYPE_UINT64));
      case UPS_TYPE_UINT64:
        return (new SumIfScanVisitor<uint64_t, uint64_t>(cfg,
                            plg, UPS_TYPE_UINT64));
      case UPS_TYPE_REAL32:
        return (new SumIfScanVisitor<float, uint64_t>(cfg,
                            plg, UPS_TYPE_REAL64));
      case UPS_TYPE_REAL64:
        return (new SumIfScanVisitor<double, uint64_t>(cfg,
                            plg, UPS_TYPE_REAL64));
      default:
        return (0);
    }
  }
};

} // namespace upscaledb

