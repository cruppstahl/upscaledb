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
#include "3btree/btree_visitor.h"
#include "4db/db_local.h"
#include "4uqi/scanvisitorfactory.h"
#include "4uqi/plugins.h"
#include "4uqi/statements.h"

#include "4uqi/average.h"
#include "4uqi/count.h"
#include "4uqi/sum.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct PluginProxyScanVisitor : public ScanVisitor {
  PluginProxyScanVisitor(const DatabaseConfiguration *dbconf,
                        uqi_plugin_t *plugin_)
    : plugin(plugin_), state(0) {
    if (plugin->init)
      state = plugin->init(dbconf->key_type, dbconf->key_size, 0);
  }

  ~PluginProxyScanVisitor() {
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
    plugin->agg_single(state, key_data, key_size, record_data, record_size,
                    duplicate_count);
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    plugin->agg_many(state, key_data, record_data, length);
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    plugin->results(state, result);
  }

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;
};

template<typename PodType>
struct PluginProxyIfScanVisitor : public ScanVisitor {
  PluginProxyIfScanVisitor(const DatabaseConfiguration *dbconf,
                        uqi_plugin_t *agg_plugin_, uqi_plugin_t *pred_plugin_)
      : agg_plugin(agg_plugin_), pred_plugin(pred_plugin_),
        agg_state(0), pred_state(0) {
    if (agg_plugin->init)
      agg_state = agg_plugin->init(dbconf->key_type, dbconf->key_size, 0);
    if (pred_plugin->init)
      pred_state = pred_plugin->init(dbconf->key_type, dbconf->key_size, 0);
  }

  ~PluginProxyIfScanVisitor() {
    // clean up the plugin's state
    if (agg_plugin->cleanup)
      agg_plugin->cleanup(agg_state);
    agg_state = 0;
    if (pred_plugin->cleanup)
      pred_plugin->cleanup(pred_state);
    pred_state = 0;
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) {
    if (pred_plugin->pred(pred_state, key_data, key_size))
      agg_plugin->agg_single(agg_state, key_data, key_size, record_data,
                    record_size, duplicate_count);
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                    size_t length) {
    PodType *data = (PodType *)key_data;
    for (size_t i = 0; i < length; i++, data++) {
      if (pred_plugin->pred(pred_state, data, sizeof(PodType)))
        agg_plugin->agg_single(agg_state, key_data, sizeof(PodType), 0, 0, 1);
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    agg_plugin->results(agg_state, result);
  }

  // The aggregate plugin
  uqi_plugin_t *agg_plugin;

  // The predicate plugin
  uqi_plugin_t *pred_plugin;

  // The state of the aggregate plugin
  void *agg_state;

  // The state of the predicate plugin
  void *pred_state;
};

ScanVisitor *
ScanVisitorFactory::from_select(SelectStatement *stmt, LocalDatabase *db)
{
  const DatabaseConfiguration *cfg = &db->config();

  // Predicate plugin required?
  if (!stmt->predicate.first.empty() && stmt->predicate_plg == 0) {
    ups_trace(("Invalid or unknown predicate function '%s'",
                stmt->predicate.first.c_str()));
    return (0);
  }

  // COUNT ... WHERE ...
  if (stmt->function.second.empty() && stmt->function.first == "count") {
    if (stmt->predicate.first == "")
      return (CountScanVisitorFactory::create(cfg, stmt));
    else
      return (CountIfScanVisitorFactory::create(cfg, stmt));
  }

  // SUM ... WHERE ...
  if (stmt->function.second.empty() && stmt->function.first == "sum") {
    if (stmt->predicate.first == "")
      return (SumScanVisitorFactory::create(cfg, stmt));
    else
      return (SumIfScanVisitorFactory::create(cfg, stmt));
  }

  // AVERAGE ... WHERE ...
  if (stmt->function.second.empty() && stmt->function.first == "average") {
    if (stmt->predicate.first == "")
      return (AverageScanVisitorFactory::create(cfg, stmt));
    else
      return (AverageIfScanVisitorFactory::create(cfg, stmt));
  }

  if (stmt->function.second.empty()) {
    ups_trace(("Invalid or unknown builtin function %s",
                stmt->function.first.c_str()));
    return (0);
  }

  // custom plugin function without predicate?
  if (stmt->predicate_plg == 0) {
    return (new PluginProxyScanVisitor(cfg, stmt->function_plg));
  }
  // custom plugin function WITH predicate?
  else {
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new PluginProxyIfScanVisitor<uint8_t>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_UINT16:
        return (new PluginProxyIfScanVisitor<uint16_t>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_UINT32:
        return (new PluginProxyIfScanVisitor<uint32_t>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_UINT64:
        return (new PluginProxyIfScanVisitor<uint64_t>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_REAL32:
        return (new PluginProxyIfScanVisitor<float>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_REAL64:
        return (new PluginProxyIfScanVisitor<double>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      case UPS_TYPE_BINARY:
      case UPS_TYPE_CUSTOM:
        return (new PluginProxyIfScanVisitor<uint8_t>(cfg,
                            stmt->function_plg, stmt->predicate_plg));
      default:
        return (0);
    }
  }

  // not found
  return (0);
}

} // namespace upscaledb

