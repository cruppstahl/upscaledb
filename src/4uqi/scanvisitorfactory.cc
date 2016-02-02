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
#include "4uqi/bottom.h"
#include "4uqi/count.h"
#include "4uqi/max.h"
#include "4uqi/min.h"
#include "4uqi/sum.h"
#include "4uqi/top.h"
#include "4uqi/value.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct PluginProxyScanVisitor : public ScanVisitor {
  PluginProxyScanVisitor(const DbConfig *dbconf,
                        SelectStatement *stmt)
    : ScanVisitor(stmt), plugin(stmt->function_plg), state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, dbconf->key_type,
                            dbconf->key_size, dbconf->record_type,
                            dbconf->record_size, 0);
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
    if (isset(statement->function.flags, UQI_STREAM_KEY))
      plugin->agg_single(state, key_data, key_size, 0,
                  0, duplicate_count);
    else if (isset(statement->function.flags, UQI_STREAM_RECORD))
      plugin->agg_single(state, 0, 0, record_data,
                  record_size, duplicate_count);
    else
      plugin->agg_single(state, key_data, key_size, record_data,
                  record_size, duplicate_count);
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (isset(statement->function.flags, UQI_STREAM_KEY))
      plugin->agg_many(state, key_data, 0, length);
    else if (isset(statement->function.flags, UQI_STREAM_RECORD))
      plugin->agg_many(state, 0, record_data, length);
    else
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
  PluginProxyIfScanVisitor(const DbConfig *dbconf,
                        SelectStatement *stmt)
      : ScanVisitor(stmt), agg_plugin(stmt->function_plg),
        pred_plugin(stmt->predicate_plg), agg_state(0), pred_state(0) {
    if (agg_plugin->init)
      agg_state = agg_plugin->init(stmt->function.flags, dbconf->key_type,
                                    dbconf->key_size, dbconf->record_type,
                                    dbconf->record_size, 0);
    if (pred_plugin->init)
      pred_state = pred_plugin->init(stmt->predicate.flags, dbconf->key_type,
                                    dbconf->key_size, dbconf->record_type,
                                    dbconf->record_size, 0);
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
    if (pred_plugin->pred(pred_state, key_data, key_size,
            record_data, record_size)) {
      if (isset(statement->function.flags, UQI_STREAM_KEY))
        agg_plugin->agg_single(agg_state, key_data, key_size, 0,
                    0, duplicate_count);
      else if (isset(statement->function.flags, UQI_STREAM_RECORD))
        agg_plugin->agg_single(agg_state, 0, 0, record_data,
                    record_size, duplicate_count);
      else
        agg_plugin->agg_single(agg_state, key_data, key_size, record_data,
                    record_size, duplicate_count);
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                    size_t length) {
    PodType *data = (PodType *)key_data;

    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      for (size_t i = 0; i < length; i++, data++) {
        if (pred_plugin->pred(pred_state, data, sizeof(PodType), 0, 0))
          agg_plugin->agg_single(agg_state, key_data, sizeof(PodType), 0, 0, 1);
      }
    }
    // TODO TODO TODO
    else if (isset(statement->function.flags, UQI_STREAM_RECORD)) {
      for (size_t i = 0; i < length; i++, data++) {
        if (pred_plugin->pred(pred_state, data, sizeof(PodType), 0, 0))
          agg_plugin->agg_single(agg_state, key_data, sizeof(PodType), 0, 0, 1);
      }
    }
    // TODO TODO TODO
    else {
      for (size_t i = 0; i < length; i++, data++) {
        if (pred_plugin->pred(pred_state, data, sizeof(PodType), 0, 0))
          agg_plugin->agg_single(agg_state, key_data, sizeof(PodType), 0, 0, 1);
      }
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
  const DbConfig *cfg = &db->config();

  // Predicate plugin required?
  if (!stmt->predicate.name.empty() && stmt->predicate_plg == 0) {
    ups_trace(("Invalid or unknown predicate function '%s'",
                stmt->predicate.name.c_str()));
    return (0);
  }

  // AVERAGE ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "average") {
    if (stmt->predicate.name == "")
      return (AverageScanVisitorFactory::create(cfg, stmt));
    else
      return (AverageIfScanVisitorFactory::create(cfg, stmt));
  }

  // BOTTOM ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "bottom") {
    if (stmt->predicate.name == "")
      return (BottomScanVisitorFactory::create(cfg, stmt));
    else
      return (BottomIfScanVisitorFactory::create(cfg, stmt));
  }

  // COUNT ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "count") {
    if (stmt->predicate.name == "")
      return (CountScanVisitorFactory::create(cfg, stmt));
    else
      return (CountIfScanVisitorFactory::create(cfg, stmt));
  }

  // MAX ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "max") {
    if (stmt->predicate.name == "")
      return (MaxScanVisitorFactory::create(cfg, stmt));
    else
      return (MaxIfScanVisitorFactory::create(cfg, stmt));
  }

  // MIN ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "min") {
    if (stmt->predicate.name == "")
      return (MinScanVisitorFactory::create(cfg, stmt));
    else
      return (MinIfScanVisitorFactory::create(cfg, stmt));
  }

  // SUM ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "sum") {
    if (stmt->predicate.name == "")
      return (SumScanVisitorFactory::create(cfg, stmt));
    else
      return (SumIfScanVisitorFactory::create(cfg, stmt));
  }

  // TOP ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "top") {
    if (stmt->predicate.name == "")
      return (TopScanVisitorFactory::create(cfg, stmt));
    else
      return (TopIfScanVisitorFactory::create(cfg, stmt));
  }

  // VALUE ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "value") {
    if (stmt->predicate.name == "")
      return (ValueScanVisitorFactory::create(cfg, stmt));
    else
      return (ValueIfScanVisitorFactory::create(cfg, stmt));
  }

  if (stmt->function_plg == 0) {
    ups_trace(("Invalid or unknown builtin function %s",
                stmt->function.name.c_str()));
    return (0);
  }

  // custom plugin function without predicate?
  if (stmt->predicate_plg == 0) {
    return (new PluginProxyScanVisitor(cfg, stmt));
  }
  // custom plugin function WITH predicate?
  else {
    switch (cfg->key_type) {
      case UPS_TYPE_UINT8:
        return (new PluginProxyIfScanVisitor<uint8_t>(cfg, stmt));
      case UPS_TYPE_UINT16:
        return (new PluginProxyIfScanVisitor<uint16_t>(cfg, stmt));
      case UPS_TYPE_UINT32:
        return (new PluginProxyIfScanVisitor<uint32_t>(cfg, stmt));
      case UPS_TYPE_UINT64:
        return (new PluginProxyIfScanVisitor<uint64_t>(cfg, stmt));
      case UPS_TYPE_REAL32:
        return (new PluginProxyIfScanVisitor<float>(cfg, stmt));
      case UPS_TYPE_REAL64:
        return (new PluginProxyIfScanVisitor<double>(cfg, stmt));
      case UPS_TYPE_BINARY:
      case UPS_TYPE_CUSTOM:
        return (new PluginProxyIfScanVisitor<uint8_t>(cfg, stmt));
      default:
        return (0);
    }
  }

  // not found
  return (0);
}

} // namespace upscaledb

