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
#include "4db/db_local.h"
#include "4uqi/plugins.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitor.h"
#include "4uqi/scanvisitorfactory.h"

#include "4uqi/average.h"
#include "4uqi/bottom.h"
#include "4uqi/count.h"
#include "4uqi/minmax.h"
#include "4uqi/sum.h"
#include "4uqi/top.h"
#include "4uqi/value.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct PluginProxyScanVisitor : public ScanVisitor {
  PluginProxyScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), plugin(cfg, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (ISSET(statement->function.flags, UQI_STREAM_KEY))
      plugin.agg_single(key_data, key_size, 0, 0);
    else if (ISSET(statement->function.flags, UQI_STREAM_RECORD))
      plugin.agg_single(0, 0, record_data, record_size);
    else
      plugin.agg_single(key_data, key_size, record_data, record_size);
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (ISSET(statement->function.flags, UQI_STREAM_KEY))
      plugin.agg_many(key_data, 0, length);
    else if (ISSET(statement->function.flags, UQI_STREAM_RECORD))
      plugin.agg_many(0, record_data, length);
    else
      plugin.agg_many(key_data, record_data, length);
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    plugin.assign_result(result);
  }

  // The aggregate plugin
  AggregatePluginWrapper plugin;
};

template<typename Key, typename Record>
struct PluginProxyIfScanVisitor : public ScanVisitor {
  PluginProxyIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
      : ScanVisitor(stmt), agg_plugin(cfg, stmt), pred_plugin(cfg, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (pred_plugin.pred(key_data, key_size, record_data, record_size)) {
      if (ISSET(statement->function.flags, UQI_STREAM_KEY))
        agg_plugin.agg_single(key_data, key_size, 0, 0);
      else if (ISSET(statement->function.flags, UQI_STREAM_RECORD))
        agg_plugin.agg_single(0, 0, record_data, record_size);
      else
        agg_plugin.agg_single(key_data, key_size, record_data, record_size);
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                    size_t length) {
    Sequence<Key> keys(key_data, length);
    Sequence<Record> records(record_data, length);
    typename Sequence<Key>::iterator kit = keys.begin();
    typename Sequence<Record>::iterator rit = records.begin();

    if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
      for (; kit != keys.end(); kit++, rit++) {
        if (pred_plugin.pred(&kit->value, kit->size(), &rit->value, rit->size()))
          agg_plugin.agg_single(&kit->value, kit->size(), 0, 0);
      }
    }
    else if (ISSET(statement->function.flags, UQI_STREAM_RECORD)) {
      for (; kit != keys.end(); kit++, rit++) {
        if (pred_plugin.pred(&kit->value, kit->size(), &rit->value, rit->size()))
          agg_plugin.agg_single(0, 0, &rit->value, rit->size());
      }
    }
    else {
      for (; kit != keys.end(); kit++, rit++) {
        if (pred_plugin.pred(&kit->value, kit->size(), &rit->value, rit->size()))
          agg_plugin.agg_single(&kit->value, kit->size(),
                          &rit->value, rit->size());
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    agg_plugin.assign_result(result);
  }

  // The aggregate plugin
  AggregatePluginWrapper agg_plugin;

  // The predicate plugin
  PredicatePluginWrapper pred_plugin;
};

ScanVisitor *
ScanVisitorFactory::from_select(SelectStatement *stmt, LocalDb *db)
{
  const DbConfig *cfg = &db->config;

  // Predicate plugin required?
  if (!stmt->predicate.name.empty() && stmt->predicate_plg == 0) {
    ups_trace(("Invalid or unknown predicate function '%s'",
                stmt->predicate.name.c_str()));
    return 0;
  }

  // AVERAGE ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "average") {
    if (stmt->predicate.name == "")
      return AverageScanVisitorFactory::create(cfg, stmt);
    else
      return AverageIfScanVisitorFactory::create(cfg, stmt);
  }

  // BOTTOM ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "bottom") {
    if (stmt->predicate.name == "")
      return BottomScanVisitorFactory::create(cfg, stmt);
    else
      return BottomIfScanVisitorFactory::create(cfg, stmt);
  }

  // COUNT ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "count") {
    if (stmt->predicate.name == "")
      return CountScanVisitorFactory::create(cfg, stmt);
    else
      return CountIfScanVisitorFactory::create(cfg, stmt);
  }

  // MAX ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "max") {
    if (stmt->predicate.name == "")
      return MaxScanVisitorFactory::create(cfg, stmt);
    else
      return MaxIfScanVisitorFactory::create(cfg, stmt);
  }

  // MIN ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "min") {
    if (stmt->predicate.name == "")
      return MinScanVisitorFactory::create(cfg, stmt);
    else
      return MinIfScanVisitorFactory::create(cfg, stmt);
  }

  // SUM ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "sum") {
    if (stmt->predicate.name == "")
      return SumScanVisitorFactory::create(cfg, stmt);
    else
      return SumIfScanVisitorFactory::create(cfg, stmt);
  }

  // TOP ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "top") {
    if (stmt->predicate.name == "")
      return TopScanVisitorFactory::create(cfg, stmt);
    else
      return TopIfScanVisitorFactory::create(cfg, stmt);
  }

  // VALUE ... WHERE ...
  if (stmt->function.library.empty() && stmt->function.name == "value") {
    if (stmt->predicate.name == "")
      return ValueScanVisitorFactory::create(cfg, stmt);
    else
      return ValueIfScanVisitorFactory::create(cfg, stmt);
  }

  if (stmt->function_plg == 0) {
    ups_trace(("Invalid or unknown builtin function %s",
                stmt->function.name.c_str()));
    return 0;
  }

  // custom plugin function without predicate?
  if (stmt->predicate_plg == 0)
    return new PluginProxyScanVisitor(cfg, stmt);
  // otherwise it's a custom plugin function WITH predicate
  return ScanVisitorFactoryHelper::create<PluginProxyIfScanVisitor>(cfg, stmt);
}

} // namespace upscaledb

