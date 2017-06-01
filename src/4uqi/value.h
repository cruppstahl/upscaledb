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
#include "4uqi/plugin_wrapper.h"
#include "4uqi/scanvisitor.h"
#include "4uqi/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename Key, typename Record>
struct ValueScanVisitor : public ScanVisitor {
  ValueScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt) {
    aggregator.initialize(cfg->key_type, cfg->record_type);
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (statement->function.flags == UQI_STREAM_KEY) {
      aggregator.add_row(key_data, key_size, 0, 0);
      return;
    }

    if (statement->function.flags == UQI_STREAM_RECORD) {
      aggregator.add_row(0, 0, record_data, record_size);
      return;
    }

    aggregator.add_row(key_data, key_size, record_data, record_size);
  }

  // Operates on an array of fixed-length keys/records
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Key *kdata = (Key *)key_data;
    Record *rdata = (Record *)record_data;

    if (statement->function.flags == UQI_STREAM_KEY) {
      for (size_t i = 0; i < length; i++, kdata++)
        aggregator.add_row(kdata, sizeof(Key), 0, 0);
      return;
    }

    if (statement->function.flags == UQI_STREAM_RECORD) {
      for (size_t i = 0; i < length; i++, rdata++)
        aggregator.add_row(0, 0, rdata, sizeof(Record));
      return;
    }

    for (size_t i = 0; i < length; i++, kdata++, rdata++)
      aggregator.add_row(kdata, sizeof(Key), rdata, sizeof(Record));
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    Result *final_result = (Result *)result;
    final_result->move_from(aggregator);
  }

  // The aggregated result
  Result aggregator;
};

struct ValueScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<ValueScanVisitor>(cfg, stmt);
  }
};

template<typename Key, typename Record>
struct ValueIfScanVisitor : public ScanVisitor {
  ValueIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), plugin(cfg, stmt) {
    aggregator.initialize(cfg->key_type, cfg->record_type);
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (plugin.pred(key_data, key_size, record_data, record_size)) {
      if (statement->function.flags == UQI_STREAM_KEY) {
        aggregator.add_row(key_data, key_size, 0, 0);
        return;
      }

      if (statement->function.flags == UQI_STREAM_RECORD) {
        aggregator.add_row(0, 0, record_data, record_size);
        return;
      }

      aggregator.add_row(key_data, key_size, record_data, record_size);
    }
  }

  // Operates on an array of fixed-length keys/records
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Key *kdata = (Key *)key_data;
    Record *rdata = (Record *)record_data;

    if (statement->function.flags == UQI_STREAM_KEY) {
      for (size_t i = 0; i < length; i++, kdata++, rdata++) {
        if (plugin.pred(kdata, sizeof(Key), rdata, sizeof(Record)))
          aggregator.add_row(kdata, sizeof(Key), 0, 0);
      }
      return;
    }

    if (statement->function.flags == UQI_STREAM_RECORD) {
      for (size_t i = 0; i < length; i++, kdata++, rdata++) {
        if (plugin.pred(kdata, sizeof(Key), rdata, sizeof(Record)))
          aggregator.add_row(0, 0, rdata, sizeof(Record));
      }
      return;
    }

    for (size_t i = 0; i < length; i++, kdata++, rdata++) {
      if (plugin.pred(kdata, sizeof(Key), rdata, sizeof(Record)))
        aggregator.add_row(kdata, sizeof(Key), rdata, sizeof(Record));
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    Result *final_result = (Result *)result;
    final_result->move_from(aggregator);
  }

  // The aggregated result
  Result aggregator;

  // The predicate plugin
  PredicatePluginWrapper plugin;
};

struct ValueIfScanVisitorFactory {
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<ValueIfScanVisitor>(cfg, stmt);
  }
};

} // namespace upscaledb

