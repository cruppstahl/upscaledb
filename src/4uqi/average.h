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
#include "4uqi/plugin_wrapper.h"
#include "4uqi/result.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename Key, typename Record>
struct AverageScanVisitor : public NumericalScanVisitor
{
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  AverageScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : NumericalScanVisitor(stmt), sum(0), count(0) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
      Key t(key_data, key_size);
      sum += t.value;
    }
    else {
      Record t(record_data, record_size);
      sum += t.value;
    }

    count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
      Sequence<Key> keys(key_data, length);
      for (typename Sequence<Key>::iterator it = keys.begin();
                      it != keys.end(); it++)
        sum += it->value;
    }
    else {
      Sequence<Record> records(record_data, length);
      for (typename Sequence<Record>::iterator it = records.begin();
                      it != records.end(); it++)
        sum += it->value;
    }

    count += length;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    double avg = sum / (double)count;

    uqi_result_initialize(result, UPS_TYPE_BINARY, UPS_TYPE_REAL64);
    uqi_result_add_row(result, "AVERAGE", 8, &avg, sizeof(avg));
  }

  // The aggregated sum
  double sum;

  // The element counter
  uint64_t count;
};

struct AverageScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<AverageScanVisitor>(cfg, stmt);
  }
};

template<typename Key, typename Record>
struct AverageIfScanVisitor : public NumericalScanVisitor
{
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  AverageIfScanVisitor(const DbConfig *dbconf, SelectStatement *stmt)
    : NumericalScanVisitor(stmt), sum(0), count(0), plugin(dbconf, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (plugin.pred(key_data, key_size, record_data, record_size)) {
      if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
        Key t(key_data, key_size);
        sum += t.value;
      }
      else {
        Record t(record_data, record_size);
        sum += t.value;
      }
      count++;
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
        if (plugin.pred(kit, kit->size(), rit, rit->size())) {
          sum += kit->value;
          count++;
        }
      }
    }
    else {
      for (; kit != keys.end(); kit++, rit++) {
        if (plugin.pred(kit, kit->size(), rit, rit->size())) {
          sum += rit->value;
          count++;
        }
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    double avg = sum / (double)count;

    uqi_result_initialize(result, UPS_TYPE_BINARY, UPS_TYPE_REAL64);
    uqi_result_add_row(result, "AVERAGE", 8, &avg, sizeof(avg));
  }

  // The aggreated sum
  double sum;

  // The element counter
  uint64_t count;

  // The predicate plugin
  PredicatePluginWrapper plugin;
};

struct AverageIfScanVisitorFactory 
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<AverageIfScanVisitor>(cfg, stmt);
  }
};

} // namespace upscaledb

