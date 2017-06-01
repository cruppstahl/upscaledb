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
#include "4uqi/type_wrapper.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitor.h"
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename Key, typename Record,
        typename ResultType, uint32_t UpsResultType>
struct SumScanVisitor : public NumericalScanVisitor {
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  SumScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : NumericalScanVisitor(stmt), sum(0) {
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
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
      typename Key::type *it = (typename Key::type *)key_data;
      typename Key::type *end = it + length;
      for (; it != end; it++)
        sum += *it;
    }
    else {
      typename Record::type *it = (typename Record::type *)record_data;
      typename Record::type *end = it + length;
      for (; it != end; it++)
        sum += *it;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, UpsResultType);
    uqi_result_add_row(result, "SUM", 4, &sum, sizeof(sum));
  }

  // The aggregated sum
  ResultType sum;
};

template<typename Key, typename Record>
struct NaturalSumScanVisitor
        : public SumScanVisitor<Key, Record, uint64_t, UPS_TYPE_UINT64> {
  NaturalSumScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : SumScanVisitor<Key, Record, uint64_t, UPS_TYPE_UINT64>(cfg, stmt) {
  }
};

template<typename Key, typename Record>
struct RealSumScanVisitor
        : public SumScanVisitor<Key, Record, double, UPS_TYPE_REAL64> {
  RealSumScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : SumScanVisitor<Key, Record, double, UPS_TYPE_REAL64>(cfg, stmt) {
  }
};

struct SumScanVisitorFactory {
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    int type = cfg->key_type;
    if (ISSET(stmt->function.flags, UQI_STREAM_RECORD)) {
      stmt->requires_keys = false;
      type = cfg->record_type;
    }
    else
      stmt->requires_records = false;

    switch (type) {
      case UPS_TYPE_UINT8:
      case UPS_TYPE_UINT16:
      case UPS_TYPE_UINT32:
      case UPS_TYPE_UINT64:
        return ScanVisitorFactoryHelper::create<NaturalSumScanVisitor>(cfg,
                                stmt);
      case UPS_TYPE_REAL32:
      case UPS_TYPE_REAL64:
        return ScanVisitorFactoryHelper::create<RealSumScanVisitor>(cfg,
                                stmt);
      default:
        // invalid type, SUM is not allowed
        return 0;
    };
  }
};



template<typename Key, typename Record,
        typename ResultType, uint32_t UpsResultType>
struct SumIfScanVisitor : public NumericalScanVisitor {
  enum {
    // only requires the target stream
    kRequiresBothStreams = 0,
  };

  SumIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : NumericalScanVisitor(stmt), sum(0), plugin(cfg, stmt) {
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
    }
  }

  // Operates on an array of keys and records (both with fixed length)
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
        }
      }
    }
    else {
      for (; kit != keys.end(); kit++, rit++) {
        if (plugin.pred(kit, kit->size(), rit, rit->size())) {
          sum += rit->value;
        }
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, UpsResultType);
    uqi_result_add_row(result, "SUM", 4, &sum, sizeof(sum));
  }

  // The aggreated sum
  ResultType sum;

  // The predicate plugin
  PredicatePluginWrapper plugin;
};

template<typename Key, typename Record>
struct NaturalSumIfScanVisitor
        : public SumIfScanVisitor<Key, Record, uint64_t, UPS_TYPE_UINT64> {
  NaturalSumIfScanVisitor(const DbConfig *cfg,
                  SelectStatement *stmt)
    : SumIfScanVisitor<Key, Record, uint64_t, UPS_TYPE_UINT64>(cfg, stmt) {
  }
};

template<typename Key, typename Record>
struct RealSumIfScanVisitor
        : public SumIfScanVisitor<Key, Record, double, UPS_TYPE_REAL64> {
  RealSumIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : SumIfScanVisitor<Key, Record, double, UPS_TYPE_REAL64>(cfg, stmt) {
  }
};

struct SumIfScanVisitorFactory {
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    int type = cfg->key_type;
    if (ISSET(stmt->function.flags, UQI_STREAM_RECORD)) {
      stmt->requires_keys = false;
      type = cfg->record_type;
    }
    else
      stmt->requires_records = false;

    switch (type) {
      case UPS_TYPE_UINT8:
      case UPS_TYPE_UINT16:
      case UPS_TYPE_UINT32:
      case UPS_TYPE_UINT64:
        return ScanVisitorFactoryHelper::create<NaturalSumIfScanVisitor>(cfg,
                                stmt);
      case UPS_TYPE_REAL32:
      case UPS_TYPE_REAL64:
        return ScanVisitorFactoryHelper::create<RealSumIfScanVisitor>(cfg,
                                stmt);
      default:
        // invalid type, SUM is not allowed
        return 0;
    };
  }
};

} // namespace upscaledb

