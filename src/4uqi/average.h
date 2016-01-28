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
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename KeyType, typename RecordType>
struct AverageScanVisitor : public ScanVisitor {
  AverageScanVisitor(const DatabaseConfiguration *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), sum(0), count(0) {
  }

  // only numerical data is allowed
  static bool validate(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    if (isset(stmt->function.flags, UQI_STREAM_RECORD)
        && isset(stmt->function.flags, UQI_STREAM_KEY))
      return (false);

    int type = cfg->key_type;
    if (isset(stmt->function.flags, UQI_STREAM_RECORD))
      type = cfg->record_type;

    return (type != UPS_TYPE_CUSTOM && type != UPS_TYPE_BINARY);
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size, 
                  size_t duplicate_count) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      KeyType t = *(KeyType *)key_data;
      sum += t * duplicate_count;
    }
    else {
      RecordType t = *(RecordType *)record_data;
      sum += t * duplicate_count;
    }

    count += duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      KeyType *data = (KeyType *)key_data;
      for (size_t i = 0; i < length; i++, data++)
        sum += *data;
    }
    else {
      RecordType *data = (RecordType *)record_data;
      for (size_t i = 0; i < length; i++, data++)
        sum += *data;
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
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<AverageScanVisitor>(cfg,
                                stmt));
  }
};

template<typename KeyType, typename RecordType>
struct AverageIfScanVisitor : public ScanVisitor {
  AverageIfScanVisitor(const DatabaseConfiguration *dbconf,
                    SelectStatement *stmt)
    : ScanVisitor(stmt), sum(0), count(0), plugin(stmt->predicate_plg),
      state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, dbconf->key_type,
                            dbconf->key_size, dbconf->record_type,
                            dbconf->record_size, 0);
  }

  ~AverageIfScanVisitor() {
    // clean up the plugin's state
    if (plugin->cleanup) {
      plugin->cleanup(state);
      state = 0;
    }
  }

  // only numerical data is allowed
  static bool validate(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (AverageScanVisitor<KeyType, RecordType>::validate(cfg, stmt));
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) {
    if (plugin->pred(state, key_data, key_size, record_data, record_size)) {
      if (isset(statement->function.flags, UQI_STREAM_KEY)) {
        KeyType t = *(KeyType *)key_data;
        sum += t * duplicate_count;
      }
      else {
        RecordType t = *(RecordType *)record_data;
        sum += t * duplicate_count;
      }
      count += duplicate_count;
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    KeyType *kdata = (KeyType *)key_data;
    RecordType *rdata = (RecordType *)record_data;

    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      for (size_t i = 0; i < length; i++, kdata++) {
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType))) {
          sum += *kdata;
          count++;
        }
      }
    }
    else {
      for (size_t i = 0; i < length; i++, rdata++) {
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType))) {
          sum += *rdata;
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
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;
};

struct AverageIfScanVisitorFactory 
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<AverageIfScanVisitor>(cfg,
                                stmt));
  }
};

} // namespace upscaledb

