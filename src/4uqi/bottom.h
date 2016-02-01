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
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

// If the vector is full then delete the old minimum to make space.
// Then append the new value.
template<typename T>
static inline T
store_max_value(T value, T old, std::vector<T> &vec, size_t limit)
{
  if (unlikely(vec.size() < limit)) {
    vec.push_back(value);
    return (value > old ? value : old);
  }
  if (value < old) {
    vec.erase(std::find(vec.begin(), vec.end(), old));
    vec.push_back(value);
    return (*std::max_element(vec.begin(), vec.end()));
  }
  return (old);
}

template<typename KeyType, typename RecordType>
struct BottomScanVisitor : public ScanVisitor {
  BottomScanVisitor(const DatabaseConfiguration *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), stored_max_key(std::numeric_limits<KeyType>::min()),
      stored_max_record(std::numeric_limits<RecordType>::min()) {
    if (isset(stmt->function.flags, UQI_STREAM_RECORD))
      result_type = cfg->record_type;
    else
      result_type = cfg->key_type;

    if (statement->limit == 0)
      statement->limit = 1;
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
      stored_max_key = store_max_value(t, stored_max_key, keys,
                      statement->limit);
    }
    else {
      RecordType t = *(RecordType *)record_data;
      stored_max_record = store_max_value(t, stored_max_record, records,
                      statement->limit);
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      KeyType *data = (KeyType *)key_data;
      for (size_t i = 0; i < length; i++, data++) {
        stored_max_key = store_max_value(*data, stored_max_key, keys,
                        statement->limit);
      }
    }
    else {
      RecordType *data = (RecordType *)record_data;
      for (size_t i = 0; i < length; i++, data++) {
        stored_max_record = store_max_value(*data, stored_max_record, records,
                        statement->limit);
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      uqi_result_initialize(result, result_type, UPS_TYPE_BINARY);
      std::sort(keys.begin(), keys.end());
      for (size_t i = 0; i < keys.size(); i++)
        uqi_result_add_row(result, &keys[i], sizeof(KeyType), 0, 0);
    }
    else {
      uqi_result_initialize(result, UPS_TYPE_BINARY, result_type);
      std::sort(records.begin(), records.end());
      for (size_t i = 0; i < records.size(); i++)
        uqi_result_add_row(result, 0, 0, &records[i], sizeof(RecordType));
    }
  }

  // The maximum value currently stored in |keys|
  KeyType stored_max_key;

  // The current set of keys
  std::vector<KeyType> keys;

  // The maximum value currently stored in |records|
  RecordType stored_max_record;

  // The current set of records
  std::vector<RecordType> records;

  // The type of the result
  int result_type;
};

struct BottomScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<BottomScanVisitor>(cfg, stmt));
  }
};

template<typename KeyType, typename RecordType>
struct BottomIfScanVisitor : public ScanVisitor {
  BottomIfScanVisitor(const DatabaseConfiguration *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), stored_max_key(std::numeric_limits<KeyType>::min()),
      stored_max_record(std::numeric_limits<RecordType>::min()),
      plugin(stmt->predicate_plg), state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, cfg->key_type,
                            cfg->key_size, cfg->record_type,
                            cfg->record_size, 0);
    if (isset(stmt->function.flags, UQI_STREAM_RECORD))
      result_type = cfg->record_type;
    else
      result_type = cfg->key_type;
    if (statement->limit == 0)
      statement->limit = 1;
  }

  // only numerical data is allowed
  static bool validate(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (BottomScanVisitor<KeyType, RecordType>::validate(cfg, stmt));
  }

  ~BottomIfScanVisitor() {
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
    if (plugin->pred(state, key_data, key_size, record_data, record_size)) {
      if (isset(statement->function.flags, UQI_STREAM_KEY)) {
        KeyType t = *(KeyType *)key_data;
        stored_max_key = store_max_value(t, stored_max_key, keys,
                        statement->limit);
      }
      else {
        RecordType t = *(RecordType *)record_data;
        stored_max_record = store_max_value(t, stored_max_record, records,
                        statement->limit);
      }
    }
  }

  // Operates on an array of keys and records (both with fixed length)
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    KeyType *kdata = (KeyType *)key_data;
    RecordType *rdata = (RecordType *)record_data;

    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      for (size_t i = 0; i < length; i++, kdata++) {
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType))) {
          stored_max_key = store_max_value(kdata[i], stored_max_key, keys,
                          statement->limit);
        }
      }
    }
    else {
      for (size_t i = 0; i < length; i++, rdata++) {
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType))) {
          stored_max_record = store_max_value(rdata[i], stored_max_record,
                          records, statement->limit);
        }
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      uqi_result_initialize(result, result_type, UPS_TYPE_BINARY);
      std::sort(keys.begin(), keys.end());
      for (size_t i = 0; i < keys.size(); i++)
        uqi_result_add_row(result, &keys[i], sizeof(KeyType), 0, 0);
    }
    else {
      uqi_result_initialize(result, UPS_TYPE_BINARY, result_type);
      std::sort(records.begin(), records.end());
      for (size_t i = 0; i < records.size(); i++)
        uqi_result_add_row(result, 0, 0, &records[i], sizeof(RecordType));
    }
  }

  // The maximum value currently stored in |keys|
  KeyType stored_max_key;

  // The current set of keys
  std::vector<KeyType> keys;

  // The maximum value currently stored in |records|
  RecordType stored_max_record;

  // The current set of records
  std::vector<RecordType> records;

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;

  // The type of the result
  int result_type;
};

struct BottomIfScanVisitorFactory
{
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<BottomIfScanVisitor>(cfg, stmt));
  }
};

} // namespace upscaledb

