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

template<typename KeyType, typename RecordType>
struct MaxScanVisitor : public ScanVisitor {
  MaxScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), key_value(std::numeric_limits<KeyType>::min()),
      record_value(std::numeric_limits<RecordType>::min()) {
    if (isset(stmt->function.flags, UQI_STREAM_RECORD))
      result_type = cfg->record_type;
    else
      result_type = cfg->key_type;
  }

  // only numerical data is allowed
  static bool validate(const DbConfig *cfg,
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
      if (t > key_value)
        key_value = t;
    }
    else {
      RecordType t = *(RecordType *)record_data;
      if (t > record_value)
        record_value = t;
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      KeyType *data = (KeyType *)key_data;
      for (size_t i = 0; i < length; i++, data++)
        if (*data > key_value)
          key_value = *data;
    }
    else {
      RecordType *data = (RecordType *)record_data;
      for (size_t i = 0; i < length; i++, data++)
        if (*data > record_value)
          record_value = *data;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, result_type);

    if (isset(statement->function.flags, UQI_STREAM_RECORD))
      uqi_result_add_row(result, "MAX", 4, &record_value, sizeof(record_value));
    else
      uqi_result_add_row(result, "MAX", 4, &key_value, sizeof(key_value));
  }

  // The maximum value (used for the keys)
  KeyType key_value;

  // The maximum value (used for the records)
  RecordType record_value;

  // The type of the result
  int result_type;
};

struct MaxScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MaxScanVisitor>(cfg, stmt));
  }
};

template<typename KeyType, typename RecordType>
struct MaxIfScanVisitor : public ScanVisitor {
  MaxIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : ScanVisitor(stmt), key_value(std::numeric_limits<KeyType>::min()),
      record_value(std::numeric_limits<RecordType>::min()),
      plugin(stmt->predicate_plg), state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, cfg->key_type,
                            cfg->key_size, cfg->record_type,
                            cfg->record_size, 0);
    if (isset(stmt->function.flags, UQI_STREAM_RECORD))
      result_type = cfg->record_type;
    else
      result_type = cfg->key_type;
  }

  // only numerical data is allowed
  static bool validate(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (MaxScanVisitor<KeyType, RecordType>::validate(cfg, stmt));
  }

  ~MaxIfScanVisitor() {
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
        if (t > key_value)
          key_value = t;
      }
      else {
        RecordType t = *(RecordType *)record_data;
        if (t > record_value)
          record_value = t;
      }
    }
  }

  // Operates on an array of keys and records (both with fixed length)
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    KeyType *kdata = (KeyType *)key_data;
    RecordType *rdata = (RecordType *)record_data;

    if (isset(statement->function.flags, UQI_STREAM_KEY)) {
      for (size_t i = 0; i < length; i++, kdata++)
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType)))
          if (*kdata > key_value)
            key_value = *kdata;
    }
    else {
      for (size_t i = 0; i < length; i++, rdata++)
        if (plugin->pred(state, &kdata[i], sizeof(KeyType),
                    &rdata[i], sizeof(RecordType)))
          if (*rdata > record_value)
            record_value = *rdata;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, UPS_TYPE_BINARY, result_type);

    if (isset(statement->function.flags, UQI_STREAM_RECORD))
      uqi_result_add_row(result, "MAX", 4, &record_value, sizeof(record_value));
    else
      uqi_result_add_row(result, "MAX", 4, &key_value, sizeof(key_value));
  }

  // The maximum value (used for the keys)
  KeyType key_value;

  // The maximum value (used for the records)
  RecordType record_value;

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;

  // The type of the result
  int result_type;
};

struct MaxIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MaxIfScanVisitor>(cfg, stmt));
  }
};

} // namespace upscaledb

