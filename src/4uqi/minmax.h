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

#include <functional>

#include "1base/error.h"
#include "2config/db_config.h"
#include "3btree/btree_visitor.h"
#include "4uqi/plugin_wrapper.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename KeyType, typename RecordType>
struct MinMaxScanVisitorBase : public ScanVisitor {
  MinMaxScanVisitorBase(const DbConfig *cfg, SelectStatement *stmt,
                  KeyType initial_key_value, RecordType initial_record_value)
    : ScanVisitor(stmt), key_value(initial_key_value),
      record_value(initial_record_value),
      key_type(cfg->key_type), record_type(cfg->record_type) {
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

    return type != UPS_TYPE_CUSTOM && type != UPS_TYPE_BINARY;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, key_type, record_type);

    if (isset(statement->function.flags, UQI_STREAM_RECORD))
      uqi_result_add_row(result, other.data(), other.size(),
              &record_value, sizeof(record_value));
    else
      uqi_result_add_row(result, &key_value, sizeof(key_value),
              other.data(), other.size());
  }

  void copy_value(const void *data, size_t size) {
    other.copy((const uint8_t *)data, size);
  }

  // The minimum value (used for the keys)
  KeyType key_value;

  // The minimum value (used for the records)
  RecordType record_value;

  // Stores the key, if MIN($record) is calculated, or the record, if
  // MIN($key) is calculated
  ByteArray other;

  // The key type and the record type
  int key_type;
  int record_type;
};

template<typename KeyType, typename RecordType,
        template<typename T> class Compare>
struct MinMaxScanVisitor : public MinMaxScanVisitorBase<KeyType, RecordType> {
  typedef MinMaxScanVisitorBase<KeyType, RecordType> P;
  MinMaxScanVisitor(const DbConfig *cfg, SelectStatement *stmt,
                  KeyType initial_key_value, RecordType initial_record_value)
    : MinMaxScanVisitorBase<KeyType, RecordType>(cfg, stmt,
                    initial_key_value, initial_record_value) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size, 
                  size_t duplicate_count) {
    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<KeyType> cmp;
      KeyType t = *(KeyType *)key_data;
      if (cmp(t, P::key_value)) {
        P::key_value = t;
        P::copy_value(record_data, record_size);
      }
    }
    else {
      Compare<RecordType> cmp;
      RecordType t = *(RecordType *)record_data;
      if (cmp(t, P::record_value)) {
        P::record_value = t;
        P::copy_value(key_data, key_size);
      }
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    KeyType *kdata = (KeyType *)key_data;
    RecordType *rdata = (RecordType *)record_data;

    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<KeyType> cmp;
      for (size_t i = 0; i < length; i++, kdata++) {
        if (cmp(*kdata, P::key_value)) {
          P::key_value = *kdata;
          P::copy_value(rdata + i, sizeof(RecordType));
        }
      }
    }
    else {
      Compare<RecordType> cmp;
      for (size_t i = 0; i < length; i++, rdata++) {
        if (cmp(*rdata, P::record_value)) {
          P::record_value = *rdata;
          P::copy_value(kdata + i, sizeof(KeyType));
        }
      }
    }
  }
};

template<typename KeyType, typename RecordType>
struct MinScanVisitor
        : public MinMaxScanVisitor<KeyType, RecordType, std::less> {
  MinScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxScanVisitor<KeyType, RecordType, std::less>(cfg, stmt,
                    std::numeric_limits<KeyType>::max(),
                    std::numeric_limits<RecordType>::max()) {
  }
};

struct MinScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MinScanVisitor>(cfg, stmt));
  }
};

template<typename KeyType, typename RecordType>
struct MaxScanVisitor
        : public MinMaxScanVisitor<KeyType, RecordType, std::greater> {
  MaxScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxScanVisitor<KeyType, RecordType, std::greater>(cfg, stmt,
                    std::numeric_limits<KeyType>::min(),
                    std::numeric_limits<RecordType>::min()) {
  }
};

struct MaxScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MaxScanVisitor>(cfg, stmt));
  }
};




template<typename KeyType, typename RecordType,
        template<typename T> class Compare>
struct MinMaxIfScanVisitor : public MinMaxScanVisitorBase<KeyType, RecordType> {
  typedef MinMaxScanVisitorBase<KeyType, RecordType> P;
  MinMaxIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt,
                  KeyType initial_key_value, RecordType initial_record_value)
    : MinMaxScanVisitorBase<KeyType, RecordType>(cfg, stmt,
                    initial_key_value, initial_record_value),
        plugin(cfg, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size, 
                  size_t duplicate_count) {
    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<KeyType> cmp;
      KeyType t = *(KeyType *)key_data;
      if (cmp(t, P::key_value)
          && plugin.pred(key_data, key_size, record_data, record_size)) {
        P::key_value = t;
        P::copy_value(record_data, record_size);
      }
    }
    else {
      Compare<RecordType> cmp;
      RecordType t = *(RecordType *)record_data;
      if (cmp(t, P::record_value)
          && plugin.pred(key_data, key_size, record_data, record_size)) {
        P::record_value = t;
        P::copy_value(key_data, key_size);
      }
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    KeyType *kdata = (KeyType *)key_data;
    RecordType *rdata = (RecordType *)record_data;

    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<KeyType> cmp;
      for (size_t i = 0; i < length; i++, kdata++, rdata++) {
        if (cmp(*kdata, P::key_value)
            && plugin.pred(kdata, sizeof(KeyType), rdata, sizeof(RecordType))) {
          P::key_value = *kdata;
          P::copy_value(rdata + i, sizeof(RecordType));
        }
      }
    }
    else {
      Compare<RecordType> cmp;
      for (size_t i = 0; i < length; i++, rdata++) {
        if (cmp(*rdata, P::record_value)
            && plugin.pred(kdata, sizeof(KeyType), rdata, sizeof(RecordType))) {
          P::record_value = *rdata;
          P::copy_value(kdata + i, sizeof(KeyType));
        }
      }
    }
  }

  PluginWrapper plugin;
};

template<typename KeyType, typename RecordType>
struct MinIfScanVisitor
        : public MinMaxIfScanVisitor<KeyType, RecordType, std::less> {
  MinIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxIfScanVisitor<KeyType, RecordType, std::less>(cfg, stmt,
                    std::numeric_limits<KeyType>::max(),
                    std::numeric_limits<RecordType>::max()) {
  }
};

struct MinIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MinIfScanVisitor>(cfg, stmt));
  }
};

template<typename KeyType, typename RecordType>
struct MaxIfScanVisitor
        : public MinMaxIfScanVisitor<KeyType, RecordType, std::greater> {
  MaxIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxIfScanVisitor<KeyType, RecordType, std::greater>(cfg, stmt,
                    std::numeric_limits<KeyType>::min(),
                    std::numeric_limits<RecordType>::min()) {
  }
};

struct MaxIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MaxIfScanVisitor>(cfg, stmt));
  }
};

} // namespace upscaledb

