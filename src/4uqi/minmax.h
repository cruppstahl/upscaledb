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
#include "4uqi/plugin_wrapper.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitor.h"
#include "4uqi/scanvisitorfactoryhelper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename Key, typename Record>
struct MinMaxScanVisitorBase : public NumericalScanVisitor {
  MinMaxScanVisitorBase(const DbConfig *cfg, SelectStatement *stmt,
                  Key initial_key, Record initial_record)
    : NumericalScanVisitor(stmt), key(initial_key), record(initial_record),
      key_type(cfg->key_type), record_type(cfg->record_type) {
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, key_type, record_type);

    if (isset(statement->function.flags, UQI_STREAM_RECORD))
      uqi_result_add_row(result, other.data(), other.size(),
              &record.value, record.size());
    else
      uqi_result_add_row(result, &key.value, key.size(),
              other.data(), other.size());
  }

  void copy_value(const void *data, size_t size) {
    other.copy((const uint8_t *)data, size);
  }

  // The current minimum/maximum key
  Key key;

  // The current minimum/maximum record
  Record record;

  // Stores the key, if MIN($record) is calculated, or the record, if
  // MIN($key) is calculated
  ByteArray other;

  // The key type and the record type
  int key_type;
  int record_type;
};

template<typename Key, typename Record, template<typename T> class Compare>
struct MinMaxScanVisitor : public MinMaxScanVisitorBase<Key, Record> {
  typedef MinMaxScanVisitorBase<Key, Record> P;

  MinMaxScanVisitor(const DbConfig *cfg, SelectStatement *stmt,
                  Key initial_key, Record initial_record)
    : MinMaxScanVisitorBase<Key, Record>(cfg, stmt,
                    initial_key, initial_record) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<typename Key::type> cmp;
      Key t(key_data, key_size);
      if (cmp(t.value, P::key.value)) {
        P::key = t;
        P::copy_value(record_data, record_size);
      }
    }
    else {
      Compare<typename Record::type> cmp;
      Record t(record_data, record_size);
      if (cmp(t.value, P::record.value)) {
        P::record = t;
        P::copy_value(key_data, key_size);
      }
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Sequence<Key> keys(key_data, length);
    Sequence<Record> records(record_data, length);
    typename Sequence<Key>::iterator kit = keys.begin();
    typename Sequence<Record>::iterator rit = records.begin();

    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<typename Key::type> cmp;
      for (; kit != keys.end(); kit++, rit++) {
        if (cmp(kit->value, P::key.value)) {
          P::key = kit->value;
          P::copy_value(&rit->value, rit->size());
        }
      }
    }
    else {
      Compare<typename Record::type> cmp;
      for (; kit != keys.end(); kit++, rit++) {
        if (cmp(rit->value, P::record.value)) {
          P::record = rit->value;
          P::copy_value(&kit->value, kit->size());
        }
      }
    }
  }
};

template<typename Key, typename Record>
struct MinScanVisitor
        : public MinMaxScanVisitor<Key, Record, std::less> {
  MinScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxScanVisitor<Key, Record, std::less>(cfg, stmt,
                    std::numeric_limits<typename Key::type>::max(),
                    std::numeric_limits<typename Record::type>::max()) {
  }
};

struct MinScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MinScanVisitor>(cfg, stmt));
  }
};

template<typename Key, typename Record>
struct MaxScanVisitor
        : public MinMaxScanVisitor<Key, Record, std::greater> {
  MaxScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxScanVisitor<Key, Record, std::greater>(cfg, stmt,
                    std::numeric_limits<typename Key::type>::min(),
                    std::numeric_limits<typename Record::type>::min()) {
  }
};

struct MaxScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg,
                        SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MaxScanVisitor>(cfg, stmt));
  }
};




template<typename Key, typename Record, template<typename T> class Compare>
struct MinMaxIfScanVisitor : public MinMaxScanVisitorBase<Key, Record> {
  typedef MinMaxScanVisitorBase<Key, Record> P;
  MinMaxIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt,
                  Key initial_key, Record initial_record)
    : MinMaxScanVisitorBase<Key, Record>(cfg, stmt,
                    initial_key, initial_record),
        plugin(cfg, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<typename Key::type> cmp;
      Key t(key_data, key_size);
      if (cmp(t.value, P::key.value)
          && plugin.pred(key_data, key_size, record_data, record_size)) {
        P::key = t;
        P::copy_value(record_data, record_size);
      }
    }
    else {
      Compare<typename Record::type> cmp;
      Record t(record_data, record_size);
      if (cmp(t.value, P::record.value)
          && plugin.pred(key_data, key_size, record_data, record_size)) {
        P::record = t;
        P::copy_value(key_data, key_size);
      }
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Sequence<Key> keys(key_data, length);
    Sequence<Record> records(record_data, length);
    typename Sequence<Key>::iterator kit = keys.begin();
    typename Sequence<Record>::iterator rit = records.begin();

    if (isset(P::statement->function.flags, UQI_STREAM_KEY)) {
      Compare<typename Key::type> cmp;
      for (; kit != keys.end(); kit++, rit++) {
        if (cmp(kit->value, P::key.value)
            && plugin.pred(kit, kit->value, rit, rit->value)) {
          P::key = kit->value;
          P::copy_value(&rit->value, rit->size());
        }
      }
    }
    else {
      Compare<typename Record::type> cmp;
      for (; kit != keys.end(); kit++, rit++) {
        if (cmp(rit->value, P::record.value)
            && plugin.pred(kit, kit->value, rit, rit->value)) {
          P::record = rit->value;
          P::copy_value(&kit->value, kit->size());
        }
      }
    }
  }

  PredicatePluginWrapper plugin;
};

template<typename Key, typename Record>
struct MinIfScanVisitor
        : public MinMaxIfScanVisitor<Key, Record, std::less> {
  MinIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxIfScanVisitor<Key, Record, std::less>(cfg, stmt,
                    std::numeric_limits<typename Key::type>::max(),
                    std::numeric_limits<typename Record::type>::max()) {
  }
};

struct MinIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return (ScanVisitorFactoryHelper::create<MinIfScanVisitor>(cfg, stmt));
  }
};

template<typename Key, typename Record>
struct MaxIfScanVisitor
        : public MinMaxIfScanVisitor<Key, Record, std::greater> {
  MaxIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : MinMaxIfScanVisitor<Key, Record, std::greater>(cfg, stmt,
                    std::numeric_limits<typename Key::type>::min(),
                    std::numeric_limits<typename Record::type>::min()) {
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

