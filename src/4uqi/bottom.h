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

#include <map>

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

typedef std::vector<uint8_t> ByteVector;

// If the vector is full then delete the old minimum to make space.
// Then append the new value.
template<typename T>
static inline T
store_max_value(T new_maximum, T old_maximum,
                const void *value_data, size_t value_size,
                std::map<T, ByteVector> &storage, size_t limit)
{
  typedef typename std::map<T, ByteVector>::value_type ValueType;

  const uint8_t *v = (const uint8_t *)value_data;

  if (unlikely(storage.size() < limit)) {
    storage.insert(ValueType(new_maximum, ByteVector(v, v + value_size)));
    return new_maximum > old_maximum ? new_maximum : old_maximum;
  }

  if (new_maximum < old_maximum) {
    storage.erase(storage.find(old_maximum));
    storage.insert(ValueType(new_maximum, ByteVector(v, v + value_size)));
    return storage.rbegin()->first;
  }

  return old_maximum;
}

template<typename Key, typename Record>
struct BottomScanVisitorBase : public NumericalScanVisitor {
  typedef std::map<Key, ByteVector> KeyMap;
  typedef std::map<Record, ByteVector> RecordMap;

  BottomScanVisitorBase(const DbConfig *cfg, SelectStatement *stmt)
    : NumericalScanVisitor(stmt),
      max_key(std::numeric_limits<typename Key::type>::min()),
      max_record(std::numeric_limits<typename Record::type>::min()),
      key_type(cfg->key_type), record_type(cfg->record_type) {
    if (statement->limit == 0)
      statement->limit = 1;
  }

  // Assigns the result to |result|
  virtual void assign_result(uqi_result_t *result) {
    uqi_result_initialize(result, key_type, record_type);

    if (ISSET(statement->function.flags, UQI_STREAM_KEY)) {
      for (typename KeyMap::iterator it = stored_keys.begin();
                      it != stored_keys.end(); it++) {
        const Key &key = it->first;
        const ByteVector &record = it->second;
        uqi_result_add_row(result, key.ptr(), key.size(), record.data(),
                        record.size());
      }
    }
    else {
      for (typename RecordMap::iterator it = stored_records.begin();
                      it != stored_records.end(); it++) {
        const Record &record = it->first;
        const ByteVector &key = it->second;
        uqi_result_add_row(result, key.data(), key.size(), record.ptr(),
                        record.size());
      }
    }
  }

  // The maximum value currently stored in |keys|
  Key max_key;

  // The current set of keys
  KeyMap stored_keys;

  // The maximum value currently stored in |records|
  Record max_record;

  // The current set of records
  RecordMap stored_records;

  // The types for keys and records
  int key_type;
  int record_type;
};

template<typename Key, typename Record>
struct BottomScanVisitor : public BottomScanVisitorBase<Key, Record>
{
  typedef BottomScanVisitorBase<Key, Record> P;

  BottomScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : BottomScanVisitorBase<Key, Record>(cfg, stmt) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (ISSET(P::statement->function.flags, UQI_STREAM_KEY)) {
      Key key(key_data, key_size);
      P::max_key = store_max_value(key, P::max_key,
                      record_data, record_size,
                      P::stored_keys, P::statement->limit);
    }
    else {
      Record record(record_data, record_size);
      P::max_record = store_max_value(record, P::max_record,
                      key_data, key_size,
                      P::stored_records, P::statement->limit);
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Sequence<Key> keys(key_data, length);
    Sequence<Record> records(record_data, length);
    typename Sequence<Key>::iterator kit = keys.begin();
    typename Sequence<Record>::iterator rit = records.begin();

    if (ISSET(P::statement->function.flags, UQI_STREAM_KEY)) {
      for (; kit != keys.end(); kit++, rit++) {
        P::max_key = store_max_value(*kit, P::max_key,
                        &rit->value, rit->size(),
                        P::stored_keys, P::statement->limit);
      }
    }
    else {
      for (; kit != keys.end(); kit++, rit++) {
        P::max_record = store_max_value(*rit, P::max_record,
                        &kit->value, kit->size(),
                        P::stored_records, P::statement->limit);
      }
    }
  }
};

struct BottomScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<BottomScanVisitor>(cfg, stmt);
  }
};

template<typename Key, typename Record>
struct BottomIfScanVisitor : public BottomScanVisitorBase<Key, Record> {
  typedef BottomScanVisitorBase<Key, Record> P;

  BottomIfScanVisitor(const DbConfig *cfg, SelectStatement *stmt)
    : BottomScanVisitorBase<Key, Record>(cfg, stmt), plugin(cfg, stmt) {
  }

  // Operates on a single key
  //
  // TODO first check if the key is > old_maximum, THEN check the predicate
  // (otherwise the predicate is checked for every key, and I think this is
  // more expensive than the other way round)
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    if (plugin.pred(key_data, key_size, record_data, record_size)) {
      if (ISSET(P::statement->function.flags, UQI_STREAM_KEY)) {
        Key key(key_data, key_size);
        P::max_key = store_max_value(key, P::max_key,
                        record_data, record_size,
                        P::stored_keys, P::statement->limit);
      }
      else {
        Record record(record_data, record_size);
        P::max_record = store_max_value(record, P::max_record,
                        key_data, key_size,
                        P::stored_records, P::statement->limit);
      }
    }
  }

  // Operates on an array of keys and records (both with fixed length)
  //
  // TODO first check if the key is > old_maximum, THEN check the predicate
  // (otherwise the predicate is checked for every key, and I think this is
  // more expensive than the other way round)
  virtual void operator()(const void *key_data, const void *record_data,
                  size_t length) {
    Sequence<Key> keys(key_data, length);
    Sequence<Record> records(record_data, length);
    typename Sequence<Key>::iterator kit = keys.begin();
    typename Sequence<Record>::iterator rit = records.begin();

    if (ISSET(P::statement->function.flags, UQI_STREAM_KEY)) {
      for (; kit != keys.end(); kit++, rit++) {
        if (plugin.pred(&kit->value, kit->size(), &rit->value, rit->size())) {
          P::max_key = store_max_value(*kit, P::max_key,
                          &rit->value, rit->size(),
                          P::stored_keys, P::statement->limit);
        }
      }
    }
    else {
      for (; kit != keys.end(); kit++, rit++) {
        if (plugin.pred(&kit->value, kit->size(), &rit->value, rit->size())) {
          P::max_record = store_max_value(*rit, P::max_record,
                          &kit->value, kit->size(),
                          P::stored_records, P::statement->limit);
        }
      }
    }
  }

  // The predicate plugin
  PredicatePluginWrapper plugin;
};

struct BottomIfScanVisitorFactory
{
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    return ScanVisitorFactoryHelper::create<BottomIfScanVisitor>(cfg, stmt);
  }
};

} // namespace upscaledb

