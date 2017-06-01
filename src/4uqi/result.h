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

/*
 * UQI Result sets
 */

#ifndef UPS_UPSCALEDB_RESULT_H
#define UPS_UPSCALEDB_RESULT_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The struct Result is the actual implementation of uqs_result_t.
 */
struct Result
{
  Result()
    : row_count(0), key_type(UPS_TYPE_BINARY), record_type(UPS_TYPE_BINARY),
      next_key_offset(0), next_record_offset(0) {
  }

  void initialize(uint32_t key_type_, uint32_t record_type_) {
    key_type = key_type_;
    record_type = record_type_;
  }

  void add_row(const void *key_data, uint32_t key_size,
                  const void *record_data, uint32_t record_size) {
    row_count++;
    add_key(key_data, key_size);
    add_record(record_data, record_size);
  }

  void move_from(Result &other) {
    row_count = other.row_count;
    key_type = other.key_type;
    record_type = other.record_type;
    next_key_offset = other.next_key_offset;
    next_record_offset = other.next_record_offset;
    std::swap(key_offsets, other.key_offsets);
    std::swap(record_offsets, other.record_offsets);
    std::swap(key_data, other.key_data);
    std::swap(record_data, other.record_data);
  }

  uint32_t row_count;
  uint32_t key_type;
  uint32_t record_type;
  uint32_t next_key_offset;
  uint32_t next_record_offset;
  std::vector<uint32_t> key_offsets;
  std::vector<uint32_t> record_offsets;

  // Use std::vector instead of ByteArray class because it has better
  // performance when growing. ByteArray calls realloc() for each append(),
  // std::vector doesn't.
  std::vector<uint8_t> key_data;
  std::vector<uint8_t> record_data;

  void add_key(const char *str) {
    add_key(str, (uint32_t)::strlen(str) + 1);
  }

  void add_key(const void *data, uint32_t size) {
    key_data.insert(key_data.end(),
                    (uint8_t *)data, (uint8_t *)data + size);

    key_offsets.push_back(next_key_offset);
    next_key_offset += size;
  }

  void key(uint32_t row, ups_key_t *key) {
    assert(row < row_count);

    uint32_t offset = key_offsets[row];
    if (row == row_count - 1)
      key->size = next_key_offset - offset;
    else
      key->size = key_offsets[row + 1] - offset;

    key->data = &key_data[0] + offset;
  }

  template<typename T>
  void add_record(T t) {
    add_record(&t, sizeof(t));
  }

  void add_record(const void *data, uint32_t size) {
    record_data.insert(record_data.end(),
                    (uint8_t *)data, (uint8_t *)data + size);

    record_offsets.push_back(next_record_offset);
    next_record_offset += size;
  }

  void record(uint32_t row, ups_record_t *record) {
    assert(row < row_count);
    uint32_t offset = record_offsets[row];
    if (row == row_count - 1)
      record->size = next_record_offset - offset;
    else
      record->size = record_offsets[row + 1] - offset;

    record->data = &record_data[0] + offset;
  }
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_RESULT_H */
