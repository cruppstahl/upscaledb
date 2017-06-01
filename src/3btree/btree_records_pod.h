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
 * RecordList for POD ("Plain Old Data") Records
 *
 * The records are stored directly in the leaf node, and not in an external
 * blob. Only for fixed length records (except UPS_TYPE_BINARY records).
 */

#ifndef UPS_BTREE_RECORDS_POD_H
#define UPS_BTREE_RECORDS_POD_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1base/array_view.h"
#include "1base/dynamic_array.h"
#include "3btree/btree_records_base.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename T>
struct PodRecordList : BaseRecordList {
  enum {
    // A flag whether this RecordList has sequential data
    kHasSequentialData = 1,

    // This RecordList implements the scan() method
    kSupportsBlockScans = 1,
  };

  PodRecordList(LocalDb *db, PBtreeNode *node)
    : BaseRecordList(db, node) {
  }

  // Sets the data pointer
  void create(uint8_t *ptr, size_t range_size_) {
    range_data = (T *)ptr;
    range_size = range_size_;
  }

  // Opens an existing RecordList
  void open(uint8_t *ptr, size_t range_size_, size_t node_count) {
    range_data = (T *)ptr;
    range_size = range_size_;
  }

  // Returns the actual record size including overhead
  size_t full_record_size() const {
    return sizeof(T);
  }

  // Calculates the required size for a range with the specified |capacity|
  size_t required_range_size(size_t node_count) const {
    return node_count * sizeof(T);
  }

  // Returns the record counter of a key
  // This record list does not support duplicates, therefore always return 1
  int record_count(Context *, int) const {
    return 1;
  }

  // Returns the record size
  uint32_t record_size(Context *, int, int = 0) const {
    return sizeof(T);
  }

  // Returns the full record and stores it in |dest|; memory must be
  // allocated by the caller
  void record(Context *, int slot, ByteArray *arena, ups_record_t *record,
                  uint32_t flags, int) const {
    record->size = sizeof(T);

    if (unlikely(ISSET(flags, UPS_DIRECT_ACCESS))) {
      record->data = (void *)&range_data[slot];
      return;
    }

    if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
      arena->resize(record->size);
      record->data = arena->data();
    }

    ::memcpy(record->data, &range_data[slot], record->size);
  }

  // Updates the record of a key
  void set_record(Context *, int slot, int, ups_record_t *record,
                  uint32_t flags, uint32_t * = 0) {
    assert(record->size == sizeof(T));
    range_data[slot] = *(T *)record->data;
  }

  // Erases the record by nulling it
  void erase_record(Context *, int slot, int = 0, bool = true) {
    range_data[slot] = 0;
  }

  // Erases a whole slot by shifting all larger records to the "left"
  void erase(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&range_data[slot], &range_data[slot + 1],
                      sizeof(T) * (node_count - slot - 1));
  }

  // Creates space for one additional record
  void insert(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count) {
      ::memmove(&range_data[(slot + 1)], &range_data[slot],
                      sizeof(T) * (node_count - slot));
    }
    range_data[slot] = 0;
  }

  // Copies |count| records from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, PodRecordList<T> &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest.range_data[dstart], &range_data[sstart],
                    sizeof(T) * (node_count - sstart));
  }

  // Returns true if there's not enough space for another record
  bool requires_split(size_t node_count) const {
    if (unlikely(range_size == 0))
      return false;
    return (node_count + 1) * sizeof(T) >= range_size;
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                  size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, range_data, node_count * sizeof(T));
    range_size = new_range_size;
    range_data = (T *)new_data_ptr;
  }

  // Iterates all records, calls the |visitor| on each
  ScanResult scan(ByteArray *, size_t node_count, uint32_t start) {
    return std::make_pair(&range_data[start], node_count - start);
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseRecordList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                        range_size - required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    out << range_data[slot];
  }

  // The actual record data
  T *range_data;
};

} // namespace upscaledb

#endif // UPS_BTREE_RECORDS_POD_H
