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
 * Internal RecordList
 *
 * Only for records of internal nodes. Internal nodes only store page IDs,
 * therefore this |InternalRecordList| is optimized for 64bit IDs
 * (and is implemented as a uint64_t[] array).
 *
 * For file-based databases the page IDs are stored modulo page size, which
 * results in smaller IDs. Small IDs can be compressed more efficiently
 * (-> upscaledb pro).
 *
 * In-memory based databases just store the raw pointers. 
 */

#ifndef UPS_BTREE_RECORDS_INTERNAL_H
#define UPS_BTREE_RECORDS_INTERNAL_H

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

struct InternalRecordList : BaseRecordList {
  enum {
    // A flag whether this RecordList has sequential data
    kHasSequentialData = 1
  };

  // Constructor
  InternalRecordList(LocalDb *db, PBtreeNode *node)
    : BaseRecordList(db, node) {
    page_size = db->env->config.page_size_bytes;
    inmemory = ISSET(db->env->config.flags, UPS_IN_MEMORY);
  }

  // Sets the data pointer
  void create(uint8_t *ptr, size_t range_size_) {
    range_size = range_size_;
    range_data = ArrayView<uint64_t>((uint64_t *)ptr, range_size / 8);
  }

  // Opens an existing RecordList
  void open(uint8_t *ptr, size_t range_size_, size_t node_count) {
    range_size = range_size_;
    range_data = ArrayView<uint64_t>((uint64_t *)ptr, range_size / 8);
  }

  // Returns the actual size including overhead
  size_t full_record_size() const {
    return sizeof(uint64_t);
  }

  // Calculates the required size for a range with the specified |capacity|
  size_t required_range_size(size_t node_count) const {
    return node_count * sizeof(uint64_t);
  }

  // Returns the record counter of a key; this implementation does not
  // support duplicates, therefore the record count is always 1
  int record_count(Context *, int) const {
    return 1;
  }

  // Returns the record size
  uint64_t record_size(Context *, int, int = 0) const {
    return sizeof(uint64_t);
  }

  // Returns the full record and stores it in |dest|; memory must be
  // allocated by the caller
  void record(Context *, int slot, ByteArray *arena, ups_record_t *record,
                  uint32_t flags, int) const {
    bool direct_access = ISSET(flags, UPS_DIRECT_ACCESS);

    // the record is stored inline
    record->size = sizeof(uint64_t);

    if (direct_access)
      record->data = (void *)&range_data[slot];
    else {
      if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
        arena->resize(record->size);
        record->data = arena->data();
      }
      ::memcpy(record->data, &range_data[slot], record->size);
    }
  }

  // Updates the record of a key
  void set_record(Context *, int slot, int, ups_record_t *record,
                  uint32_t flags, uint32_t * = 0) {
    assert(record->size == sizeof(uint64_t));
    range_data[slot] = *(uint64_t *)record->data;
  }

  // Erases the record
  void erase_record(Context *, int slot, int = 0, bool = true) {
    range_data[slot] = 0;
  }

  // Erases a whole slot by shifting all larger records to the "left"
  void erase(Context *context, size_t node_count, int slot) {
    if (likely(slot < (int)node_count - 1))
      ::memmove(&range_data[slot], &range_data[slot + 1],
                    sizeof(uint64_t) * (node_count - slot - 1));
  }

  // Creates space for one additional record
  void insert(Context *context, size_t node_count, int slot) {
    if (slot < (int)node_count)
      ::memmove(&range_data[slot + 1], &range_data[slot],
                     sizeof(uint64_t) * (node_count - slot));
    range_data[slot] = 0;
  }

  // Copies |count| records from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, InternalRecordList &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest.range_data[dstart], &range_data[sstart],
                    sizeof(uint64_t) * (node_count - sstart));
  }

  // Sets the record id
  void set_record_id(int slot, uint64_t value) {
    assert(inmemory ? 1 : value % page_size == 0);
    range_data[slot] = inmemory ? value : value / page_size;
  }

  // Returns the record id
  uint64_t record_id(int slot, int = 0) const {
    return inmemory ? range_data[slot] : page_size * range_data[slot];
  }

  // Returns true if there's not enough space for another record
  bool requires_split(size_t node_count) const {
    return (node_count + 1) * sizeof(uint64_t)
            >= range_data.size * sizeof(uint64_t);
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
              size_t new_range_size, size_t capacity_hint) {
    if ((uint64_t *)new_data_ptr != range_data.data) {
      ::memmove(new_data_ptr, range_data.data, node_count * sizeof(uint64_t));
      range_data = ArrayView<uint64_t>((uint64_t *)new_data_ptr,
                      new_range_size / 8);
    }
    range_size = new_range_size;
  }

  // Iterates all records, calls the |visitor| on each
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseRecordList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                        range_data.size * sizeof(uint64_t)
                            - required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    out << "(" << record_id(slot) << ")";
  }

  // The record data is an array of page IDs
  ArrayView<uint64_t> range_data;

  // The page size
  size_t page_size;

  // Store page ID % page size or the raw page ID?
  bool inmemory;
};

} // namespace upscaledb

#endif // UPS_BTREE_RECORDS_INTERNAL_H
