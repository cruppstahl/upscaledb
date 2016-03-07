/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

/*
 * RecordList for Inline Records
 *
 * Inline Records are records that are stored directly in the leaf node, and
 * not in an external blob. Only for fixed length records.
 */

#ifndef UPS_BTREE_RECORDS_INLINE_H
#define UPS_BTREE_RECORDS_INLINE_H

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

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with btree_impl_default.h
//
namespace PaxLayout {

struct InlineRecordList : public BaseRecordList
{
  enum {
    // A flag whether this RecordList has sequential data
    kHasSequentialData = 1,

    // A flag whether this RecordList supports the scan() call
    kSupportsBlockScans = 1,
  };

  // Constructor
  InlineRecordList(LocalDatabase *db, PBtreeNode *)
    : record_size_(db->config().record_size) {
    assert(record_size_ != UPS_RECORD_SIZE_UNLIMITED);
  }

  // Sets the data pointer
  void create(uint8_t *ptr, size_t range_size) {
    data = ByteArrayView(ptr, range_size);
    m_range_size = range_size;
  }

  // Opens an existing RecordList
  void open(uint8_t *ptr, size_t range_size, size_t node_count) {
    data = ByteArrayView(ptr, range_size);
    m_range_size = range_size;
  }

  // Returns the actual record size including overhead
  size_t full_record_size() const {
    return record_size_;
  }

  // Calculates the required size for a range with the specified |capacity|
  size_t required_range_size(size_t node_count) const {
    return node_count * record_size_;
  }

  // Returns the record counter of a key
  int record_count(Context *, int) const {
    return 1;
  }

  // Returns the record size
  uint32_t record_size(Context *, int, int = 0) const {
    return record_size_;
  }

  // Returns the full record and stores it in |dest|; memory must be
  // allocated by the caller
  void record(Context *, int slot, ByteArray *arena, ups_record_t *record,
                  uint32_t flags, int) const {
    bool direct_access = isset(flags, UPS_DIRECT_ACCESS);

    // the record is stored inline
    record->size = record_size_;

    if (record_size_ == 0)
      record->data = 0;
    else if (direct_access)
      record->data = (void *)&data[slot * record_size_];
    else {
      if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
        arena->resize(record->size);
        record->data = arena->data();
      }
      ::memcpy(record->data, &data[slot * record_size_], record->size);
    }
  }

  // Updates the record of a key
  void set_record(Context *, int slot, int, ups_record_t *record,
                  uint32_t flags, uint32_t * = 0) {
    assert(record->size == record_size_);
    // it's possible that the records have size 0 - then don't copy anything
    if (record_size_)
      ::memcpy(&data[record_size_ * slot], record->data, record_size_);
  }

  // Iterates all records, calls the |visitor| on each
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    return std::make_pair(&data[record_size_ * start], node_count - start);
  }

  // Erases the record
  void erase_record(Context *, int slot, int = 0, bool = true) {
    if (record_size_)
      ::memset(&data[record_size_ * slot], 0, record_size_);
  }

  // Erases a whole slot by shifting all larger records to the "left"
  void erase(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&data[record_size_ * slot],
                      &data[record_size_ * (slot + 1)],
                      record_size_ * (node_count - slot - 1));
  }

  // Creates space for one additional record
  void insert(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count) {
      ::memmove(&data[record_size_ * (slot + 1)],
                      &data[record_size_ * slot],
                      record_size_ * (node_count - slot));
    }
    ::memset(&data[record_size_ * slot], 0, record_size_);
  }

  // Copies |count| records from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, InlineRecordList &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest.data[record_size_ * dstart],
                    &data[record_size_ * sstart],
                    record_size_ * (node_count - sstart));
  }

  // Returns true if there's not enough space for another record
  bool requires_split(size_t node_count) const {
    if (data.size == 0)
      return false;
    return (node_count + 1) * record_size_ >= data.size;
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                  size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, data.data, node_count * record_size_);
    data = ByteArrayView(new_data_ptr, new_range_size);
    m_range_size = new_range_size;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseRecordList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                        data.size - required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *, int, std::stringstream &out) const {
    out << "(" << record_size_ << " bytes)";
  }

  // The record size, as specified when the database was created
  size_t record_size_;

  // The actual record data
  ByteArrayView data;
};

} // namespace PaxLayout

} // namespace upscaledb

#endif /* UPS_BTREE_RECORDS_INLINE_H */
