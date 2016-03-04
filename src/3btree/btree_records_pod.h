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
#include "1globals/globals.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3btree/btree_records_base.h"
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with btree_impl_default.h
//
namespace PaxLayout {

template<typename PodType>
struct PodRecordList : public BaseRecordList
{
  enum {
    // A flag whether this RecordList has sequential data
    kHasSequentialData = 1
  };

  PodRecordList(LocalDatabase *, PBtreeNode *) {
  }

  // Sets the data pointer
  void create(uint8_t *data, size_t range_size) {
    m_data = ArrayView<PodType>((PodType *)data, range_size / sizeof(PodType));
    m_range_size = range_size;
  }

  // Opens an existing RecordList
  void open(uint8_t *data, size_t range_size, size_t node_count) {
    m_data = ArrayView<PodType>((PodType *)data, range_size / sizeof(PodType));
    m_range_size = range_size;
  }

  // Returns the actual record size including overhead
  size_t full_record_size() const {
    return sizeof(PodType);
  }

  // Calculates the required size for a range with the specified |capacity|
  size_t required_range_size(size_t node_count) const {
    return node_count * sizeof(PodType);
  }

  // Returns the record counter of a key
  // This record list does not support duplicates, therefore always return 1
  int record_count(Context *, int) const {
    return 1;
  }

  // Returns the record size
  uint32_t record_size(Context *, int, int = 0) const {
    return sizeof(PodType);
  }

  // Returns the full record and stores it in |dest|; memory must be
  // allocated by the caller
  void record(Context *, int slot, ByteArray *arena, ups_record_t *record,
                  uint32_t flags, int) const {
    record->size = sizeof(PodType);

    if (unlikely(isset(flags, UPS_DIRECT_ACCESS))) {
      record->data = (void *)&m_data[slot];
      return;
    }

    if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
      arena->resize(record->size);
      record->data = arena->data();
    }

    ::memcpy(record->data, &m_data[slot], record->size);
  }

  // Updates the record of a key
  void set_record(Context *, int slot, int, ups_record_t *record,
                  uint32_t flags, uint32_t * = 0) {
    assert(record->size == sizeof(PodType));
    m_data[slot] = *(PodType *)record->data;
  }

  // Erases the record by nulling it
  void erase_record(Context *, int slot, int = 0, bool = true) {
    m_data[slot] = 0;
  }

  // Erases a whole slot by shifting all larger records to the "left"
  void erase(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&m_data[slot], &m_data[slot + 1],
                      sizeof(PodType) * (node_count - slot - 1));
  }

  // Creates space for one additional record
  void insert(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count) {
      ::memmove(&m_data[(slot + 1)], &m_data[slot],
                      sizeof(PodType) * (node_count - slot));
    }
    m_data[slot] = 0;
  }

  // Copies |count| records from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, PodRecordList<PodType> &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest.m_data[dstart], &m_data[sstart],
                    sizeof(PodType) * (node_count - sstart));
  }

  // Returns true if there's not enough space for another record
  bool requires_split(size_t node_count) const {
    if (unlikely(m_range_size == 0))
      return false;
    return (node_count + 1) * sizeof(PodType) >= m_range_size;
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                  size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, m_data.data, node_count * sizeof(PodType));
    m_range_size = new_range_size;
    m_data = ArrayView<PodType>((PodType *)new_data_ptr,
                    new_range_size / sizeof(PodType));
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseRecordList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                        m_range_size - required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    out << m_data[slot];
  }

  // The actual record data
  ArrayView<PodType> m_data;
};

} // namespace PaxLayout

} // namespace upscaledb

#endif /* UPS_BTREE_RECORDS_POD_H */
