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
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
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
class PodRecordList : public BaseRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    PodRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_data(0) {
    }

    // Sets the data pointer
    void create(uint8_t *data, size_t range_size) {
      m_data = (PodType *)data;
      m_range_size = range_size;
    }

    // Opens an existing RecordList
    void open(uint8_t *ptr, size_t range_size, size_t node_count) {
      m_data = (PodType *)ptr;
      m_range_size = range_size;
    }

    // Returns the actual record size including overhead
    size_t get_full_record_size() const {
      return (sizeof(PodType));
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t get_required_range_size(size_t node_count) const {
      return (node_count * sizeof(PodType));
    }

    // Returns the record counter of a key
    int get_record_count(Context *context, int slot) const {
      return (1);
    }

    // Returns the record size
    uint64_t get_record_size(Context *context, int slot,
                    int duplicate_index = 0) const {
      return (sizeof(PodType));
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(Context *context, int slot, ByteArray *arena,
                    ups_record_t *record, uint32_t flags,
                    int duplicate_index) const {
      if (unlikely(flags & UPS_PARTIAL)) {
        ups_trace(("flag UPS_PARTIAL is not allowed if record is "
                   "stored inline"));
        throw Exception(UPS_INV_PARAMETER);
      }

      record->size = sizeof(PodType);

      if (unlikely((flags & UPS_DIRECT_ACCESS) != 0)) {
        record->data = &m_data[slot];
        return;
      }

      if ((record->flags & UPS_RECORD_USER_ALLOC) == 0) {
        arena->resize(record->size);
        record->data = arena->data();
      }

      ::memcpy(record->data, &m_data[slot], record->size);
    }

    // Updates the record of a key
    void set_record(Context *context, int slot, int duplicate_index,
                ups_record_t *record, uint32_t flags,
                uint32_t *new_duplicate_index = 0) {
      assert(record->size == sizeof(PodType));
      m_data[slot] = *(PodType *)record->data;
    }

    // Erases the record by nulling it
    void erase_record(Context *context, int slot, int duplicate_index = 0,
                    bool all_duplicates = true) {
      m_data[slot] = 0;
    }

    // Erases a whole slot by shifting all larger records to the "left"
    void erase(Context *context, size_t node_count, int slot) {
      if (slot < (int)node_count - 1)
        ::memmove(&m_data[slot], &m_data[slot + 1],
                        sizeof(PodType) * (node_count - slot - 1));
    }

    // Creates space for one additional record
    void insert(Context *context, size_t node_count, int slot) {
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
      if (m_range_size == 0)
        return (false);
      return ((node_count + 1) * sizeof(PodType) >= m_range_size);
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                    size_t new_range_size, size_t capacity_hint) {
      ::memmove(new_data_ptr, m_data, node_count * sizeof(PodType));
      m_data = (PodType *)new_data_ptr;
      m_range_size = new_range_size;
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
      BaseRecordList::fill_metrics(metrics, node_count);
      BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                          m_range_size - get_required_range_size(node_count));
    }

    // Prints a slot to |out| (for debugging)
    void print(Context *context, int slot, std::stringstream &out) const {
      out << m_data[slot];
    }

  private:
    // The parent database of this btree
    LocalDatabase *m_db;

    // The actual record data
    PodType *m_data;
};

} // namespace PaxLayout

} // namespace upscaledb

#endif /* UPS_BTREE_RECORDS_POD_H */
