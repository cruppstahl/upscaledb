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
 * The DefaultRecordList provides simplified access to a list of records,
 * where each record is either a 8-byte record identifier (specifying the
 * address of a blob) or is stored inline, if the record's size is <= 8 bytes.
 *
 * Stores 1 byte of flags per record (see btree_flags.h).
 */

#ifndef UPS_BTREE_RECORDS_DEFAULT_H
#define UPS_BTREE_RECORDS_DEFAULT_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1base/array_view.h"
#include "1base/dynamic_array.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_records_base.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct DefaultRecordList : BaseRecordList {
  enum {
    // A flag whether this RecordList has sequential data
    kHasSequentialData = 1
  };

  // Constructor
  DefaultRecordList(LocalDb *db, PBtreeNode *node)
    : BaseRecordList(db, node),
      is_record_size_unlimited(db->config.record_size
                                  == UPS_RECORD_SIZE_UNLIMITED), flags(0) {
    LocalEnv *env = (LocalEnv *)db->env;
    blob_manager = env->blob_manager.get();
  }

  // Sets the data pointer; required for initialization
  void create(uint8_t *ptr, size_t range_size_) {
    size_t capacity = range_size_ / full_record_size();
    range_size = range_size_;

    if (is_record_size_unlimited) {
      flags = ptr;
      data = ArrayView<uint64_t>((uint64_t *)&ptr[capacity], capacity);
    }
    else {
      flags = 0;
      data = ArrayView<uint64_t>((uint64_t *)ptr, capacity);
    }
  }

  // Opens an existing RecordList
  void open(uint8_t *ptr, size_t range_size_, size_t node_count) {
    size_t capacity = range_size_ / full_record_size();
    range_size = range_size_;

    if (is_record_size_unlimited) {
      flags = ptr;
      data = ArrayView<uint64_t>((uint64_t *)&ptr[capacity], capacity);
    }
    else {
      flags = 0;
      data = ArrayView<uint64_t>((uint64_t *)ptr, capacity);
    }
  }

  // Calculates the required size for a range
  size_t required_range_size(size_t node_count) {
    return node_count * full_record_size();
  }

  // Returns the actual record size including overhead
  size_t full_record_size() const {
    return sizeof(uint64_t) + (is_record_size_unlimited ? 1 : 0);
  }

  // Returns the record counter of a key
  int record_count(Context *, int slot) const {
    if (unlikely(!is_record_inline(slot) && record_id(slot) == 0))
      return 0;
    return 1;
  }

  // Returns the record size
  uint32_t record_size(Context *context, int slot, int = 0) const {
    if (is_record_inline(slot))
      return inline_record_size(slot);
    else
      return blob_manager->blob_size(context, record_id(slot));
  }

  // Returns the full record and stores it in |dest|; memory must be
  // allocated by the caller
  void record(Context *context, int slot, ByteArray *arena,
                  ups_record_t *record, uint32_t flags,
                  int duplicate_index) const {
    bool direct_access = ISSET(flags, UPS_DIRECT_ACCESS);

    // the record is stored inline
    if (is_record_inline(slot)) {
      record->size = inline_record_size(slot);
      if (record->size == 0) {
        record->data = 0;
        return;
      }
      if (direct_access)
        record->data = (void *)&data[slot];
      else {
        if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
          arena->resize(record->size);
          record->data = arena->data();
        }
        ::memcpy(record->data, &data[slot], record->size);
      }
      return;
    }

    // still here? then the record is stored as a blob
    blob_manager->read(context, record_id(slot), record, flags, arena);
  }

  // Updates the record of a key
  void set_record(Context *context, int slot, int,
              ups_record_t *record, uint32_t flags, uint32_t * = 0) {
    uint64_t ptr = record_id(slot);

    // key does not yet exist
    if (!ptr && !is_record_inline(slot)) {
      if (record->size <= sizeof(uint64_t))
        set_record_data(slot, record->data, record->size);
      else
        set_record_id(slot, blob_manager->allocate(context, record, flags));
      return;
    }

    // an inline key exists and will be overwritten
    if (is_record_inline(slot)) {
      // disable small/tiny/empty flags
      set_record_flags(slot, record_flags(slot)
                                & ~(BtreeRecord::kBlobSizeSmall
                                      | BtreeRecord::kBlobSizeTiny
                                      | BtreeRecord::kBlobSizeEmpty));
      if (record->size <= sizeof(uint64_t))
        set_record_data(slot, record->data, record->size);
      else
        set_record_id(slot, blob_manager->allocate(context, record, flags));
      return;
    }

    // a (non-inline) key exists and will be overwritten
    if (ptr) {
      if (record->size <= sizeof(uint64_t)) {
        blob_manager->erase(context, ptr);
        set_record_data(slot, record->data, record->size);
      }
      else {
        ptr = blob_manager->overwrite(context, ptr, record, flags);
        set_record_id(slot, ptr);
      }
      return;
    }

    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  // Erases the record
  void erase_record(Context *context, int slot, int = 0, bool = true) {
    if (is_record_inline(slot)) {
      remove_inline_record(slot);
    }
    else {
      blob_manager->erase(context, record_id(slot), 0);
      set_record_id(slot, 0);
    }
  }

  // Erases a whole slot by shifting all larger records to the "left"
  void erase(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count - 1) {
      if (flags)
        ::memmove(&flags[slot], &flags[slot + 1], node_count - slot - 1);
      ::memmove(&data[slot], &data[slot + 1],
                      sizeof(uint64_t) * (node_count - slot - 1));
    }
  }

  // Creates space for one additional record
  void insert(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count) {
      if (flags)
        ::memmove(&flags[slot + 1], &flags[slot], node_count - slot);
      ::memmove(&data[slot + 1], &data[slot],
                     sizeof(uint64_t) * (node_count - slot));
    }
    if (flags)
      flags[slot] = 0;
    data[slot] = 0;
  }

  // Copies |count| records from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, DefaultRecordList &dest,
                  size_t other_count, int dstart) {
    if (flags)
      ::memcpy(&dest.flags[dstart], &flags[sstart], (node_count - sstart));
    ::memcpy(&dest.data[dstart], &data[sstart],
                    sizeof(uint64_t) * (node_count - sstart));
  }

  // Sets the record id
  void set_record_id(int slot, uint64_t ptr) {
    data[slot] = ptr;
  }

  // Returns the record id
  uint64_t record_id(int slot, int duplicate_index = 0) const {
    return data[slot];
  }

  // Returns true if there's not enough space for another record
  bool requires_split(size_t node_count) const {
    return (node_count + 1) * full_record_size() >= range_size;
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
          size_t new_range_size, size_t capacity_hint) {
    size_t new_capacity = capacity_hint
                            ? capacity_hint
                            : new_range_size / full_record_size();
    // shift "to the right"? then first shift key data, otherwise
    // the flags might overwrite the data
    if (flags == 0) {
      ::memmove(new_data_ptr, data.data, node_count * sizeof(uint64_t));
    }
    else {
      if (new_data_ptr > flags) {
        ::memmove(&new_data_ptr[new_capacity], data.data,
                node_count * sizeof(uint64_t));
        ::memmove(new_data_ptr, flags, node_count);
      }
      else {
        ::memmove(new_data_ptr, flags, node_count);
        ::memmove(&new_data_ptr[new_capacity], data.data,
                node_count * sizeof(uint64_t));
      }
    }

    if (is_record_size_unlimited) {
      flags = new_data_ptr;
      data = ArrayView<uint64_t>((uint64_t *)&new_data_ptr[new_capacity],
                      new_capacity);
    }
    else {
      flags = 0;
      data = ArrayView<uint64_t>((uint64_t *)new_data_ptr, new_capacity);
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
                        range_size - required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    out << "(" << record_size(context, slot) << " bytes)";
  }

  // Implementation follows below...

  // Sets record data
  void set_record_data(int slot, const void *ptr, size_t size) {
    uint8_t flags = record_flags(slot);
    flags &= ~(BtreeRecord::kBlobSizeSmall
                    | BtreeRecord::kBlobSizeTiny
                    | BtreeRecord::kBlobSizeEmpty);

    if (size == 0) {
      data[slot] = 0;
      set_record_flags(slot, flags | BtreeRecord::kBlobSizeEmpty);
    }
    else if (size < 8) {
      /* the highest byte of the record id is the size of the blob */
      char *p = (char *)&data[slot];
      p[sizeof(uint64_t) - 1] = size;
      ::memcpy(&data[slot], ptr, size);
      set_record_flags(slot, flags | BtreeRecord::kBlobSizeTiny);
    }
    else if (size == 8) {
      ::memcpy(&data[slot], ptr, size);
      set_record_flags(slot, flags | BtreeRecord::kBlobSizeSmall);
    }
    else {
      assert(!"shouldn't be here");
      set_record_flags(slot, flags);
    }
  }

  // Returns the record flags of a given |slot|
  uint8_t record_flags(int slot, int duplicate_index = 0)
                  const {
    return flags ? flags[slot] : 0;
  }

  // Sets the record flags of a given |slot|
  void set_record_flags(int slot, uint8_t f) {
    assert(flags != 0);
    flags[slot] = f;
  }

  // Returns the size of an inline record
  uint32_t inline_record_size(int slot) const {
    uint8_t flags = record_flags(slot);
    assert(is_record_inline(slot));
    if (ISSET(flags, BtreeRecord::kBlobSizeTiny)) {
      /* the highest byte of the record id is the size of the blob */
      char *p = (char *)&data[slot];
      return p[sizeof(uint64_t) - 1];
    }
    if (ISSET(flags, BtreeRecord::kBlobSizeSmall))
      return sizeof(uint64_t);
    if (ISSET(flags, BtreeRecord::kBlobSizeEmpty))
      return 0;
    assert(!"shouldn't be here");
    return 0;
  }

  // Returns true if the record is inline, false if the record is a blob
  bool is_record_inline(int slot) const {
    uint8_t flags = record_flags(slot);
    return ISSETANY(flags, BtreeRecord::kBlobSizeTiny
                                | BtreeRecord::kBlobSizeSmall
                                | BtreeRecord::kBlobSizeEmpty);
  }

  // Removes an inline record; returns the updated record flags
  void remove_inline_record(int slot) {
    uint8_t flags = record_flags(slot);
    data[slot] = 0;
    set_record_flags(slot,
                    flags & ~(BtreeRecord::kBlobSizeSmall
                                | BtreeRecord::kBlobSizeTiny
                                | BtreeRecord::kBlobSizeEmpty));
  }

  // The blob manager - allocates and frees blobs
  BlobManager *blob_manager;

  // True if the record size is unlimited
  bool is_record_size_unlimited;

  // Pointer to the record flags - only used if record size is unlimited
  uint8_t *flags;

  // The actual record data - an array of 64bit record IDs
  ArrayView<uint64_t> data;
};

} // namespace upscaledb

#endif // UPS_BTREE_RECORDS_DEFAULT_H
