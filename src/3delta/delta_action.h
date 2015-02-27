/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_DELTA_ACTION_H
#define HAM_DELTA_ACTION_H

#include "0root/root.h"

#include <string.h>

#include <ham/hamsterdb.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class DeltaUpdate;

//
// A DeltaAction describes a single operation on a key
//
class DeltaAction
{
  public:
    enum {
      // An Insert operation
      kInsert           = 0x000001u,

      // An Insert/Overwrite operation
      kInsertOverwrite  = 0x000002u,

      // An Insert/Duplicate operation
      kInsertDuplicate  = 0x000004u,

      // An Erase operation
      kErase            = 0x000008u,

      // This DeltaUpdate belongs to a committed Transaction
      kIsCommitted      = 0x000100u,

      // This DeltaUpdate belongs to an aborted Transaction
      kIsAborted        = 0x000200u,
    };

    // Returns the record (if available)
    ham_record_t *record() {
      return (&m_record);
    }

    // Returns the flags
    uint32_t flags() const {
      return (m_flags);
    }

    // Sets the flags
    void set_flags(uint32_t flags) {
      m_flags = flags;
    }

    // Returns the original flags of ham_insert/ham_cursor_insert/ham_erase...
    uint32_t original_flags() const {
      return (m_original_flags);
    }

    // Returns the referenced duplicate id
    int referenced_duplicate() const {
      return (m_referenced_duplicate);
    }

    // Sets the referenced duplicate id
    void set_referenced_duplicate(int id) {
      m_referenced_duplicate = id;
    }

    // Returns the txn-id of this operation
    uint64_t txn_id() const {
      return (m_txn_id);
    }

    // Returns the lsn of this operation
    uint64_t lsn() const {
      return (m_lsn);
    }

    // Returns the next item in the linked list of DeltaActions
    DeltaAction *next() {
      return (m_next);
    }

    // Sets the next item in the linked list of DeltaActions
    void set_next(DeltaAction *next) {
      m_next = next;
    }

    // Returns the DeltaUpdate which "owns" this action
    DeltaUpdate *delta_update() {
      return (m_delta_update);
    }

    // Returns the accumulated data size of this action
    size_t data_size() const;

  private:
    friend class DeltaUpdateFactory;

    // Initialization
    void initialize(DeltaUpdate *update, uint64_t txn_id, uint64_t lsn,
                    uint32_t flags, uint32_t original_flags,
                    int referenced_duplicate, ham_record_t *record) {
      m_delta_update = update;
      m_txn_id = txn_id;
      m_flags = flags;
      m_lsn = lsn;
      m_original_flags = original_flags;
      m_referenced_duplicate = referenced_duplicate;
      m_next = 0;

      /* copy the record data */
      if (record) {
        m_record = *record;
        if (record->size) {
          m_record.data = &m_data[0];
          ::memcpy(m_record.data, record->data, record->size);
        }
      }
    }

    // the transaction id
    uint64_t m_txn_id;

    // the log serial number (lsn) of this operation
    uint64_t m_lsn;

    // flags and type of this operation; defined in this file
    uint32_t m_flags;

    // the original flags of this operation, used when calling
    // ham_cursor_insert, ham_insert, ham_erase etc
    uint32_t m_original_flags;

    // the referenced duplicate id (if neccessary) - used if this is
    // i.e. a ham_cursor_erase, ham_cursor_overwrite or ham_cursor_insert
    // with a DUPLICATE_AFTER/BEFORE flag
    // -1 if unused
    int m_referenced_duplicate;

    // Pointer to next element in a linked list
    DeltaAction *m_next;

    // The "owner" of this action
    DeltaUpdate *m_delta_update;

    // the record which is inserted or overwritten
    ham_record_t m_record;

    // Storage for record->data. This saves us one memory allocation.
    uint8_t m_data[1];
};

} // namespace hamsterdb

#endif /* HAM_DELTA_ACTION_H */
