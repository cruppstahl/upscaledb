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
 * journal entries for insert, erase, begin, commit, abort...
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef UPS_JOURNAL_ENTRIES_H
#define UPS_JOURNAL_ENTRIES_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

#include "1base/packstart.h"

/*
 * A journal entry for all txn related operations (begin, commit, abort)
 *
 * This structure can be followed by one of the structures below
 * (PJournalEntryInsert or PJournalEntryERASE); the field |followup_size|
 * is the structure size of this follow-up structure.
 */
UPS_PACK_0 struct UPS_PACK_1 PJournalEntry {
  // Constructor - sets all fields to 0
  PJournalEntry()
    : lsn(0), followup_size(0), txn_id(0), type(0),
        dbname(0), _reserved(0) {
  }

  // the lsn of this entry
  uint64_t lsn;

  // the size of the follow-up entry in bytes (may be padded)
  uint64_t followup_size;

  // the transaction id
  uint64_t txn_id;

  // the type of this entry
  uint32_t type;

  // the name of the database which is modified by this entry
  uint16_t dbname;

  // a reserved value - reqd for padding
  uint16_t _reserved;
} UPS_PACK_2;

#include "1base/packstop.h"


#include "1base/packstart.h"

//
// a Journal entry for an 'insert' operation
//
UPS_PACK_0 struct UPS_PACK_1 PJournalEntryInsert {
  // Constructor - sets all fields to 0
  PJournalEntryInsert()
    : key_size(0), compressed_key_size(0), record_size(0),
      compressed_record_size(0), insert_flags(0) {
    data[0] = 0;
  }

  // key size
  uint16_t key_size;

  // compressed key size
  uint16_t compressed_key_size;

  // record size
  uint32_t record_size;

  // compressed record size
  uint32_t compressed_record_size;

  // flags of ups_insert(), ups_cursor_insert()
  uint32_t insert_flags;

  // data follows here - first |key_size| bytes for the key, then
  // |record_size| bytes for the record (and maybe some padding)
  //
  // this data can be compressed
  uint8_t data[1];

  // Returns a pointer to the key data
  uint8_t *key_data() {
    return &data[0];
  }

  // Returns a pointer to the record data
  uint8_t *record_data() {
    return &data[key_size];
  }
} UPS_PACK_2;

#include "1base/packstop.h"


#include "1base/packstart.h"

//
// a Journal entry for 'erase' operations
//
UPS_PACK_0 struct UPS_PACK_1 PJournalEntryErase {
  // Constructor - sets all fields to 0
  PJournalEntryErase()
    : key_size(0), compressed_key_size(0), erase_flags(0), duplicate(0) {
    data[0] = 0;
  }

  // key size
  uint16_t key_size;

  // compressed key size
  uint16_t compressed_key_size;

  // flags of ups_erase(), ups_cursor_erase()
  uint32_t erase_flags;

  // which duplicate to erase
  int duplicate;

  // the key data
  //
  // this data can be compressed
  uint8_t data[1];

  // Returns a pointer to the key data
  uint8_t *key_data() {
    return &data[0];
  }
} UPS_PACK_2;

#include "1base/packstop.h"


#include "1base/packstart.h"

//
// a Journal entry for a 'changeset' group
//
UPS_PACK_0 struct UPS_PACK_1 PJournalEntryChangeset {
  // Constructor - sets all fields to 0
  PJournalEntryChangeset()
    : num_pages(0), last_blob_page(0) {
  }

  // number of pages in this changeset
  uint32_t num_pages;

  // address of the last blob page
  uint64_t last_blob_page;
} UPS_PACK_2;

#include "1base/packstop.h"


#include "1base/packstart.h"

//
// a Journal entry for a single page
//
UPS_PACK_0 struct UPS_PACK_1 PJournalEntryPageHeader {
  // Constructor - sets all fields to 0
  PJournalEntryPageHeader(uint64_t _address = 0)
    : address(_address), compressed_size(0) {
  }

  // the page address
  uint64_t address;

  // the compressed size, if compression is enabled
  uint32_t compressed_size;
} UPS_PACK_2;

#include "1base/packstop.h"

} // namespace upscaledb

#endif /* UPS_JOURNAL_ENTRIES_H */
