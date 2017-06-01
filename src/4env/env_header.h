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

#ifndef UPS_ENV_HEADER_H
#define UPS_ENV_HEADER_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "2page/page.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

#include "1base/packstart.h"

/*
 * the persistent file header
 */
typedef UPS_PACK_0 struct UPS_PACK_1
{
  // magic cookie - always "ham\0"
  uint8_t magic[4];

  // version information - major, minor, rev, file
  uint8_t version[4];

  // reserved
  uint64_t _reserved1;

  // size of the page
  uint32_t page_size;

  // maximum number of databases in this environment
  uint16_t max_databases;

  // for storing journal compression algorithm
  uint8_t journal_compression;

  // reserved
  uint8_t _reserved2;

  // blob id of the PageManager's state
  uint64_t page_manager_blobid;

  /*
   * following here:
   *
   * 1. the private data of the index btree(s)
   *      -> see get_btree_header()
   */
} UPS_PACK_2 PEnvironmentHeader;

#include "1base/packstop.h"

struct EnvHeader
{
  // Constructor
  EnvHeader(Page *page)
    : header_page(page) {
  }

  // Sets the 'magic' field of a file header
  void set_magic(uint8_t m1, uint8_t m2, uint8_t m3, uint8_t m4) {
    header()->magic[0] = m1;
    header()->magic[1] = m2;
    header()->magic[2] = m3;
    header()->magic[3] = m4;
  }

  // Returns true if the magic matches
  bool verify_magic(uint8_t m1, uint8_t m2, uint8_t m3, uint8_t m4) {
    if (header()->magic[0] != m1)
      return false;
    if (header()->magic[1] != m2)
      return false;
    if (header()->magic[2] != m3)
      return false;
    if (header()->magic[3] != m4)
      return false;
    return true;
  }

  // Returns byte |i| of the 'version'-header
  uint8_t version(int i) {
    return header()->version[i];
  }

  // Sets the version of a file header
  void set_version(uint8_t major, uint8_t minor, uint8_t revision,
                  uint8_t file) {
    header()->version[0] = major;
    header()->version[1] = minor;
    header()->version[2] = revision;
    header()->version[3] = file;
  }

  // Returns get the maximum number of databases for this file
  uint16_t max_databases() {
    return header()->max_databases;
  }

  // Sets the maximum number of databases for this file
  void set_max_databases(uint16_t max_databases) {
    header()->max_databases = max_databases;
  }

  // Returns the page size from the header page
  uint32_t page_size() {
    return header()->page_size;
  }

  // Sets the page size in the header page
  void set_page_size(uint32_t page_size) {
    header()->page_size = page_size;
  }

  // Returns the PageManager's blob id
  uint64_t page_manager_blobid() {
    return header()->page_manager_blobid;
  }

  // Sets the page size in the header page
  void set_page_manager_blobid(uint64_t blobid) {
    header()->page_manager_blobid = blobid;
  }

  // Returns the Journal compression configuration
  int journal_compression() {
    return header()->journal_compression >> 4;
  }

  // Sets the Journal compression configuration
  void set_journal_compression(int algorithm) {
    header()->journal_compression = algorithm << 4;
  }

  // Returns a pointer to the header data
  PEnvironmentHeader *header() {
    return (PEnvironmentHeader *)(header_page->payload());
  }

  // The header page of the Environment
  Page *header_page;
};

} // namespace upscaledb

#endif /* UPS_ENV_HEADER_H */
