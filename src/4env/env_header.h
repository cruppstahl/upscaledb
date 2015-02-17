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

#ifndef HAM_ENV_HEADER_H
#define HAM_ENV_HEADER_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "2page/page.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

#include "1base/packstart.h"

/**
 * the persistent file header
 */
typedef HAM_PACK_0 struct HAM_PACK_1
{
  /** magic cookie - always "ham\0" */
  uint8_t  _magic[4];

  /** version information - major, minor, rev, file */
  uint8_t  _version[4];

  /** reserved */
  uint64_t _reserved1;

  /** size of the page */
  uint32_t _page_size;

  /** maximum number of databases for this environment */
  uint16_t _max_databases;

  /** PRO: for storing journal compression algorithm */
  uint8_t _journal_compression;

  /** reserved */
  uint8_t _reserved3;

  /** blob id of the PageManager's state */
  uint64_t _page_manager_blobid;

  /*
   * following here:
   *
   * 1. the private data of the index btree(s)
   *      -> see get_btree_header()
   */
} HAM_PACK_2 PEnvironmentHeader;

#include "1base/packstop.h"

class EnvironmentHeader
{
  public:
    // Constructor
    EnvironmentHeader(Page *page)
      : m_header_page(page) {
    }

    // Sets the 'magic' field of a file header
    void set_magic(uint8_t m1, uint8_t m2, uint8_t m3, uint8_t m4) {
      get_header()->_magic[0] = m1;
      get_header()->_magic[1] = m2;
      get_header()->_magic[2] = m3;
      get_header()->_magic[3] = m4;
    }

    // Returns true if the magic matches
    bool verify_magic(uint8_t m1, uint8_t m2, uint8_t m3, uint8_t m4) {
      if (get_header()->_magic[0] != m1)
        return (false);
      if (get_header()->_magic[1] != m2)
        return (false);
      if (get_header()->_magic[2] != m3)
        return (false);
      if (get_header()->_magic[3] != m4)
        return (false);
      return (true);
    }

    // Returns byte |i| of the 'version'-header
    uint8_t get_version(int i) {
      return (get_header()->_version[i]);
    }

    // Sets the version of a file header
    void set_version(uint8_t major, uint8_t minor, uint8_t revision,
                    uint8_t file) {
      get_header()->_version[0] = major;
      get_header()->_version[1] = minor;
      get_header()->_version[2] = revision;
      get_header()->_version[3] = file;
    }

    // Returns get the maximum number of databases for this file
    uint16_t get_max_databases() {
      return (get_header()->_max_databases);
    }

    // Sets the maximum number of databases for this file
    void set_max_databases(uint16_t max_databases) {
      get_header()->_max_databases = max_databases;
    }

    // Returns the page size from the header page
    uint32_t page_size() {
      return (get_header()->_page_size);
    }

    // Sets the page size in the header page
    void set_page_size(uint32_t page_size) {
      get_header()->_page_size = page_size;
    }

    // Returns the PageManager's blob id
    uint64_t get_page_manager_blobid() {
      return (get_header()->_page_manager_blobid);
    }

    // Sets the page size in the header page
    void set_page_manager_blobid(uint64_t blobid) {
      get_header()->_page_manager_blobid = blobid;
    }

    // Returns the Journal compression configuration
    int get_journal_compression(int *level) {
      *level = get_header()->_journal_compression & 0x0f;
      return (get_header()->_journal_compression >> 4);
    }

    // Sets the Journal compression configuration
    void set_journal_compression(int algorithm, int level) {
      get_header()->_journal_compression = (algorithm << 4) | level;
    }

    // Returns the header page with persistent configuration settings
    Page *get_header_page() {
      return (m_header_page);
    }

  private:
    // Returns a pointer to the header data
    PEnvironmentHeader *get_header() {
      return ((PEnvironmentHeader *)(m_header_page->get_payload()));
    }

    // The header page of the Environment
    Page *m_header_page;
};

} // namespace hamsterdb

#endif /* HAM_ENV_HEADER_H */
