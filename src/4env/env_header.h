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
 * @exception_safe: unknown
 * @thread_safe: unknown
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

namespace hamsterdb {

#include "1base/packstart.h"

/*
 * the persistent file header
 */
typedef UPS_PACK_0 struct UPS_PACK_1
{
  // magic cookie - always "ham\0"
  uint8_t  magic[4];

  // version information - major, minor, rev, file
  uint8_t  version[4];

  // reserved
  uint64_t _reserved1;

  // size of the page
  uint32_t page_size;

  // maximum number of databases in this environment
  uint16_t max_databases;

  // PRO: for storing journal compression algorithm
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

class EnvironmentHeader
{
  public:
    // Constructor
    EnvironmentHeader(Page *page)
      : m_header_page(page) {
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
        return (false);
      if (header()->magic[1] != m2)
        return (false);
      if (header()->magic[2] != m3)
        return (false);
      if (header()->magic[3] != m4)
        return (false);
      return (true);
    }

    // Returns byte |i| of the 'version'-header
    uint8_t version(int i) {
      return (header()->version[i]);
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
      return (header()->max_databases);
    }

    // Sets the maximum number of databases for this file
    void set_max_databases(uint16_t max_databases) {
      header()->max_databases = max_databases;
    }

    // Returns the page size from the header page
    uint32_t page_size() {
      return (header()->page_size);
    }

    // Sets the page size in the header page
    void set_page_size(uint32_t page_size) {
      header()->page_size = page_size;
    }

    // Returns the PageManager's blob id
    uint64_t page_manager_blobid() {
      return (header()->page_manager_blobid);
    }

    // Sets the page size in the header page
    void set_page_manager_blobid(uint64_t blobid) {
      header()->page_manager_blobid = blobid;
    }

    // Returns the Journal compression configuration
    int journal_compression() {
      return (header()->journal_compression >> 4);
    }

    // Sets the Journal compression configuration
    void set_journal_compression(int algorithm) {
      header()->journal_compression = algorithm << 4;
    }

    // Returns the header page with persistent configuration settings
    Page *header_page() {
      return (m_header_page);
    }

  private:
    // Returns a pointer to the header data
    PEnvironmentHeader *header() {
      return ((PEnvironmentHeader *)(m_header_page->get_payload()));
    }

    // The header page of the Environment
    Page *m_header_page;
};

} // namespace hamsterdb

#endif /* UPS_ENV_HEADER_H */
