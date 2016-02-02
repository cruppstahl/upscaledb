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
 * The configuration settings of an Environment.
 *
 * @exception_safe nothrow
 * @thread_safe no
 */

#ifndef UPS_ENV_CONFIG_H
#define UPS_ENV_CONFIG_H

#include "0root/root.h"

#include <string>
#include <limits>

#include <ups/upscaledb.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

#undef max

namespace upscaledb {

struct EnvConfig
{
  // Constructor initializes with default values
  EnvConfig()
    : flags(0), file_mode(0644), max_databases(0),
      page_size_bytes(UPS_DEFAULT_PAGE_SIZE),
      cache_size_bytes(UPS_DEFAULT_CACHE_SIZE),
      file_size_limit_bytes(std::numeric_limits<size_t>::max()), 
      remote_timeout_sec(0), journal_compressor(0),
      is_encryption_enabled(false), journal_switch_threshold(0),
      posix_advice(UPS_POSIX_FADVICE_NORMAL) {
  }

  // the environment's flags
  uint32_t flags;

  // the file mode
  int file_mode;

  // the number of databases
  int max_databases;

  // the page size (in bytes)
  size_t page_size_bytes;

  // the cache size (in bytes)
  uint64_t cache_size_bytes;

  // the file size limit (in bytes)
  size_t file_size_limit_bytes;

  // the remote timeout (in seconds)
  size_t remote_timeout_sec;

  // the path (or remote location)
  std::string filename;

  // the path of the logfile
  std::string log_filename;

  // the algorithm for journal compression
  int journal_compressor;

  // true if AES encryption is enabled
  bool is_encryption_enabled;

  // the AES encryption key
  uint8_t encryption_key[16];

  // threshold for switching journal files
  size_t journal_switch_threshold;

  // parameter for posix_fadvise()
  int posix_advice;
};

} // namespace upscaledb

#endif // UPS_ENV_CONFIG_H
