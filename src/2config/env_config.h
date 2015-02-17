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
 * The configuration settings of an Environment.
 *
 * @exception_safe nothrow
 * @thread_safe no
 */

#ifndef HAM_ENV_CONFIG_H
#define HAM_ENV_CONFIG_H

#include "0root/root.h"

#include <string>
#include <limits>

#include <ham/hamsterdb.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

#undef max

namespace hamsterdb {

struct EnvironmentConfiguration
{
  // Constructor initializes with default values
  EnvironmentConfiguration()
    : flags(0), file_mode(0644), max_databases(0),
      page_size_bytes(HAM_DEFAULT_PAGE_SIZE),
      cache_size_bytes(HAM_DEFAULT_CACHE_SIZE),
      file_size_limit_bytes(std::numeric_limits<size_t>::max()), 
      remote_timeout_sec(0), journal_compressor(0),
      is_encryption_enabled(false), journal_switch_threshold(0),
      posix_advice(HAM_POSIX_FADVICE_NORMAL) {
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

} // namespace hamsterdb

#endif // HAM_ENV_CONFIG_H
