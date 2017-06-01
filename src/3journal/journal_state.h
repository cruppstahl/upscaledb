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

#ifndef UPS_JOURNAL_STATE_H
#define UPS_JOURNAL_STATE_H

#include "0root/root.h"

#include <vector>
#include <string>

#include "ups/types.h" // for metrics

#include "1base/dynamic_array.h"
#include "1base/scoped_ptr.h"
#include "1os/file.h"
#include "2page/page_collection.h"
#include "2compressor/compressor.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Db;
struct LocalEnv;

struct JournalState {
  JournalState(LocalEnv *env_);

  // References the Environment this journal file is for
  LocalEnv *env;

  // The index of the file descriptor we are currently writing to (0 or 1)
  uint32_t current_fd;

  // The two file descriptors
  File files[2];

  // Buffer for writing data to the files
  ByteArray buffer;

  // Counts all transactions in the current file
  uint32_t num_transactions;

  // When having more than these Txns in one file, we
  // swap the files
  uint32_t threshold;

  // Set to false to disable logging; used during recovery
  bool disable_logging;

  // Counting the flushed bytes (for ups_env_get_metrics)
  uint64_t count_bytes_flushed;

  // Counting the bytes before compression (for ups_env_get_metrics)
  uint64_t count_bytes_before_compression;

  // Counting the bytes after compression (for ups_env_get_metrics)
  uint64_t count_bytes_after_compression;

  // A map of all opened databases
  typedef std::map<uint16_t, Db *> DatabaseMap;
  DatabaseMap database_map;

  // The compressor; can be null
  ScopedPtr<Compressor> compressor;
};

} // namespace upscaledb

#endif /* UPS_JOURNAL_STATE_H */
