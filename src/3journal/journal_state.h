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
 * The Journal's state
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_JOURNAL_STATE_H
#define HAM_JOURNAL_STATE_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ham/hamsterdb_int.h" // for metrics

#include "1base/dynamic_array.h"
#include "1os/file.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Database;
class LocalEnvironment;

struct JournalState
{
  enum {
    // switch log file after |kSwitchTxnThreshold| transactions
    kSwitchTxnThreshold = 32,

    // flush buffers if this limit is exceeded
    kBufferLimit = 1024 * 1024 // 1 mb
  };

  JournalState(LocalEnvironment *env);

  // References the Environment this journal file is for
  LocalEnvironment *env;

  // The index of the file descriptor we are currently writing to (0 or 1)
  uint32_t current_fd;

  // The two file descriptors
  File files[2];

  // Buffers for writing data to the files
  ByteArray buffer[2];

  // For counting all open transactions in the files
  size_t open_txn[2];

  // For counting all closed transactions in the files
  size_t closed_txn[2];

  // The lsn of the previous checkpoint
  uint64_t last_cp_lsn;

  // When having more than these Transactions in one file, we
  // swap the files
  size_t threshold;

  // Set to false to disable logging; used during recovery
  bool disable_logging;

  // Counting the flushed bytes (for ham_env_get_metrics)
  uint64_t count_bytes_flushed;

  // Counting the bytes before compression (for ham_env_get_metrics)
  uint64_t count_bytes_before_compression;

  // Counting the bytes after compression (for ham_env_get_metrics)
  uint64_t count_bytes_after_compression;

  // A map of all opened Databases
  typedef std::map<uint16_t, Database *> DatabaseMap;
  DatabaseMap database_map;
};

} // namespace hamsterdb

#endif /* HAM_JOURNAL_STATE_H */
