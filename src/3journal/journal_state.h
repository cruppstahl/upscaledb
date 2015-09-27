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
 * The Journal's state
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef UPS_JOURNAL_STATE_H
#define UPS_JOURNAL_STATE_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ups/upscaledb_int.h" // for metrics

#include "1base/dynamic_array.h"
#include "1base/scoped_ptr.h"
#include "1os/file.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

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
  uint64_t open_txn[2];

  // For counting all closed transactions in the files
  // This needs to be atomic since it's updated from the worker thread
  boost::atomic<uint64_t> closed_txn[2];

  // The lsn of the previous checkpoint
  uint64_t last_cp_lsn;

  // When having more than these Transactions in one file, we
  // swap the files
  uint64_t threshold;

  // Set to false to disable logging; used during recovery
  bool disable_logging;

  // Counting the flushed bytes (for ups_env_get_metrics)
  uint64_t count_bytes_flushed;

  // Counting the bytes before compression (for ups_env_get_metrics)
  uint64_t count_bytes_before_compression;

  // Counting the bytes after compression (for ups_env_get_metrics)
  uint64_t count_bytes_after_compression;

  // A map of all opened Databases
  typedef std::map<uint16_t, Database *> DatabaseMap;
  DatabaseMap database_map;

  // The compressor; can be null
  ScopedPtr<Compressor> compressor;
};

} // namespace upscaledb

#endif /* UPS_JOURNAL_STATE_H */
