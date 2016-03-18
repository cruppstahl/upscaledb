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
class LocalEnv;

struct JournalState
{
  JournalState(LocalEnv *env_);

  // References the Environment this journal file is for
  LocalEnv *env;

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

  // When having more than these Txns in one file, we
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

  // A map of all opened databases
  typedef std::map<uint16_t, Db *> DatabaseMap;
  DatabaseMap database_map;

  // The compressor; can be null
  ScopedPtr<Compressor> compressor;
};

} // namespace upscaledb

#endif /* UPS_JOURNAL_STATE_H */
