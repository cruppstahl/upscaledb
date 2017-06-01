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

#ifndef UPS_ENV_REMOTE_H
#define UPS_ENV_REMOTE_H

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/socket.h"
#include "1base/dynamic_array.h"
#include "2protobuf/protocol.h"
#include "2protoserde/messages.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The Environment implementation for remote file access
//
struct RemoteEnv : public Env
{
  // Constructor
  RemoteEnv(EnvConfig &config);

  // Sends a |request| message with the Google Protocol Buffers API. Blocks
  // till the reply was fully received. Returns the reply structure.
  Protocol *perform_request(Protocol *request);

  // Sends |request| message with the builtin Serde API. Blocks till the
  // reply was fully received. Fills |reply| with the received data.
  void perform_request(SerializedWrapper *request, SerializedWrapper *reply);

  // Creates a new Environment (ups_env_create)
  virtual ups_status_t create();

  // Opens a new Environment (ups_env_open)
  virtual ups_status_t open();

  // Returns all database names (ups_env_get_database_names)
  virtual std::vector<uint16_t> get_database_names();

  // Returns environment parameters and flags (ups_env_get_parameters)
  virtual ups_status_t get_parameters(ups_parameter_t *param);

  // Flushes the environment and its databases to disk (ups_env_flush)
  virtual ups_status_t flush(uint32_t flags);

  // Begins a new transaction (ups_txn_begin)
  virtual Txn *txn_begin(const char *name, uint32_t flags);

  // Commits a transaction (ups_txn_commit)
  virtual ups_status_t txn_commit(Txn *txn, uint32_t flags);

  // Commits a transaction (ups_txn_abort)
  virtual ups_status_t txn_abort(Txn *txn, uint32_t flags);

  // Renames a database in the Environment (ups_env_rename_db)
  virtual ups_status_t rename_db(uint16_t oldname, uint16_t newname,
                  uint32_t flags);

  // Erases (deletes) a database from the Environment (ups_env_erase_db)
  virtual ups_status_t erase_db(uint16_t name, uint32_t flags);

  // Fills in the current metrics
  virtual void fill_metrics(ups_env_metrics_t *metrics);

  // Performs a UQI select
  virtual ups_status_t select_range(const char *query, Cursor *begin,
                          const Cursor *end, Result **result);

  // Creates a new database in the environment (ups_env_create_db)
  virtual Db *do_create_db(DbConfig &config, const ups_parameter_t *param);

  // Opens an existing database in the environment (ups_env_open_db)
  virtual Db *do_open_db(DbConfig &config, const ups_parameter_t *param);

  // Closes the Environment (ups_env_close)
  virtual ups_status_t do_close(uint32_t flags);

  // the remote handle
  uint64_t remote_handle;

  // the socket
  Socket _socket;

  // a buffer to avoid frequent memory allocations
  ByteArray _buffer;
};

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

#endif /* UPS_ENV_REMOTE_H */
