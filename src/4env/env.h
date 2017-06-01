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

#ifndef UPS_ENV_H
#define UPS_ENV_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ups/upscaledb_int.h"
#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/mutex.h"
#include "1base/scoped_ptr.h"
#include "2config/db_config.h"
#include "2config/env_config.h"
#include "4txn/txn.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ups_env_t is declared in ups/upscaledb.h as an
// opaque C structure, but internally we use a C++ class. The ups_env_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ups_env_t {
  int dummy;
};

namespace upscaledb {

struct Cursor;
struct Db;
struct Txn;
struct Result;

//
// The Environment is the "root" of all upscaledb objects. It's a container
// for multiple databases and transactions.
//
// This class provides exception handling and locking mechanisms, then
// dispatches all calls to LocalEnv or RemoteEnv.
//
struct Env
{
  // Constructor
  Env(EnvConfig &config_)
    : config(config_) {
  }

  virtual ~Env() {
  }

  // Convenience function which returns the flags
  uint32_t flags() const {
    return config.flags;
  }

  // Creates a new Environment (ups_env_create)
  virtual ups_status_t create() = 0;

  // Opens a new Environment (ups_env_open)
  virtual ups_status_t open() = 0;

  // Returns all database names (ups_env_get_database_names)
  virtual std::vector<uint16_t> get_database_names() = 0;

  // Returns environment parameters and flags (ups_env_get_parameters)
  virtual ups_status_t get_parameters(ups_parameter_t *param) = 0;

  // Flushes the environment and its databases to disk (ups_env_flush)
  // Accepted flags: UPS_FLUSH_BLOCKING
  virtual ups_status_t flush(uint32_t flags) = 0;

  // Creates a new database in the environment (ups_env_create_db)
  Db *create_db(DbConfig &config, const ups_parameter_t *param);

  // Opens an existing database in the environment (ups_env_open_db)
  Db *open_db(DbConfig &config, const ups_parameter_t *param);

  // Renames a database in the Environment (ups_env_rename_db)
  virtual ups_status_t rename_db(uint16_t oldname, uint16_t newname,
                  uint32_t flags) = 0;

  // Erases (deletes) a database from the Environment (ups_env_erase_db)
  virtual ups_status_t erase_db(uint16_t name, uint32_t flags) = 0;

  // Closes an existing database in the environment (ups_db_close)
  ups_status_t close_db(Db *db, uint32_t flags);

  // Begins a new transaction (ups_txn_begin)
  virtual Txn *txn_begin(const char *name, uint32_t flags) = 0;

  // Commits a transaction (ups_txn_commit)
  virtual ups_status_t txn_commit(Txn *txn, uint32_t flags) = 0;

  // Commits a transaction (ups_txn_abort)
  virtual ups_status_t txn_abort(Txn *txn, uint32_t flags) = 0;

  // Fills in the current metrics
  virtual void fill_metrics(ups_env_metrics_t *metrics) = 0;

  // Performs a UQI select
  virtual ups_status_t select_range(const char *query, Cursor *begin,
                          const Cursor *end, Result **result) = 0;

  // Creates a new database in the environment (ups_env_create_db)
  virtual Db *do_create_db(DbConfig &config, const ups_parameter_t *param) = 0;

  // Opens an existing database in the environment (ups_env_open_db)
  virtual Db *do_open_db(DbConfig &config, const ups_parameter_t *param) = 0;

  // Closes the Environment (ups_env_close)
  virtual ups_status_t do_close(uint32_t flags) = 0;

  // Closes the Environment (ups_env_close)
  ups_status_t close(uint32_t flags);

  // A mutex to serialize access to this Environment
  Mutex mutex;

  // The Environment's configuration
  EnvConfig config;

  // The Txn manager; can be null
  ScopedPtr<TxnManager> txn_manager;

  // A map of all opened Databases
  typedef std::map<uint16_t, Db *> DatabaseMap;
  DatabaseMap _database_map;
};

} // namespace upscaledb

#endif /* UPS_ENV_H */
