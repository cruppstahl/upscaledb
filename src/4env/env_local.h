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

#ifndef UPS_ENV_LOCAL_H
#define UPS_ENV_LOCAL_H

#include "ups/upscaledb.h"

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
#include "2lsn_manager/lsn_manager.h"
#include "2device/device.h"
#include "3journal/journal.h"
#include "3blob_manager/blob_manager.h"
#include "3page_manager/page_manager.h"
#include "4env/env.h"
#include "4env/env_header.h"
#include "4context/context.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct PBtreeHeader;
class PFreelistPayload;
struct LocalTxn;

//
// The Environment implementation for local file access
//
struct LocalEnv : public Env
{
  LocalEnv(EnvConfig &config)
    : Env(config) {
  }

  // Creates a new Environment (ups_env_create)
  virtual ups_status_t create();

  // Opens a new Environment (ups_env_open)
  virtual ups_status_t open();

  // Creates a new database in the environment (ups_env_create_db)
  virtual Db *do_create_db(DbConfig &config, const ups_parameter_t *param);

  // Opens an existing database in the environment (ups_env_open_db)
  virtual Db *do_open_db(DbConfig &config, const ups_parameter_t *param);

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

  // Closes the Environment (ups_env_close)
  virtual ups_status_t do_close(uint32_t flags);

  // The Environment's header page/configuration
  ScopedPtr<EnvHeader> header;

  // The device instance (either a file or an in-memory-db)
  ScopedPtr<Device> device;

  // The BlobManager instance
  ScopedPtr<BlobManager> blob_manager;

  // The PageManager instance
  ScopedPtr<PageManager> page_manager;

  // The logical journal
  ScopedPtr<Journal> journal;

  // The lsn manager
  LsnManager lsn_manager;
};

} // namespace upscaledb

#endif /* UPS_ENV_LOCAL_H */
