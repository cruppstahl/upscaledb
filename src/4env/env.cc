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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4cursor/cursor.h"
#include "4db/db.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

namespace upscaledb {

Db *
Env::create_db(DbConfig &config, const ups_parameter_t *param)
{
  Db *db = do_create_db(config, param);
  assert(db != 0);

  // on success: store the open database in the environment's list of
  // opened databases
  _database_map[config.db_name] = db;

  // flush the environment to make sure that the header page is written
  // to disk
  ups_status_t st = flush(0);
  if (unlikely(st))
    throw Exception(st);
  return db;
}

Db *
Env::open_db(DbConfig &config, const ups_parameter_t *param)
{
  // make sure that this database is not yet open
  if (unlikely(_database_map.find(config.db_name) != _database_map.end()))
    throw Exception(UPS_DATABASE_ALREADY_OPEN);

  Db *db = do_open_db(config, param);
  assert(db != 0);

  // on success: store the open database in the environment's list of
  // opened databases
  _database_map[config.db_name] = db;
  return db;
}

ups_status_t
Env::close_db(Db *db, uint32_t flags)
{
  uint16_t dbname = db->name();

  // flush committed Txns
  ups_status_t st = flush(UPS_FLUSH_COMMITTED_TRANSACTIONS);
  if (unlikely(st))
    return (st);

  st = db->close(flags);
  if (unlikely(st))
    return (st);

  _database_map.erase(dbname);
  delete db;

  /* in-memory database: make sure that a database with the same name
   * can be re-created */
  if (isset(config.flags, UPS_IN_MEMORY))
    erase_db(dbname, 0);

  return 0;
}

ups_status_t
Env::close(uint32_t flags)
{
  ups_status_t st = 0;

  ScopedLock lock(mutex);

  /* auto-abort (or commit) all pending transactions */
  if (txn_manager.get()) {
    Txn *t;

    while ((t = txn_manager->oldest_txn)) {
      if (!t->is_aborted() && !t->is_committed()) {
        if (isset(flags, UPS_TXN_AUTO_COMMIT))
          st = txn_manager->commit(t, 0);
        else /* if (flags & UPS_TXN_AUTO_ABORT) */
          st = txn_manager->abort(t, 0);
        if (unlikely(st))
          return st;
      }
      txn_manager->flush_committed_txns();
    }
  }

  /* close all databases */
  Env::DatabaseMap::iterator it = _database_map.begin();
  while (it != _database_map.end()) {
    Env::DatabaseMap::iterator it2 = it; it++;
    Db *db = it2->second;
    if (isset(flags, UPS_AUTO_CLEANUP))
      st = ups_db_close((ups_db_t *)db, flags | UPS_DONT_LOCK);
    else
      st = db->close(flags);
    if (unlikely(st))
      return st;
  }

  _database_map.clear();

  return do_close(flags);
}

} // namespace upscaledb
