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
  if (ISSET(config.flags, UPS_IN_MEMORY))
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

    while ((t = txn_manager->oldest_txn())) {
      if (!t->is_aborted() && !t->is_committed()) {
        if (ISSET(flags, UPS_TXN_AUTO_COMMIT))
          st = txn_manager->commit(t);
        else /* if (flags & UPS_TXN_AUTO_ABORT) */
          st = txn_manager->abort(t);
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
    if (ISSET(flags, UPS_AUTO_CLEANUP))
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
