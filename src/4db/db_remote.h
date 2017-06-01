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

#ifndef UPS_DB_REMOTE_H
#define UPS_DB_REMOTE_H

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
struct Env;
struct RemoteEnv;

/*
 * The database implementation for remote file access
 */
struct RemoteDb : public Db
{
  RemoteDb(Env *env, DbConfig &config,
                  uint64_t remote_handle_)
    : Db(env, config), remote_handle(remote_handle_) {
  }

  // Fills in the current metrics
  virtual void fill_metrics(ups_env_metrics_t *metrics) { }

  // Returns database parameters (ups_db_get_parameters)
  virtual ups_status_t get_parameters(ups_parameter_t *param);

  // Checks database integrity (ups_db_check_integrity)
  virtual ups_status_t check_integrity(uint32_t flags);

  // Returns the number of keys
  virtual uint64_t count(Txn *txn, bool distinct);

  // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
  virtual ups_status_t insert(Cursor *cursor, Txn *txn,
                  ups_key_t *key, ups_record_t *record, uint32_t flags);

  // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
  virtual ups_status_t erase(Cursor *cursor, Txn *txn, ups_key_t *key,
                  uint32_t flags);

  // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
  virtual ups_status_t find(Cursor *cursor, Txn *txn, ups_key_t *key,
                  ups_record_t *record, uint32_t flags);

  // Moves a cursor, returns key and/or record (ups_cursor_move)
  virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                  ups_record_t *record, uint32_t flags);

  // Creates a cursor (ups_cursor_create)
  virtual Cursor *cursor_create(Txn *txn, uint32_t flags);

  // Clones a cursor (ups_cursor_clone)
  virtual Cursor *cursor_clone(Cursor *src);

  // Performs bulk operations
  virtual ups_status_t bulk_operations(Txn *txn, ups_operation_t *operations,
                  size_t operations_length, uint32_t flags);

  // Closes the database (ups_db_close)
  virtual ups_status_t close(uint32_t flags);

  // the remote database handle
  uint64_t remote_handle;
};

} // namespace upscaledb

#endif /* UPS_ENABLE_REMOTE */

#endif /* UPS_DB_REMOTE_H */
