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

#ifndef UPS_DB_H
#define UPS_DB_H

#include "0root/root.h"

#include "ups/upscaledb_int.h"
#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "2config/db_config.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ups_db_t is declared in ups/upscaledb.h as an
// opaque C structure, but internally we use a C++ class. The ups_db_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ups_db_t {
  int dummy;
};

namespace upscaledb {

struct Cursor;
struct ScanVisitor;

/*
 * An abstract base class for a Database; is overwritten for local and
 * remote implementations
 */
struct Db
{
  // Constructor
  Db(Env *env_, DbConfig &config_)
    : env(env_), context(0), cursor_list(0), config(config_) {
  }

  virtual ~Db() {
  }

  // Returns the runtime-flags - the flags are "mixed" with the flags from
  // the Environment
  uint32_t flags() const {
    return env->flags() | config.flags;
  }

  // Returns the database name
  uint16_t name() const {
    return config.db_name;
  }

  // Sets the database name; required when renaming the local proxy of
  // a remote database
  void set_name(uint16_t name) {
    config.db_name = name;
  }

  // Fills in the current metrics
  virtual void fill_metrics(ups_env_metrics_t *metrics) = 0;

  // Returns the database parameters (ups_db_get_parameters)
  virtual ups_status_t get_parameters(ups_parameter_t *param) = 0;

  // Checks the database integrity (ups_db_check_integrity)
  virtual ups_status_t check_integrity(uint32_t flags) = 0;

  // Returns the number of keys (ups_db_count)
  virtual uint64_t count(Txn *txn, bool distinct) = 0;

  // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
  virtual ups_status_t insert(Cursor *cursor, Txn *txn,
                  ups_key_t *key, ups_record_t *record, uint32_t flags) = 0;

  // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
  virtual ups_status_t erase(Cursor *cursor, Txn *txn, ups_key_t *key,
                  uint32_t flags) = 0;

  // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
  virtual ups_status_t find(Cursor *cursor, Txn *txn, ups_key_t *key,
                  ups_record_t *record, uint32_t flags) = 0;

  // Creates a cursor (ups_cursor_create)
  virtual Cursor *cursor_create(Txn *txn, uint32_t flags) = 0;

  // Clones a cursor (ups_cursor_clone)
  virtual Cursor *cursor_clone(Cursor *src) = 0;

  // Moves a cursor, returns key and/or record (ups_cursor_move)
  virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                  ups_record_t *record, uint32_t flags) = 0;

  // Performs bulk operations
  virtual ups_status_t bulk_operations(Txn *txn, ups_operation_t *operations,
                  size_t operations_length, uint32_t flags) = 0;

  // Closes the database (ups_db_close)
  virtual ups_status_t close(uint32_t flags) = 0;

  // Adds a cursor to the linked list of cursors
  void add_cursor(Cursor *cursor);

  // Removes a cursor from the linked list of cursors
  void remove_cursor(Cursor *cursor);

  // Returns the memory buffer for the key data: the per-database buffer
  // if |txn| is null or temporary, otherwise the buffer from the |txn|
  ByteArray &key_arena(Txn *txn) {
    return (txn == 0 || ISSET(txn->flags, UPS_TXN_TEMPORARY))
               ? _key_arena
               : txn->key_arena;
  }

  // Returns the memory buffer for the record data: the per-database buffer
  // if |txn| is null or temporary, otherwise the buffer from the |txn|
  ByteArray &record_arena(Txn *txn) {
    return (txn == 0 || ISSET(txn->flags, UPS_TXN_TEMPORARY))
               ? _record_arena
               : txn->record_arena;
  }

  // the current Environment
  Env *env;

  // the user-provided context data
  void *context;

  // linked list of all cursors
  Cursor *cursor_list;

  // the configuration settings
  DbConfig config;

  // This is where key->data points to when returning a
  // key to the user; used if Txns are disabled
  ByteArray _key_arena;

  // This is where record->data points to when returning a
  // record to the user; used if Txns are disabled
  ByteArray _record_arena;
};

} // namespace upscaledb

#endif /* UPS_DB_H */
