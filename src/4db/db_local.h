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

#ifndef UPS_DB_LOCAL_H
#define UPS_DB_LOCAL_H

#include "0root/root.h"

#include <limits>

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
// need to include the header file, a forward declaration of class Compressor
// is not sufficient because std::auto_ptr then fails to call the
// destructor
#include "2compressor/compressor.h"
#include "3btree/btree_index.h"
#include "4txn/txn_local.h"
#include "4db/db.h"
#include "4db/histogram.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct TxnNode;
struct TxnIndex;
struct TxnCursor;
struct TxnOperation;
struct LocalEnv;
struct LocalTxn;
struct SelectStatement;
struct Result;

//
// The database implementation for local file access
//
struct LocalDb : public Db {
  // Constructor
  LocalDb(Env *env, DbConfig &config)
    : Db(env, config), compare_function(0), _current_record_number(0),
      histogram(this) {
  }

  // Creates a new database
  ups_status_t create(Context *context, PBtreeHeader *btree_header);

  // Opens an existing database
  ups_status_t open(Context *context, PBtreeHeader *btree_header);

  // Erases this database
  ups_status_t drop(Context *context);

  // Fills in the current metrics
  virtual void fill_metrics(ups_env_metrics_t *metrics);

  // Returns database parameters (ups_db_get_parameters)
  virtual ups_status_t get_parameters(ups_parameter_t *param);

  // Checks database integrity (ups_db_check_integrity)
  virtual ups_status_t check_integrity(uint32_t flags);

  // Returns the number of keys
  virtual uint64_t count(Txn *txn, bool distinct);

  // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
  virtual ups_status_t insert(Cursor *cursor, Txn *txn, ups_key_t *key,
                  ups_record_t *record, uint32_t flags);

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
  virtual Cursor *cursor_create(Txn *txn, uint32_t);

  // Clones a cursor (ups_cursor_clone)
  virtual Cursor *cursor_clone(Cursor *src);

  // Performs bulk operations
  virtual ups_status_t bulk_operations(Txn *txn, ups_operation_t *operations,
                  size_t operations_length, uint32_t flags);

  // Closes the database (ups_db_close)
  virtual ups_status_t close(uint32_t flags);

  // (Non-virtual) Performs a range select over the database
  ups_status_t select_range(SelectStatement *stmt, LocalCursor *begin,
                  LocalCursor *end, Result **result);

  // Flushes a TxnOperation to the btree
  ups_status_t flush_txn_operation(Context *context, LocalTxn *txn,
                  TxnOperation *op);

  // the btree index
  ScopedPtr<BtreeIndex> btree_index;

  // the transaction index
  ScopedPtr<TxnIndex> txn_index;

  // the comparison function
  ups_compare_func_t compare_function;

  // The record compressor; can be null
  ScopedPtr<Compressor> record_compressor;

  // the current record number
  uint64_t _current_record_number;

  // Lower/upper boundaries
  Histogram histogram;
};

} // namespace upscaledb

#endif /* UPS_DB_LOCAL_H */
