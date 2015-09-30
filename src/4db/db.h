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
 * @exception_safe: nothrow
 * @thread_safe: no
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

class Cursor;
struct ScanVisitor;

/*
 * An abstract base class for a Database; is overwritten for local and
 * remote implementations
 */
class Database
{
  public:
    // Constructor
    Database(Environment *env, DatabaseConfiguration &config);

    virtual ~Database() {
    }

    // Returns the Environment pointer
    Environment *get_env() {
      return (m_env);
    }

    // Returns the Database's configuration
    const DatabaseConfiguration &config() const {
      return (m_config);
    }

    // Returns the runtime-flags - the flags are "mixed" with the flags from
    // the Environment
    uint32_t get_flags() {
      return (m_env->get_flags() | m_config.flags);
    }

    // Returns the database name
    uint16_t name() const {
      return (m_config.db_name);
    }

    // Sets the database name
    void set_name(uint16_t name) {
      m_config.db_name = name;
    }

    // Fills in the current metrics
    virtual void fill_metrics(ups_env_metrics_t *metrics) = 0;

    // Returns Database parameters (ups_db_get_parameters)
    virtual ups_status_t get_parameters(ups_parameter_t *param) = 0;

    // Checks Database integrity (ups_db_check_integrity)
    virtual ups_status_t check_integrity(uint32_t flags) = 0;

    // Returns the number of keys (ups_db_count)
    virtual ups_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount) = 0;

    // Scans the whole database, applies a processor function
    virtual ups_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct) = 0;

    // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
    virtual ups_status_t insert(Cursor *cursor, Transaction *txn,
                    ups_key_t *key, ups_record_t *record, uint32_t flags) = 0;

    // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
    virtual ups_status_t erase(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    uint32_t flags) = 0;

    // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
    virtual ups_status_t find(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    ups_record_t *record, uint32_t flags) = 0;

    // Creates a cursor (ups_cursor_create)
    virtual ups_status_t cursor_create(Cursor **pcursor, Transaction *txn,
                    uint32_t flags);

    // Clones a cursor (ups_cursor_clone)
    virtual ups_status_t cursor_clone(Cursor **pdest, Cursor *src);

    // Moves a cursor, returns key and/or record (ups_cursor_move)
    virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record, uint32_t flags) = 0;

    // Closes a cursor (ups_cursor_close)
    ups_status_t cursor_close(Cursor *cursor);

    // Closes the Database (ups_db_close)
    ups_status_t close(uint32_t flags);

    // Returns the user-provided context pointer (ups_get_context_data)
    void *get_context_data() {
      return (m_context);
    }

    // Sets the user-provided context pointer (ups_set_context_data)
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    // Returns the head of the linked list with all cursors
    Cursor *cursor_list() {
      return (m_cursor_list);
    }

    // Returns the memory buffer for the key data: the per-database buffer
    // if |txn| is null or temporary, otherwise the buffer from the |txn|
    ByteArray &key_arena(Transaction *txn) {
      return ((txn == 0 || (txn->get_flags() & UPS_TXN_TEMPORARY))
                 ? m_key_arena
                 : txn->key_arena());
    }

    // Returns the memory buffer for the record data: the per-database buffer
    // if |txn| is null or temporary, otherwise the buffer from the |txn|
    ByteArray &record_arena(Transaction *txn) {
      return ((txn == 0 || (txn->get_flags() & UPS_TXN_TEMPORARY))
                 ? m_record_arena
                 : txn->record_arena());
    }

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn) = 0;

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src) = 0;

    // Closes a database; this is the actual implementation
    virtual ups_status_t close_impl(uint32_t flags) = 0;

    // the current Environment
    Environment *m_env;

    // the configuration settings
    DatabaseConfiguration m_config;

    // the user-provided context data
    void *m_context;

    // linked list of all cursors
    Cursor *m_cursor_list;

    // This is where key->data points to when returning a
    // key to the user; used if Transactions are disabled
    ByteArray m_key_arena;

    // This is where record->data points to when returning a
    // record to the user; used if Transactions are disabled
    ByteArray m_record_arena;
};

} // namespace upscaledb

#endif /* UPS_DB_H */
