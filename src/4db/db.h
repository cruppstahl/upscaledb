/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_DB_H
#define HAM_DB_H

#include "0root/root.h"

#include "ham/hamsterdb_int.h"
#include "ham/hamsterdb_ola.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "2config/db_config.h"
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ham_db_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_db_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ham_db_t {
  int dummy;
};

namespace hamsterdb {

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
    virtual void fill_metrics(ham_env_metrics_t *metrics) = 0;

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(uint32_t flags) = 0;

    // Returns the number of keys (ham_db_get_key_count)
    virtual ham_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount) = 0;

    // Scans the whole database, applies a processor function
    virtual ham_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct) = 0;

    // Inserts a key/value pair (ham_db_insert, ham_cursor_insert)
    virtual ham_status_t insert(Cursor *cursor, Transaction *txn,
                    ham_key_t *key, ham_record_t *record, uint32_t flags) = 0;

    // Erase a key/value pair (ham_db_erase, ham_cursor_erase)
    virtual ham_status_t erase(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    uint32_t flags) = 0;

    // Lookup of a key/value pair (ham_db_find, ham_cursor_find)
    virtual ham_status_t find(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    ham_record_t *record, uint32_t flags) = 0;

    // Creates a cursor (ham_cursor_create)
    virtual ham_status_t cursor_create(Cursor **pcursor, Transaction *txn,
                    uint32_t flags);

    // Clones a cursor (ham_cursor_clone)
    virtual ham_status_t cursor_clone(Cursor **pdest, Cursor *src);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor,
                    uint32_t flags, uint32_t *pcount) = 0;

    // Returns position in duplicate list (ham_cursor_get_duplicate_position)
    virtual ham_status_t cursor_get_duplicate_position(Cursor *cursor,
                    uint32_t *pposition) = 0;

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    uint64_t *psize) = 0;

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, uint32_t flags) = 0;

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags) = 0;

    // Closes a cursor (ham_cursor_close)
    ham_status_t cursor_close(Cursor *cursor);

    // Closes the Database (ham_db_close)
    ham_status_t close(uint32_t flags);

    // Returns the last error code
    ham_status_t get_error() const {
      return (m_error);
    }

    // Sets the last error code
    ham_status_t set_error(ham_status_t e) {
      return ((m_error = e));
    }

    // Returns the user-provided context pointer (ham_get_context_data)
    void *get_context_data() {
      return (m_context);
    }

    // Sets the user-provided context pointer (ham_set_context_data)
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
      return ((txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                 ? m_key_arena
                 : txn->key_arena());
    }

    // Returns the memory buffer for the record data: the per-database buffer
    // if |txn| is null or temporary, otherwise the buffer from the |txn|
    ByteArray &record_arena(Transaction *txn) {
      return ((txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                 ? m_record_arena
                 : txn->record_arena());
    }

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, uint32_t flags) = 0;

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src) = 0;

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c) = 0;

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(uint32_t flags) = 0;

    // the current Environment
    Environment *m_env;

    // the configuration settings
    DatabaseConfiguration m_config;

    // the last error code
    ham_status_t m_error;

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

} // namespace hamsterdb

#endif /* HAM_DB_H */
