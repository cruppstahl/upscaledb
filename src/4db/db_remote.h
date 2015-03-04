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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_DB_REMOTE_H
#define HAM_DB_REMOTE_H

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
class Environment;
class RemoteEnvironment;

/*
 * The database implementation for remote file access
 */
class RemoteDatabase : public Database
{
  public:
    RemoteDatabase(Environment *env, DatabaseConfiguration config,
                    uint64_t remote_handle)
      : Database(env, config), m_remote_handle(remote_handle) {
    }

    // Fills in the current metrics
    virtual void fill_metrics(ham_env_metrics_t *metrics) { }

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual ham_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount);

    // Scans the whole database, applies a processor function
    virtual ham_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct) {
      return (HAM_NOT_IMPLEMENTED);
    }

    // Inserts a key/value pair (ham_db_insert, ham_cursor_insert)
    virtual ham_status_t insert(Cursor *cursor, Transaction *txn,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // Erase a key/value pair (ham_db_erase, ham_cursor_erase)
    virtual ham_status_t erase(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ham_db_find, ham_cursor_find)
    virtual ham_status_t find(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor, uint32_t flags,
                    uint32_t *pcount);

    // Returns position in duplicate list (ham_cursor_get_duplicate_position)
    virtual ham_status_t cursor_get_duplicate_position(Cursor *cursor,
                    uint32_t *pposition);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    uint64_t *psize);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, uint32_t flags);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c);

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(uint32_t flags);

  private:
    // Returns the RemoteEnvironment instance
    RemoteEnvironment *renv() {
      return ((RemoteEnvironment *)m_env);
    }

    // the remote database handle
    uint64_t m_remote_handle;
};

} // namespace hamsterdb

#endif /* HAM_ENABLE_REMOTE */

#endif /* HAM_DB_REMOTE_H */
