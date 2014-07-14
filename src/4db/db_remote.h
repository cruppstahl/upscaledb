/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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

class Environment;
class RemoteEnvironment;

/*
 * The database implementation for remote file access
 */
class RemoteDatabase : public Database
{
  public:
    RemoteDatabase(Environment *env, DatabaseConfiguration config)
      : Database(env, config), m_remote_handle(0) {
    }

    // Returns the RemoteEnvironment instance
    RemoteEnvironment *get_remote_env() {
      return ((RemoteEnvironment *)m_env);
    }

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual void count(Transaction *txn, bool distinct,
                    uint64_t *keycount);

    // Scans the whole database, applies a processor function
    virtual void scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct) { /* TODO */ }

    // Inserts a key/value pair (ham_db_insert)
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Erase a key/value pair (ham_db_erase)
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ham_db_find)
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Inserts a key with a cursor (ham_cursor_insert)
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Erases the key of a cursor (ham_cursor_erase)
    virtual ham_status_t cursor_erase(Cursor *cursor, uint32_t flags);

    // Positions the cursor on a key and returns the record (ham_cursor_find)
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor,
                    uint32_t *count, uint32_t flags);

    // Returns position in duplicate list (ham_cursor_get_duplicate_position)
    virtual uint32_t cursor_get_duplicate_position(Cursor *cursor);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    uint64_t *size);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Returns the remote database handle
    uint64_t get_remote_handle() {
        return (m_remote_handle);
    }

    // Sets the remote database handle
    // TODO make this private
    void set_remote_handle(uint64_t handle) {
        m_remote_handle = handle;
    }

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
    // the remote database handle
    uint64_t m_remote_handle;
};

} // namespace hamsterdb

#endif /* HAM_ENABLE_REMOTE */

#endif /* HAM_DB_REMOTE_H */
