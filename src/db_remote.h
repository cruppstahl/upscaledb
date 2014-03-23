/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef HAM_DB_REMOTE_H__
#define HAM_DB_REMOTE_H__

#  ifdef HAM_ENABLE_REMOTE

#include "db.h"

namespace hamsterdb {

class Environment;
class RemoteEnvironment;

/*
 * The database implementation for remote file access
 */
class RemoteDatabase : public Database
{
  public:
    RemoteDatabase(Environment *env, ham_u16_t name, ham_u32_t flags)
      : Database(env, name, flags), m_remote_handle(0) {
    }

    // Returns the RemoteEnvironment instance
    RemoteEnvironment *get_remote_env() {
      return ((RemoteEnvironment *)m_env);
    }

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(ham_u32_t flags);

    // Returns the number of keys (ham_db_get_key_count)
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount);

    // Inserts a key/value pair (ham_db_insert)
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erase a key/value pair (ham_db_erase)
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags);

    // Lookup of a key/value pair (ham_db_find)
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Inserts a key with a cursor (ham_cursor_insert)
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erases the key of a cursor (ham_cursor_erase)
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags);

    // Positions the cursor on a key and returns the record (ham_cursor_find)
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor,
                    ham_u32_t *count, ham_u32_t flags);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Returns the remote database handle
    ham_u64_t get_remote_handle() {
        return (m_remote_handle);
    }

    // Sets the remote database handle
    // TODO make this private
    void set_remote_handle(ham_u64_t handle) {
        m_remote_handle = handle;
    }

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, ham_u32_t flags);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c);

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(ham_u32_t flags);

  private:
    // the remote database handle
    ham_u64_t m_remote_handle;
};

} // namespace hamsterdb

#  endif /* HAM_ENABLE_REMOTE */
#endif /* HAM_DB_REMOTE_H__ */
