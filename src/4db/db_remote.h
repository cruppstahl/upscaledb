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
 * @exception_safe: unknown
 * @thread_safe: unknown
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
    virtual void fill_metrics(ups_env_metrics_t *metrics) { }

    // Returns Database parameters (ups_db_get_parameters)
    virtual ups_status_t get_parameters(ups_parameter_t *param);

    // Checks Database integrity (ups_db_check_integrity)
    virtual ups_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual ups_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount);

    // Scans the whole database, applies a processor function
    virtual ups_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct) {
      return (UPS_NOT_IMPLEMENTED);
    }

    // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
    virtual ups_status_t insert(Cursor *cursor, Transaction *txn,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
    virtual ups_status_t erase(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
    virtual ups_status_t find(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ups_cursor_move)
    virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a database; this is the actual implementation
    virtual ups_status_t close_impl(uint32_t flags);

  private:
    // Returns the RemoteEnvironment instance
    RemoteEnvironment *renv() {
      return ((RemoteEnvironment *)m_env);
    }

    // the remote database handle
    uint64_t m_remote_handle;
};

} // namespace upscaledb

#endif /* UPS_ENABLE_REMOTE */

#endif /* UPS_DB_REMOTE_H */
