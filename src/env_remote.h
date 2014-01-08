/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_ENV_REMOTE_H__
#define HAM_ENV_REMOTE_H__

#ifdef HAM_ENABLE_REMOTE

#include <ham/hamsterdb.h>

#include "env.h"
#include "util.h"
#include "protocol/protocol.h"

namespace hamsterdb {

//
// The Environment implementation for remote file access
//
class RemoteEnvironment : public Environment
{
  public:
    RemoteEnvironment()
      : Environment(), m_remote_handle(0), m_socket(HAM_INVALID_FD),
        m_buffer(1024 * 4), m_timeout(0) {
      set_flags(get_flags() | HAM_IS_REMOTE_INTERNAL);
    }

    virtual ~RemoteEnvironment();

    // Sets the timeout (in seconds)
    void set_timeout(ham_u32_t seconds) {
      m_timeout = seconds;
    }

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
            ham_u32_t mode, ham_u32_t page_size, ham_u64_t cache_size,
            ham_u16_t maxdbs);

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
            ham_u64_t cache_size);

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
            ham_u32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags);

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_u32_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(ham_u32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
                    ham_u32_t flags, const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
                    ham_u32_t flags, const ham_parameter_t *param);

    // Begins a new transaction (ham_txn_begin)
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
                    ham_u32_t flags);

    // Aborts a transaction (ham_txn_abort)
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags);

    // Commits a transaction (ham_txn_commit)
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(ham_u32_t flags);

    // Sends |request| to the remote server and blocks till the reply
    // was fully received
    Protocol *perform_request(Protocol *request);

  private:
    // the remote database handle
    ham_u64_t m_remote_handle;

    // the socket
    ham_socket_t m_socket;

    // a buffer to avoid frequent memory allocations
    ByteArray m_buffer;

    // the timeout (in seconds)
    ham_u32_t m_timeout;
};

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

#endif /* HAM_ENV_REMOTE_H__ */
