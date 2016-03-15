/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_ENV_REMOTE_H
#define UPS_ENV_REMOTE_H

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/socket.h"
#include "1base/dynamic_array.h"
#include "2protobuf/protocol.h"
#include "2protoserde/messages.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The Environment implementation for remote file access
//
class RemoteEnvironment : public Environment
{
  public:
    // Constructor
    RemoteEnvironment(EnvConfig config);

    // Sends a |request| message with the Google Protocol Buffers API. Blocks
    // till the reply was fully received. Returns the reply structure.
    Protocol *perform_request(Protocol *request);

    // Sends |request| message with the builtin Serde API. Blocks till the
    // reply was fully received. Fills |reply| with the received data.
    void perform_request(SerializedWrapper *request, SerializedWrapper *reply);

    // Performs a UQI select
    virtual ups_status_t select_range(const char *query, Cursor *begin,
                            const Cursor *end, Result **result);

  protected:
    // Creates a new Environment (ups_env_create)
    virtual ups_status_t do_create();

    // Opens a new Environment (ups_env_open)
    virtual ups_status_t do_open();

    // Returns all database names (ups_env_get_database_names)
    virtual ups_status_t do_get_database_names(uint16_t *names,
                    uint32_t *count);

    // Returns environment parameters and flags (ups_env_get_parameters)
    virtual ups_status_t do_get_parameters(ups_parameter_t *param);

    // Flushes the environment and its databases to disk (ups_env_flush)
    virtual ups_status_t do_flush(uint32_t flags);

    // Creates a new database in the environment (ups_env_create_db)
    virtual ups_status_t do_create_db(Db **db, DbConfig &config,
                    const ups_parameter_t *param);

    // Opens an existing database in the environment (ups_env_open_db)
    virtual ups_status_t do_open_db(Db **db, DbConfig &config,
                    const ups_parameter_t *param);

    // Renames a database in the Environment (ups_env_rename_db)
    virtual ups_status_t do_rename_db(uint16_t oldname, uint16_t newname,
                    uint32_t flags);

    // Erases (deletes) a database from the Environment (ups_env_erase_db)
    virtual ups_status_t do_erase_db(uint16_t name, uint32_t flags);

    // Begins a new transaction (ups_txn_begin)
    virtual Txn *do_txn_begin(const char *name, uint32_t flags);

    // Commits a transaction (ups_txn_commit)
    virtual ups_status_t do_txn_commit(Txn *txn, uint32_t flags);

    // Commits a transaction (ups_txn_abort)
    virtual ups_status_t do_txn_abort(Txn *txn, uint32_t flags);

    // Closes the Environment (ups_env_close)
    virtual ups_status_t do_close(uint32_t flags);

    // Fills in the current metrics
    virtual void do_fill_metrics(ups_env_metrics_t *metrics) const;

  private:
    // the remote handle
    uint64_t m_remote_handle;

    // the socket
    Socket m_socket;

    // a buffer to avoid frequent memory allocations
    ByteArray m_buffer;
};

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

#endif /* UPS_ENV_REMOTE_H */
