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

#ifndef HAM_ENV_REMOTE_H
#define HAM_ENV_REMOTE_H

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/socket.h"
#include "1base/dynamic_array.h"
#include "2protobuf/protocol.h"
#include "2protoserde/messages.h"
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The Environment implementation for remote file access
//
class RemoteEnvironment : public Environment
{
  public:
    // Constructor
    RemoteEnvironment(EnvironmentConfiguration config);

    // Sends a |request| message with the Google Protocol Buffers API. Blocks
    // till the reply was fully received. Returns the reply structure.
    Protocol *perform_request(Protocol *request);

    // Sends |request| message with the builtin Serde API. Blocks till the
    // reply was fully received. Fills |reply| with the received data.
    void perform_request(SerializedWrapper *request, SerializedWrapper *reply);

  protected:
    // Creates a new Environment (ham_env_create)
    virtual ham_status_t do_create();

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t do_open();

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t do_get_database_names(uint16_t *names,
                    uint32_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t do_get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t do_flush(uint32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t do_create_db(Database **db,
                    DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t do_open_db(Database **db,
                    DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t do_rename_db(uint16_t oldname, uint16_t newname,
                    uint32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t do_erase_db(uint16_t name, uint32_t flags);

    // Begins a new transaction (ham_txn_begin)
    virtual Transaction *do_txn_begin(const char *name, uint32_t flags);

    // Commits a transaction (ham_txn_commit)
    virtual ham_status_t do_txn_commit(Transaction *txn, uint32_t flags);

    // Commits a transaction (ham_txn_abort)
    virtual ham_status_t do_txn_abort(Transaction *txn, uint32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t do_close(uint32_t flags);

    // Fills in the current metrics
    virtual void do_fill_metrics(ham_env_metrics_t *metrics) const;

  private:
    // the remote handle
    uint64_t m_remote_handle;

    // the socket
    Socket m_socket;

    // a buffer to avoid frequent memory allocations
    ByteArray m_buffer;
};

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

#endif /* HAM_ENV_REMOTE_H */
