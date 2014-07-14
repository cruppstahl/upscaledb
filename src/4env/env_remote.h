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

#ifndef HAM_ENV_REMOTE_H
#define HAM_ENV_REMOTE_H

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/socket.h"
#include "1base/byte_array.h"
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

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create();

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open();

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(uint16_t oldname, uint16_t newname,
                    uint32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(uint16_t name, uint32_t flags);

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(uint16_t *names,
                    uint32_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(uint32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Begins a new transaction (ham_txn_begin)
    virtual Transaction *txn_begin(const char *name, uint32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(uint32_t flags);

    // Sends |request| to the remote server and blocks till the reply
    // was fully received; returns the reply structure
    Protocol *perform_request(Protocol *request);

    // Sends |request| to the remote server and blocks till the reply
    // was fully received
    void perform_request(SerializedWrapper *request, SerializedWrapper *reply);

    // Returns the remote handle
    uint64_t get_remote_handle() const {
      return (m_remote_handle);
    }

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
