/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_ENV_H
#define HAM_ENV_H

#include "0root/root.h"

#include <map>
#include <string>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/mutex.h"
#include "1base/scoped_ptr.h"
#include "2config/db_config.h"
#include "2config/env_config.h"
#include "4txn/txn.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ham_env_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_env_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ham_env_t {
  int dummy;
};

namespace hamsterdb {

class Database;
class Transaction;

//
// The Environment is the "root" of all hamsterdb objects. It's a container
// for multiple databases and transactions.
//
class Environment
{
  public:
    // A map of all opened Databases
    typedef std::map<ham_u16_t, Database *> DatabaseMap;

    // Constructor
    Environment(EnvironmentConfiguration &config)
      : m_config(config), m_context(0) {
    }

    // Must provide virtual destructor to avoid undefined behaviour (according
    // to g++)
    virtual ~Environment() {
    }

    // Returns the flags which were set when creating/opening the Environment
    ham_u32_t get_flags() const {
      return (m_config.flags);
    }

    // Returns the Environment's configuration
    const EnvironmentConfiguration &get_config() const {
      return (m_config);
    }

    // Sets the filename of the Environment; only for testing!
    void test_set_filename(const std::string &filename) {
      m_config.filename = filename;
    }

    // Returns the user-provided context pointer (ham_env_get_context_data)
    void *get_context_data() {
      return (m_context);
    }

    // Sets the user-provided context pointer (ham_env_set_context_data)
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    // Returns this Environment's mutex
    Mutex &get_mutex() {
      return (m_mutex);
    }

    // Returns the Database Map
    DatabaseMap &get_database_map() {
      return (m_database_map);
    }

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create() = 0;

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open() = 0;

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
                    ham_u32_t flags) = 0;

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags) = 0;

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(ham_u16_t *names,
                    ham_u32_t *count) = 0;

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(ham_u32_t flags) = 0;

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param) = 0;

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param) = 0;

    // Begins a new transaction (ham_txn_begin)
    virtual Transaction *txn_begin(const char *name, ham_u32_t flags) = 0;

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(ham_u32_t flags) = 0;

    // Fills in the current metrics
    virtual void get_metrics(ham_env_metrics_t *metrics) const { };

    // The transaction manager
    TransactionManager *get_txn_manager() {
      return (m_txn_manager.get());
    }

  protected:
    // A mutex to serialize access to this Environment
    Mutex m_mutex;

    // The Environment's configuration
    EnvironmentConfiguration m_config;

    // The Transaction manager; can be 0
    ScopedPtr<TransactionManager> m_txn_manager;

  private:
    // The user-provided context data
    void *m_context;

    // A map of all opened Databases
    DatabaseMap m_database_map;
};

} // namespace hamsterdb

#endif /* HAM_ENV_H */
