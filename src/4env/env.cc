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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

namespace hamsterdb {

ham_status_t
Environment::create()
{
  try {
    return (do_create());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::open()
{
  try {
    return (do_open());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::get_database_names(uint16_t *names, uint32_t *count)
{
  try {
    return (do_get_database_names(names, count));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::get_parameters(ham_parameter_t *param)
{
  try {
    return (do_get_parameters(param));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::flush(uint32_t flags)
{
  try {
    return (do_flush(flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::create_db(Database **pdb, DatabaseConfiguration &config,
                    const ham_parameter_t *param)
{
  try {
    ham_status_t st = do_create_db(pdb, config, param);

    // on success: store the open database in the environment's list of
    // opened databases
    if (st == 0)
      m_database_map[config.db_name] = *pdb;
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::open_db(Database **pdb, DatabaseConfiguration &config,
                    const ham_parameter_t *param)
{
  try {
    /* make sure that this database is not yet open */
    if (m_database_map.find(config.db_name) != m_database_map.end())
      return (HAM_DATABASE_ALREADY_OPEN);

    ham_status_t st = do_open_db(pdb, config, param);

    // on success: store the open database in the environment's list of
    // opened databases
    if (st == 0)
      m_database_map[config.db_name] = *pdb;
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::rename_db(uint16_t oldname, uint16_t newname, uint32_t flags)
{
  try {
    return (do_rename_db(oldname, newname, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::erase_db(uint16_t dbname, uint32_t flags)
{
  try {
    return (do_erase_db(dbname, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::close_db(Database *db, uint32_t flags)
{
  ham_status_t st = 0;

  try {
    uint16_t dbname = db->get_name();

    // flush committed Transactions
    st = flush(HAM_FLUSH_COMMITTED_TRANSACTIONS);
    if (st)
      return (st);

    st = db->close(flags);
    if (st)
      return (st);

    m_database_map.erase(dbname);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::txn_begin(Transaction **ptxn, const char *name, uint32_t flags)
{
  try {
    return (do_txn_begin(ptxn, name, flags));
  }
  catch (Exception &ex) {
    *ptxn = 0;
    return (ex.code);
  }
}

ham_status_t
Environment::txn_commit(Transaction *txn, uint32_t flags)
{
  try {
    return (do_txn_commit(txn, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::txn_abort(Transaction *txn, uint32_t flags)
{
  try {
    return (do_txn_abort(txn, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::close(uint32_t flags)
{
  ham_status_t st = 0;

  try {
    /* auto-abort (or commit) all pending transactions */
    if (m_txn_manager.get()) {
      Transaction *t;

      while ((t = m_txn_manager->get_oldest_txn())) {
        if (!t->is_aborted() && !t->is_committed()) {
          if (flags & HAM_TXN_AUTO_COMMIT)
            st = m_txn_manager->commit(t, 0);
          else /* if (flags & HAM_TXN_AUTO_ABORT) */
            st = m_txn_manager->abort(t, 0);
          if (st)
            return (st);
        }

        m_txn_manager->flush_committed_txns();
      }
    }

    /* close all databases */
    Environment::DatabaseMap::iterator it = m_database_map.begin();
    while (it != m_database_map.end()) {
      Environment::DatabaseMap::iterator it2 = it; it++;
      Database *db = it2->second;
      if (flags & HAM_AUTO_CLEANUP)
        st = ham_db_close((ham_db_t *)db, flags | HAM_DONT_LOCK);
      else
        st = db->close(flags);
      if (st)
        return (st);
    }
    m_database_map.clear();

    return (do_close(flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Environment::fill_metrics(ham_env_metrics_t *metrics) const
{
  try {
    do_fill_metrics(metrics);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

} // namespace hamsterdb
