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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

namespace upscaledb {

ups_status_t
Environment::create()
{
  try {
    return (do_create());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::open()
{
  try {
    return (do_open());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::get_database_names(uint16_t *names, uint32_t *count)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_get_database_names(names, count));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::get_parameters(ups_parameter_t *param)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_get_parameters(param));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::flush(uint32_t flags)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_flush(flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::create_db(Database **pdb, DatabaseConfiguration &config,
                    const ups_parameter_t *param)
{
  try {
    ScopedLock lock(m_mutex);

    ups_status_t st = do_create_db(pdb, config, param);

    // on success: store the open database in the environment's list of
    // opened databases
    if (st == 0) {
      m_database_map[config.db_name] = *pdb;
      /* flush the environment to make sure that the header page is written
       * to disk */
      if (st == 0)
        st = do_flush(0);
    }
    else {
      if (*pdb)
        (void)ups_db_close((ups_db_t *)*pdb, UPS_DONT_LOCK);
    }
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::open_db(Database **pdb, DatabaseConfiguration &config,
                    const ups_parameter_t *param)
{
  try {
    ScopedLock lock(m_mutex);

    /* make sure that this database is not yet open */
    if (m_database_map.find(config.db_name) != m_database_map.end())
      return (UPS_DATABASE_ALREADY_OPEN);

    ups_status_t st = do_open_db(pdb, config, param);

    // on success: store the open database in the environment's list of
    // opened databases
    if (st == 0)
      m_database_map[config.db_name] = *pdb;
    else {
      if (*pdb)
        (void)ups_db_close((ups_db_t *)*pdb, UPS_DONT_LOCK);
    }
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::rename_db(uint16_t oldname, uint16_t newname, uint32_t flags)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_rename_db(oldname, newname, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::erase_db(uint16_t dbname, uint32_t flags)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_erase_db(dbname, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::close_db(Database *db, uint32_t flags)
{
  ups_status_t st = 0;

  try {
    ScopedLock lock;
    if (!(flags & UPS_DONT_LOCK))
      lock = ScopedLock(m_mutex);

    uint16_t dbname = db->name();

    // flush committed Transactions
    st = do_flush(UPS_FLUSH_COMMITTED_TRANSACTIONS);
    if (st)
      return (st);

    st = db->close(flags);
    if (st)
      return (st);

    m_database_map.erase(dbname);
    delete db;

    /* in-memory database: make sure that a database with the same name
     * can be re-created */
    if (m_config.flags & UPS_IN_MEMORY)
      do_erase_db(dbname, 0);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::txn_begin(Transaction **ptxn, const char *name, uint32_t flags)
{
  try {
    ScopedLock lock;
    if (!(flags & UPS_DONT_LOCK))
      lock = ScopedLock(m_mutex);

    if (!(m_config.flags & UPS_ENABLE_TRANSACTIONS)) {
      ups_trace(("transactions are disabled (see UPS_ENABLE_TRANSACTIONS)"));
      return (UPS_INV_PARAMETER);
    }

    *ptxn = do_txn_begin(name, flags);
    return (0);
  }
  catch (Exception &ex) {
    *ptxn = 0;
    return (ex.code);
  }
}

std::string
Environment::txn_get_name(Transaction *txn)
{
  try {
    ScopedLock lock(m_mutex);
    return (txn->get_name());
  }
  catch (Exception &) {
    return ("");
  }
}

ups_status_t
Environment::txn_commit(Transaction *txn, uint32_t flags)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_txn_commit(txn, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::txn_abort(Transaction *txn, uint32_t flags)
{
  try {
    ScopedLock lock(m_mutex);
    return (do_txn_abort(txn, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
Environment::close(uint32_t flags)
{
  ups_status_t st = 0;

  try {
    ScopedLock lock(m_mutex);

    /* auto-abort (or commit) all pending transactions */
    if (m_txn_manager.get()) {
      Transaction *t;

      while ((t = m_txn_manager->get_oldest_txn())) {
        if (!t->is_aborted() && !t->is_committed()) {
          if (flags & UPS_TXN_AUTO_COMMIT)
            st = m_txn_manager->commit(t, 0);
          else /* if (flags & UPS_TXN_AUTO_ABORT) */
            st = m_txn_manager->abort(t, 0);
          if (st)
            return (st);
        }

        m_txn_manager->flush_committed_txns();
      }
    }

    /* flush all remaining transactions */
    if (m_txn_manager)
      m_txn_manager->flush_committed_txns();

    /* close all databases */
    Environment::DatabaseMap::iterator it = m_database_map.begin();
    while (it != m_database_map.end()) {
      Environment::DatabaseMap::iterator it2 = it; it++;
      Database *db = it2->second;
      if (flags & UPS_AUTO_CLEANUP)
        st = close_db(db, flags | UPS_DONT_LOCK);
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

ups_status_t
Environment::fill_metrics(ups_env_metrics_t *metrics)
{
  try {
    ScopedLock lock(m_mutex);
    do_fill_metrics(metrics);
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

EnvironmentTest
Environment::test()
{
  return (EnvironmentTest(m_config));
}

} // namespace upscaledb
