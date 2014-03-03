/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "journal.h"
#include "txn_local.h"
#include "txn_factory.h"
#include "env_local.h"

namespace hamsterdb {

LocalTransaction::LocalTransaction(LocalEnvironment *env, const char *name,
        ham_u32_t flags)
  : Transaction(env, name, flags), m_log_desc(0), m_oldest_op(0),
    m_newest_op(0)
{
  /* append journal entry */
  if (env->get_flags() & HAM_ENABLE_RECOVERY
      && env->get_flags() & HAM_ENABLE_TRANSACTIONS
      && !(flags & HAM_TXN_TEMPORARY)) {
    env->get_journal()->append_txn_begin(this, env, name,
            env->get_incremented_lsn());
  }

  /* link this txn with the Environment */
  env->append_txn_at_tail(this);
}

LocalTransaction::~LocalTransaction()
{
  free_operations();
}

void
LocalTransaction::commit(ham_u32_t flags)
{
  /* are cursors attached to this txn? if yes, fail */
  if (get_cursor_refcount()) {
    ham_trace(("Transaction cannot be committed till all attached "
          "Cursors are closed"));
    throw Exception(HAM_CURSOR_STILL_OPEN);
  }

  /* append journal entry */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS
      && !(m_flags & HAM_TXN_TEMPORARY)) {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    lenv->get_journal()->append_txn_commit(this, lenv->get_incremented_lsn());
  }

  /* this transaction is now committed!  */
  m_flags |= kStateCommitted;

  // TODO ugly - better move flush_committed_txns() in the caller
  LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(m_env);
  if (lenv)
    lenv->flush_committed_txns();
}

void
LocalTransaction::abort(ham_u32_t flags)
{
  /* are cursors attached to this txn? if yes, fail */
  if (get_cursor_refcount()) {
    ham_trace(("Transaction cannot be aborted till all attached "
          "Cursors are closed"));
    throw Exception(HAM_CURSOR_STILL_OPEN);
  }

  /* append journal entry */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS
      && !(m_flags & HAM_TXN_TEMPORARY)) {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    lenv->get_journal()->append_txn_abort(this, lenv->get_incremented_lsn());
  }

  /* this transaction is now aborted!  */
  m_flags |= kStateAborted;

  /* immediately release memory of the cached operations */
  free_operations();

  /* clean up the changeset */
  LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(m_env);
  if (lenv)
    lenv->get_changeset().clear();
}

void
LocalTransaction::free_operations()
{
  TransactionOperation *n, *op = get_oldest_op();

  while (op) {
    n = op->get_next_in_txn();
    TransactionFactory::destroy_operation(op);
    op = n;
  }

  set_oldest_op(0);
  set_newest_op(0);
}

} // namespace hamsterdb
