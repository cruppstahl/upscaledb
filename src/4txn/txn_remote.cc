/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "2protobuf/protocol.h"
#include "4txn/txn_remote.h"
#include "4env/env_remote.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

RemoteTransaction::RemoteTransaction(Environment *env, const char *name,
                uint32_t flags, uint64_t remote_handle)
  : Transaction(env, name, flags), m_remote_handle(remote_handle)
{
}

void
RemoteTransaction::commit(uint32_t flags)
{
  /* There's nothing else to do for this Transaction, therefore set it
   * to 'aborted' (although it was committed) */
  m_flags |= kStateAborted;
}

void
RemoteTransaction::abort(uint32_t flags)
{
  /* this transaction is now aborted! */
  m_flags |= kStateAborted;
}

void
RemoteTransactionManager::begin(Transaction *txn)
{
  append_txn_at_tail(txn);
}

ham_status_t 
RemoteTransactionManager::commit(Transaction *txn, uint32_t flags)
{
  try {
    txn->commit(flags);

    /* "flush" (remove) committed and aborted transactions */
    flush_committed_txns();
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

ham_status_t 
RemoteTransactionManager::abort(Transaction *txn, uint32_t flags)
{
  try {
    txn->abort(flags);

    /* "flush" (remove) committed and aborted transactions */
    flush_committed_txns();
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

void 
RemoteTransactionManager::flush_committed_txns(Context *context /* = 0 */)
{
  Transaction *oldest;

  while ((oldest = get_oldest_txn())) {
    if (oldest->is_committed() || oldest->is_aborted()) {
      remove_txn_from_head(oldest);
      delete oldest;
    }
    else
      return;
  }
}

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE
