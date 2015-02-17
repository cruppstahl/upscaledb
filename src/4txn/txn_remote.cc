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
