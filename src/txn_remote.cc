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

#ifdef HAM_ENABLE_REMOTE

#include "config.h"

#include <string.h>

#include "protocol/protocol.h"
#include "txn_remote.h"
#include "env_remote.h"

namespace hamsterdb {

RemoteTransaction::RemoteTransaction(Environment *env, const char *name,
                ham_u32_t flags)
  : Transaction(env, name, flags), m_remote_handle(0)
{
  RemoteEnvironment *renv = dynamic_cast<RemoteEnvironment *>(m_env);
  Protocol request(Protocol::TXN_BEGIN_REQUEST);
  request.mutable_txn_begin_request()->set_env_handle(renv->get_remote_handle());
  request.mutable_txn_begin_request()->set_flags(flags);
  if (name)
    request.mutable_txn_begin_request()->set_name(name);

  std::auto_ptr<Protocol> reply(renv->perform_request(&request));

  ham_assert(reply->has_txn_begin_reply());

  ham_status_t st = reply->txn_begin_reply().status();
  if (st)
    throw Exception(st);

  /* this transaction is now committed! */
  m_flags |= kStateCommitted;

  set_remote_handle(reply->txn_begin_reply().txn_handle());
}

void
RemoteTransaction::commit(ham_u32_t flags)
{
  Protocol request(Protocol::TXN_COMMIT_REQUEST);
  request.mutable_txn_commit_request()->set_txn_handle(get_remote_handle());
  request.mutable_txn_commit_request()->set_flags(flags);

  RemoteEnvironment *renv = dynamic_cast<RemoteEnvironment *>(m_env);
  std::auto_ptr<Protocol> reply(renv->perform_request(&request));

  ham_assert(reply->has_txn_commit_reply());

  ham_status_t st = reply->txn_commit_reply().status();
  if (st)
    throw Exception(st);

  /* this transaction is now aborted! */
  m_flags |= kStateAborted;
}

void
RemoteTransaction::abort(ham_u32_t flags)
{
  Protocol request(Protocol::TXN_ABORT_REQUEST);
  request.mutable_txn_abort_request()->set_txn_handle(get_remote_handle());
  request.mutable_txn_abort_request()->set_flags(flags);

  RemoteEnvironment *renv = (RemoteEnvironment *)m_env;
  std::auto_ptr<Protocol> reply(renv->perform_request(&request));

  ham_assert(reply->has_txn_abort_reply());

  ham_status_t st = reply->txn_abort_reply().status();
  if (st)
    throw Exception(st);
}

Transaction *
RemoteTransactionManager::begin(const char *name, ham_u32_t flags)
{
  Transaction *txn = new RemoteTransaction(m_env, name, flags);

  append_txn_at_tail(txn);
  return (txn);
}

void 
RemoteTransactionManager::commit(Transaction *txn, ham_u32_t flags)
{
  txn->commit(flags);

  /* "flush" (remove) committed and aborted transactions */
  flush_committed_txns();
}

void 
RemoteTransactionManager::abort(Transaction *txn, ham_u32_t flags)
{
  txn->abort(flags);

  /* "flush" (remove) committed and aborted transactions */
  flush_committed_txns();
}

void 
RemoteTransactionManager::flush_committed_txns()
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
