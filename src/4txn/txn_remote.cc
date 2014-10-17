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
                uint32_t flags)
  : Transaction(env, name, flags), m_remote_handle(0)
{
  RemoteEnvironment *renv = dynamic_cast<RemoteEnvironment *>(m_env);

  SerializedWrapper request;
  request.id = kTxnBeginRequest;
  request.txn_begin_request.env_handle = renv->get_remote_handle();
  request.txn_begin_request.flags = flags;
  if (name) {
    request.txn_begin_request.name.value = (uint8_t *)name;
    request.txn_begin_request.name.size = strlen(name) + 1;
  }

  SerializedWrapper reply;
  renv->perform_request(&request, &reply);
  ham_assert(reply.id == kTxnBeginReply);

  ham_status_t st = reply.txn_begin_reply.status;
  if (st)
    throw Exception(st);

  /* this transaction is now committed! */
  m_flags |= kStateCommitted;

  set_remote_handle(reply.txn_begin_reply.txn_handle);
}

void
RemoteTransaction::commit(uint32_t flags)
{
  RemoteEnvironment *renv = dynamic_cast<RemoteEnvironment *>(m_env);

  SerializedWrapper request;
  request.id = kTxnCommitRequest;
  request.txn_commit_request.txn_handle = get_remote_handle();
  request.txn_commit_request.flags = flags;

  SerializedWrapper reply;
  renv->perform_request(&request, &reply);
  ham_assert(reply.id == kTxnCommitReply);

  ham_status_t st = reply.txn_commit_reply.status;
  if (st)
    throw Exception(st);

  /* this transaction is now aborted! */
  m_flags |= kStateAborted;
}

void
RemoteTransaction::abort(uint32_t flags)
{
  RemoteEnvironment *renv = dynamic_cast<RemoteEnvironment *>(m_env);

  SerializedWrapper request;
  request.id = kTxnAbortRequest;
  request.txn_abort_request.txn_handle = get_remote_handle();
  request.txn_abort_request.flags = flags;

  SerializedWrapper reply;
  renv->perform_request(&request, &reply);
  ham_assert(reply.id == kTxnAbortReply);
  ham_status_t st = reply.txn_abort_reply.status;
  if (st)
    throw Exception(st);
}

Transaction *
RemoteTransactionManager::begin(const char *name, uint32_t flags)
{
  Transaction *txn = new RemoteTransaction(m_env, name, flags);

  append_txn_at_tail(txn);
  return (txn);
}

void 
RemoteTransactionManager::commit(Transaction *txn, uint32_t flags)
{
  txn->commit(flags);

  /* "flush" (remove) committed and aborted transactions */
  flush_committed_txns();
}

void 
RemoteTransactionManager::abort(Transaction *txn, uint32_t flags)
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
