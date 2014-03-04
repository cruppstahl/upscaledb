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
  renv->append_txn_at_tail(this);
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

  /* "flush" (remove) committed and aborted transactions */
  renv->flush_committed_txns();
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

  /* "flush" (remove) committed and aborted transactions */
  renv->flush_committed_txns();
}

#endif // HAM_ENABLE_REMOTE

} // namespace hamsterdb
