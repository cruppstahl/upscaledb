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

#include "protocol/protocol.h"
#include "txn_remote.h"
#include "env_remote.h"

namespace hamsterdb {

void
RemoteTransaction::commit(ham_u32_t flags)
{
  Protocol request(Protocol::TXN_COMMIT_REQUEST);
  request.mutable_txn_commit_request()->set_txn_handle(get_remote_handle());
  request.mutable_txn_commit_request()->set_flags(flags);

  RemoteEnvironment *renv = (RemoteEnvironment *)m_env;
  std::auto_ptr<Protocol> reply(renv->perform_request(&request));

  ham_assert(reply->has_txn_commit_reply());

  ham_status_t st = reply->txn_commit_reply().status();
  if (st)
    throw Exception(st);
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


} // namespace hamsterdb
