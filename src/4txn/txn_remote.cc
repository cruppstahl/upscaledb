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

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "2protobuf/protocol.h"
#include "4txn/txn_remote.h"
#include "4env/env_remote.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

RemoteTxn::RemoteTxn(Env *env, const char *name, uint32_t flags,
                uint64_t remote_handle_)
  : Txn(env, name, flags), remote_handle(remote_handle_)
{
}

void
RemoteTxn::commit(uint32_t)
{
  /* There's nothing else to do for this Txn, therefore set it
   * to 'aborted' (although it was committed) */
  flags |= kStateAborted;
}

void
RemoteTxn::abort(uint32_t)
{
  /* this transaction is now aborted! */
  flags |= kStateAborted;
}

void
RemoteTxnManager::begin(Txn *txn)
{
  append_txn_at_tail(txn);
}

ups_status_t
RemoteTxnManager::commit(Txn *txn, uint32_t flags)
{
  try {
    txn->commit(flags);

    /* "flush" (remove) committed and aborted transactions */
    flush_committed_txns();
  }
  catch (Exception &ex) {
    return ex.code;
  }
  return 0;
}

ups_status_t
RemoteTxnManager::abort(Txn *txn, uint32_t flags)
{
  try {
    txn->abort(flags);

    /* "flush" (remove) committed and aborted transactions */
    flush_committed_txns();
  }
  catch (Exception &ex) {
    return ex.code;
  }
  return 0;
}

void
RemoteTxnManager::flush_committed_txns(Context * /* = 0 */)
{
  Txn *oldest;

  while ((oldest = oldest_txn)) {
    if (oldest->is_committed() || oldest->is_aborted()) {
      remove_txn_from_head(oldest);
      delete oldest;
    }
    else
      return;
  }
}

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE
