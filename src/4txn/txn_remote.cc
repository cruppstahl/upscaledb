/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
RemoteTxn::commit()
{
  /* There's nothing else to do for this Txn, therefore set it
   * to 'aborted' (although it was committed) */
  flags |= kStateAborted;
}

void
RemoteTxn::abort()
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
RemoteTxnManager::commit(Txn *txn)
{
  try {
    txn->commit();

    /* "flush" (remove) committed and aborted transactions */
    flush_committed_txns();
  }
  catch (Exception &ex) {
    return ex.code;
  }
  return 0;
}

ups_status_t
RemoteTxnManager::abort(Txn *txn)
{
  try {
    txn->abort();

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

  while ((oldest = oldest_txn())) {
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
