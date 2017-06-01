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

#ifndef UPS_TXN_REMOTE_H
#define UPS_TXN_REMOTE_H

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4txn/txn.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;

//
// A remote Txn
//
struct RemoteTxn : Txn {
  // Constructor; "begins" the Txn
  // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
  RemoteTxn(Env *env, const char *name, uint32_t flags, uint64_t remote_handle);

  // Commits the Txn
  virtual void commit();

  // Aborts the Txn
  virtual void abort();

  // The remote Txn handle
  uint64_t remote_handle;
};


//
// A TxnManager for remote Txns
//
struct RemoteTxnManager : TxnManager {
  // Constructor
  RemoteTxnManager(Env *env)
    : TxnManager(env) {
  }

  // Begins a new Txn
  virtual void begin(Txn *txn);

  // Commits a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t commit(Txn *txn);

  // Aborts a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t abort(Txn *txn);

  // Flushes committed (queued) transactions
  virtual void flush_committed_txns(Context *context = 0);
};

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

#endif /* UPS_TXN_REMOTE_H */
