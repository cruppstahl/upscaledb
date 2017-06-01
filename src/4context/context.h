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

#ifndef UPS_CONTEXT_H
#define UPS_CONTEXT_H

#include "0root/root.h"

#include "3changeset/changeset.h"

namespace upscaledb {

struct Cursor;
struct LocalDb;
struct LocalTxn;
struct LocalEnv;

struct Context {
  Context(LocalEnv *env, LocalTxn *txn = 0, LocalDb *db = 0)
    : txn(txn), db(db), changeset(env) {
  }

  ~Context() {
    changeset.clear();
  }

  LocalTxn *txn;
  LocalDb *db;

  // Each operation has its own changeset which stores all locked pages
  Changeset changeset;
};

} // namespace upscaledb

#endif /* UPS_CONTEXT_H */
