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

/*
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_CONTEXT_H
#define HAM_CONTEXT_H

#include "0root/root.h"

#include "3changeset/changeset.h"

namespace hamsterdb {

class Cursor;
class LocalDatabase;
class LocalEnvironment;
class LocalTransaction;

struct Context
{
  Context(LocalEnvironment *env, LocalTransaction *txn = 0,
                  LocalDatabase *db = 0)
    : env(env), txn(txn), db(db), changeset(env) {
  }

  ~Context() {
    changeset.clear();
  }

  LocalEnvironment *env;
  LocalTransaction *txn;
  LocalDatabase *db;

  // Each operation has its own changeset which stores all locked pages
  Changeset changeset;
};

} // namespace hamsterdb

#endif /* HAM_CONTEXT_H */
