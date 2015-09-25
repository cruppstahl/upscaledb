/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
