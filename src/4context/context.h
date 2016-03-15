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

#ifndef UPS_CONTEXT_H
#define UPS_CONTEXT_H

#include "0root/root.h"

#include "3changeset/changeset.h"

namespace upscaledb {

class Cursor;
class LocalDb;
class LocalEnvironment;
class LocalTxn;

struct Context
{
  Context(LocalEnvironment *env, LocalTxn *txn = 0,
                  LocalDb *db = 0)
    : env(env), txn(txn), db(db), changeset(env) {
  }

  ~Context() {
    changeset.clear();
  }

  LocalEnvironment *env;
  LocalTxn *txn;
  LocalDb *db;

  // Each operation has its own changeset which stores all locked pages
  Changeset changeset;
};

} // namespace upscaledb

#endif /* UPS_CONTEXT_H */
