/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
