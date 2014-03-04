/**
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

#include "txn.h"
#include "env.h"

namespace hamsterdb {

void
Environment::append_txn_at_tail(Transaction *txn)
{
  if (!get_newest_txn()) {
    ham_assert(get_oldest_txn() == 0);
    set_oldest_txn(txn);
    set_newest_txn(txn);
  }
  else {
    get_newest_txn()->set_next(txn);
    set_newest_txn(txn);
    /* if there's no oldest txn (this means: all txn's but the
     * current one were already flushed) then set this txn as
     * the oldest txn */
    if (!get_oldest_txn())
      set_oldest_txn(txn);
  }
}

void
Environment::remove_txn_from_head(Transaction *txn)
{
  if (get_newest_txn() == txn)
    set_newest_txn(0);

  ham_assert(get_oldest_txn() == txn);
  set_oldest_txn(txn->get_next());
}

} // namespace hamsterdb
