/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include "db.h"
#include "cursor.h"

namespace hamsterdb {

Database::Database(Environment *env, ham_u16_t name, ham_u32_t flags)
  : m_env(env), m_name(name), m_error(0), m_context(0), m_cursors(0),
    m_rt_flags(flags)
{
}

Cursor *
Database::cursor_clone(Cursor *src)
{
  Cursor *dest = cursor_clone_impl(src);

  // fix the linked list of cursors
  dest->set_previous(0);
  dest->set_next(get_cursors());
  ham_assert(get_cursors() != 0);
  get_cursors()->set_previous(dest);
  set_cursors(dest);

  // initialize the remaining fields
  dest->set_txn(src->get_txn());

  if (src->get_txn())
    src->get_txn()->set_cursor_refcount(
            src->get_txn()->get_cursor_refcount() + 1);

  return (dest);
}

void
Database::cursor_close(Cursor *cursor)
{
  Cursor *p, *n;

  // decrease the transaction refcount; the refcount specifies how many
  // cursors are attached to the transaction
  if (cursor->get_txn()) {
    ham_assert(cursor->get_txn()->get_cursor_refcount() > 0);
    cursor->get_txn()->set_cursor_refcount(
            cursor->get_txn()->get_cursor_refcount() - 1);
  }

  // now finally close the cursor
  cursor_close_impl(cursor);

  // fix the linked list of cursors
  p = cursor->get_previous();
  n = cursor->get_next();

  if (p)
    p->set_next(n);
  else
    set_cursors(n);

  if (n)
    n->set_previous(p);

  cursor->set_next(0);
  cursor->set_previous(0);

  delete cursor;
}

ham_status_t
Database::close(ham_u32_t flags)
{
  // auto-cleanup cursors?
  if (flags & HAM_AUTO_CLEANUP) {
    Cursor *cursor;
    while ((cursor = get_cursors()))
      cursor_close(cursor);
  }
  else if (get_cursors()) {
    ham_trace(("cannot close Database if Cursors are still open"));
    return (set_error(HAM_CURSOR_STILL_OPEN));
  }

  // the derived classes can now do the bulk of the work
  ham_status_t st = close_impl(flags);
  if (st)
    return (set_error(st));

  // remove from the Environment's list
  // TODO the Environment should be responsible to do that??
  m_env->get_database_map().erase(m_name);

  // free cached memory
  get_key_arena().clear();
  get_record_arena().clear();

  m_env = 0;
  return (0);
}

} // namespace hamsterdb
