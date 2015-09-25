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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

Database::Database(Environment *env, DatabaseConfiguration &config)
  : m_env(env), m_config(config), m_error(0), m_context(0), m_cursor_list(0)
{
}

ham_status_t
Database::cursor_create(Cursor **pcursor, Transaction *txn, uint32_t flags)
{
  try {
    Cursor *cursor = cursor_create_impl(txn);

    /* fix the linked list of cursors */
    cursor->set_next(m_cursor_list);
    if (m_cursor_list)
      m_cursor_list->set_previous(cursor);
    m_cursor_list = cursor;

    if (txn)
      txn->increase_cursor_refcount();

    *pcursor = cursor;
    return (0);
  }
  catch (Exception &ex) {
    *pcursor = 0;
    return (ex.code);
  }
}

ham_status_t
Database::cursor_clone(Cursor **pdest, Cursor *src)
{
  try {
    Cursor *dest = cursor_clone_impl(src);

    // fix the linked list of cursors
    dest->set_previous(0);
    dest->set_next(m_cursor_list);
    ham_assert(m_cursor_list != 0);
    m_cursor_list->set_previous(dest);
    m_cursor_list = dest;

    // initialize the remaining fields
    if (src->get_txn())
      src->get_txn()->increase_cursor_refcount();

    *pdest = dest;
    return (0);
  }
  catch (Exception &ex) {
    *pdest = 0;
    return (ex.code);
  }
}

ham_status_t
Database::cursor_close(Cursor *cursor)
{
  try {
    Cursor *p, *n;

    // first close the cursor
    cursor->close();

    // decrease the transaction refcount; the refcount specifies how many
    // cursors are attached to the transaction
    if (cursor->get_txn())
      cursor->get_txn()->decrease_cursor_refcount();

    // fix the linked list of cursors
    p = cursor->get_previous();
    n = cursor->get_next();

    if (p)
      p->set_next(n);
    else
      m_cursor_list = n;

    if (n)
      n->set_previous(p);

    cursor->set_next(0);
    cursor->set_previous(0);

    delete cursor;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

// No need to catch Exceptions - they're caught in Environment::close_db
ham_status_t
Database::close(uint32_t flags)
{
  // auto-cleanup cursors?
  if (flags & HAM_AUTO_CLEANUP) {
    Cursor *cursor;
    while ((cursor = m_cursor_list))
      cursor_close(cursor);
  }
  else if (m_cursor_list) {
    ham_trace(("cannot close Database if Cursors are still open"));
    return (set_error(HAM_CURSOR_STILL_OPEN));
  }

  // the derived classes can now do the bulk of the work
  ham_status_t st = close_impl(flags);
  if (st)
    return (set_error(st));

  m_env = 0;
  return (0);
}

} // namespace hamsterdb
