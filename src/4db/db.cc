/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

Cursor *
Database::cursor_create(Transaction *txn, ham_u32_t flags)
{
  Cursor *cursor = cursor_create_impl(txn, flags);

  /* fix the linked list of cursors */
  cursor->set_next(m_cursor_list);
  if (m_cursor_list)
    m_cursor_list->set_previous(cursor);
  m_cursor_list = cursor;

  if (txn)
    txn->increase_cursor_refcount();

  return (cursor);
}

Cursor *
Database::cursor_clone(Cursor *src)
{
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

  return (dest);
}

void
Database::cursor_close(Cursor *cursor)
{
  Cursor *p, *n;

  // first close the cursor
  cursor_close_impl(cursor);

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
}

ham_status_t
Database::close(ham_u32_t flags)
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

  // remove from the Environment's list
  m_env->get_database_map().erase(m_config.db_name);

  m_env = 0;
  return (0);
}

} // namespace hamsterdb
