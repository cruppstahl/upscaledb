/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "config.h"

#include "db.h"
#include "cursor.h"

namespace hamsterdb {

Database::Database(Environment *env, ham_u16_t name, ham_u32_t flags)
  : m_env(env), m_name(name), m_error(0), m_context(0), m_cursor_list(0),
    m_rt_flags(flags)
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

  // decrease the transaction refcount; the refcount specifies how many
  // cursors are attached to the transaction
  if (cursor->get_txn())
    cursor->get_txn()->decrease_cursor_refcount();

  // now finally close the cursor
  cursor_close_impl(cursor);

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
  // TODO the Environment should be responsible to do that??
  m_env->get_database_map().erase(m_name);

  // free cached memory
  get_key_arena().clear();
  get_record_arena().clear();

  m_env = 0;
  return (0);
}

} // namespace hamsterdb
