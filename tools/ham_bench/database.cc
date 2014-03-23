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

#include "configuration.h"
#include "database.h"

void 
Database::create_env()
{
  if (do_create_env() != 0)
    exit(-1);
}

void 
Database::open_env()
{
  if (do_open_env() != 0)
    exit(-1);
}

void 
Database::close_env()
{
  if (do_close_env() != 0)
    exit(-1);
}

ham_status_t 
Database::create_db(int id)
{
  return (do_create_db(id));
}

ham_status_t 
Database::open_db(int id)
{
  return (do_open_db(id));
}

ham_status_t 
Database::close_db()
{
  return (do_close_db());
}

ham_status_t 
Database::flush()
{
  return (do_flush());
}

ham_status_t 
Database::insert(Transaction *txn, ham_key_t *key, ham_record_t *record)
{
  return (do_insert(txn, key, record));
}

ham_status_t 
Database::erase(Transaction *txn, ham_key_t *key)
{
  return (do_erase(txn, key));
}

ham_status_t 
Database::find(Transaction *txn, ham_key_t *key, ham_record_t *record)
{
  return (do_find(txn, key, record));
}

ham_status_t 
Database::check_integrity()
{
  return (do_check_integrity());
}

Database::Transaction *
Database::txn_begin()
{
  return (do_txn_begin());
}

ham_status_t 
Database::txn_commit(Database::Transaction *txn)
{
  return (do_txn_commit(txn));
}

ham_status_t 
Database::txn_abort(Database::Transaction *txn)
{
  return (do_txn_abort(txn));
}

Database::Cursor *
Database::cursor_create()
{
  return (do_cursor_create());
}

ham_status_t 
Database::cursor_insert(Database::Cursor *cursor, ham_key_t *key,
                ham_record_t *record)
{
  return (do_cursor_insert(cursor, key, record));
}

ham_status_t 
Database::cursor_erase(Database::Cursor *cursor, ham_key_t *key)
{
  return (do_cursor_erase(cursor, key));
}

ham_status_t 
Database::cursor_find(Database::Cursor *cursor, ham_key_t *key,
                ham_record_t *record)
{
  return (do_cursor_find(cursor, key, record));
}

ham_status_t 
Database::cursor_get_previous(Database::Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
{
  return (do_cursor_get_previous(cursor, key, record, skip_duplicates));
}

ham_status_t 
Database::cursor_get_next(Database::Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates)
{
  return (do_cursor_get_next(cursor, key, record, skip_duplicates));
}

ham_status_t 
Database::cursor_close(Database::Cursor *cursor)
{
  return (do_cursor_close(cursor));
}

