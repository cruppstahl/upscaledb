/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
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

ups_status_t 
Database::create_db(int id)
{
  return (do_create_db(id));
}

ups_status_t 
Database::open_db(int id)
{
  return (do_open_db(id));
}

ups_status_t 
Database::close_db()
{
  return (do_close_db());
}

ups_status_t 
Database::flush()
{
  return (do_flush());
}

ups_status_t 
Database::insert(Txn *txn, ups_key_t *key, ups_record_t *record)
{
  return (do_insert(txn, key, record));
}

ups_status_t 
Database::erase(Txn *txn, ups_key_t *key)
{
  return (do_erase(txn, key));
}

ups_status_t 
Database::find(Txn *txn, ups_key_t *key, ups_record_t *record)
{
  return (do_find(txn, key, record));
}

ups_status_t 
Database::check_integrity()
{
  return (do_check_integrity());
}

Database::Txn *
Database::txn_begin()
{
  return (do_txn_begin());
}

ups_status_t 
Database::txn_commit(Database::Txn *txn)
{
  return (do_txn_commit(txn));
}

ups_status_t 
Database::txn_abort(Database::Txn *txn)
{
  return (do_txn_abort(txn));
}

Database::Cursor *
Database::cursor_create()
{
  return (do_cursor_create());
}

ups_status_t 
Database::cursor_insert(Database::Cursor *cursor, ups_key_t *key,
                ups_record_t *record)
{
  return (do_cursor_insert(cursor, key, record));
}

ups_status_t 
Database::cursor_erase(Database::Cursor *cursor, ups_key_t *key)
{
  return (do_cursor_erase(cursor, key));
}

ups_status_t 
Database::cursor_find(Database::Cursor *cursor, ups_key_t *key,
                ups_record_t *record)
{
  return (do_cursor_find(cursor, key, record));
}

ups_status_t 
Database::cursor_get_previous(Database::Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  return (do_cursor_get_previous(cursor, key, record, skip_duplicates));
}

ups_status_t 
Database::cursor_get_next(Database::Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates)
{
  return (do_cursor_get_next(cursor, key, record, skip_duplicates));
}

ups_status_t 
Database::cursor_close(Database::Cursor *cursor)
{
  return (do_cursor_close(cursor));
}

