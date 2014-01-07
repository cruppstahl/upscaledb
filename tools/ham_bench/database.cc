/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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
Database::check_integrity(Transaction *txn)
{
  return (do_check_integrity(txn));
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
Database::cursor_create(Transaction *txn)
{
  return (do_cursor_create(txn));
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

