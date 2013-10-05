/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef BERKELEYDB_H__
#define BERKELEYDB_H__

#ifdef HAM_WITH_BERKELEYDB

#include <db.h>
#include <ham/hamsterdb.h> // for ham_status_t, ham_key_t etc

#include "mutex.h"
#include "database.h"

//
// Database implementation for BerkeleyDb
//
class BerkeleyDatabase : public Database
{
  public:
    BerkeleyDatabase(int id, Configuration *config)
      : Database(id, config), m_db(0), m_cursor(0) {
    }

    // Returns a descriptive name
    //
    // !!
    // This typo ("berkeleydb" -> "berkleydb") is intentional; it makes
    // sure that print_metrics() (in main.cc) creates can properly
    // align its output.
    virtual const char *get_name() const {
      return ("berkleydb");
    }

    // Fills |metrics| with additional metrics
    virtual void get_metrics(Metrics *metrics, bool live = false);

  protected:
    // the actual implementation(s)
    virtual ham_status_t do_create_env();
    virtual ham_status_t do_open_env();
    virtual ham_status_t do_close_env();
    virtual ham_status_t do_create_db(int id);
    virtual ham_status_t do_open_db(int id);
    virtual ham_status_t do_close_db();
    virtual ham_status_t do_flush();
    virtual ham_status_t do_insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record);
    virtual ham_status_t do_erase(Transaction *txn, ham_key_t *key);
    virtual ham_status_t do_find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record);
    virtual ham_status_t do_check_integrity(Transaction *txn);

    virtual Transaction *do_txn_begin();
    virtual ham_status_t do_txn_commit(Transaction *txn);
    virtual ham_status_t do_txn_abort(Transaction *txn);

	virtual Cursor *do_cursor_create(Transaction *txn);
    virtual ham_status_t do_cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record);
    virtual ham_status_t do_cursor_erase(Cursor *cursor, ham_key_t *key);
    virtual ham_status_t do_cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record);
    virtual ham_status_t do_cursor_get_previous(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates);
    virtual ham_status_t do_cursor_get_next(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates);
    virtual ham_status_t do_cursor_close(Cursor *cursor);

  private:
    ham_status_t db2ham(int ret);

    DB *m_db;
    DBC *m_cursor;
};

#endif // HAM_WITH_BERKELEYDB

#endif /* BERKELEYDB_H__ */
