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

    // Returns true if the database is currently open
    virtual bool is_open() const {
      return (m_db != 0);
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
    virtual ham_status_t do_check_integrity();

    virtual Transaction *do_txn_begin();
    virtual ham_status_t do_txn_commit(Transaction *txn);
    virtual ham_status_t do_txn_abort(Transaction *txn);

	virtual Cursor *do_cursor_create();
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
