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

#ifndef UPS_BENCH_BERKELEYDB_H
#define UPS_BENCH_BERKELEYDB_H

#ifdef UPS_WITH_BERKELEYDB

#include <db.h>
#include <ups/upscaledb.h> // for ups_status_t, ups_key_t etc

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
    virtual ups_status_t do_create_env();
    virtual ups_status_t do_open_env();
    virtual ups_status_t do_close_env();
    virtual ups_status_t do_create_db(int id);
    virtual ups_status_t do_open_db(int id);
    virtual ups_status_t do_close_db();
    virtual ups_status_t do_flush();
    virtual ups_status_t do_insert(Txn *txn, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_erase(Txn *txn, ups_key_t *key);
    virtual ups_status_t do_find(Txn *txn, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_check_integrity();

    virtual Txn *do_txn_begin();
    virtual ups_status_t do_txn_commit(Txn *txn);
    virtual ups_status_t do_txn_abort(Txn *txn);

	virtual Cursor *do_cursor_create();
    virtual ups_status_t do_cursor_insert(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_cursor_erase(Cursor *cursor, ups_key_t *key);
    virtual ups_status_t do_cursor_find(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_cursor_get_previous(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates);
    virtual ups_status_t do_cursor_get_next(Cursor *cursor, ups_key_t *key, 
                    ups_record_t *record, bool skip_duplicates);
    virtual ups_status_t do_cursor_close(Cursor *cursor);

  private:
    ups_status_t db2ham(int ret);

    DB *m_db;
    DBC *m_cursor;
};

#endif // UPS_WITH_BERKELEYDB

#endif /* UPS_BENCH_BERKELEYDB_H */
