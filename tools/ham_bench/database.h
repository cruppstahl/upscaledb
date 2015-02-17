/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BENCH_DATABASE_H
#define HAM_BENCH_DATABASE_H

#include <boost/cstdint.hpp>
using namespace boost;

#include <ham/hamsterdb.h>

struct Metrics;
struct Configuration;

//
// abstract base class wrapping a database backend (i.e. hamsterdb,
// berkeleydb)
//
class Database
{
  public:
    enum {
      kDatabaseHamsterdb = 0,
      kDatabaseBerkeleydb
    };

    // A transparent cursor handle 
    typedef uint64_t Cursor;

    // A transparent transaction handle 
    typedef uint64_t Transaction;

    Database(int id, Configuration *config)
      : m_id(id), m_config(config) {
    }

    virtual ~Database() {
    }

    // Returns the ID (i.e. |kDatabaseHamsterdb|)
    int get_id() const {
      return (m_id);
    }

    // Returns true if the database is currently open
    virtual bool is_open() const = 0;

    // Returns a descriptive name
    virtual const char *get_name() const = 0;

    // Creates a global Environment
    void create_env();

    // Opens a global Environment
    void open_env();

    // Closes the global Environment
    void close_env();

    // Actual database functions, calling do_*() (and tracking the time
    // spent in these functions)
    ham_status_t create_db(int id);
    ham_status_t open_db(int id);
    ham_status_t close_db();
    ham_status_t flush();
    ham_status_t insert(Transaction *txn, ham_key_t *key, ham_record_t *record);
    ham_status_t erase(Transaction *txn, ham_key_t *key);
    ham_status_t find(Transaction *txn, ham_key_t *key, ham_record_t *record);
    ham_status_t check_integrity();

    Transaction *txn_begin();
    ham_status_t txn_commit(Transaction *txn);
    ham_status_t txn_abort(Transaction *txn);

	Cursor *cursor_create();
    ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record);
    ham_status_t cursor_erase(Cursor *cursor, ham_key_t *key);
    ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record);
    ham_status_t cursor_get_previous(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates);
    ham_status_t cursor_get_next(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates);
    ham_status_t cursor_close(Cursor *cursor);

    // Fills |metrics| with additional metrics
    virtual void get_metrics(Metrics *metrics, bool live = false) = 0;

  protected:
    // the actual implementation(s)
    virtual ham_status_t do_create_env() = 0;
    virtual ham_status_t do_open_env() = 0;
    virtual ham_status_t do_close_env() = 0;
    virtual ham_status_t do_create_db(int id) = 0;
    virtual ham_status_t do_open_db(int id) = 0;
    virtual ham_status_t do_close_db() = 0;
    virtual ham_status_t do_flush() = 0;
    virtual ham_status_t do_insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record) = 0;
    virtual ham_status_t do_erase(Transaction *txn, ham_key_t *key) = 0;
    virtual ham_status_t do_find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record) = 0;
    virtual ham_status_t do_check_integrity() = 0;

    virtual Transaction *do_txn_begin() = 0;
    virtual ham_status_t do_txn_commit(Transaction *txn) = 0;
    virtual ham_status_t do_txn_abort(Transaction *txn) = 0;

	virtual Cursor *do_cursor_create() = 0;
    virtual ham_status_t do_cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record) = 0;
    virtual ham_status_t do_cursor_erase(Cursor *cursor, ham_key_t *key) = 0;
    virtual ham_status_t do_cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record) = 0;
    virtual ham_status_t do_cursor_get_previous(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates) = 0;
    virtual ham_status_t do_cursor_get_next(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, bool skip_duplicates) = 0;
    virtual ham_status_t do_cursor_close(Cursor *cursor) = 0;

    int m_id;
    Configuration *m_config;
};

#endif /* HAM_BENCH_DATABASE_H */
