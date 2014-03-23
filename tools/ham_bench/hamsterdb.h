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

#ifndef HAMSTERDB_H__
#define HAMSTERDB_H__

#include <ham/hamsterdb_int.h>
#ifdef HAM_ENABLE_REMOTE
#  include <ham/hamsterdb_srv.h>
#endif

#include "mutex.h"
#include "database.h"

//
// abstract base class wrapping a database backend (i.e. hamsterdb,
// berkeleydb)
//
class HamsterDatabase : public Database
{
  public:
    HamsterDatabase(int id, Configuration *config)
      : Database(id, config), m_env(0), m_db(0), m_txn(0) {
      memset(&m_hamster_metrics, 0, sizeof(m_hamster_metrics));
    }

    // Returns a descriptive name
    virtual const char *get_name() const {
      return ("hamsterdb");
    }

    // Returns true if the database is currently open
    virtual bool is_open() const {
      return (m_db != 0);
    }

    // Fills |metrics| with additional metrics
    virtual void get_metrics(Metrics *metrics, bool live = false) {
      if (live)
        ham_env_get_metrics(ms_env, &metrics->hamster_metrics);
      metrics->hamster_metrics = m_hamster_metrics;
    }

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
    static Mutex ms_mutex;
    static ham_env_t *ms_env;
#ifdef HAM_ENABLE_REMOTE
    static ham_env_t *ms_remote_env;
    static ham_srv_t *ms_srv;
#endif
    static int ms_refcount; // counts threads currently accessing ms_env

    ham_env_t *m_env; // only used to access remote servers
    ham_db_t *m_db;
    ham_env_metrics_t m_hamster_metrics;
    ham_txn_t *m_txn;
};

#endif /* HAMSTERDB_H__ */
