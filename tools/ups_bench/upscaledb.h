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

#ifndef UPS_BENCH_UPSCALEDB_H
#define UPS_BENCH_UPSCALEDB_H

#include <ups/upscaledb_int.h>
#ifdef UPS_ENABLE_REMOTE
#  include <ups/upscaledb_srv.h>
#endif

#include "mutex.h"
#include "database.h"

//
// abstract base class wrapping a database backend (i.e. upscaledb,
// berkeleydb)
//
class UpscaleDatabase : public Database
{
  public:
    UpscaleDatabase(int id, Configuration *config)
      : Database(id, config), m_env(0), m_db(0), m_txn(0) {
      memset(&m_upscaledb_metrics, 0, sizeof(m_upscaledb_metrics));
    }

    // Returns a descriptive name
    virtual const char *get_name() const {
      return ("upscaledb");
    }

    // Returns true if the database is currently open
    virtual bool is_open() const {
      return (m_db != 0);
    }

    // Fills |metrics| with additional metrics
    virtual void get_metrics(Metrics *metrics, bool live = false) {
      if (live)
        ups_env_get_metrics(ms_env, &metrics->upscaledb_metrics);
      metrics->upscaledb_metrics = m_upscaledb_metrics;
    }

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
    static Mutex ms_mutex;
    static ups_env_t *ms_env;
#ifdef UPS_ENABLE_REMOTE
    static ups_env_t *ms_remote_env;
    static ups_srv_t *ms_srv;
#endif
    static int ms_refcount; // counts threads currently accessing ms_env

    ups_env_t *m_env; // only used to access remote servers
    ups_db_t *m_db;
    ups_env_metrics_t m_upscaledb_metrics;
    ups_txn_t *m_txn;
};

#endif /* UPS_BENCH_UPSCALEDB_H */
