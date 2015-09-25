/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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
// abstract base class wrapping a database backend (i.e. hamsterdb,
// berkeleydb)
//
class UpscaleDatabase : public Database
{
  public:
    UpscaleDatabase(int id, Configuration *config)
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
        ups_env_get_metrics(ms_env, &metrics->hamster_metrics);
      metrics->hamster_metrics = m_hamster_metrics;
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
    virtual ups_status_t do_insert(Transaction *txn, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_erase(Transaction *txn, ups_key_t *key);
    virtual ups_status_t do_find(Transaction *txn, ups_key_t *key,
                    ups_record_t *record);
    virtual ups_status_t do_check_integrity();

    virtual Transaction *do_txn_begin();
    virtual ups_status_t do_txn_commit(Transaction *txn);
    virtual ups_status_t do_txn_abort(Transaction *txn);

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
    ups_env_metrics_t m_hamster_metrics;
    ups_txn_t *m_txn;
};

#endif /* UPS_BENCH_UPSCALEDB_H */
