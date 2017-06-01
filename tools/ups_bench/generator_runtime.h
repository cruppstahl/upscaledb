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

#ifndef UPS_BENCH_RUNTIME_GENERATOR_H
#define UPS_BENCH_RUNTIME_GENERATOR_H

#include <iostream>
#include <fstream>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/progress.hpp>

#include "metrics.h"
#include "timer.h"
#include "generator.h"
#include "datasource.h"
#include "database.h"

//
// generates data based on configuration settings
//
class RuntimeGenerator : public ::Generator
{
    enum {
      kStateRunning = 0,
      kStateReopening,
      kStateStopped
    };

  public:
    // constructor
    RuntimeGenerator(int id, Configuration *conf, Database *db,
            bool show_progress);

    // destructor
    virtual ~RuntimeGenerator() {
      assert(m_txn == 0);
      assert(m_cursor == 0);
      delete m_datasource;
      delete m_progress;
    }

    // executes the next command from the Datasource
    virtual bool execute();

    // opens the Environment; used for 'reopen'
    virtual void open();

    // closes the Environment; used for 'reopen'
    virtual void close();

    // returns true if the test was successful
    virtual bool was_successful() const {
      return (m_success);
    }

    // returns the collected metrics/statistics
    virtual void get_metrics(Metrics *metrics) {
      m_db->get_metrics(&m_metrics);
      m_metrics.name = m_db->get_name();
      *metrics = m_metrics;
    }

    // commits the currently active transaction; ignored if transactions
    // are disabled or if none is active
    virtual void commit_active_transaction() {
      if (m_txn)
        txn_commit();
    }

    // "tee"s the generated test data to a file (and/or to stdout 
    // if 'verbose' is enabled)
    virtual void tee(const char *foo, const ups_key_t *key = 0,
                    const ups_record_t *record = 0);
 
  private:
    // creates the Environment
    void create();

    // inserts a key/value pair
    double insert();

    // erases a key/value pair
    double erase();

    // lookup of a key/value pair
    double find();

    // perform a table scan
    void tablescan();

    // begins a new transaction
    void txn_begin();

    // commits a transaction
    double txn_commit();

    // aborts a transaction
    void txn_abort();

    // generates a new key, based on the Datasource
    ups_key_t generate_key();

    // generates a new record
    ups_record_t generate_record();

    // which command to execute next?
    int get_next_command();

    // returs true if test should stop now
    bool limit_reached();

    // the current state (running, reopening etc)
    int m_state;

    // counting the number of operations
    uint64_t m_opcount;

    // the datasource
    Datasource *m_datasource;

    // a vector which temporarily stores the data from the Datasource
    std::vector<uint8_t> m_key_data;

    // a vector which temporarily stores the data for the records
    std::vector<uint8_t> m_record_data;

    // rng
    boost::mt19937 m_rng;

    // uniform distribution from 0..1
    boost::uniform_01<boost::mt19937> m_u01;

    // start time
    Timer<boost::chrono::system_clock> m_start;

    // elapsed time
    double m_elapsed_seconds;

    // the currently active Txn
    Database::Txn *m_txn;

    // the currently used Cursor
    Database::Cursor *m_cursor;

    // boost progress bar, can be null if progress is not shown
    boost::progress_display *m_progress;

    // file to dump the generated test data ("tee")
    std::ofstream m_tee;

    // test was successful?
    bool m_success;

    // the collected metrics/statistics
    Metrics m_metrics;

    // only erase everything?
    bool m_erase_only;
};

#endif /* UPS_BENCH_RUNTIME_GENERATOR_H */
