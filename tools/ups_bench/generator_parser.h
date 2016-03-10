/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_BENCH_PARSER_GENERATOR_H
#define UPS_BENCH_PARSER_GENERATOR_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <boost/progress.hpp>

#include "timer.h"
#include "metrics.h"
#include "database.h"
#include "generator.h"

//
// executes test scripts
//
class ParserGenerator : public ::Generator
{
  public:
    // constructor
    ParserGenerator(int id, Configuration *conf, Database *db,
            bool show_progress);

    // destructor
    virtual ~ParserGenerator() {
      assert(m_txn == 0);
      assert(m_cursor == 0);
      delete m_progress;
      if (m_data_ptr)
        free(m_data_ptr);
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

  private:
    // creates the Environment
    void create();

    // inserts a key/value pair
    void insert(const char *keydata, const char *recdata, const char *flags);

    // erases a key/value pair
    void erase(const char *keydata);

    // lookup of a key/value pair
    void find(const char *keydata);

    // performs a tablescan
    void tablescan();

    // flushes the database
    void flush();

    // begins a new transaction
    void txn_begin();

    // commits a transaction
    void txn_commit();

    // aborts a transaction
    void txn_abort();

    // Reads the whole test file into m_lines
    void read_file();

    // which command to execute next?
    int get_next_command(const char **pflags, const char **pkeydata,
            const char **precdata = 0);

    // Returns the number of lines
    unsigned get_line_count() const {
      return ((unsigned)m_lines.size());
    }

    // generates a key from a string
    ups_key_t generate_key(const char *keydata, char *buffer) const;

    // generates a record from a string
    ups_record_t generate_record(const char *recdata);

    // Tokenizes a line and returns the tokens in a vector
    std::vector<std::string> tokenize(const std::string &str);

    // the currently active Txn
    Database::Txn *m_txn;

    // the currently used Cursor
    Database::Cursor *m_cursor;

    // boost progress bar, can be null if progress is not shown
    boost::progress_display *m_progress;

    // test was successful?
    bool m_success;

    // the collected metrics/statistics
    Metrics m_metrics;

    // All lines from the file
    std::vector<std::string> m_lines;

    // The current line in m_lines
    uint32_t m_cur_line;

    // start time
    Timer<boost::chrono::system_clock> m_start;

    // size of the m_data_ptr array
    uint32_t m_data_size;

    // cached pointer for record data
    void *m_data_ptr;

    // cached tokens of the current line
    std::vector<std::string> m_tokens;
};

#endif /* UPS_BENCH_PARSER_GENERATOR_H */
