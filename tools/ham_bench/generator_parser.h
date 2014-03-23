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

#ifndef PARSER_GENERATOR_H__
#define PARSER_GENERATOR_H__

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
    ham_key_t generate_key(const char *keydata, char *buffer) const;

    // generates a record from a string
    ham_record_t generate_record(const char *recdata);

    // Tokenizes a line and returns the tokens in a vector
    std::vector<std::string> tokenize(const std::string &str);

    // the currently active Transaction
    Database::Transaction *m_txn;

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
    ham_u32_t m_data_size;

    // cached pointer for record data
    void *m_data_ptr;

    // cached tokens of the current line
    std::vector<std::string> m_tokens;
};

#endif /* PARSER_GENERATOR_H__ */

