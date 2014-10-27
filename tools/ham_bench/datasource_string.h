/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BENCH_DATASOURCE_STRING_H
#define HAM_BENCH_DATASOURCE_STRING_H

#include <string>
#include <fstream>
#include <boost/limits.hpp>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

// The file with the (sorted) words
#ifdef WIN32
#  define DICT "words"
#  undef min   // clashes with std::min
#  undef max   // clashes with std::max
#else
#  define DICT "/usr/share/dict/words"
#endif

//
// abstract base class for a data source - generates test data
//
class StringRandomDatasource : public Datasource
{
  public:
    StringRandomDatasource(int size, bool fixed_size, unsigned int seed = 0)
      : m_size(size), m_fixed_size(fixed_size), m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed)
        m_rng.seed(m_seed);
      std::ifstream infile(DICT);
      std::string line;
      m_data.clear();
      while (std::getline(infile, line)) {
        m_data.push_back(line);
      }
      if (m_data.size() == 0) {
        printf("Sorry, %s seems to be empty or does not exist\n", DICT);
        exit(-1);
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.clear();
      int pos = m_rng() % m_data.size();
      size_t i;
      for (i = 0; i < std::min(m_size, m_data[pos].size()); i++)
        vec.push_back(m_data[pos][i]);

      while (vec.size() < m_size) {
        vec.push_back('_');
        pos = m_rng() % m_data.size();
        for (i = 0; vec.size() < m_size && i < m_data[pos].size(); i++)
          vec.push_back(m_data[pos][i]);
      }
    }

  private:
    boost::mt19937 m_rng;
    std::vector<std::string> m_data;
    size_t m_size;
    bool m_fixed_size;
    unsigned int m_seed;
};

class StringAscendingDatasource : public Datasource
{
  public:
    StringAscendingDatasource(int size, bool fixed_size)
      : m_size(size), m_next(0), m_fixed_size(fixed_size) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      std::ifstream infile(DICT);
      std::string line;
      m_data.clear();
      while (std::getline(infile, line)) {
        m_data.push_back(line);
      }
      if (m_data.size() == 0) {
        printf("Sorry, %s seems to be empty or does not exist\n", DICT);
        exit(-1);
      }
    }

    // returns the next piece of data; overflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.clear();
      size_t i;
      for (i = 0; i < std::min(m_data[m_next].size(), m_size); i++)
        vec.push_back(m_data[m_next][i]);
      if (m_fixed_size) {
        for (; i < m_size; i++)
          vec.push_back('_');
      }
      if (++m_next == m_data.size())
        m_next = 0;
    }

  private:
    size_t m_size;
    size_t m_next;
    std::vector<std::string> m_data;
    bool m_fixed_size;
};

class StringDescendingDatasource : public Datasource
{
  public:
    StringDescendingDatasource(int size, bool fixed_size)
      : m_size(size), m_fixed_size(fixed_size) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      std::ifstream infile(DICT);
      std::string line;
      m_data.clear();
      while (std::getline(infile, line)) {
        m_data.push_back(line);
      }
      if (m_data.size() == 0) {
        printf("Sorry, %s seems to be empty or does not exist\n", DICT);
        exit(-1);
      }
      m_next = m_data.size() - 1;
    }

    // returns the next piece of data; overflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.clear();
      size_t i;
      for (i = 0; i < std::min(m_data[m_next].size(), m_size); i++)
        vec.push_back(m_data[m_next][i]);
      if (m_fixed_size) {
        for (; i < m_size; i++)
          vec.push_back('_');
      }
      if (m_next == 0)
        m_next = m_data.size() - 1;
      else
        m_next--;
    }

  private:
    size_t m_size;
    size_t m_next;
    std::vector<std::string> m_data;
    bool m_fixed_size;
};

// Zipfian distribution is based on
// http://www.cse.usf.edu/~christen/tools/toolpage.html
class StringZipfianDatasource : public Datasource
{
  // vorberechnen eines datenstroms, der groß genug ist um daraus die
  // ganzen werte abzuleiten (N * size)
  // dann eine NumericZipfianDatasource erzeugen und in diesem binary
  // array entsprechend die daten rauskopieren
  public:
    StringZipfianDatasource(size_t n, size_t size, bool fixed_size,
            long seed = 0, double alpha = 0.8)
      : m_size(size), m_fixed_size(fixed_size), m_zipf(n, seed, alpha),
        m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed)
        m_rng.seed(m_seed);
      std::ifstream infile(DICT);
      std::string line;
      m_data.clear();
      while (std::getline(infile, line)) {
        m_data.push_back(line);
      }
      if (m_data.size() == 0) {
        printf("Sorry, %s seems to be empty or does not exist\n", DICT);
        exit(-1);
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.clear();
      size_t i;
      int pos = m_zipf.get_next_number() % m_data.size(); 
      for (i = 0; i < std::min(m_size, m_data[pos].size()); i++)
        vec.push_back(m_data[pos][i]);

      if (m_fixed_size) {
        for (; i < m_size; i++)
          vec.push_back('_');
      }
    }

  private:
    boost::mt19937 m_rng;
    size_t m_size;
    bool m_fixed_size;
    NumericZipfianDatasource<int> m_zipf;
    std::vector<std::string> m_data;
    long m_seed;
};

#endif /* HAM_BENCH_DATASOURCE_STRING_H */
