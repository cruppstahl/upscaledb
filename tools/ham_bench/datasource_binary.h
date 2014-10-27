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

#ifndef HAM_BENCH_DATASOURCE_BINARY_H
#define HAM_BENCH_DATASOURCE_BINARY_H

#include <string>
#include <boost/limits.hpp>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

//
// abstract base class for a data source - generates test data
//
class BinaryRandomDatasource : public Datasource
{
  public:
    BinaryRandomDatasource(int size, bool fixed_size, unsigned int seed = 0)
      : m_size(size), m_fixed_size(fixed_size), m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed)
        m_rng.seed(m_seed);
      uint8_t ch = 0;
      for (size_t i = 0; i < sizeof(m_data); i++) {
        while (!std::isalnum(ch))
          ch++;
        m_data[i] = ch++;
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      int size = m_size;
      if (m_fixed_size == false)
        size = (m_rng() % m_size) + 1;
      vec.resize(size);

      for (int i = 0; i < size; i++)
        vec[i] = m_data[m_rng() % sizeof(m_data)];
    }

  private:
    boost::mt19937 m_rng;
    unsigned char m_data[256];
    int m_size;
    bool m_fixed_size;
    unsigned int m_seed;
};

class BinaryAscendingDatasource : public Datasource
{
  public:
    BinaryAscendingDatasource(int size, bool fixed_size)
      : m_size(size), m_fixed_size(fixed_size) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_alphabet = "0123456789"
              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
              "abcdefghijklmnopqrstuvwxyz";
      if (m_fixed_size) {
        m_data.resize(m_size);
        for (size_t i = 0; i < m_size; i++)
          m_data[i] = 0;
      }
      else {
        m_data.resize(1);
        m_data[0] = 0;
      }
    }

    // returns the next piece of data; overflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.resize(m_data.size());
      for (size_t i = 0; i < m_data.size(); i++)
        vec[i] = m_alphabet[m_data[i]];

      size_t size = m_data.size();
      if (m_fixed_size || m_data.size() == m_size) {
        for (int s = (int)size - 1; s >= 0; s--) {
          // if we have an overflow: continue with the next digit
          // otherwise stop
          if (m_data[s] == m_alphabet.size() - 1)
            m_data[s] = 0;
          else {
            m_data[s]++;
            break;
          }
        }
        // arrived at 'zzzzz...'? restart from beginning
        if (!m_fixed_size) {
          size_t s;
          for (s = size; s > 0; s--) {
            if (m_data[s - 1] != m_alphabet.size() - 1)
              break;
          }
          if (s == 0)
            m_data.resize(0);
        }
      }
      else {
        if (m_data.size() < m_size) {
          m_data.resize(m_data.size() + 1);
          m_data[m_data.size() - 1] = 0;
        }
      }
    }

  private:
    size_t m_size;
    std::vector<unsigned char> m_data;
    std::string m_alphabet;
    bool m_fixed_size;
};

class BinaryDescendingDatasource : public Datasource
{
  public:
    BinaryDescendingDatasource(int size, bool fixed_size)
      : m_size(size), m_fixed_size(fixed_size) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_alphabet = "0123456789"
              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
              "abcdefghijklmnopqrstuvwxyz";
      if (m_fixed_size) {
        m_data.resize(m_size);
        for (size_t i = 0; i < m_size; i++)
          m_data[i] = (unsigned char)m_alphabet.size() - 1;
      }
      else {
        m_data.resize(1);
        m_data[0] = (unsigned char)m_alphabet.size() - 1;
      }
    }

    // returns the next piece of data; overflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      vec.resize(m_data.size());
      for (size_t i = 0; i < m_data.size(); i++)
        vec[i] = m_alphabet[m_data[i]];

      size_t size = m_data.size();
      if (m_fixed_size || m_data.size() == m_size) {
        for (int s = (int)size - 1; s >= 0; s--) {
          if (m_data[s] == 0)
            m_data[s] = (unsigned char)m_alphabet.size() - 1;
          else {
            m_data[s]--;
            break;
          }
        }
        // arrived at '00000...'? restart from scratch
        if (!m_fixed_size) {
          size_t s;
          for (s = 0; s < size; s++) {
            if (m_data[s] != 0)
              break;
          }
          if (s == size)
            m_data.resize(0);
        }
      }
      else {
        if (m_data.size() < m_size) {
          m_data.resize(m_data.size() + 1);
          m_data[m_data.size() - 1] = (unsigned char)m_alphabet.size() - 1;
        }
      }
    }

  private:
    size_t m_size;
    std::vector<unsigned char> m_data;
    std::string m_alphabet;
    bool m_fixed_size;
};

// Zipfian distribution is based on
// http://www.cse.usf.edu/~christen/tools/toolpage.html
class BinaryZipfianDatasource : public Datasource
{
  // vorberechnen eines datenstroms, der groß genug ist um daraus die
  // ganzen werte abzuleiten (N * size)
  // dann eine NumericZipfianDatasource erzeugen und in diesem binary
  // array entsprechend die daten rauskopieren
  public:
    BinaryZipfianDatasource(size_t n, size_t size, bool fixed_size,
            long seed = 0, double alpha = 0.8)
      : m_n(n), m_size(size), m_fixed_size(fixed_size), m_zipf(n, seed, alpha),
        m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed)
        m_rng.seed(m_seed);
      m_data.resize((size_t)m_n * m_size);
      for (unsigned i = 0; i < (m_n * m_size); i++) {
        do {
          m_data[i] = m_rng() % 0xff;
        } while (!isalnum(m_data[i]));
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      size_t size = m_size;
      if (!m_fixed_size)
        size = (m_rng() % m_size) + 1;

      vec.resize(size);

      int pos = m_zipf.get_next_number(); 
      for (size_t i = 0; i < size; i++)
        vec[i] = m_data[pos + i];
    }

  private:
    uint64_t m_n;
    boost::mt19937 m_rng;
    size_t m_size;
    bool m_fixed_size;
    NumericZipfianDatasource<int> m_zipf;
    std::vector<unsigned char> m_data;
    unsigned int m_seed;
};

#endif /* HAM_BENCH_DATASOURCE_BINARY_H */
