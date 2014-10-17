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

#ifndef HAM_BENCH_DATASOURCE_NUMERIC_H
#define HAM_BENCH_DATASOURCE_NUMERIC_H

#include <limits>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

#include "datasource.h"

#undef max // avoid MSVC conflict with std::max

//
// abstract base class for a data source - generates test data
//
template<typename T>
class NumericRandomDatasource : public Datasource
{
  public:
    NumericRandomDatasource(unsigned int seed = 0)
      : m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed) {
        m_rng.seed(m_seed);
        m_rng64.seed(m_seed);
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      if (sizeof(T) == 8) {
        T t = (T)m_rng64();
        vec.resize(8);
        memcpy(&vec[0], &t, 8);
      }
      else {
        T t = (T)m_rng();
        vec.resize(sizeof(t));
        memcpy(&vec[0], &t, sizeof(t));
      }
    }

  private:
    boost::mt19937 m_rng;
    boost::mt19937_64 m_rng64;
    unsigned int m_seed;
};

template<typename T>
class NumericAscendingDatasource : public Datasource
{
  public:
    NumericAscendingDatasource() {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_value = 1;
    }

    // returns the next piece of data; overflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      T t = m_value++;
      vec.resize(sizeof(t));
      memcpy(&vec[0], &t, sizeof(t));
    }

  private:
    T m_value;
};

template<typename T>
class NumericDescendingDatasource : public Datasource
{
  public:
    NumericDescendingDatasource() {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_value = std::numeric_limits<T>::max();
    }

    // returns the next piece of data; underflows are ignored
    virtual void get_next(std::vector<uint8_t> &vec) {
      T t = m_value--;
      vec.resize(sizeof(t));
      memcpy(&vec[0], &t, sizeof(t));
    }

  private:
    T m_value;
};

// Zipfian distribution is based on
// http://www.cse.usf.edu/~christen/tools/toolpage.html
template<typename T>
class NumericZipfianDatasource : public Datasource
{
  public:
    NumericZipfianDatasource(uint64_t n, long seed = 0, double alpha = 0.8)
      : m_n(n), m_alpha(alpha), m_u01(m_rng), m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      if (m_seed)
        m_rng.seed(m_seed);

      // Compute normalization constant
      for (uint64_t i = 1; i <= m_n; i++)
        m_c = m_c + (1.0 / pow((double)i, m_alpha));
      m_c = 1.0 / m_c;

      m_values.resize(m_n);
      double sum_prob = 0;
      for (uint64_t i = 1; i <= m_n; i++) {
        sum_prob = sum_prob + m_c / pow((double) i, m_alpha);
        m_values[i - 1] = sum_prob;
      }
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      T t = get_next_number();
      vec.resize(sizeof(t));
      memcpy(&vec[0], &t, sizeof(t));
    }

    T get_next_number() {
      // Pull a uniform random number (0 < z < 1)
      double z = m_u01();
      while (z == 0.0 || z == 1.0)
        z = m_u01();

      // Map z to the value
      for (uint64_t i = 0; i < m_values.size(); i++) {
        if (m_values[i] >= z)
          return ((T)(m_values[i] * m_values.size()));
      }

      assert(!"shouldn't be here");
      return ((T)0);
    }

  private:
    uint64_t m_n;
    double m_alpha;
    double m_c;
    std::vector<double> m_values;
    boost::mt19937 m_rng;
    boost::uniform_01<boost::mt19937> m_u01;
    long m_seed;
};

#endif /* HAM_BENCH_DATASOURCE_NUMERIC_H */
