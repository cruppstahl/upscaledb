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

struct ZipfianGenerator {
  ZipfianGenerator(size_t items, double alpha, uint32_t seed)
      : proba(items), m_u01(m_rng) {
    init(items, alpha, seed);
  }

  void init(size_t items, double alpha, uint32_t seed) {
    assert(items > 0);

    if (seed)
      m_rng.seed(seed);

    double theta = alpha;
    if (theta > 0) {
      double zetan = 1 / zeta(items, theta);
      proba.clear();
      proba.resize(items, 0);
      proba[0] = zetan;
      for (size_t i = 1; i < items; ++i)
        proba[i] = proba[i - 1] + zetan / pow(i + 1, theta);
    }
    else {
      proba.resize(items, 1.0 / items);
    }
  }

  double zeta(int n, double theta) {
    double sum = 0;
    for (long i = 0; i < n; i++) {
      sum += 1.0 / (pow(static_cast<double>(i + 1), theta));
    }
    return sum;
  }

  int nextInt() {
    // Map z to the value
    const double u = m_u01();
    return static_cast<int>(lower_bound(proba.begin(), proba.end(), u)
                    - proba.begin());
  }

  std::vector<double> proba;
  boost::mt19937 m_rng;
  boost::uniform_01<boost::mt19937> m_u01;
};

// Zipfian distribution is based on Daniel Lemire's
// https://github.com/lemire/FastPFor/blob/74c0dc37dcea42c73d3af91e45e234ddc490c091/headers/synthetic.h#L135
template<typename T>
class NumericZipfianDatasource : public Datasource
{
  public:
    NumericZipfianDatasource(size_t n, long seed = 0, double alpha = 0.8)
      : m_n(n), m_cur(0), m_alpha(alpha), m_seed(seed) {
      reset();
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_values.resize(m_n);

      ZipfianGenerator zipf(m_n, m_alpha, m_seed);
      for (size_t k = 0; k < m_n; ++k)
        m_values[k] = zipf.nextInt();
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      T t = get_next_number();
      vec.resize(sizeof(t));
      memcpy(&vec[0], &t, sizeof(t));
    }

    T get_next_number() {
      return (m_values[m_cur++]);
    }

  private:
    size_t m_n;
    size_t m_cur;
    double m_alpha;
    std::vector<double> m_values;
    long m_seed;
};

#endif /* HAM_BENCH_DATASOURCE_NUMERIC_H */
