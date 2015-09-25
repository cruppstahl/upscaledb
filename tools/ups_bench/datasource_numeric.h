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

#ifndef UPS_BENCH_DATASOURCE_NUMERIC_H
#define UPS_BENCH_DATASOURCE_NUMERIC_H

#include <limits>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/unordered_set.hpp>

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
    std::vector<T> m_values;
    long m_seed;
};

/*
 * based on Daniel Lemire's implementation of UniformDataGenerator and
 * ClusteredDataGenerator:
 * https://github.com/lemire/SIMDCompressionAndIntersection/blob/master/include/synthetic.h
 */
struct UniformDataGenerator {
  UniformDataGenerator(uint32_t seed)
    : rand(seed) {
  }

  void negate(std::vector<uint32_t> &in, std::vector<uint32_t> &out,
                uint32_t max) {
    out.resize(max - in.size());
    in.push_back(max);
    uint32_t i = 0;
    size_t c = 0;
    for (size_t j = 0; j < in.size() ; ++j) {
      const uint32_t v = in[j];
      for (; i < v; ++i)
        out[c++] = i;
      ++i;
    }
    assert(c == out.size());
  }

  /**
   * fill the vector with N numbers uniformly picked from 0 to max,
   * not including max
   */
  std::vector<uint32_t> generate_uniform_hash(uint32_t n, uint32_t max,
                    std::vector <uint32_t> &ans) {
    assert(max >= n);
    ans.clear();
    if (n == 0)
      return (ans); // nothing to do
    ans.reserve(n);
    assert(max >= 1);
    boost::unordered_set<uint32_t> s;
    while (s.size() < n)
      s.insert(rand() % (max - 1));
    ans.assign(s.begin(), s.end());
    sort(ans.begin(), ans.end());
    assert(n == ans.size());
    return ans;
  }

  void fast_generate_uniform(uint32_t n, uint32_t max,
                    std::vector<uint32_t> &ans) {
    if (2 * n > max) {
      std::vector<uint32_t> buf(n);
      fast_generate_uniform(max - n, max, buf);
      negate(buf, ans, max);
    }
    else {
      generate_uniform_hash(n, max, ans);
    }
  }

  // max value is excluded from range
  std::vector<uint32_t> generate(uint32_t n, uint32_t max) {
    std::vector<uint32_t> ans;
    ans.reserve(n);
    fast_generate_uniform(n, max, ans);
    return (ans);
  }

  boost::mt19937 rand;
};

struct ClusteredGenerator
{
  ClusteredGenerator(uint32_t seed)
    : buffer(), unidg(seed) {
  }

  // Max value is excluded from range
  template<class iterator>
  void fill_uniform(iterator begin, iterator end,
                  uint32_t min, uint32_t max) {
    unidg.fast_generate_uniform(static_cast<uint32_t>(end - begin),
                  max - min, buffer);
    for (size_t k = 0; k < buffer.size(); ++k)
      *(begin + k) = min + buffer[k];
  }

  // Max value is excluded from range
  template<class iterator>
  void fill_clustered(iterator begin, iterator end,
                  uint32_t min, uint32_t max) {
    const uint32_t n = static_cast<uint32_t>(end - begin);
    const uint32_t range = max - min;
    assert(range >= n);
    if ((range == n) || (n < 10)) {
      fill_uniform(begin, end, min, max);
      return;
    }
    const uint32_t cut = n / 2 + (unidg.rand() % (range - n));
    assert(cut >= n / 2);
    assert(max - min - cut >= n - n / 2);
    const int p = unidg.rand() % 101;
    assert(p >= 0 && p <= 100);
    if (p <= 25) {
      fill_uniform(begin, begin + n / 2, min, min + cut);
      fill_clustered(begin + n / 2, end, min + cut, max);
    }
    else if (p <= 50) {
      fill_clustered(begin, begin + n / 2, min, min + cut);
      fill_uniform(begin + n / 2, end, min + cut, max);
    }
    else {
      fill_clustered(begin, begin + n / 2, min, min + cut);
      fill_clustered(begin + n / 2, end, min + cut, max);
    }
  }

  // Max value is excluded from range
  std::vector<uint32_t> generate(uint32_t n, uint32_t max) {
    return (generate_clustered(n, max));
  }

  // Max value is excluded from range
  std::vector<uint32_t> generate_clustered(uint32_t n, uint32_t max) {
    std::vector<uint32_t> ans(n);
    fill_clustered(ans.begin(), ans.end(), 0, max);
    return (ans);
  }

  std::vector<uint32_t> buffer;
  UniformDataGenerator unidg;
};

class NumericClusteredDatasource : public Datasource
{
  public:
    NumericClusteredDatasource(size_t n, long seed = 0)
      : m_gen(seed), m_n(n), m_cur(0) {
      uint32_t max = m_n + (m_n / 8);
      // check against overflow
      if (max < m_n)
        max = std::numeric_limits<uint32_t>::max();
      m_values = m_gen.generate(n, max);
    }

    // resets the input and restarts delivering the same sequence
    // from scratch
    virtual void reset() {
      m_cur = 0;
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) {
      uint32_t t = get_next_number();
      vec.resize(sizeof(t));
      memcpy(&vec[0], &t, sizeof(t));
    }

    uint32_t get_next_number() {
      return (m_values[m_cur++]);
    }

  private:
    ClusteredGenerator m_gen;
    size_t m_n;
    size_t m_cur;
    std::vector<uint32_t> m_values;
};

#endif /* UPS_BENCH_DATASOURCE_NUMERIC_H */
