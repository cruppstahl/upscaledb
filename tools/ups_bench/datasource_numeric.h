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

#ifndef UPS_BENCH_DATASOURCE_NUMERIC_H
#define UPS_BENCH_DATASOURCE_NUMERIC_H

#include <limits>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/unordered_set.hpp>

#include "datasource.h"

#undef max // avoid MSVC conflict with std::max

//
// abstract base class for a data source - generates test data with a
// uniform distribution
//
template<typename T>
struct NumericRandomDatasource : public Datasource
{
  NumericRandomDatasource(uint32_t seed = 0)
    : seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (seed_) {
      rng_.seed(seed_);
      rng64_.seed(seed_);
    }
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    if (sizeof(T) == 8) {
      T t = static_cast<T>(rng64_());
      vec.resize(8);
      ::memcpy(&vec[0], &t, 8);
    }
    else {
      T t = static_cast<T>(rng_());
      vec.resize(sizeof(t));
      ::memcpy(&vec[0], &t, sizeof(t));
    }
  }

  boost::mt19937 rng_;
  boost::mt19937_64 rng64_;
  uint32_t seed_;
};

template<typename T>
struct NumericAscendingDatasource : public Datasource
{
  NumericAscendingDatasource() {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    value_ = 1;
  }

  // returns the next piece of data; overflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    T t = value_++;
    vec.resize(sizeof(t));
    ::memcpy(&vec[0], &t, sizeof(t));
  }

  T value_;
};

template<typename T>
struct NumericDescendingDatasource : public Datasource
{
  NumericDescendingDatasource() {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    value_ = std::numeric_limits<T>::max();
  }

  // returns the next piece of data; underflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    T t = value_--;
    vec.resize(sizeof(t));
    ::memcpy(&vec[0], &t, sizeof(t));
  }

  T value_;
};

struct ZipfianGenerator {
  ZipfianGenerator(uint32_t items, double alpha, uint32_t seed)
      : proba_(items), u01_(rng_) {
    init(items, alpha, seed);
  }

  void init(uint32_t items, double alpha, uint32_t seed) {
    assert(items > 0);

    if (seed)
      rng_.seed(seed);

    double theta = alpha;
    if (theta > 0) {
      double zetan = 1 / zeta(items, theta);
      proba_.clear();
      proba_.resize(items, 0);
      proba_[0] = zetan;
      for (uint32_t i = 1; i < items; ++i)
        proba_[i] = proba_[i - 1] + zetan / pow(i + 1, theta);
    }
    else {
      proba_.resize(items, 1.0 / items);
    }
  }

  double zeta(int n, double theta) {
    double sum = 0;
    for (long i = 0; i < n; i++) {
      sum += 1.0 / (pow(static_cast<double>(i + 1), theta));
    }
    return sum;
  }

  int next_int() {
    // Map z to the value
    const double u = u01_();
    return static_cast<int>(lower_bound(proba_.begin(), proba_.end(), u)
                    - proba_.begin());
  }

  std::vector<double> proba_;
  boost::mt19937 rng_;
  boost::uniform_01<boost::mt19937> u01_;
};

// Zipfian distribution is based on Daniel Lemire's
// https://github.com/lemire/FastPFor/blob/74c0dc37dcea42c73d3af91e45e234ddc490c091/headers/synthetic.h#L135
template<typename T>
struct NumericZipfianDatasource : public Datasource
{
  NumericZipfianDatasource(uint32_t n, uint32_t seed = 0, double alpha = 0.8)
    : n_(n), cur_(0), alpha_(alpha), seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    values_.resize(n_);

    ZipfianGenerator zipf(n_, alpha_, seed_);
    for (uint32_t k = 0; k < n_; ++k)
      values_[k] = zipf.next_int();
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    T t = next();
    vec.resize(sizeof(t));
    ::memcpy(&vec[0], &t, sizeof(t));
  }

  T next() {
    return (values_[cur_++]);
  }

  uint32_t n_;
  uint32_t cur_;
  double alpha_;
  std::vector<T> values_;
  uint32_t seed_;
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
    : unidg_(seed) {
  }

  // Max value is excluded from range
  template<class iterator>
  void fill_uniform(iterator begin, iterator end, uint32_t min, uint32_t max) {
    unidg_.fast_generate_uniform(static_cast<uint32_t>(end - begin),
                  max - min, buffer_);
    for (size_t k = 0; k < buffer_.size(); ++k)
      *(begin + k) = min + buffer_[k];
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
    const uint32_t cut = n / 2 + (unidg_.rand() % (range - n));
    assert(cut >= n / 2);
    assert(max - min - cut >= n - n / 2);
    const int p = unidg_.rand() % 101;
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

  std::vector<uint32_t> buffer_;
  UniformDataGenerator unidg_;
};

struct NumericClusteredDatasource : public Datasource
{
  NumericClusteredDatasource(uint32_t n, long seed = 0)
    : gen_(seed), n_(n), cur_(0) {
    // this distribution is similar to the timestamps in the leaked AOL
    // search results
    uint32_t max = n_ + (n_ / 8);
    // check against overflow
    if (max < n_)
      max = std::numeric_limits<uint32_t>::max();
    values_ = gen_.generate(n_, max);
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    cur_ = 0;
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    uint32_t t = next();
    vec.resize(sizeof(t));
    ::memcpy(&vec[0], &t, sizeof(t));
  }

  uint32_t next() {
    return (values_[cur_++]);
  }

  ClusteredGenerator gen_;
  size_t n_;
  size_t cur_;
  std::vector<uint32_t> values_;
};

#endif /* UPS_BENCH_DATASOURCE_NUMERIC_H */
