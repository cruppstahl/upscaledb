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

#ifndef UPS_BENCH_DATASOURCE_BINARY_H
#define UPS_BENCH_DATASOURCE_BINARY_H

#include <string>
#include <boost/limits.hpp>
#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

static std::string alphabet = "0123456789"
                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                              "abcdefghijklmnopqrstuvwxyz";

//
// abstract base class for a data source - generates test data
//
struct BinaryDatasource : public Datasource
{
  BinaryDatasource(uint32_t size, bool is_fixed_size)
    : size_(size), is_fixed_size_(is_fixed_size) {
  }

  uint32_t size_;
  bool is_fixed_size_;
};

struct BinaryRandomDatasource : public BinaryDatasource
{
  BinaryRandomDatasource(uint32_t size, bool is_fixed_size, uint32_t seed = 0)
    : BinaryDatasource(size, is_fixed_size), seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (seed_)
      rng_.seed(seed_);
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    uint32_t current_size = size_;
    if (is_fixed_size_ == false)
      current_size = (rng_() % size_) + 1;
    vec.resize(current_size);

    for (uint32_t i = 0; i < current_size; i++)
      vec[i] = alphabet[rng_() % alphabet.size()];
  }

  boost::mt19937 rng_;
  uint32_t seed_;
};

struct BinaryAscendingDatasource : public BinaryDatasource
{
  BinaryAscendingDatasource(uint32_t size, bool is_fixed_size)
    : BinaryDatasource(size, is_fixed_size) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (is_fixed_size_) {
      data_.resize(size_);
      for (size_t i = 0; i < size_; i++)
        data_[i] = 0;
    }
    else {
      data_.resize(1);
      data_[0] = 0;
    }
  }

  // returns the next piece of data; overflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    vec.resize(data_.size());
    for (size_t i = 0; i < data_.size(); i++)
      vec[i] = alphabet[data_[i % alphabet.size()]];

    size_t size = data_.size();
    if (is_fixed_size_ || data_.size() == size_) {
      for (uint32_t s = size - 1; s >= 0; s--) {
        // if we have an overflow: continue with the next digit
        // otherwise stop
        if (data_[s] == alphabet.size() - 1)
          data_[s] = 0;
        else {
          data_[s]++;
          break;
        }
      }
      // arrived at 'zzzzz...'? restart from beginning
      if (!is_fixed_size_) {
        size_t s;
        for (s = size; s > 0; s--) {
          if (data_[s - 1] != alphabet.size() - 1)
            break;
        }
        if (s == 0)
          data_.resize(0);
      }
    }
    else {
      if (data_.size() < size_) {
        data_.resize(data_.size() + 1);
        data_[data_.size() - 1] = 0;
      }
    }
  }

  std::vector<uint8_t> data_;
};

struct BinaryDescendingDatasource : public BinaryDatasource
{
  BinaryDescendingDatasource(uint32_t size, bool is_fixed_size)
    : BinaryDatasource(size, is_fixed_size) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (is_fixed_size_) {
      data_.resize(size_);
      for (size_t i = 0; i < size_; i++)
        data_[i] = static_cast<uint8_t>(alphabet.size() - 1);
    }
    else {
      data_.resize(1);
      data_[0] = static_cast<uint8_t>(alphabet.size() - 1);
    }
  }

  // returns the next piece of data; overflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    vec.resize(data_.size());
    for (size_t i = 0; i < data_.size(); i++)
      vec[i] = alphabet[data_[i % alphabet.size()]];

    size_t size = data_.size();
    if (is_fixed_size_ || data_.size() == size_) {
      for (uint32_t s = size - 1; s >= 0; s--) {
        if (data_[s] == 0)
          data_[s] = static_cast<uint8_t>(alphabet.size() - 1);
        else {
          data_[s]--;
          break;
        }
      }
      // arrived at '00000...'? restart from scratch
      if (!is_fixed_size_) {
        size_t s;
        for (s = 0; s < size; s++) {
          if (data_[s] != 0)
            break;
        }
        if (s == size)
          data_.resize(0);
      }
    }
    else {
      if (data_.size() < size_) {
        data_.resize(data_.size() + 1);
        data_[data_.size() - 1] = static_cast<uint32_t>(alphabet.size() - 1);
      }
    }
  }

  std::vector<uint8_t> data_;
};

struct BinaryZipfianDatasource : public BinaryDatasource
{
  BinaryZipfianDatasource(size_t n, uint32_t size, bool is_fixed_size,
          long seed = 0, double alpha = 0.8)
    : BinaryDatasource(size, is_fixed_size), n_(n), zipf_(n, seed, alpha),
      seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (seed_)
      rng_.seed(seed_);
    data_.resize(n_ * size_);
    for (uint32_t i = 0; i < (n_ * size_); i++) {
      do {
        data_[i] = rng_() % 0xff;
      } while (!::isalnum(data_[i]));
    }
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    uint32_t current_size = size_;
    if (!is_fixed_size_)
      current_size = (rng_() % size_) + 1;
    vec.resize(current_size);

    int pos = zipf_.next(); 
    for (uint32_t i = 0; i < current_size; i++)
      vec[i] = data_[pos + i];
  }

  uint64_t n_;
  boost::mt19937 rng_;
  NumericZipfianDatasource<int> zipf_;
  std::vector<uint8_t> data_;
  uint32_t seed_;
};

#endif /* UPS_BENCH_DATASOURCE_BINARY_H */
