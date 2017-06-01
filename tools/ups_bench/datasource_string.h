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

#ifndef UPS_BENCH_DATASOURCE_STRING_H
#define UPS_BENCH_DATASOURCE_STRING_H

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
// abstract base class for a string data source
//
struct StringDatasource : public Datasource
{
  StringDatasource(uint32_t size, bool is_fixed_size)
    : size_(size), is_fixed_size_(is_fixed_size) {
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    std::ifstream infile(DICT);
    std::string line;
    data_.clear();

    while (std::getline(infile, line))
      data_.push_back(line);

    if (data_.size() == 0) {
      ::printf("Sorry, %s seems to be empty or does not exist\n", DICT);
      ::exit(-1);
    }
  }

  void copy_next_and_fill(std::vector<uint8_t> &vec, uint32_t index) {
    // copy the original string
    size_t copied = data_[index].size();
    vec.assign(data_[index].begin(), data_[index].end());

    // make sure that the maximum size (as requested by the user)
    // is not exceeded, or fill with '_' if the string is shorter than
    // the fixed size
    if (copied > size_) {
      vec.resize(size_);
    }
    else if (is_fixed_size_ && copied < size_) {
      vec.resize(size_);
      std::fill_n(vec.begin() + copied, size_ - copied, '_');
    }
  }

  std::vector<std::string> data_;
  uint32_t size_;
  bool is_fixed_size_;
};

struct StringRandomDatasource : public StringDatasource
{
  StringRandomDatasource(uint32_t size, bool is_fixed_size, uint32_t seed = 0)
    : StringDatasource(size, is_fixed_size), seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (seed_)
      rng_.seed(seed_);
    StringDatasource::reset();
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    copy_next_and_fill(vec, rng_() % data_.size());
  }

  boost::mt19937 rng_;
  uint32_t seed_;
};

struct StringAscendingDatasource : public StringDatasource
{
  StringAscendingDatasource(uint32_t size, bool is_fixed_size)
    : StringDatasource(size, is_fixed_size), next_(0) {
    reset();
  }

  // returns the next piece of data; overflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    copy_next_and_fill(vec, next_);

    if (++next_ == data_.size())
      next_ = 0;
  }

  uint32_t next_;
};

struct StringDescendingDatasource : public StringDatasource
{
  StringDescendingDatasource(uint32_t size, bool is_fixed_size)
    : StringDatasource(size, is_fixed_size) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    StringDatasource::reset();
    next_ = data_.size() - 1;
  }

  // returns the next piece of data; overflows are ignored
  virtual void next(std::vector<uint8_t> &vec) {
    copy_next_and_fill(vec, next_);

    if (next_ == 0)
      next_ = data_.size() - 1;
    else
      next_--;
  }

  uint32_t next_;
};

// Zipfian distribution is based on
// http://www.cse.usf.edu/~christen/tools/toolpage.html
struct StringZipfianDatasource : public StringDatasource
{
  // vorberechnen eines datenstroms, der groß genug ist um daraus die
  // ganzen werte abzuleiten (N * size)
  // dann eine NumericZipfianDatasource erzeugen und in diesem binary
  // array entsprechend die daten rauskopieren
  StringZipfianDatasource(uint32_t n, uint32_t size, bool is_fixed_size,
          uint32_t seed = 0, double alpha = 0.8)
    : StringDatasource(size, is_fixed_size), zipf_(n, seed, alpha),
      seed_(seed) {
    reset();
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() {
    if (seed_)
      rng_.seed(seed_);
    StringDatasource::reset();
  }

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) {
    copy_next_and_fill(vec, zipf_.next() % data_.size());
  }

  boost::mt19937 rng_;
  NumericZipfianDatasource<int> zipf_;
  uint32_t seed_;
};

#endif /* UPS_BENCH_DATASOURCE_STRING_H */
