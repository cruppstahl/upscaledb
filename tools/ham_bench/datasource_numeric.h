/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef DATASOURCE_NUMERIC_H__
#define DATASOURCE_NUMERIC_H__

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
    NumericRandomDatasource(unsigned int seed = 0) {
      if (seed) {
        m_rng.seed(seed);
        m_rng64.seed(seed);
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
};

template<typename T>
class NumericAscendingDatasource : public Datasource
{
  public:
    NumericAscendingDatasource()
      : m_value(0) {
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
    NumericDescendingDatasource()
      : m_value(std::numeric_limits<T>::max()) {
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
      : m_alpha(alpha), m_u01(m_rng) {
      if (seed)
        m_rng.seed(seed);

      // Compute normalization constant
      for (uint64_t i = 1; i <= n; i++)
        m_c = m_c + (1.0 / pow((double)i, alpha));
      m_c = 1.0 / m_c;

      m_values.resize(n);
      double sum_prob = 0;
      for (uint64_t i = 1; i <= n; i++) {
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
      for (uint64_t i = 1; i <= m_values.size(); i++) {
        if (m_values[i] >= z)
          return ((T)(m_values[i] * m_values.size()));
      }

      assert(!"shouldn't be here");
      return ((T)0);
    }

  private:
    double m_alpha;
    double m_c;
    std::vector<double> m_values;
    boost::mt19937 m_rng;
    boost::uniform_01<boost::mt19937> m_u01;
};

#endif /* DATASOURCE_NUMERIC_H__ */

