/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_BENCH_TIMER_H
#define UPS_BENCH_TIMER_H

#include <boost/chrono.hpp>
#include <iostream>
#include <iomanip>

using namespace boost::chrono;

// based on
// http://www.boost.org/doc/libs/1_54_0/libs/chrono/example/await_keystroke.cpp
template<class Clock>
struct Timer
{
  Timer()
    : start_(Clock::now()) {
  }

  typename Clock::duration elapsed() const {
    return (Clock::now() - start_);
  }

  double seconds() const {
    return (elapsed().count() *
          ((double)Clock::period::num / Clock::period::den));
  }

  typename Clock::time_point start_;
};

#endif // UPS_BENCH_TIMER_H
