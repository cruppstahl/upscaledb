/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef TIMER_H__
#define TIMER_H__

#include <boost/chrono.hpp>
#include <iostream>
#include <iomanip>

using namespace boost::chrono;

// based on
// http://www.boost.org/doc/libs/1_54_0/libs/chrono/example/await_keystroke.cpp
template<class Clock>
class Timer
{
    typename Clock::time_point start;

  public:
    Timer()
      : start(Clock::now()) {
    }

    typename Clock::duration elapsed() const {
      return (Clock::now() - start);
    }

    double seconds() const {
      return (elapsed().count() *
            ((double)Clock::period::num / Clock::period::den));
    }
};

#endif // TIMER_H__

