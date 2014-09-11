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

#ifndef TIMER_H
#define TIMER_H

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

#endif // TIMER_H

