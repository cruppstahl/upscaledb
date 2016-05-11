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

/*
 * A operating-system dependent mutex
 */

#ifndef UPS_MUTEX_H
#define UPS_MUTEX_H

#include "0root/root.h"

#define BOOST_ALL_NO_LIB // disable MSVC auto-linking
#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/condition.hpp>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

typedef boost::mutex::scoped_lock ScopedLock;
typedef boost::thread Thread;
typedef boost::condition Condition;
typedef boost::recursive_mutex RecursiveMutex;

struct Mutex : public boost::mutex 
{
  void acquire_ownership() {
  }

  void safe_unlock() {
    try_lock();
    unlock();
  }
};

template<typename T>
struct ScopedTryLock
{
  ScopedTryLock(T &mutex)
    : mutex_(mutex) {
    locked_ = mutex_.try_lock();
  }

  ~ScopedTryLock() {
    if (locked_)
      mutex_.unlock();
  }

  bool is_locked() const {
    return locked_;
  }

  bool locked_;
  T &mutex_;
};

} // namespace upscaledb

#endif /* UPS_MUTEX_H */
