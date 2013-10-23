/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_MUTEX_H__
#define HAM_MUTEX_H__

#define BOOST_ALL_NO_LIB // disable MSVC auto-linking
#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/condition.hpp>

namespace hamsterdb {

typedef boost::mutex::scoped_lock ScopedLock;
typedef boost::thread Thread;
typedef boost::condition Condition;

class Mutex : public boost::mutex {
  public:
#if BOOST_VERSION < 103500
    typedef boost::detail::thread::lock_ops<boost::mutex> Ops;

    void lock() {
      Ops::lock(*this);
    }

    void unlock() {
      Ops::unlock(*this);
    }
#endif
};


} // namespace hamsterdb

#endif /* HAM_MUTEX_H__ */
