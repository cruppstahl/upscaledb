/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * A operating-system dependent mutex
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_MUTEX_H
#define HAM_MUTEX_H

#include "0root/root.h"

#define BOOST_ALL_NO_LIB // disable MSVC auto-linking
#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/condition.hpp>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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

#endif /* HAM_MUTEX_H */
