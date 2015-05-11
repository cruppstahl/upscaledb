/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A fast spinlock, taken from the boost documentation
 * http://www.boost.org/doc/libs/1_57_0/doc/html/atomic/usage_examples.html
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_SPINLOCK_H
#define HAM_SPINLOCK_H

#include "0root/root.h"

#include <stdio.h>
#ifndef HAM_OS_WIN32
#  include <sched.h>
#  include <unistd.h>
#endif
#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/mutex.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

#ifdef HAM_ENABLE_HELGRIND
typedef Mutex Spinlock;
#else

class Spinlock {
    typedef enum {
      kUnlocked      = 0,
      kLocked        = 1,
      kSpinThreshold = 10
    } LockState;

  public:
    Spinlock()
      : m_state(kUnlocked) {
    }

    // Need user-defined copy constructor because boost::atomic<> is not
    // copyable. Initializes an *unlocked* Spinlock.
    Spinlock(const Spinlock &other)
      : m_state(kUnlocked) {
    }

    ~Spinlock() {
      ham_assert(m_state == kUnlocked);
    }

    bool try_lock() {
      if (m_state.exchange(kLocked, boost::memory_order_acquire)
                      != kLocked) {
#ifdef HAM_DEBUG
        m_owner = boost::this_thread::get_id();
#endif
        return (true);
      }
      return (false);
    }

    void lock() {
      int k = 0;
      while (!try_lock())
        spin(k++);
    }

    void unlock() {
      ham_assert(m_state == kLocked);
      ham_assert(m_owner == boost::this_thread::get_id());
      m_state.store(kUnlocked, boost::memory_order_release);
    }

    static void spin(int loop) {
      if (loop < kSpinThreshold) {
#ifdef HAM_OS_WIN32
        ::Sleep(0);
#elif HAVE_SCHED_YIELD
        ::sched_yield();
#else
        ham_assert(!"Please implement me");
#endif 
      }
      else {
#ifdef HAM_OS_WIN32
        ::Sleep(25);
#elif HAVE_USLEEP
        ::usleep(25);
#else
        ham_assert(!"Please implement me");
#endif 
      }
    }

  private:
    boost::atomic<LockState> m_state;
#ifdef HAM_DEBUG
    boost::thread::id m_owner;
#endif
};
#endif // HAM_ENABLE_HELGRIND

class ScopedSpinlock {
  public:
    ScopedSpinlock(Spinlock &lock)
      : m_spinlock(lock) {
      m_spinlock.lock();
    }

    ~ScopedSpinlock() {
      m_spinlock.unlock();
    }

  private:
    Spinlock &m_spinlock;
};

} // namespace hamsterdb

#endif /* HAM_SPINLOCK_H */
