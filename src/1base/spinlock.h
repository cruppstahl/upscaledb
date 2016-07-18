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
 * A fast spinlock, taken from the boost documentation
 * http://www.boost.org/doc/libs/1_57_0/doc/html/atomic/usage_examples.html
 */

#ifndef UPS_SPINLOCK_H
#define UPS_SPINLOCK_H

#include "0root/root.h"

#include <stdio.h>
#ifndef WIN32
#  include <sched.h>
#  include <unistd.h>
#endif
#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/mutex.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

#ifdef UPS_ENABLE_HELGRIND
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
      assert(m_state == kUnlocked);
    }

    // Only for test verification: lets the current thread acquire ownership
    // of a locked mutex
    void acquire_ownership() {
#ifndef NDEBUG
      assert(m_state != kUnlocked);
      m_owner = boost::this_thread::get_id();
#endif
    }

    // For debugging and verification; unlocks the mutex, even if it was
    // locked by a different thread
    void safe_unlock() {
#ifndef NDEBUG
      m_owner = boost::this_thread::get_id();
#endif
      m_state.store(kUnlocked, boost::memory_order_release);
    }

    bool try_lock() {
      if (m_state.exchange(kLocked, boost::memory_order_acquire)
                      != kLocked) {
#ifndef NDEBUG
        m_owner = boost::this_thread::get_id();
#endif
        return true;
      }
      return false;
    }

    void lock() {
      int k = 0;
      while (!try_lock())
        spin(k++);
    }

    void unlock() {
      assert(m_state == kLocked);
      assert(m_owner == boost::this_thread::get_id());
      m_state.store(kUnlocked, boost::memory_order_release);
    }

    static void spin(int loop) {
      if (loop < kSpinThreshold) {
#ifdef WIN32
        ::Sleep(0);
#elif HAVE_SCHED_YIELD
        ::sched_yield();
#else
        assert(!"Please implement me");
#endif 
      }
      else {
#ifdef WIN32
        ::Sleep(0);
#elif HAVE_USLEEP
        ::usleep(25);
#else
        assert(!"Please implement me");
#endif 
      }
    }

  private:
    boost::atomic<LockState> m_state;
#ifndef NDEBUG
    boost::thread::id m_owner;
#endif
};
#endif // UPS_ENABLE_HELGRIND

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

} // namespace upscaledb

#endif /* UPS_SPINLOCK_H */
