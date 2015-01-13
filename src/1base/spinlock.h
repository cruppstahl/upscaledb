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

#include <sched.h>
#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Spinlock {
    typedef enum {
      kLocked,
      kUnlocked,
      kSpinThreshold = 1000
    } LockState;

  public:
    Spinlock()
      : m_state(kUnlocked) {
    }

    void lock() {
      int k = 0;
      while (m_state.exchange(kLocked, boost::memory_order_acquire)
                      == kLocked) {
        if (++k > kSpinThreshold) {
#ifdef HAM_OS_WIN32
          Sleep(1);
#elif HAVE_SCHED_YIELD
          ::sched_yield();
#else
          // TODO what now?
#endif 
        }
      }
    }

    void unlock() {
      m_state.store(kUnlocked, boost::memory_order_release);
    }

  private:
    boost::atomic<LockState> m_state;
};

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
