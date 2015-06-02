/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A signal to wait for.
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_SIGNAL_H
#define HAM_SIGNAL_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/mutex.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Signal
{
  Signal()
    : completed(false) {
  }

  void wait() {
    ScopedLock lock(mutex);
    while (!completed)
      cond.wait(lock);
  }

  void notify() {
    ScopedLock lock(mutex);
    completed = true;
    cond.notify_one();
  }

  bool completed;
  Mutex mutex;
  Condition cond;
};

} // namespace hamsterdb

#endif /* HAM_SIGNAL_H */
