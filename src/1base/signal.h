/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
