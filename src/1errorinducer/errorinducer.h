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
 * Facility to simulate errors
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_ERRORINDUCER_H
#define HAM_ERRORINDUCER_H

#include "0root/root.h"

#include <string.h>

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

// a macro to invoke errors
#define HAM_INDUCE_ERROR(id)                                        \
  while (ErrorInducer::is_active()) {                               \
    ham_status_t st = ErrorInducer::get_instance()->induce(id);     \
    if (st)                                                         \
      throw Exception(st);                                          \
    break;                                                          \
  }

namespace hamsterdb {

class ErrorInducer {
  struct State {
    State()
      : loops(0), error(HAM_INTERNAL_ERROR) {
    }

    int loops;
    ham_status_t error;
  };

  public:
    enum Action {
      // simulates a failure in Changeset::flush
      kChangesetFlush,

      // simulates a hang in hamserver-connect
      kServerConnect,

      kMaxActions
    };

    // Activates or deactivates the error inducer
    static void activate(bool active) {
      ms_is_active = active;
    }

    // Returns true if the error inducer is active
    static bool is_active() {
      return (ms_is_active);
    }

    // Returns the singleton instance
    static ErrorInducer *get_instance() {
      return (&ms_instance);
    }

    ErrorInducer() {
      memset(&m_state[0], 0, sizeof(m_state));
    }

    void add(Action action, int loops,
            ham_status_t error = HAM_INTERNAL_ERROR) {
      m_state[action].loops = loops;
      m_state[action].error = error;
    }

    ham_status_t induce(Action action) {
      ham_assert(m_state[action].loops >= 0);
      if (m_state[action].loops > 0 && --m_state[action].loops == 0)
        return (m_state[action].error);
      return (0);
    }

  private:
    State m_state[kMaxActions];

    // The singleton instance
    static ErrorInducer ms_instance;

    // Is the ErrorInducer active?
    static bool ms_is_active;
};

} // namespace hamsterdb

#endif /* HAM_ERRORINDUCER_H */
