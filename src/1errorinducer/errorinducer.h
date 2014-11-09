/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * unless prior written permission is obtained from Christoph Rupp.
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
