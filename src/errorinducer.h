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

#ifndef HAM_ERRORINDUCER_H__
#define HAM_ERRORINDUCER_H__

#include <string.h>

#include <ham/hamsterdb.h>
#include "error.h"

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
};

} // namespace hamsterdb

#endif /* HAM_ERRORINDUCER_H__ */
