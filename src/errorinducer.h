/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_ERRORINDUCER_H__
#define HAM_ERRORINDUCER_H__


#include <string.h>

#include <ham/hamsterdb.h>
#include "error.h"

class ErrorInducer
{
  struct State {
    State() : loops(0), error(HAM_INTERNAL_ERROR) { }

    int loops;
    ham_status_t error;
  };

  public:
    enum Action {
      CHANGESET_FLUSH,
      MAX_ACTIONS
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
    State m_state[MAX_ACTIONS];
};

#endif /* HAM_ERRORINDUCER_H__ */
