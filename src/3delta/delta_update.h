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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_DELTA_UPDATE_H
#define HAM_DELTA_UPDATE_H

#include "0root/root.h"

#include <string.h>

#include <ham/hamsterdb.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "3delta/delta_action.h"
#include "3delta/delta_binding.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class BtreeCursor;
class LocalDatabase;

//
// A DeltaUpdate groups several actions related to the same key
//
class DeltaUpdate
{
  public:
    DeltaUpdate()
      : m_binding(this) {
    }

    // Returns the database of this operation
    LocalDatabase *db() {
      return (m_db);
    }

    // Returns the key of this operation
    ham_key_t *key() {
      return (&m_key);
    }

    // Returns the head of the linked list of DeltaActions
    DeltaAction *actions() {
      return (m_actions);
    }

    // Appends a new DeltaAction to this key
    void append(DeltaAction *action) {
      if (!m_actions_tail) {
        ham_assert(m_actions == 0);
        m_actions = m_actions_tail = action;
      }
      else {
        m_actions_tail->set_next(action);
        m_actions_tail = action;
      }
    }

    // Removes a DeltaAction from this key
    void remove(DeltaAction *action) {
      DeltaAction *previous = 0;

      for (DeltaAction *a = m_actions; a != 0; a = a->next()) {
        if (a == action) {
          if (previous == 0)
            m_actions = a->next();
          else
            previous->set_next(a->next());
          if (m_actions_tail == action)
            m_actions_tail = previous;
          return;
        }
      }

      ham_assert(!"shouldn't be here");
    }

    // Next/Previous pointers in the double-linked list of DeltaUpdates
    DeltaUpdate *next() {
      return (m_next);
    }

    DeltaUpdate *previous() {
      return (m_previous);
    }

    void set_next(DeltaUpdate *du) {
      m_next = du;
    }

    void set_previous(DeltaUpdate *du) {
      m_previous = du;
    }

    // Returns the binding object which can be used to attach cursors
    DeltaBinding &binding() {
      return (m_binding);
    }

  private:
    friend class DeltaUpdateFactory;

    // Initialization
    void initialize(LocalDatabase *db, ham_key_t *key) {
      m_db = db;
      m_actions = 0;
      m_actions_tail = 0;
      m_next = 0;
      m_previous = 0;

      /* copy the key data */
      if (key) {
        m_key = *key;
        if (key->size) {
          m_key.data = &m_data[0];
          ::memcpy(m_key.data, key->data, key->size);
        }
      }
    }

    // the Binding stores a list of attached cursors
    DeltaBinding m_binding;

    // The database
    LocalDatabase *m_db;

    // a list of actions
    DeltaAction *m_actions;

    // the tail of the list
    DeltaAction *m_actions_tail;

    // next/previous pointers of a double-linked list
    DeltaUpdate *m_next, *m_previous;

    // the key which is inserted or overwritten
    ham_key_t m_key;

    // Storage for key->data. This saves us one memory allocation.
    uint8_t m_data[1];
};

} // namespace hamsterdb

#endif /* HAM_DELTA_UPDATE_H */
