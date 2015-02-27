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
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_DELTA_BINDING_H
#define HAM_DELTA_BINDING_H

#include "0root/root.h"

#include <vector>

#include <ham/hamsterdb.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class SortedDeltaUpdates;
class BtreeCursor;
class DeltaUpdate;

//
// Bidirectionally attaches and detaches cursors and DeltaUpdates
//
class DeltaBinding
{
  public:
    DeltaBinding(DeltaUpdate *update)
      : m_update(update), m_first(0) {
    }

    // Attaches a BtreeCursor to the DeltaUpdate
    void attach(BtreeCursor *cursor);

    // Returns true if there are cursors attached
    size_t size() const {
      return (m_others.size() + (m_first ? 1 : 0));
    }

    // Returns a cursor from the list
    BtreeCursor *any() {
      return (m_first
                ? m_first
                : m_others.size() > 0
                    ? m_others[0]
                    : 0);
    }

    // Calls t(DeltaUpdate, BtreeCursor) on each attached cursor
    template<typename T>
    void perform(T t) {
      if (m_first)
        t(m_update, m_first);

      for (std::vector<BtreeCursor *>::iterator it = m_others.begin();
            it != m_others.end(); it++)
        t(m_update, *it);
    }

  private:
    // The DeltaUpdate
    DeltaUpdate *m_update;

    // The first pointer to an attached Cursor
    BtreeCursor *m_first;

    // All others are stored in a vector
    std::vector<BtreeCursor *> m_others;
};

} // namespace hamsterdb

#endif /* HAM_DELTA_BINDING_H */
