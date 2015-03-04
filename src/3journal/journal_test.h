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
 * Test gateway for the Journal
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_JOURNAL_TEST_H
#define HAM_JOURNAL_TEST_H

#include "0root/root.h"

#include "ham/hamsterdb_int.h" // for metrics

#include "3journal/journal_state.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class JournalTest
{
  public:
    JournalTest(JournalState *state)
      : m_state(state) {
    }

    // Returns the state
    JournalState *state() { return (m_state); }

  private:
    // The journal's state
    JournalState *m_state;  
};

} // namespace hamsterdb

#endif /* HAM_JOURNAL_TEST_H */
