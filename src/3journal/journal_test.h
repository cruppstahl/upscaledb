/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
