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
 * Test gateway for the Journal
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef UPS_JOURNAL_TEST_H
#define UPS_JOURNAL_TEST_H

#include "0root/root.h"

#include "ups/upscaledb_int.h" // for metrics

#include "3journal/journal_state.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
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

#endif /* UPS_JOURNAL_TEST_H */
