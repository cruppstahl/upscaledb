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
 * Test gateway for LsnManager
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef HAM_LSN_MANAGER_TEST_H
#define HAM_LSN_MANAGER_TEST_H

#include "0root/root.h"

#include "2lsn_manager/lsn_manager.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct LsnManagerTest
{
  // Constructor
  LsnManagerTest(LsnManager *lsn_manager)
    : m_state(lsn_manager->m_state) {
  }

  // Returns the current lsn
  uint64_t lsn() const {
    return (m_state);
  }

  uint64_t &m_state;
};

} // namespace hamsterdb

#endif /* HAM_LSN_MANAGER_TEST_H */
