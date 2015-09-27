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
 * Manager for the log sequence number (lsn)
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef UPS_LSN_MANAGER_H
#define UPS_LSN_MANAGER_H

#include "0root/root.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class LsnManager
{
  public:
    // Constructor
    LsnManager()
      : m_state(1) {
    }

    // Returns the next lsn
    uint64_t next() {
      return (m_state++);
    }

  private:
    friend struct LsnManagerTest;

    // the actual lsn
    uint64_t m_state;
};

} // namespace upscaledb

#endif /* UPS_LSN_MANAGER_H */
