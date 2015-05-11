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
 * Manager for the log sequence number (lsn)
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef HAM_LSN_MANAGER_H
#define HAM_LSN_MANAGER_H

#include "0root/root.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

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

} // namespace hamsterdb

#endif /* HAM_LSN_MANAGER_H */
