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
 * Implementation for remote cursors
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_CURSOR_REMOTE_H
#define HAM_CURSOR_REMOTE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db_remote.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
class RemoteEnvironment;

class RemoteCursor : public Cursor
{
  public:
    // Constructor; retrieves pointer to db and txn, initializes all members
    RemoteCursor(RemoteDatabase *db, Transaction *txn = 0)
      : Cursor(db, txn), m_remote_handle(0) {
    }

    // Returns the remote Cursor handle
    uint64_t remote_handle() {
      return (m_remote_handle);
    }

    // Returns the remote Cursor handle
    void set_remote_handle(uint64_t handle) {
      m_remote_handle = handle;
    }

    // Closes the cursor (ham_cursor_close)
    virtual void close();

  private:
    // Implementation of overwrite()
    virtual ham_status_t do_overwrite(ham_record_t *record, uint32_t flags);

    // Returns number of duplicates (ham_cursor_get_duplicate_count)
    virtual ham_status_t do_get_duplicate_count(uint32_t flags,
                                uint32_t *pcount);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t do_get_record_size(uint64_t *psize);

    // Implementation of get_duplicate_position()
    virtual ham_status_t do_get_duplicate_position(uint32_t *pposition);

    // Returns the RemoteDatabase instance
    RemoteDatabase *rdb() {
      return ((RemoteDatabase *)m_db);
    }

    // Returns the RemoteEnvironment instance
    RemoteEnvironment *renv() {
      return ((RemoteEnvironment *)m_db->get_env());
    }

    // The remote handle
    uint64_t m_remote_handle;
};

} // namespace hamsterdb

#endif /* HAM_CURSOR_REMOTE_H */
