/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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
 * Implementation for remote cursors
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifdef UPS_ENABLE_REMOTE

#ifndef UPS_CURSOR_REMOTE_H
#define UPS_CURSOR_REMOTE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db_remote.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
class RemoteEnvironment;

class RemoteCursor : public Cursor
{
  public:
    // Constructor; retrieves pointer to db and txn, initializes all members
    RemoteCursor(RemoteDatabase *db, Txn *txn = 0)
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

    // Closes the cursor (ups_cursor_close)
    virtual void close();

  private:
    // Implementation of overwrite()
    virtual ups_status_t do_overwrite(ups_record_t *record, uint32_t flags);

    // Returns number of duplicates (ups_cursor_get_duplicate_count)
    virtual ups_status_t do_get_duplicate_count(uint32_t flags,
                                uint32_t *pcount);

    // Get current record size (ups_cursor_get_record_size)
    virtual ups_status_t do_get_record_size(uint32_t *psize);

    // Implementation of get_duplicate_position()
    virtual ups_status_t do_get_duplicate_position(uint32_t *pposition);

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

} // namespace upscaledb

#endif /* UPS_CURSOR_REMOTE_H */

#endif /* UPS_ENABLE_REMOTE */
