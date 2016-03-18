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
class RemoteEnv;

struct RemoteCursor : public Cursor
{
  // Constructor; retrieves pointer to db and txn, initializes all members
  RemoteCursor(RemoteDb *db, Txn *txn = 0)
    : Cursor(db, txn), remote_handle(0) {
  }

  // Overwrites the current record
  virtual ups_status_t overwrite(ups_record_t *record, uint32_t flags);

  // Get current record size (ups_cursor_get_record_size)
  virtual uint32_t get_record_size();

  // Implementation of get_duplicate_position()
  virtual uint32_t get_duplicate_position();

  // Implementation of get_duplicate_count()
  virtual uint32_t get_duplicate_count(uint32_t flags);

  // Closes the cursor (ups_cursor_close)
  virtual void close();

  // The remote handle
  uint64_t remote_handle;
};

} // namespace upscaledb

#endif /* UPS_CURSOR_REMOTE_H */

#endif /* UPS_ENABLE_REMOTE */
