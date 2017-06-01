/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * Implementation for remote cursors
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
struct RemoteEnv;

struct RemoteCursor : Cursor {
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
