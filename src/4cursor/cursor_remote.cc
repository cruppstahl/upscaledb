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

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2protobuf/protocol.h"
#include "4cursor/cursor_remote.h"
#include "4db/db_remote.h"
#include "4env/env_remote.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

// Returns the RemoteEnv instance
static inline RemoteEnv *
renv(RemoteCursor *cursor)
{
  return (RemoteEnv *)cursor->db->env;
}

ups_status_t
RemoteCursor::overwrite(ups_record_t *record, uint32_t flags)
{
  SerializedWrapper request;
  request.id = kCursorOverwriteRequest;
  request.cursor_overwrite_request.cursor_handle = remote_handle;
  request.cursor_overwrite_request.flags = flags;

  if (likely(record->size > 0)) {
    request.cursor_overwrite_request.record.has_data = true;
    request.cursor_overwrite_request.record.data.size = record->size;
    request.cursor_overwrite_request.record.data.value = (uint8_t *)record->data;
  }
  request.cursor_overwrite_request.record.flags = record->flags;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorOverwriteReply);

  return reply.cursor_overwrite_reply.status;
}

uint32_t
RemoteCursor::get_duplicate_position()
{
  SerializedWrapper request;
  request.id = kCursorGetDuplicatePositionRequest;
  request.cursor_get_duplicate_position_request.cursor_handle = remote_handle;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorGetDuplicatePositionReply);

  ups_status_t st = reply.cursor_get_duplicate_position_reply.status;
  if (unlikely(st != 0))
    throw Exception(st);
  return reply.cursor_get_duplicate_position_reply.position;
}

uint32_t
RemoteCursor::get_duplicate_count(uint32_t flags)
{
  SerializedWrapper request;
  request.id = kCursorGetRecordCountRequest;
  request.cursor_get_record_count_request.cursor_handle = remote_handle;
  request.cursor_get_record_count_request.flags = flags;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorGetRecordCountReply);

  ups_status_t st = reply.cursor_get_record_count_reply.status;
  if (unlikely(st != 0))
    throw Exception(st);
  return reply.cursor_get_record_count_reply.count;
}

uint32_t
RemoteCursor::get_record_size()
{
  SerializedWrapper request;
  request.id = kCursorGetRecordSizeRequest;
  request.cursor_get_record_size_request.cursor_handle = remote_handle;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorGetRecordSizeReply);

  ups_status_t st = reply.cursor_get_record_size_reply.status;
  if (unlikely(st != 0))
    throw Exception(st);
  return reply.cursor_get_record_size_reply.size;
}

void
RemoteCursor::close()
{
  SerializedWrapper request;
  request.id = kCursorCloseRequest;
  request.cursor_close_request.cursor_handle = remote_handle;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorCloseReply);
}

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE
