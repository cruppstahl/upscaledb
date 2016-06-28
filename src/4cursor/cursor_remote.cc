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
