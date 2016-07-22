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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
#include "2protobuf/protocol.h"
#include "4db/db_remote.h"
#include "4env/env_remote.h"
#include "4txn/txn_remote.h"
#include "4cursor/cursor_remote.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

static inline RemoteEnv *
renv(RemoteDb *db)
{
  return (RemoteEnv *)db->env;
}

ups_status_t
RemoteDb::get_parameters(ups_parameter_t *param)
{
  Protocol request(Protocol::DB_GET_PARAMETERS_REQUEST);
  request.mutable_db_get_parameters_request()->set_db_handle(remote_handle);

  ups_parameter_t *p = param;
  if (likely(p != 0)) {
    for (; p->name; p++)
      request.mutable_db_get_parameters_request()->add_names(p->name);
  }

  ScopedPtr<Protocol> reply(renv(this)->perform_request(&request));

  assert(reply->has_db_get_parameters_reply());

  ups_status_t st = reply->db_get_parameters_reply().status();
  if (unlikely(st))
    return st;

  p = param;
  while (p && p->name) {
    switch (p->name) {
    case UPS_PARAM_RECORD_COMPRESSION:
      assert(reply->db_get_parameters_reply().has_record_compression());
      p->value = reply->db_get_parameters_reply().record_compression();
      break;
    case UPS_PARAM_KEY_COMPRESSION:
      assert(reply->db_get_parameters_reply().has_key_compression());
      p->value = reply->db_get_parameters_reply().key_compression();
      break;
    case UPS_PARAM_FLAGS:
      assert(reply->db_get_parameters_reply().has_flags());
      p->value = reply->db_get_parameters_reply().flags();
      break;
    case UPS_PARAM_KEY_SIZE:
      assert(reply->db_get_parameters_reply().has_key_size());
      p->value = reply->db_get_parameters_reply().key_size();
      break;
    case UPS_PARAM_RECORD_SIZE:
      assert(reply->db_get_parameters_reply().has_record_size());
      p->value = reply->db_get_parameters_reply().record_size();
      break;
    case UPS_PARAM_KEY_TYPE:
      assert(reply->db_get_parameters_reply().has_key_type());
      p->value = reply->db_get_parameters_reply().key_type();
      break;
    case UPS_PARAM_RECORD_TYPE:
      assert(reply->db_get_parameters_reply().has_record_type());
      p->value = reply->db_get_parameters_reply().record_type();
      break;
    case UPS_PARAM_DATABASE_NAME:
      assert(reply->db_get_parameters_reply().has_dbname());
      p->value = reply->db_get_parameters_reply().dbname();
      break;
    case UPS_PARAM_MAX_KEYS_PER_PAGE:
      assert(reply->db_get_parameters_reply().has_keys_per_page());
      p->value = reply->db_get_parameters_reply().keys_per_page();
      break;
    default:
      ups_trace(("unknown parameter %d", (int)p->name));
      break;
    }
    p++;
  }
  return 0;
}

ups_status_t
RemoteDb::check_integrity(uint32_t flags)
{
  Protocol request(Protocol::DB_CHECK_INTEGRITY_REQUEST);
  request.mutable_db_check_integrity_request()->set_db_handle(remote_handle);
  request.mutable_db_check_integrity_request()->set_flags(flags);

  std::auto_ptr<Protocol> reply(renv(this)->perform_request(&request));

  assert(reply->has_db_check_integrity_reply());
  return reply->db_check_integrity_reply().status();
}

uint64_t
RemoteDb::count(Txn *htxn, bool distinct)
{
  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(htxn);

  SerializedWrapper request;
  request.id = kDbGetKeyCountRequest;
  request.db_count_request.db_handle = remote_handle;
  request.db_count_request.txn_handle = txn ? txn->remote_handle : 0;
  request.db_count_request.distinct = distinct;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);

  assert(reply.id == kDbGetKeyCountReply);

  ups_status_t st = reply.db_count_reply.status;
  if (unlikely(st))
    throw Exception(st);

  return reply.db_count_reply.keycount;
}

ups_status_t
RemoteDb::insert(Cursor *hcursor, Txn *htxn, ups_key_t *key,
            ups_record_t *record, uint32_t flags)
{
  RemoteCursor *cursor = (RemoteCursor *)hcursor;
  bool recno = issetany(this->flags(),
                  UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64);

  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(htxn);

  SerializedWrapper request;
  SerializedWrapper reply;
  ByteArray *arena = &key_arena(txn);

  if (cursor) {
    SerializedWrapper request;
    request.id = kCursorInsertRequest;
    request.cursor_insert_request.cursor_handle = cursor->remote_handle;
    request.cursor_insert_request.flags = flags;
    if (likely(key != 0)) {
      request.cursor_insert_request.has_key = true;
      request.cursor_insert_request.key.has_data = true;
      request.cursor_insert_request.key.data.size = key->size;
      request.cursor_insert_request.key.data.value = (uint8_t *)key->data;
      request.cursor_insert_request.key.flags = key->flags;
      request.cursor_insert_request.key.intflags = key->_flags;
    }
    if (likely(record != 0)) {
      request.cursor_insert_request.has_record = true;
      request.cursor_insert_request.record.has_data = true;
      request.cursor_insert_request.record.data.size = record->size;
      request.cursor_insert_request.record.data.value = (uint8_t *)record->data;
      request.cursor_insert_request.record.flags = record->flags;
    }

    // Record number: ask remote server to send back the key
    request.cursor_insert_request.send_key = recno;

    renv(this)->perform_request(&request, &reply);
    assert(reply.id == kCursorInsertReply);
    ups_status_t st = reply.cursor_insert_reply.status;
    if (unlikely(st))
      return st;

    if (reply.cursor_insert_reply.has_key) {
      key->size = reply.cursor_insert_reply.key.data.size;
      if (!key->data && notset(key->flags, UPS_KEY_USER_ALLOC)) {
        arena->resize(key->size);
        key->data = arena->data();
      }
      ::memcpy(key->data, reply.cursor_insert_reply.key.data.value, key->size);
    }

    return 0;
  }

  request.id = kDbInsertRequest;
  request.db_insert_request.db_handle = remote_handle;
  request.db_insert_request.txn_handle = txn ? txn->remote_handle : 0;
  request.db_insert_request.flags = flags;
  if (likely(key != 0)) {
    request.db_insert_request.has_key = true;
    request.db_insert_request.key.has_data = true;
    request.db_insert_request.key.data.size = key->size;
    request.db_insert_request.key.data.value = (uint8_t *)key->data;
    request.db_insert_request.key.flags = key->flags;
    request.db_insert_request.key.intflags = key->_flags;
  }
  if (likely(record != 0)) {
    request.db_insert_request.has_record = true;
    request.db_insert_request.record.has_data = true;
    request.db_insert_request.record.data.size = record->size;
    request.db_insert_request.record.data.value = (uint8_t *)record->data;
    request.db_insert_request.record.flags = record->flags;
  }

  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kDbInsertReply);
  ups_status_t st = reply.db_insert_reply.status;
  if (unlikely(st))
    return st;

  if (reply.db_insert_reply.has_key) {
    key->size = reply.db_insert_reply.key.data.size;
    if (!key->data && notset(key->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(key->size);
      key->data = arena->data();
    }
    ::memcpy(key->data, reply.db_insert_reply.key.data.value, key->size);
  }

  return 0;
}

ups_status_t
RemoteDb::erase(Cursor *hcursor, Txn *htxn, ups_key_t *key,
            uint32_t flags)
{
  RemoteCursor *cursor = (RemoteCursor *)hcursor;

  if (cursor) {
    SerializedWrapper request;
    request.id = kCursorEraseRequest;
    request.cursor_erase_request.cursor_handle = cursor->remote_handle;
    request.cursor_erase_request.flags = flags;

    SerializedWrapper reply;
    renv(this)->perform_request(&request, &reply);
    assert(reply.id == kCursorEraseReply);
    return reply.cursor_erase_reply.status;
  }

  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(htxn);

  SerializedWrapper request;
  request.id = kDbEraseRequest;
  request.db_erase_request.db_handle = remote_handle;
  request.db_erase_request.txn_handle = txn ? txn->remote_handle : 0;
  request.db_erase_request.flags = flags;
  request.db_erase_request.key.has_data = true;
  request.db_erase_request.key.data.size = key->size;
  request.db_erase_request.key.data.value = (uint8_t *)key->data;
  request.db_erase_request.key.flags = key->flags;
  request.db_erase_request.key.intflags = key->_flags;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kDbEraseReply);
  return reply.db_erase_reply.status;
}

ups_status_t
RemoteDb::find(Cursor *hcursor, Txn *htxn, ups_key_t *key,
              ups_record_t *record, uint32_t flags)
{
  RemoteCursor *cursor = (RemoteCursor *)hcursor;

  if (cursor && !htxn)
    htxn = cursor->txn;

  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(htxn);

  SerializedWrapper request;
  request.id = kDbFindRequest;
  request.db_find_request.db_handle = remote_handle;
  request.db_find_request.cursor_handle = cursor ? cursor->remote_handle : 0;
  request.db_find_request.txn_handle = txn ? txn->remote_handle : 0;
  request.db_find_request.flags = flags;
  request.db_find_request.key.has_data = true;
  request.db_find_request.key.data.size = key->size;
  request.db_find_request.key.data.value = (uint8_t *)key->data;
  request.db_find_request.key.flags = key->flags;
  request.db_find_request.key.intflags = key->_flags;
  if (likely(record != 0)) {
    request.db_find_request.has_record = true;
    request.db_find_request.record.has_data = true;
    request.db_find_request.record.data.size = record->size;
    request.db_find_request.record.data.value = (uint8_t *)record->data;
    request.db_find_request.record.flags = record->flags;
  }

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kDbFindReply);

  ByteArray *pkey_arena = &key_arena(txn);
  ByteArray *rec_arena = &record_arena(txn);

  ups_status_t st = reply.db_find_reply.status;
  if (unlikely(st != 0))
    return st;

  /* approx. matching: need to copy the _flags and the key data! */
  if (reply.db_find_reply.has_key) {
    assert(key);
    key->_flags = reply.db_find_reply.key.intflags;
    key->size = (uint16_t)reply.db_find_reply.key.data.size;
    if (notset(key->flags, UPS_KEY_USER_ALLOC)) {
      pkey_arena->resize(key->size);
      key->data = pkey_arena->data();
    }
    ::memcpy(key->data, (void *)reply.db_find_reply.key.data.value,
                    key->size);
  }
  if (record && reply.db_find_reply.has_record) {
    record->size = reply.db_find_reply.record.data.size;
    if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
      rec_arena->resize(record->size);
      record->data = rec_arena->data();
    }
    ::memcpy(record->data, (void *)reply.db_find_reply.record.data.value,
                    record->size);
  }

  return 0;
}

Cursor *
RemoteDb::cursor_create(Txn *htxn, uint32_t flags)
{
  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(htxn);

  SerializedWrapper request;
  request.id = kCursorCreateRequest;
  request.cursor_create_request.db_handle = remote_handle;
  request.cursor_create_request.txn_handle = txn ? txn->remote_handle : 0;
  request.cursor_create_request.flags = flags;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorCreateReply);
  ups_status_t st = reply.cursor_create_reply.status;
  if (unlikely(st))
    throw Exception(st);

  RemoteCursor *c = new RemoteCursor(this);
  c->remote_handle = reply.cursor_create_reply.cursor_handle;
  return c;
}

Cursor *
RemoteDb::cursor_clone(Cursor *hsrc)
{
  RemoteCursor *src = (RemoteCursor *)hsrc;

  SerializedWrapper request;
  request.id = kCursorCloneRequest;
  request.cursor_clone_request.cursor_handle = src->remote_handle;

  SerializedWrapper reply;
  renv(this)->perform_request(&request, &reply);
  assert(reply.id == kCursorCloneReply);
  ups_status_t st = reply.cursor_clone_reply.status;
  if (unlikely(st))
    return 0;

  RemoteCursor *c = new RemoteCursor(this);
  c->remote_handle = reply.cursor_clone_reply.cursor_handle;
  return c;
}

ups_status_t
RemoteDb::cursor_move(Cursor *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  RemoteCursor *cursor = (RemoteCursor *)hcursor;

  RemoteTxn *txn = dynamic_cast<RemoteTxn *>(cursor->txn);
  ByteArray *pkey_arena = &key_arena(txn);
  ByteArray *prec_arena = &record_arena(txn);

  Protocol request(Protocol::CURSOR_MOVE_REQUEST);
  request.mutable_cursor_move_request()->set_cursor_handle(cursor->remote_handle);
  request.mutable_cursor_move_request()->set_flags(flags);
  if (key)
    Protocol::assign_key(request.mutable_cursor_move_request()->mutable_key(),
                  key, false);
  if (record)
    Protocol::assign_record(request.mutable_cursor_move_request()->mutable_record(),
                  record, false);

  ScopedPtr<Protocol> reply(renv(this)->perform_request(&request));
  assert(reply->has_cursor_move_reply() != 0);
  ups_status_t st = reply->cursor_move_reply().status();
  if (unlikely(st))
    return st;

  /* modify key/record, but make sure that USER_ALLOC is respected! */
  if (reply->cursor_move_reply().has_key()) {
    assert(key);
    key->_flags = reply->cursor_move_reply().key().intflags();
    key->size = (uint16_t)reply->cursor_move_reply().key().data().size();
    if (notset(key->flags, UPS_KEY_USER_ALLOC)) {
      pkey_arena->resize(key->size);
      key->data = pkey_arena->data();
    }
    ::memcpy(key->data, (void *)&reply->cursor_move_reply().key().data()[0],
            key->size);
  }

  /* same for the record */
  if (reply->cursor_move_reply().has_record()) {
    assert(record);
    record->size = reply->cursor_move_reply().record().data().size();
    if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
      prec_arena->resize(record->size);
      record->data = prec_arena->data();
    }
    ::memcpy(record->data, (void *)&reply->cursor_move_reply().record().data()[0],
            record->size);
  }

  return 0;
}

ups_status_t
RemoteDb::close(uint32_t flags)
{
  // do not set UPS_DONT_LOCK over the network
  flags &= ~UPS_DONT_LOCK;

  Protocol request(Protocol::DB_CLOSE_REQUEST);
  request.mutable_db_close_request()->set_db_handle(remote_handle);
  request.mutable_db_close_request()->set_flags(flags);

  ScopedPtr<Protocol> reply(renv(this)->perform_request(&request));

  assert(reply->has_db_close_reply());

  ups_status_t st = reply->db_close_reply().status();
  if (unlikely(st != 0))
    return st;

  remote_handle = 0;
  env = 0;
  return 0;
}


} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

