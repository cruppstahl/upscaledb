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

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
#include "2protobuf/protocol.h"
#include "4db/db_remote.h"
#include "4env/env_remote.h"
#include "4txn/txn_remote.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

ham_status_t
RemoteDatabase::get_parameters(ham_parameter_t *param)
{
  try {
    RemoteEnvironment *env = renv();

    Protocol request(Protocol::DB_GET_PARAMETERS_REQUEST);
    request.mutable_db_get_parameters_request()->set_db_handle(m_remote_handle);

    ham_parameter_t *p = param;
    if (p) {
      for (; p->name; p++)
        request.mutable_db_get_parameters_request()->add_names(p->name);
    }

    ScopedPtr<Protocol> reply(env->perform_request(&request));

    ham_assert(reply->has_db_get_parameters_reply());

    ham_status_t st = reply->db_get_parameters_reply().status();
    if (st)
      throw Exception(st);

    p = param;
    while (p && p->name) {
      switch (p->name) {
      case HAM_PARAM_FLAGS:
        ham_assert(reply->db_get_parameters_reply().has_flags());
        p->value = reply->db_get_parameters_reply().flags();
        break;
      case HAM_PARAM_KEY_SIZE:
        ham_assert(reply->db_get_parameters_reply().has_key_size());
        p->value = reply->db_get_parameters_reply().key_size();
        break;
      case HAM_PARAM_RECORD_SIZE:
        ham_assert(reply->db_get_parameters_reply().has_record_size());
        p->value = reply->db_get_parameters_reply().record_size();
        break;
      case HAM_PARAM_KEY_TYPE:
        ham_assert(reply->db_get_parameters_reply().has_key_type());
        p->value = reply->db_get_parameters_reply().key_type();
        break;
      case HAM_PARAM_DATABASE_NAME:
        ham_assert(reply->db_get_parameters_reply().has_dbname());
        p->value = reply->db_get_parameters_reply().dbname();
        break;
      case HAM_PARAM_MAX_KEYS_PER_PAGE:
        ham_assert(reply->db_get_parameters_reply().has_keys_per_page());
        p->value = reply->db_get_parameters_reply().keys_per_page();
        break;
      default:
        ham_trace(("unknown parameter %d", (int)p->name));
        break;
      }
      p++;
    }
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::check_integrity(uint32_t flags)
{
  try {
    RemoteEnvironment *env = renv();

    Protocol request(Protocol::DB_CHECK_INTEGRITY_REQUEST);
    request.mutable_db_check_integrity_request()->set_db_handle(m_remote_handle);
    request.mutable_db_check_integrity_request()->set_flags(flags);

    std::auto_ptr<Protocol> reply(env->perform_request(&request));

    ham_assert(reply->has_db_check_integrity_reply());

    return (reply->db_check_integrity_reply().status());
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::count(Transaction *htxn, bool distinct, uint64_t *pcount)
{
  try {
    RemoteEnvironment *env = renv();
    RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

    SerializedWrapper request;
    request.id = kDbGetKeyCountRequest;
    request.db_count_request.db_handle = m_remote_handle;
    request.db_count_request.txn_handle = txn
              ? txn->get_remote_handle()
              : 0;
    request.db_count_request.distinct = distinct;

    SerializedWrapper reply;
    env->perform_request(&request, &reply);

    ham_assert(reply.id == kDbGetKeyCountReply);

    ham_status_t st = reply.db_count_reply.status;
    if (st)
      return (st);

    *pcount = reply.db_count_reply.keycount;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::insert(Cursor *cursor, Transaction *htxn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  try {
    bool send_key = true;
    RemoteEnvironment *env = renv();
    RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

    ByteArray *arena = &key_arena(txn);

    /* recno: do not send the key */
    if (get_flags() & HAM_RECORD_NUMBER32) {
      send_key = false;
      if (!key->data) {
        arena->resize(sizeof(uint32_t));
        key->data = arena->get_ptr();
        key->size = sizeof(uint32_t);
      }
    }
    else if (get_flags() & HAM_RECORD_NUMBER64) {
      send_key = false;
      if (!key->data) {
        arena->resize(sizeof(uint64_t));
        key->data = arena->get_ptr();
        key->size = sizeof(uint64_t);
      }
    }

    SerializedWrapper request;
    SerializedWrapper reply;

    if (cursor) {
      SerializedWrapper request;
      request.id = kCursorInsertRequest;
      request.cursor_insert_request.cursor_handle = cursor->get_remote_handle();
      request.cursor_insert_request.flags = flags;
      if (send_key) {
        request.cursor_insert_request.has_key = true;
        request.cursor_insert_request.key.has_data = true;
        request.cursor_insert_request.key.data.size = key->size;
        request.cursor_insert_request.key.data.value = (uint8_t *)key->data;
        request.cursor_insert_request.key.flags = key->flags;
        request.cursor_insert_request.key.intflags = key->_flags;
      }
      if (record) {
        request.cursor_insert_request.has_record = true;
        request.cursor_insert_request.record.has_data = true;
        request.cursor_insert_request.record.data.size = record->size;
        request.cursor_insert_request.record.data.value = (uint8_t *)record->data;
        request.cursor_insert_request.record.flags = record->flags;
        request.cursor_insert_request.record.partial_size = record->partial_size;
        request.cursor_insert_request.record.partial_offset = record->partial_offset;
      }

      env->perform_request(&request, &reply);

      ham_assert(reply.id == kCursorInsertReply);

      ham_status_t st = reply.cursor_insert_reply.status;
      if (st)
        return (st);

      if (reply.cursor_insert_reply.has_key) {
        ham_assert(key->size == reply.cursor_insert_reply.key.data.size);
        ham_assert(key->data != 0);
        ::memcpy(key->data, reply.cursor_insert_reply.key.data.value, key->size);
      }
    }
    else {
      request.id = kDbInsertRequest;
      request.db_insert_request.db_handle = m_remote_handle;
      request.db_insert_request.txn_handle = txn ? txn->get_remote_handle() : 0;
      request.db_insert_request.flags = flags;
      if (key && !(get_flags() & (HAM_RECORD_NUMBER32 | HAM_RECORD_NUMBER64))) {
        request.db_insert_request.has_key = true;
        request.db_insert_request.key.has_data = true;
        request.db_insert_request.key.data.size = key->size;
        request.db_insert_request.key.data.value = (uint8_t *)key->data;
        request.db_insert_request.key.flags = key->flags;
        request.db_insert_request.key.intflags = key->_flags;
      }
      if (record) {
        request.db_insert_request.has_record = true;
        request.db_insert_request.record.has_data = true;
        request.db_insert_request.record.data.size = record->size;
        request.db_insert_request.record.data.value = (uint8_t *)record->data;
        request.db_insert_request.record.flags = record->flags;
        request.db_insert_request.record.partial_size = record->partial_size;
        request.db_insert_request.record.partial_offset = record->partial_offset;
      }

      env->perform_request(&request, &reply);

      ham_assert(reply.id == kDbInsertReply);

      ham_status_t st = reply.db_insert_reply.status;
      if (st)
        return (st);

      if (reply.db_insert_reply.has_key) {
        ham_assert(key->data != 0);
        ham_assert(key->size == reply.db_insert_reply.key.data.size);
        ::memcpy(key->data, reply.db_insert_reply.key.data.value, key->size);
      }
    }
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::erase(Cursor *cursor, Transaction *htxn, ham_key_t *key,
            uint32_t flags)
{
  try {
    if (cursor) {
      SerializedWrapper request;
      request.id = kCursorEraseRequest;
      request.cursor_erase_request.cursor_handle = cursor->get_remote_handle();
      request.cursor_erase_request.flags = flags;

      SerializedWrapper reply;
      renv()->perform_request(&request, &reply);
      ham_assert(reply.id == kCursorEraseReply);
      return (reply.cursor_erase_reply.status);
    }

    RemoteEnvironment *env = renv();
    RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

    SerializedWrapper request;
    request.id = kDbEraseRequest;
    request.db_erase_request.db_handle = m_remote_handle;
    request.db_erase_request.txn_handle = txn ? txn->get_remote_handle() : 0;
    request.db_erase_request.flags = flags;
    request.db_erase_request.key.has_data = true;
    request.db_erase_request.key.data.size = key->size;
    request.db_erase_request.key.data.value = (uint8_t *)key->data;
    request.db_erase_request.key.flags = key->flags;
    request.db_erase_request.key.intflags = key->_flags;

    SerializedWrapper reply;
    env->perform_request(&request, &reply);

    ham_assert(reply.id == kDbEraseReply);

    return (reply.db_erase_reply.status);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::find(Cursor *cursor, Transaction *htxn, ham_key_t *key,
              ham_record_t *record, uint32_t flags)
{
  try {
    if (cursor && !htxn)
      htxn = cursor->get_txn();

    RemoteEnvironment *env = renv();
    RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

    SerializedWrapper request;
    request.id = kDbFindRequest;
    request.db_find_request.db_handle = m_remote_handle;
    request.db_find_request.cursor_handle = cursor ? cursor->get_remote_handle() : 0;
    request.db_find_request.txn_handle = txn ? txn->get_remote_handle() : 0;
    request.db_find_request.flags = flags;
    request.db_find_request.key.has_data = true;
    request.db_find_request.key.data.size = key->size;
    request.db_find_request.key.data.value = (uint8_t *)key->data;
    request.db_find_request.key.flags = key->flags;
    request.db_find_request.key.intflags = key->_flags;
    if (record) {
      request.db_find_request.has_record = true;
      request.db_find_request.record.has_data = true;
      request.db_find_request.record.data.size = record->size;
      request.db_find_request.record.data.value = (uint8_t *)record->data;
      request.db_find_request.record.flags = record->flags;
      request.db_find_request.record.partial_size = record->partial_size;
      request.db_find_request.record.partial_offset = record->partial_offset;
    }

    SerializedWrapper reply;
    env->perform_request(&request, &reply);
    ham_assert(reply.id == kDbFindReply);

    ByteArray *pkey_arena = &key_arena(txn);
    ByteArray *rec_arena = &record_arena(txn);

    ham_status_t st = reply.db_find_reply.status;
    if (st == 0) {
      /* approx. matching: need to copy the _flags and the key data! */
      if (reply.db_find_reply.has_key) {
        ham_assert(key);
        key->_flags = reply.db_find_reply.key.intflags;
        key->size = (uint16_t)reply.db_find_reply.key.data.size;
        if (!(key->flags & HAM_KEY_USER_ALLOC)) {
          pkey_arena->resize(key->size);
          key->data = pkey_arena->get_ptr();
        }
        ::memcpy(key->data, (void *)reply.db_find_reply.key.data.value,
                        key->size);
      }
      if (record && reply.db_find_reply.has_record) {
        record->size = reply.db_find_reply.record.data.size;
        if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
          rec_arena->resize(record->size);
          record->data = rec_arena->get_ptr();
        }
        ::memcpy(record->data, (void *)reply.db_find_reply.record.data.value,
                        record->size);
      }
    }
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

Cursor *
RemoteDatabase::cursor_create_impl(Transaction *htxn, uint32_t flags)
{
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  SerializedWrapper request;
  request.id = kCursorCreateRequest;
  request.cursor_create_request.db_handle = m_remote_handle;
  request.cursor_create_request.txn_handle = txn
                                                ? txn->get_remote_handle()
                                                : 0;
  request.cursor_create_request.flags = flags;

  SerializedWrapper reply;
  renv()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorCreateReply);
  ham_status_t st = reply.cursor_create_reply.status;
  if (st)
    return (0);

  Cursor *c = new Cursor((LocalDatabase *)this); // TODO this cast is evil!!
  c->set_remote_handle(reply.cursor_create_reply.cursor_handle);
  return (c);
}

Cursor *
RemoteDatabase::cursor_clone_impl(Cursor *src)
{
  SerializedWrapper request;
  request.id = kCursorCloneRequest;
  request.cursor_clone_request.cursor_handle = src->get_remote_handle();

  SerializedWrapper reply;
  renv()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorCloneReply);
  ham_status_t st = reply.cursor_clone_reply.status;
  if (st)
    return (0);

  Cursor *c = new Cursor(src->get_db());
  c->set_remote_handle(reply.cursor_clone_reply.cursor_handle);
  return (c);
}

ham_status_t
RemoteDatabase::cursor_get_record_count(Cursor *cursor, uint32_t flags,
                    uint32_t *pcount)
{
  try {
    RemoteEnvironment *env = renv();

    SerializedWrapper request;
    request.id = kCursorGetRecordCountRequest;
    request.cursor_get_record_count_request.cursor_handle =
                    cursor->get_remote_handle();
    request.cursor_get_record_count_request.flags = flags;

    SerializedWrapper reply;
    env->perform_request(&request, &reply);
    ham_assert(reply.id == kCursorGetRecordCountReply);

    ham_status_t st = reply.cursor_get_record_count_reply.status;
    if (st == 0)
      *pcount = reply.cursor_get_record_count_reply.count;
    else
      *pcount = 0;
    return (st);
  }
  catch (Exception &ex) {
    *pcount = 0;
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::cursor_get_duplicate_position(Cursor *cursor,
                uint32_t *pposition)
{
  try {
    RemoteEnvironment *env = renv();

    SerializedWrapper request;
    request.id = kCursorGetDuplicatePositionRequest;
    request.cursor_get_duplicate_position_request.cursor_handle =
                    cursor->get_remote_handle();

    SerializedWrapper reply;
    env->perform_request(&request, &reply);
    ham_assert(reply.id == kCursorGetDuplicatePositionReply);

    ham_status_t st = reply.cursor_get_duplicate_position_reply.status;
    if (st == 0)
      *pposition = reply.cursor_get_duplicate_position_reply.position;
    return (st);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::cursor_get_record_size(Cursor *cursor, uint64_t *psize)
{
  try {
    RemoteEnvironment *env = renv();

    SerializedWrapper request;
    request.id = kCursorGetRecordSizeRequest;
    request.cursor_get_record_size_request.cursor_handle =
                    cursor->get_remote_handle();

    SerializedWrapper reply;
    env->perform_request(&request, &reply);
    ham_assert(reply.id == kCursorGetRecordSizeReply);

    ham_status_t st = reply.cursor_get_record_size_reply.status;
    if (st == 0)
      *psize = reply.cursor_get_record_size_reply.size;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::cursor_overwrite(Cursor *cursor,
            ham_record_t *record, uint32_t flags)
{
  try {
    RemoteEnvironment *env = renv();

    SerializedWrapper request;
    request.id = kCursorOverwriteRequest;
    request.cursor_overwrite_request.cursor_handle = cursor->get_remote_handle();
    request.cursor_overwrite_request.flags = flags;

    if (record->size > 0) {
      request.cursor_overwrite_request.record.has_data = true;
      request.cursor_overwrite_request.record.data.size = record->size;
      request.cursor_overwrite_request.record.data.value = (uint8_t *)record->data;
    }
    request.cursor_overwrite_request.record.flags = record->flags;
    request.cursor_overwrite_request.record.partial_size = record->partial_size;
    request.cursor_overwrite_request.record.partial_offset = record->partial_offset;

    SerializedWrapper reply;
    env->perform_request(&request, &reply);
    ham_assert(reply.id == kCursorOverwriteReply);

    return (reply.cursor_overwrite_reply.status);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
RemoteDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  try {
    RemoteEnvironment *env = renv();

    RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(cursor->get_txn());
    ByteArray *pkey_arena = &key_arena(txn);
    ByteArray *prec_arena = &record_arena(txn);

    Protocol request(Protocol::CURSOR_MOVE_REQUEST);
    request.mutable_cursor_move_request()->set_cursor_handle(cursor->get_remote_handle());
    request.mutable_cursor_move_request()->set_flags(flags);
    if (key)
      Protocol::assign_key(request.mutable_cursor_move_request()->mutable_key(),
                    key, false);
    if (record)
      Protocol::assign_record(request.mutable_cursor_move_request()->mutable_record(),
                    record, false);

    ScopedPtr<Protocol> reply(env->perform_request(&request));

    ham_assert(reply->has_cursor_move_reply() != 0);

    ham_status_t st = reply->cursor_move_reply().status();
    if (st)
      return (st);

    /* modify key/record, but make sure that USER_ALLOC is respected! */
    if (reply->cursor_move_reply().has_key()) {
      ham_assert(key);
      key->_flags = reply->cursor_move_reply().key().intflags();
      key->size = (uint16_t)reply->cursor_move_reply().key().data().size();
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        pkey_arena->resize(key->size);
        key->data = pkey_arena->get_ptr();
      }
      memcpy(key->data, (void *)&reply->cursor_move_reply().key().data()[0],
              key->size);
    }

    /* same for the record */
    if (reply->cursor_move_reply().has_record()) {
      ham_assert(record);
      record->size = reply->cursor_move_reply().record().data().size();
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        prec_arena->resize(record->size);
        record->data = prec_arena->get_ptr();
      }
      memcpy(record->data, (void *)&reply->cursor_move_reply().record().data()[0],
              record->size);
    }
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

void
RemoteDatabase::cursor_close_impl(Cursor *cursor)
{
  SerializedWrapper request;
  request.id = kCursorCloseRequest;
  request.cursor_close_request.cursor_handle = cursor->get_remote_handle();

  SerializedWrapper reply;
  renv()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorCloseReply);
}

ham_status_t
RemoteDatabase::close_impl(uint32_t flags)
{
  RemoteEnvironment *env = renv();

  // do not set HAM_DONT_LOCK over the network
  flags &= ~HAM_DONT_LOCK;

  Protocol request(Protocol::DB_CLOSE_REQUEST);
  request.mutable_db_close_request()->set_db_handle(m_remote_handle);
  request.mutable_db_close_request()->set_flags(flags);

  ScopedPtr<Protocol> reply(env->perform_request(&request));

  ham_assert(reply->has_db_close_reply());

  ham_status_t st = reply->db_close_reply().status();
  if (st == 0)
    m_remote_handle = 0;

  return (st);
}


} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

