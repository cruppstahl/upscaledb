/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "2protobuf/protocol.h"
#include "4db/db_remote.h"
#include "env_remote.h"
#include "txn_remote.h"
#include "cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

ham_status_t
RemoteDatabase::get_parameters(ham_parameter_t *param)
{
  RemoteEnvironment *env = get_remote_env();

  Protocol request(Protocol::DB_GET_PARAMETERS_REQUEST);
  request.mutable_db_get_parameters_request()->set_db_handle(get_remote_handle());

  ham_parameter_t *p = param;
  if (p) {
    for (; p->name; p++)
      request.mutable_db_get_parameters_request()->add_names(p->name);
  }

  std::auto_ptr<Protocol> reply(env->perform_request(&request));

  ham_assert(reply->has_db_get_parameters_reply());

  ham_status_t st = reply->db_get_parameters_reply().status();
  if (st)
    return (st);

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

ham_status_t
RemoteDatabase::check_integrity(ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

  Protocol request(Protocol::DB_CHECK_INTEGRITY_REQUEST);
  request.mutable_db_check_integrity_request()->set_db_handle(get_remote_handle());
  request.mutable_db_check_integrity_request()->set_flags(flags);

  std::auto_ptr<Protocol> reply(env->perform_request(&request));

  ham_assert(reply->has_db_check_integrity_reply());

  return (reply->db_check_integrity_reply().status());
}


void
RemoteDatabase::count(Transaction *htxn, bool distinct,
              ham_u64_t *keycount)
{
  RemoteEnvironment *env = get_remote_env();
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  SerializedWrapper request;
  request.id = kDbGetKeyCountRequest;
  request.db_count_request.db_handle = get_remote_handle();
  request.db_count_request.txn_handle = txn
            ? txn->get_remote_handle()
            : 0;
  request.db_count_request.distinct = distinct;

  SerializedWrapper reply;
  env->perform_request(&request, &reply);

  ham_assert(reply.id == kDbGetKeyCountReply);

  ham_status_t st = reply.db_count_reply.status;
  if (st)
    throw Exception(st);

  *keycount = reply.db_count_reply.keycount;
}

ham_status_t
RemoteDatabase::insert(Transaction *htxn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();

  /* recno: do not send the key */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }
  }

  SerializedWrapper request;
  request.id = kDbInsertRequest;
  request.db_insert_request.db_handle = get_remote_handle();
  request.db_insert_request.txn_handle = txn ? txn->get_remote_handle() : 0;
  request.db_insert_request.flags = flags;
  if (key && !(get_rt_flags() & HAM_RECORD_NUMBER)) {
    request.db_insert_request.has_key = true;
    request.db_insert_request.key.has_data = true;
    request.db_insert_request.key.data.size = key->size;
    request.db_insert_request.key.data.value = (ham_u8_t *)key->data;
    request.db_insert_request.key.flags = key->flags;
    request.db_insert_request.key.intflags = key->_flags;
  }
  if (record) {
    request.db_insert_request.has_record = true;
    request.db_insert_request.record.has_data = true;
    request.db_insert_request.record.data.size = record->size;
    request.db_insert_request.record.data.value = (ham_u8_t *)record->data;
    request.db_insert_request.record.flags = record->flags;
    request.db_insert_request.record.partial_size = record->partial_size;
    request.db_insert_request.record.partial_offset = record->partial_offset;
  }

  SerializedWrapper reply;
  env->perform_request(&request, &reply);

  ham_assert(reply.id == kDbInsertReply);

  ham_status_t st = reply.db_insert_reply.status;

  /* recno: the key was modified! */
  if (st == 0 && reply.db_insert_reply.has_key) {
    if (reply.db_insert_reply.key.data.size == sizeof(ham_u64_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_u64_t));
      memcpy(key->data, reply.db_insert_reply.key.data.value,
                sizeof(ham_u64_t));
    }
  }

  return (st);
}

ham_status_t
RemoteDatabase::erase(Transaction *htxn, ham_key_t *key,
            ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  SerializedWrapper request;
  request.id = kDbEraseRequest;
  request.db_erase_request.db_handle = get_remote_handle();
  request.db_erase_request.txn_handle = txn ? txn->get_remote_handle() : 0;
  request.db_erase_request.flags = flags;
  request.db_erase_request.key.has_data = true;
  request.db_erase_request.key.data.size = key->size;
  request.db_erase_request.key.data.value = (ham_u8_t *)key->data;
  request.db_erase_request.key.flags = key->flags;
  request.db_erase_request.key.intflags = key->_flags;

  SerializedWrapper reply;
  env->perform_request(&request, &reply);

  ham_assert(reply.id == kDbEraseReply);

  return (reply.db_erase_reply.status);
}

ham_status_t
RemoteDatabase::find(Transaction *htxn, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  SerializedWrapper request;
  request.id = kDbFindRequest;
  request.db_find_request.db_handle = get_remote_handle();
  request.db_find_request.txn_handle = txn ? txn->get_remote_handle() : 0;
  request.db_find_request.flags = flags;
  request.db_find_request.key.has_data = true;
  request.db_find_request.key.data.size = key->size;
  request.db_find_request.key.data.value = (ham_u8_t *)key->data;
  request.db_find_request.key.flags = key->flags;
  request.db_find_request.key.intflags = key->_flags;
  if (record) {
    request.db_find_request.has_record = true;
    request.db_find_request.record.has_data = true;
    request.db_find_request.record.data.size = record->size;
    request.db_find_request.record.data.value = (ham_u8_t *)record->data;
    request.db_find_request.record.flags = record->flags;
    request.db_find_request.record.partial_size = record->partial_size;
    request.db_find_request.record.partial_offset = record->partial_offset;
  }

  SerializedWrapper reply;
  env->perform_request(&request, &reply);
  ham_assert(reply.id == kDbFindReply);

  ByteArray *key_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
                : &txn->get_record_arena();

  ham_status_t st = reply.db_find_reply.status;
  if (st == 0) {
    /* approx. matching: need to copy the _flags and the key data! */
    if (reply.db_find_reply.has_key) {
      ham_assert(key);
      key->_flags = reply.db_find_reply.key.intflags;
      key->size = (ham_u16_t)reply.db_find_reply.key.data.size;
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key_arena->resize(key->size);
        key->data = key_arena->get_ptr();
      }
      memcpy(key->data, (void *)reply.db_find_reply.key.data.value, key->size);
    }
    if (reply.db_find_reply.has_record) {
      record->size = reply.db_find_reply.record.data.size;
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        rec_arena->resize(record->size);
        record->data = rec_arena->get_ptr();
      }
      memcpy(record->data, (void *)reply.db_find_reply.record.data.value,
                record->size);
    }
  }

  return (st);
}

Cursor *
RemoteDatabase::cursor_create_impl(Transaction *htxn, ham_u32_t flags)
{
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(htxn);

  SerializedWrapper request;
  request.id = kCursorCreateRequest;
  request.cursor_create_request.db_handle = get_remote_handle();
  request.cursor_create_request.txn_handle = txn
                                                ? txn->get_remote_handle()
                                                : 0;
  request.cursor_create_request.flags = flags;

  SerializedWrapper reply;
  get_remote_env()->perform_request(&request, &reply);
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
  get_remote_env()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorCloneReply);
  ham_status_t st = reply.cursor_clone_reply.status;
  if (st)
    return (0);

  Cursor *c = new Cursor(src->get_db());
  c->set_remote_handle(reply.cursor_clone_reply.cursor_handle);
  return (c);
}

ham_status_t
RemoteDatabase::cursor_insert(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();
  bool send_key = true;
  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(cursor->get_txn());

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();

  /* recno: do not send the key */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    send_key = false;

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }
  }

  SerializedWrapper request;
  request.id = kCursorInsertRequest;
  request.cursor_insert_request.cursor_handle = cursor->get_remote_handle();
  request.cursor_insert_request.flags = flags;
  if (send_key) {
    request.cursor_insert_request.has_key = true;
    request.cursor_insert_request.key.has_data = true;
    request.cursor_insert_request.key.data.size = key->size;
    request.cursor_insert_request.key.data.value = (ham_u8_t *)key->data;
    request.cursor_insert_request.key.flags = key->flags;
    request.cursor_insert_request.key.intflags = key->_flags;
  }
  if (record) {
    request.cursor_insert_request.has_record = true;
    request.cursor_insert_request.record.has_data = true;
    request.cursor_insert_request.record.data.size = record->size;
    request.cursor_insert_request.record.data.value = (ham_u8_t *)record->data;
    request.cursor_insert_request.record.flags = record->flags;
    request.cursor_insert_request.record.partial_size = record->partial_size;
    request.cursor_insert_request.record.partial_offset = record->partial_offset;
  }

  SerializedWrapper reply;
  env->perform_request(&request, &reply);

  ham_assert(reply.id == kCursorInsertReply);

  ham_status_t st = reply.cursor_insert_reply.status;

  /* recno: the key was modified! */
  if (st == 0 && reply.cursor_insert_reply.has_key) {
    if (reply.cursor_insert_reply.key.data.size == sizeof(ham_u64_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_u64_t));
      memcpy(key->data, reply.cursor_insert_reply.key.data.value,
                      sizeof(ham_u64_t));
    }
  }

  return (st);
}

ham_status_t
RemoteDatabase::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
  SerializedWrapper request;
  request.id = kCursorEraseRequest;
  request.cursor_erase_request.cursor_handle = cursor->get_remote_handle();
  request.cursor_erase_request.flags = flags;

  SerializedWrapper reply;
  get_remote_env()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorEraseReply);
  return (reply.cursor_erase_reply.status);
}

ham_status_t
RemoteDatabase::cursor_find(Cursor *cursor, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

  SerializedWrapper request;
  request.id = kCursorFindRequest;
  request.cursor_find_request.cursor_handle = cursor->get_remote_handle();
  request.cursor_find_request.flags = flags;
  if (key->size > 0) {
    request.cursor_find_request.key.has_data = true;
    request.cursor_find_request.key.data.size = key->size;
    request.cursor_find_request.key.data.value = (ham_u8_t *)key->data;
  }
  request.cursor_find_request.key.flags = key->flags;
  request.cursor_find_request.key.intflags = key->_flags;
  if (record) {
    request.cursor_find_request.has_record = true;
    if (record->size > 0) {
      request.cursor_find_request.record.has_data = true;
      request.cursor_find_request.record.data.size = record->size;
      request.cursor_find_request.record.data.value = (ham_u8_t *)record->data;
    }
    request.cursor_find_request.record.flags = record->flags;
    request.cursor_find_request.record.partial_size = record->partial_size;
    request.cursor_find_request.record.partial_offset = record->partial_offset;
  }

  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(cursor->get_txn());

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
                : &txn->get_record_arena();


  SerializedWrapper reply;
  env->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorFindReply);

  ham_status_t st = reply.cursor_find_reply.status;
  if (st == 0) {
    /* approx. matching: need to copy the _flags! */
    if (reply.cursor_find_reply.has_key)
      key->_flags = reply.cursor_find_reply.key.intflags;
    if (reply.cursor_find_reply.has_record) {
      ham_assert(record);
      record->size = reply.cursor_find_reply.record.data.size;
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        arena->resize(record->size);
        record->data = arena->get_ptr();
      }
      memcpy(record->data, reply.cursor_find_reply.record.data.value,
              record->size);
    }
  }

  return (st);
}

ham_status_t
RemoteDatabase::cursor_get_record_count(Cursor *cursor,
                ham_u32_t *count, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

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
    *count = reply.cursor_get_record_count_reply.count;

  return (st);
}

ham_u32_t
RemoteDatabase::cursor_get_duplicate_position(Cursor *cursor)
{
  RemoteEnvironment *env = get_remote_env();

  SerializedWrapper request;
  request.id = kCursorGetDuplicatePositionRequest;
  request.cursor_get_duplicate_position_request.cursor_handle =
                  cursor->get_remote_handle();

  SerializedWrapper reply;
  env->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorGetDuplicatePositionReply);

  ham_status_t st = reply.cursor_get_duplicate_position_reply.status;
  if (st == 0)
    return (reply.cursor_get_duplicate_position_reply.position);

  throw Exception(st);
}

ham_status_t
RemoteDatabase::cursor_get_record_size(Cursor *cursor, ham_u64_t *size)
{
  (void)cursor;
  (void)size;
  /* need this? send me a mail and i will implement it */
  return (HAM_NOT_IMPLEMENTED);
}

ham_status_t
RemoteDatabase::cursor_overwrite(Cursor *cursor,
            ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

  SerializedWrapper request;
  request.id = kCursorOverwriteRequest;
  request.cursor_overwrite_request.cursor_handle = cursor->get_remote_handle();
  request.cursor_overwrite_request.flags = flags;

  if (record->size > 0) {
    request.cursor_overwrite_request.record.has_data = true;
    request.cursor_overwrite_request.record.data.size = record->size;
    request.cursor_overwrite_request.record.data.value = (ham_u8_t *)record->data;
  }
  request.cursor_overwrite_request.record.flags = record->flags;
  request.cursor_overwrite_request.record.partial_size = record->partial_size;
  request.cursor_overwrite_request.record.partial_offset = record->partial_offset;

  SerializedWrapper reply;
  env->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorOverwriteReply);

  return (reply.cursor_overwrite_reply.status);
}

ham_status_t
RemoteDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

  RemoteTransaction *txn = dynamic_cast<RemoteTransaction *>(cursor->get_txn());
  ByteArray *key_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
                : &txn->get_record_arena();

  Protocol request(Protocol::CURSOR_MOVE_REQUEST);
  request.mutable_cursor_move_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_move_request()->set_flags(flags);
  if (key)
    Protocol::assign_key(request.mutable_cursor_move_request()->mutable_key(),
                  key, false);
  if (record)
    Protocol::assign_record(request.mutable_cursor_move_request()->mutable_record(),
                  record, false);

  std::auto_ptr<Protocol> reply(env->perform_request(&request));

  ham_assert(reply->has_cursor_move_reply() != 0);

  ham_status_t st = reply->cursor_move_reply().status();
  if (st)
    goto bail;

  /* modify key/record, but make sure that USER_ALLOC is respected! */
  if (reply->cursor_move_reply().has_key()) {
    ham_assert(key);
    key->_flags = reply->cursor_move_reply().key().intflags();
    key->size = (ham_u16_t)reply->cursor_move_reply().key().data().size();
    if (!(key->flags & HAM_KEY_USER_ALLOC)) {
      key_arena->resize(key->size);
      key->data = key_arena->get_ptr();
    }
    memcpy(key->data, (void *)&reply->cursor_move_reply().key().data()[0],
            key->size);
  }

  /* same for the record */
  if (reply->cursor_move_reply().has_record()) {
    ham_assert(record);
    record->size = reply->cursor_move_reply().record().data().size();
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
      rec_arena->resize(record->size);
      record->data = rec_arena->get_ptr();
    }
    memcpy(record->data, (void *)&reply->cursor_move_reply().record().data()[0],
            record->size);
  }

bail:
  return (st);
}

void
RemoteDatabase::cursor_close_impl(Cursor *cursor)
{
  SerializedWrapper request;
  request.id = kCursorCloseRequest;
  request.cursor_close_request.cursor_handle = cursor->get_remote_handle();

  SerializedWrapper reply;
  get_remote_env()->perform_request(&request, &reply);
  ham_assert(reply.id == kCursorCloseReply);
}

ham_status_t
RemoteDatabase::close_impl(ham_u32_t flags)
{
  RemoteEnvironment *env = get_remote_env();

  Protocol request(Protocol::DB_CLOSE_REQUEST);
  request.mutable_db_close_request()->set_db_handle(get_remote_handle());
  request.mutable_db_close_request()->set_flags(flags);

  std::auto_ptr<Protocol> reply(env->perform_request(&request));

  ham_assert(reply->has_db_close_reply());

  ham_status_t st = reply->db_close_reply().status();
  if (st == 0)
    set_remote_handle(0);

  return (st);
}


} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

