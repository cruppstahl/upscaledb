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
#include "1os/os.h"
#include "1base/scoped_ptr.h"
#include "2protobuf/protocol.h"
#include "4cursor/cursor.h"
#include "4cursor/cursor_remote.h"
#include "4db/db_remote.h"
#include "4env/env_remote.h"
#include "4txn/txn_remote.h"
#include "4uqi/result.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

RemoteEnv::RemoteEnv(EnvConfig &config)
  : Env(config), remote_handle(0), _buffer(1024 * 4)
{
}

Protocol *
RemoteEnv::perform_request(Protocol *request)
{
  // use ByteArray to avoid frequent reallocs!
  _buffer.clear();

  if (unlikely(!request->pack(&_buffer))) {
    ups_log(("protoype Protocol::pack failed"));
    throw Exception(UPS_INTERNAL_ERROR);
  }

  _socket.send(_buffer.data(), _buffer.size());

  // now block and wait for the reply; first read the header, then the
  // remaining data
  _socket.recv(_buffer.data(), 8);

  uint32_t magic = *(uint32_t *)_buffer.data();
  if (magic != UPS_TRANSFER_MAGIC_V1
        && magic != UPS_TRANSFER_MAGIC_V2) {
    ups_log(("Invalid protocol magic"));
    throw Exception(UPS_IO_ERROR);
  }

  uint32_t size = *(uint32_t *)(_buffer.data() + 4);
  _buffer.resize(size + 8);
  _socket.recv(_buffer.data() + 8, size);

  return Protocol::unpack(_buffer.data(), size + 8);
}

void
RemoteEnv::perform_request(SerializedWrapper *request, SerializedWrapper *reply)
{
  int size_left = (int)request->get_size();
  request->size = size_left;
  request->magic = UPS_TRANSFER_MAGIC_V2;
  _buffer.resize(request->size);

  uint8_t *ptr = _buffer.data();
  request->serialize(&ptr, &size_left);
  assert(size_left == 0);

  _socket.send(_buffer.data(), request->size);

  // now block and wait for the reply; first read the header, then the
  // remaining data
  _socket.recv(_buffer.data(), 8);

  // now check the magic and receive the remaining data
  uint32_t magic = *(uint32_t *)(_buffer.data() + 0);
  if (unlikely(magic != UPS_TRANSFER_MAGIC_V2))
    throw Exception(UPS_INTERNAL_ERROR);
  // TODO check the magic
  int size = *(int *)(_buffer.data() + 4);
  _buffer.resize(size);
  _socket.recv(_buffer.data() + 8, size - 8);

  ptr = _buffer.data();
  reply->deserialize(&ptr, &size);
  assert(size == 0);
}

static void
add_result_keys(Result *r, const upscaledb::SelectRangeReply *reply)
{
  uint8_t *p = (uint8_t *)&reply->key_data()[0];
  uint32_t *offsets = (uint32_t *)reply->key_offsets().begin();
  uint32_t size = reply->key_offsets().size();
  for (uint32_t i = 0; i < size; i++) {
    if (likely(i < size - 1))
      r->add_key(p + offsets[i], offsets[i + 1] - offsets[i]);
    else
      r->add_key(p + offsets[i], reply->key_data().size() - offsets[i]);
  }
}

static void
add_result_records(Result *r, const upscaledb::SelectRangeReply *reply)
{
  r->record_type = reply->record_type();
  uint8_t *p = (uint8_t *)&reply->record_data()[0];
  uint32_t *offsets = (uint32_t *)reply->record_offsets().begin();
  uint32_t size = reply->record_offsets().size();
  for (uint32_t i = 0; i < size; i++) {
    if (likely(i < size - 1))
      r->add_record(p + offsets[i], offsets[i + 1] - offsets[i]);
    else
      r->add_record(p + offsets[i], reply->record_data().size() - offsets[i]);
  }
}

ups_status_t
RemoteEnv::select_range(const char *query, Cursor *begin, const Cursor *end,
                Result **presult)
{
  Protocol request(Protocol::SELECT_RANGE_REQUEST);
  request.mutable_select_range_request();
  request.mutable_select_range_request()->set_env_handle(remote_handle);
  request.mutable_select_range_request()->set_query(query);
  if (begin) {
    RemoteCursor *c = (RemoteCursor *)begin;
    request.mutable_select_range_request()->set_begin_cursor_handle(
                    c->remote_handle);
  }
  if (end) {
    RemoteCursor *c = (RemoteCursor *)end;
    request.mutable_select_range_request()->set_end_cursor_handle(
                    c->remote_handle);
  }

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->has_select_range_reply());

  ups_status_t st = reply->select_range_reply().status();
  if (unlikely(st))
    return st;

  /* copy the result */
  Result *r = new Result;
  r->row_count = reply->select_range_reply().row_count();
  r->key_type = reply->select_range_reply().key_type();
  add_result_keys(r, &reply->select_range_reply());

  r->record_type = reply->select_range_reply().record_type();
  add_result_records(r, &reply->select_range_reply());

  *presult = r;
  return 0;
}

ups_status_t
RemoteEnv::create()
{
  // the 'create' operation is identical to 'open'
  return open();
}

ups_status_t
RemoteEnv::open()
{
  _socket.close();

  const char *url = config.filename.c_str();
  assert(url != 0);
  assert(::strstr(url, "ups://") == url);
  const char *ip = url + 6;
  const char *port_str = strstr(ip, ":");
  if (unlikely(!port_str)) {
    ups_trace(("remote uri does not include port - expected "
                "`ups://<ip>:<port>`"));
    return UPS_INV_PARAMETER;
  }
  uint16_t port = (uint16_t)::atoi(port_str + 1);
  if (unlikely(!port)) {
    ups_trace(("remote uri includes invalid port - expected "
                "`ups://<ip>:<port>`"));
    return UPS_INV_PARAMETER;
  }

  const char *filename = ::strstr(port_str, "/");

  std::string hostname(ip, port_str);
  _socket.connect(hostname.c_str(), port, config.remote_timeout_sec);

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->type() == Protocol::CONNECT_REPLY);

  ups_status_t st = reply->connect_reply().status();
  if (likely(st == 0)) {
    config.flags |= reply->connect_reply().env_flags();
    remote_handle = reply->connect_reply().env_handle();

    if (ISSET(flags(), UPS_ENABLE_TRANSACTIONS))
      txn_manager.reset(new RemoteTxnManager(this));
  }

  return st;
}

std::vector<uint16_t>
RemoteEnv::get_database_names()
{
  Protocol request(Protocol::ENV_GET_DATABASE_NAMES_REQUEST);
  request.mutable_env_get_database_names_request();
  request.mutable_env_get_database_names_request()->set_env_handle(remote_handle);

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->has_env_get_database_names_reply());

  ups_status_t st = reply->env_get_database_names_reply().status();
  if (unlikely(st))
    throw Exception(st);

  /* copy the retrieved names */
  std::vector<uint16_t> vec;
  for (int i = 0;
        i < reply->env_get_database_names_reply().names_size();
        i++) {
    vec.push_back((uint16_t)reply->env_get_database_names_reply().names(i));
  }

  return vec;
}

ups_status_t
RemoteEnv::get_parameters(ups_parameter_t *param)
{
  static char filename[1024]; // TODO not threadsafe!!
  ups_parameter_t *p = param;

  Protocol request(Protocol::ENV_GET_PARAMETERS_REQUEST);
  request.mutable_env_get_parameters_request()->set_env_handle(remote_handle);
  while (p && p->name != 0) {
    request.mutable_env_get_parameters_request()->add_names(p->name);
    p++;
  }

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->has_env_get_parameters_reply());

  ups_status_t st = reply->env_get_parameters_reply().status();
  if (unlikely(st))
    return st;

  p = param;
  while (p && p->name) {
    switch (p->name) {
    case UPS_PARAM_CACHESIZE:
      assert(reply->env_get_parameters_reply().has_cache_size());
      p->value = reply->env_get_parameters_reply().cache_size();
      break;
    case UPS_PARAM_PAGESIZE:
      assert(reply->env_get_parameters_reply().has_page_size());
      p->value = reply->env_get_parameters_reply().page_size();
      break;
    case UPS_PARAM_MAX_DATABASES:
      assert(reply->env_get_parameters_reply().has_max_env_databases());
      p->value = reply->env_get_parameters_reply().max_env_databases();
      break;
    case UPS_PARAM_FLAGS:
      assert(reply->env_get_parameters_reply().has_flags());
      p->value = reply->env_get_parameters_reply().flags();
      break;
    case UPS_PARAM_FILEMODE:
      assert(reply->env_get_parameters_reply().has_filemode());
      p->value = reply->env_get_parameters_reply().filemode();
      break;
    case UPS_PARAM_JOURNAL_COMPRESSION:
      assert(reply->env_get_parameters_reply().has_journal_compression());
      p->value = reply->env_get_parameters_reply().journal_compression();
      break;
    case UPS_PARAM_FILENAME:
      if (reply->env_get_parameters_reply().has_filename()) {
        strncpy(filename, reply->env_get_parameters_reply().filename().c_str(),
              sizeof(filename) - 1);
        filename[sizeof(filename) - 1] = 0;
        p->value = (uint64_t)(&filename[0]);
      }
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
RemoteEnv::flush(uint32_t flags)
{
  Protocol request(Protocol::ENV_FLUSH_REQUEST);
  request.mutable_env_flush_request()->set_flags(flags);
  request.mutable_env_flush_request()->set_env_handle(remote_handle);

  ScopedPtr<Protocol> reply(perform_request(&request));
  assert(reply->has_env_flush_reply());
  return reply->env_flush_reply().status();
}

Db *
RemoteEnv::do_create_db(DbConfig &config, const ups_parameter_t *param)
{
  Protocol request(Protocol::ENV_CREATE_DB_REQUEST);
  request.mutable_env_create_db_request()->set_env_handle(remote_handle);
  request.mutable_env_create_db_request()->set_dbname(config.db_name);
  request.mutable_env_create_db_request()->set_flags(config.flags);

  const ups_parameter_t *p = param;
  if (p) {
    for (; p->name; p++) {
      if (p->name == UPS_PARAM_CUSTOM_COMPARE_NAME) {
        const char *zname = reinterpret_cast<const char *>(p->value);
        request.mutable_env_create_db_request()->set_compare_name(zname);
      }
      else {
        request.mutable_env_create_db_request()->add_param_names(p->name);
        request.mutable_env_create_db_request()->add_param_values(p->value);
      }
    }
  }

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->has_env_create_db_reply());

  ups_status_t st = reply->env_create_db_reply().status();
  if (unlikely(st))
    throw Exception(st);

  config.flags = reply->env_create_db_reply().db_flags();
  return new RemoteDb(this, config, reply->env_create_db_reply().db_handle());
}

Db *
RemoteEnv::do_open_db(DbConfig &config, const ups_parameter_t *param)
{
  Protocol request(Protocol::ENV_OPEN_DB_REQUEST);
  request.mutable_env_open_db_request()->set_env_handle(remote_handle);
  request.mutable_env_open_db_request()->set_dbname(config.db_name);
  request.mutable_env_open_db_request()->set_flags(config.flags);

  const ups_parameter_t *p = param;
  if (p) {
    for (; p->name; p++) {
      request.mutable_env_open_db_request()->add_param_names(p->name);
      request.mutable_env_open_db_request()->add_param_values(p->value);
    }
  }

  ScopedPtr<Protocol> reply(perform_request(&request));

  assert(reply->has_env_open_db_reply());

  ups_status_t st = reply->env_open_db_reply().status();
  if (unlikely(st))
    throw Exception(st);

  config.flags = reply->env_open_db_reply().db_flags();
  return new RemoteDb(this, config, reply->env_open_db_reply().db_handle());
}

ups_status_t
RemoteEnv::rename_db( uint16_t oldname, uint16_t newname, uint32_t flags)
{
  Protocol request(Protocol::ENV_RENAME_REQUEST);
  request.mutable_env_rename_request()->set_env_handle(remote_handle);
  request.mutable_env_rename_request()->set_oldname(oldname);
  request.mutable_env_rename_request()->set_newname(newname);
  request.mutable_env_rename_request()->set_flags(flags);

  ScopedPtr<Protocol> reply(perform_request(&request));
  assert(reply->has_env_rename_reply());
  return reply->env_rename_reply().status();
}

ups_status_t
RemoteEnv::erase_db(uint16_t name, uint32_t flags)
{
  Protocol request(Protocol::ENV_ERASE_DB_REQUEST);
  request.mutable_env_erase_db_request()->set_env_handle(remote_handle);
  request.mutable_env_erase_db_request()->set_name(name);
  request.mutable_env_erase_db_request()->set_flags(flags);

  ScopedPtr<Protocol> reply(perform_request(&request));
  assert(reply->has_env_erase_db_reply());
  return reply->env_erase_db_reply().status();
}

Txn *
RemoteEnv::txn_begin(const char *name, uint32_t flags)
{
  SerializedWrapper request;
  request.id = kTxnBeginRequest;
  request.txn_begin_request.env_handle = remote_handle;
  request.txn_begin_request.flags = flags;
  if (name) {
    request.txn_begin_request.name.value = (uint8_t *)name;
    request.txn_begin_request.name.size = strlen(name) + 1;
  }

  SerializedWrapper reply;
  perform_request(&request, &reply);
  assert(reply.id == kTxnBeginReply);

  ups_status_t st = reply.txn_begin_reply.status;
  if (unlikely(st))
    throw Exception(st);

  Txn *txn = new RemoteTxn(this, name, flags, reply.txn_begin_reply.txn_handle);
  txn_manager->begin(txn);
  return txn;
}

ups_status_t
RemoteEnv::txn_commit(Txn *txn, uint32_t flags)
{
  RemoteTxn *rtxn = dynamic_cast<RemoteTxn *>(txn);

  SerializedWrapper request;
  request.id = kTxnCommitRequest;
  request.txn_commit_request.txn_handle = rtxn->remote_handle;
  request.txn_commit_request.flags = flags;

  SerializedWrapper reply;
  perform_request(&request, &reply);
  assert(reply.id == kTxnCommitReply);

  ups_status_t st = reply.txn_commit_reply.status;
  if (unlikely(st))
    return st;

  return txn_manager->commit(txn);
}

ups_status_t
RemoteEnv::txn_abort(Txn *txn, uint32_t flags)
{
  RemoteTxn *rtxn = dynamic_cast<RemoteTxn *>(txn);

  SerializedWrapper request;
  request.id = kTxnAbortRequest;
  request.txn_abort_request.txn_handle = rtxn->remote_handle;
  request.txn_abort_request.flags = flags;

  SerializedWrapper reply;
  perform_request(&request, &reply);
  assert(reply.id == kTxnAbortReply);
  ups_status_t st = reply.txn_abort_reply.status;
  if (unlikely(st))
    return st;

  return txn_manager->abort(txn);
}

ups_status_t
RemoteEnv::do_close(uint32_t flags)
{
  Protocol request(Protocol::DISCONNECT_REQUEST);
  request.mutable_disconnect_request()->set_env_handle(remote_handle);

  ScopedPtr<Protocol> reply(perform_request(&request));

  // ignore the reply
  _socket.close();
  remote_handle = 0;
  return 0;
}

void
RemoteEnv::fill_metrics(ups_env_metrics_t *metrics)
{
  throw Exception(UPS_NOT_IMPLEMENTED);
}

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

