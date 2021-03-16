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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1mem/mem.h"
#include "2protobuf/protocol.h"
#include "2protoserde/messages.h"
#include "4env/env.h"
#include "4db/db_local.h"
#include "4uqi/result.h"
#include "5server/upsserver.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

static inline void
send_wrapper(Session *session, Protocol *reply)
{
  uint8_t *data;
  uint32_t data_size;

  if (unlikely(!reply->pack(&data, &data_size)))
    return;

  session->send(data, data_size);
  Memory::release(data);
}

static inline void
send_wrapper(Session *session, SerializedWrapper *reply)
{
  int size_left = (int)reply->get_size();
  reply->magic = UPS_TRANSFER_MAGIC_V2;
  reply->size = size_left;

  if (size_left < 2048) {
    uint8_t buffer[2048];
    uint8_t *ptr = &buffer[0];
    reply->serialize(&ptr, &size_left);
    session->send(&buffer[0], reply->size);
  }
  else {
    ByteArray buffer(size_left);
    uint8_t *ptr = buffer.data();
    reply->serialize(&ptr, &size_left);
    session->send(buffer.data(), reply->size);
  }
}

static void
handle_connect(Session *session, Protocol *request)
{
  assert(request != 0);
  Env *env;

  {
    ScopedLock lock(session->server->open_env_mutex);
    env = session->server->open_envs[request->connect_request().path()];
  }

  if (unlikely(ErrorInducer::is_active())) {
    if (ErrorInducer::induce(ErrorInducer::kServerConnect)) {
#ifdef WIN32
      Sleep(5);
#else
      sleep(5);
#endif
      ErrorInducer::activate(false);
    }
  }

  Protocol reply(Protocol::CONNECT_REPLY);
  if (unlikely(!env)) {
    reply.mutable_connect_reply()->set_status(UPS_FILE_NOT_FOUND);
  }
  else {
    reply.mutable_connect_reply()->set_status(0);
    reply.mutable_connect_reply()->set_env_flags(((Env *)env)->flags());
    reply.mutable_connect_reply()->set_env_handle(session->server->environments.allocate(env));
  }

  send_wrapper(session, &reply);
}

static void
handle_disconnect(Session *session, Protocol *request)
{
  assert(request != 0);
  session->server->environments.remove(request->disconnect_request().env_handle());

  Protocol reply(Protocol::DISCONNECT_REPLY);
  reply.mutable_disconnect_reply()->set_status(0);

  send_wrapper(session, &reply);
}

static void
handle_env_get_parameters(Session *session, Protocol *request)
{
  uint32_t i;
  ups_status_t st = 0;
  Env *env = 0;
  ups_parameter_t params[100]; /* 100 should be enough... */

  assert(request != 0);
  assert(request->has_env_get_parameters_request());

  /* initialize the ups_parameters_t array */
  ::memset(&params[0], 0, sizeof(params));
  for (i = 0;
      i < (uint32_t)request->env_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_env_get_parameters_request()->mutable_names()->mutable_data()[i];

  Protocol reply(Protocol::ENV_GET_PARAMETERS_REPLY);

  env = session->server->environments.get(request->env_get_parameters_request().env_handle());

  /* and request the parameters from the Environment */
  st = ups_env_get_parameters((ups_env_t *)env, &params[0]);
  reply.mutable_env_get_parameters_reply()->set_status(st);
  if (unlikely(st)) {
    send_wrapper(session, &reply);
    return;
  }

  /* initialize the reply package */
  for (i = 0;
      i < (uint32_t)request->env_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case UPS_PARAM_JOURNAL_COMPRESSION:
      reply.mutable_env_get_parameters_reply()->set_journal_compression(
              (int)params[i].value);
      break;
    case UPS_PARAM_CACHESIZE:
      reply.mutable_env_get_parameters_reply()->set_cache_size(
              (int)params[i].value);
      break;
    case UPS_PARAM_PAGESIZE:
      reply.mutable_env_get_parameters_reply()->set_page_size(
              (int)params[i].value);
      break;
    case UPS_PARAM_MAX_DATABASES:
      reply.mutable_env_get_parameters_reply()->set_max_env_databases(
              (int)params[i].value);
      break;
    case UPS_PARAM_FLAGS:
      reply.mutable_env_get_parameters_reply()->set_flags(
              (int)params[i].value);
      break;
    case UPS_PARAM_FILEMODE:
      reply.mutable_env_get_parameters_reply()->set_filemode(
              (int)params[i].value);
      break;
    case UPS_PARAM_FILENAME:
      if (params[i].value)
        reply.mutable_env_get_parameters_reply()->set_filename(
              (const char *)(params[i].value));
      break;
    default:
      ups_trace(("unsupported parameter %u", (unsigned)params[i].name));
      break;
    }
  }

  send_wrapper(session, &reply);
}

static void
handle_env_get_database_names(Session *session, Protocol *request)
{
  uint32_t num_names = 1024;
  uint16_t names[1024]; /* should be enough */
  Env *env = 0;

  assert(request != 0);
  assert(request->has_env_get_database_names_request());

  env = session->server->environments.get(request->env_get_database_names_request().env_handle());

  /* request the database names from the Environment */
  ups_status_t st = ups_env_get_database_names((ups_env_t *)env,
          &names[0], &num_names);

  Protocol reply(Protocol::ENV_GET_DATABASE_NAMES_REPLY);
  reply.mutable_env_get_database_names_reply()->set_status(st);
  for (uint32_t i = 0; i < num_names; i++)
    reply.mutable_env_get_database_names_reply()->add_names(names[i]);

  send_wrapper(session, &reply);
}

static void
handle_env_flush(Session *session, Protocol *request)
{
  Env *env = 0;

  assert(request != 0);
  assert(request->has_env_flush_request());

  env = session->server->environments.get(request->env_flush_request().env_handle());

  /* request the database names from the Environment */
  Protocol reply(Protocol::ENV_FLUSH_REPLY);
  reply.mutable_env_flush_reply()->set_status(ups_env_flush((ups_env_t *)env,
            request->env_flush_request().flags()));

  send_wrapper(session, &reply);
}

static void
handle_env_rename(Session *session, Protocol *request)
{
  Env *env = 0;

  assert(request != 0);
  assert(request->has_env_rename_request());

  env = session->server->environments.get(request->env_rename_request().env_handle());

  /* rename the databases */
  ups_status_t st = ups_env_rename_db((ups_env_t *)env,
          request->env_rename_request().oldname(),
          request->env_rename_request().newname(),
          request->env_rename_request().flags());

  Protocol reply(Protocol::ENV_RENAME_REPLY);
  reply.mutable_env_rename_reply()->set_status(st);
  send_wrapper(session, &reply);
}

static void
handle_env_create_db(Session *session, Protocol *request)
{
  ups_db_t *db;
  Env *env;
  ups_status_t st = 0;
  uint64_t db_handle = 0;
  std::vector<ups_parameter_t> params;

  assert(request != 0);
  assert(request->has_env_create_db_request());

  env = session->server->environments.get(request->env_create_db_request().env_handle());

  /* convert parameters */
  assert(request->env_create_db_request().param_names().size() < 100);
  for (uint32_t i = 0;
      i < (uint32_t)request->env_create_db_request().param_names().size();
      i++) {
    ups_parameter_t p;
    p.name  = request->mutable_env_create_db_request()->mutable_param_names()->data()[i];
    p.value = request->mutable_env_create_db_request()->mutable_param_values()->data()[i];
    params.push_back(p);
  }

  if (request->env_create_db_request().has_compare_name()) {
    const char *zname = request->env_create_db_request().compare_name().c_str();
    ups_parameter_t p = {
        UPS_PARAM_CUSTOM_COMPARE_NAME,
        reinterpret_cast<uint64_t>(zname)
    };
    params.push_back(p);
  }

  ups_parameter_t last = {0, 0};
  params.push_back(last);

  /* create the database */
  st = ups_env_create_db((ups_env_t *)env, &db,
            request->env_create_db_request().dbname(),
            request->env_create_db_request().flags(), &params[0]);

  if (likely(st == 0)) {
    /* allocate a new database handle in the Env wrapper structure */
    db_handle = session->server->databases.allocate((Db *)db);
  }

  Protocol reply(Protocol::ENV_CREATE_DB_REPLY);
  reply.mutable_env_create_db_reply()->set_status(st);
  if (likely(db_handle != 0)) {
    reply.mutable_env_create_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_create_db_reply()->set_db_flags(((Db *)db)->config.flags);
  }

  send_wrapper(session, &reply);
}

static void
handle_env_open_db(Session *session, Protocol *request)
{
  ups_db_t *db = 0;
  uint64_t db_handle = 0;
  ups_status_t st = 0;
  uint16_t dbname = request->env_open_db_request().dbname();
  ups_parameter_t params[100] = {{0, 0}};

  assert(request != 0);
  assert(request->has_env_open_db_request());

  Env *env = session->server->environments.get(request->env_open_db_request().env_handle());

  /* convert parameters */
  assert(request->env_open_db_request().param_names().size() < 100);
  for (uint32_t i = 0;
      i < (uint32_t)request->env_open_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_open_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_open_db_request()->mutable_param_values()->data()[i];
  }

  /* check if the database is already open */
  Handle<Db> handle = session->server->get_db_by_name(dbname);
  db = (ups_db_t *)handle.object;
  db_handle = handle.index;

  /* if the database is not yet available: check if it was opened externally */
  if (!db) {
    db = ups_env_get_open_database((ups_env_t *)env, dbname);
    if (likely(db != 0))
      db_handle = session->server->databases.allocate((Db *)db, false);
  }

  /* still not found: open the database */
  if (!db) {
    st = ups_env_open_db((ups_env_t *)env, &db, dbname,
                request->env_open_db_request().flags(), &params[0]);
    if (likely(st == 0))
      db_handle = session->server->databases.allocate((Db *)db);
  }

  Protocol reply(Protocol::ENV_OPEN_DB_REPLY);
  reply.mutable_env_open_db_reply()->set_status(st);
  if (likely(st == 0)) {
    reply.mutable_env_open_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_open_db_reply()->set_db_flags(((Db *)db)->config.flags);
  }

  send_wrapper(session, &reply);
}

static void
handle_env_erase_db(Session *session, Protocol *request)
{
  assert(request != 0);
  assert(request->has_env_erase_db_request());

  Env *env = session->server->environments.get(request->env_erase_db_request().env_handle());

  ups_status_t st = ups_env_erase_db((ups_env_t *)env,
            request->env_erase_db_request().name(),
            request->env_erase_db_request().flags());

  Protocol reply(Protocol::ENV_ERASE_DB_REPLY);
  reply.mutable_env_erase_db_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_db_close(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_db_close_request());

  Handle<Db> *dbh = session->server->databases.get_handle(request->db_close_request().db_handle());

  if (dbh && dbh->own) {
    Db *db = dbh->object;
    st = ups_db_close((ups_db_t *)db, request->db_close_request().flags());
  }

  if (likely(st == 0))
    session->server->databases.remove(request->db_close_request().db_handle());

  Protocol reply(Protocol::DB_CLOSE_REPLY);
  reply.mutable_db_close_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_db_get_parameters(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  ups_parameter_t params[100]; /* 100 should be enough... */

  assert(request != 0);
  assert(request->has_db_get_parameters_request());

  /* initialize the ups_parameters_t array */
  ::memset(&params[0], 0, sizeof(params));
  for (uint32_t i = 0;
      i < (uint32_t)request->db_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_db_get_parameters_request()->mutable_names()->mutable_data()[i];

  /* and request the parameters from the Db */
  Db *db = session->server->databases.get(request->db_get_parameters_request().db_handle());
  if (unlikely(!db))
    st = UPS_INV_PARAMETER;
  else
    st = ups_db_get_parameters((ups_db_t *)db, &params[0]);

  Protocol reply(Protocol::DB_GET_PARAMETERS_REPLY);
  reply.mutable_db_get_parameters_reply()->set_status(st);

  if (unlikely(st)) {
    send_wrapper(session, &reply);
    return;
  }

  /* initialize the reply package */
  for (uint32_t i = 0;
      i < (uint32_t)request->db_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case 0:
      continue;
    case UPS_PARAM_RECORD_COMPRESSION:
      reply.mutable_db_get_parameters_reply()->set_record_compression(
              (int)params[i].value);
      break;
    case UPS_PARAM_KEY_COMPRESSION:
      reply.mutable_db_get_parameters_reply()->set_key_compression(
              (int)params[i].value);
      break;
    case UPS_PARAM_FLAGS:
      reply.mutable_db_get_parameters_reply()->set_flags(
              (int)params[i].value);
      break;
    case UPS_PARAM_KEY_SIZE:
      reply.mutable_db_get_parameters_reply()->set_key_size(
              (int)params[i].value);
      break;
    case UPS_PARAM_RECORD_SIZE:
      reply.mutable_db_get_parameters_reply()->set_record_size(
              (int)params[i].value);
      break;
    case UPS_PARAM_KEY_TYPE:
      reply.mutable_db_get_parameters_reply()->set_key_type(
              (int)params[i].value);
      break;
    case UPS_PARAM_RECORD_TYPE:
      reply.mutable_db_get_parameters_reply()->set_record_type(
              (int)params[i].value);
      break;
    case UPS_PARAM_DATABASE_NAME:
      reply.mutable_db_get_parameters_reply()->set_dbname(
              (int)params[i].value);
      break;
    case UPS_PARAM_MAX_KEYS_PER_PAGE:
      reply.mutable_db_get_parameters_reply()->set_keys_per_page(
              (int)params[i].value);
      break;
    default:
      ups_trace(("unsupported parameter %u", (unsigned)params[i].name));
      break;
    }
  }

  send_wrapper(session, &reply);
}

static void
handle_db_check_integrity(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_db_check_integrity_request());

  uint32_t flags = request->db_check_integrity_request().flags();

  Db *db = session->server->databases.get(request->db_check_integrity_request().db_handle());
  if (unlikely(!db))
    st = UPS_INV_PARAMETER;
  else
    st = ups_db_check_integrity((ups_db_t *)db, flags);

  Protocol reply(Protocol::DB_CHECK_INTEGRITY_REPLY);
  reply.mutable_db_check_integrity_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_db_count(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  uint64_t keycount;

  assert(request != 0);
  assert(request->has_db_count_request());

  Txn *txn = 0;
  Db *db = 0;
  
  if (request->db_count_request().txn_handle()) {
    txn = session->server->transactions.get(request->db_count_request().txn_handle());
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
   db = session->server->databases.get(request->db_count_request().db_handle());
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else
      st = ups_db_count((ups_db_t *)db, (ups_txn_t *)txn,
                request->db_count_request().distinct(), &keycount);
  }

  Protocol reply(Protocol::DB_GET_KEY_COUNT_REPLY);
  reply.mutable_db_count_reply()->set_status(st);
  if (likely(st == 0))
    reply.mutable_db_count_reply()->set_keycount(keycount);

  send_wrapper(session, &reply);
}

static void
handle_db_count(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint64_t keycount;

  Txn *txn = 0;
  Db *db = 0;
  
  if (request->db_count_request.txn_handle) {
    txn = session->server->transactions.get(request->db_count_request.txn_handle);
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
   db = session->server->databases.get(request->db_count_request.db_handle);
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else
      st = ups_db_count((ups_db_t *)db, (ups_txn_t *)txn,
                request->db_count_request.distinct, &keycount);
  }

  SerializedWrapper reply;
  reply.id = kDbGetKeyCountReply;
  reply.db_count_reply.status = st;
  reply.db_count_reply.keycount = keycount;

  send_wrapper(session, &reply);
}

static void
handle_db_insert(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  bool send_key = false;
  ups_key_t key = {0};
  ups_record_t rec = {0};

  assert(request != 0);
  assert(request->has_db_insert_request());

  Txn *txn = 0;
  Db *db = 0;

  if (request->db_insert_request().txn_handle()) {
    txn = session->server->transactions.get(request->db_insert_request().txn_handle());
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
    db = session->server->databases.get(request->db_insert_request().db_handle());
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else {
      if (request->db_insert_request().has_key()) {
        key.size = (uint16_t)request->db_insert_request().key().data().size();
        if (key.size)
          key.data = (void *)&request->db_insert_request().key().data()[0];
        key.flags = request->db_insert_request().key().flags()
                    & (~UPS_KEY_USER_ALLOC);
      }

      if (likely(request->db_insert_request().has_record())) {
        rec.size = (uint32_t)request->db_insert_request().record().data().size();
        if (likely(rec.size))
          rec.data = (void *)&request->db_insert_request().record().data()[0];
        rec.flags = request->db_insert_request().record().flags()
                    & (~UPS_RECORD_USER_ALLOC);
      }
      st = ups_db_insert((ups_db_t *)db, (ups_txn_t *)txn, &key, &rec,
                    request->db_insert_request().flags());

      /* recno: return the modified key */
      if ((st == 0)
          && (((Db *)db)->flags()
                  & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
        send_key = true;
      }
    }
  }

  Protocol reply(Protocol::DB_INSERT_REPLY);
  reply.mutable_db_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_db_insert_reply()->mutable_key(), &key);

  send_wrapper(session, &reply);
}

static void
handle_db_insert(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  bool send_key = false;
  ups_key_t key = {0};
  ups_record_t rec = {0};

  Txn *txn = 0;
  Db *db = 0;

  if (request->db_insert_request.txn_handle) {
    txn = session->server->transactions.get(request->db_insert_request.txn_handle);
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
    db = session->server->databases.get(request->db_insert_request.db_handle);
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else {
      if (request->db_insert_request.has_key) {
        key.size = (uint16_t)request->db_insert_request.key.data.size;
        if (key.size)
          key.data = (void *)request->db_insert_request.key.data.value;
        key.flags = request->db_insert_request.key.flags
                        & (~UPS_KEY_USER_ALLOC);
      }

      if (likely(request->db_insert_request.has_record)) {
        rec.size = (uint32_t)request->db_insert_request.record.data.size;
        if (likely(rec.size))
          rec.data = (void *)request->db_insert_request.record.data.value;
        rec.flags = request->db_insert_request.record.flags
                        & (~UPS_RECORD_USER_ALLOC);
      }
      st = ups_db_insert((ups_db_t *)db, (ups_txn_t *)txn, &key, &rec,
                    request->db_insert_request.flags);

      /* recno: return the modified key */
      if ((st == 0)
          && (((Db *)db)->flags()
                  & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
        send_key = true;
      }
    }
  }

  SerializedWrapper reply;
  reply.id = kDbInsertReply;
  reply.db_insert_reply.status = st;
  if (st == 0 && send_key) {
    reply.db_insert_reply.has_key = true;
    reply.db_insert_reply.key.has_data = true;
    reply.db_insert_reply.key.data.size = key.size;
    reply.db_insert_reply.key.data.value = (uint8_t *)key.data;
    reply.db_insert_reply.key.flags = key.flags;
    reply.db_insert_reply.key.intflags = key._flags;
  }

  send_wrapper(session, &reply);
}

static void
handle_db_find(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  ups_key_t key = {0};
  ups_record_t rec = {0};
  bool send_key = false;

  assert(request != 0);
  assert(request->has_db_find_request());

  Txn *txn = 0;
  Db *db = 0;
  Cursor *cursor = 0;

  if (request->db_find_request().txn_handle()) {
    txn = session->server->transactions.get(request->db_find_request().txn_handle());
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request().cursor_handle()) {
    cursor = session->server->cursors.get(request->db_find_request().cursor_handle());
    if (unlikely(!cursor))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0 && request->db_find_request().db_handle())) {
    db = session->server->databases.get(request->db_find_request().db_handle());
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    key.data = (void *)&request->db_find_request().key().data()[0];
    key.size = (uint16_t)request->db_find_request().key().data().size();
    key.flags = request->db_find_request().key().flags()
                & (~UPS_KEY_USER_ALLOC);

    if (request->db_find_request().has_record()) {
      rec.data = (void *)&request->db_find_request().record().data()[0];
      rec.size = (uint32_t)request->db_find_request().record().data().size();
      rec.flags = request->db_find_request().record().flags()
                  & (~UPS_RECORD_USER_ALLOC);
    }

    if (cursor)
      st = ups_cursor_find((ups_cursor_t *)cursor, &key,
                      request->db_find_request().has_record()
                          ? &rec
                          : 0,
                      request->db_find_request().flags());
    else
      st = ups_db_find((ups_db_t *)db, (ups_txn_t *)txn, &key,
                      &rec, request->db_find_request().flags());
    if (st == 0) {
      /* approx matching: key->_flags was modified! */
      if (key._flags)
        send_key = true;
    }
  }

  Protocol reply(Protocol::DB_FIND_REPLY);
  reply.mutable_db_find_reply()->set_status(st);
  if (likely(st == 0)) {
    if (send_key)
      Protocol::assign_key(reply.mutable_db_find_reply()->mutable_key(), &key);
    Protocol::assign_record(reply.mutable_db_find_reply()->mutable_record(),
                &rec);
  }

  send_wrapper(session, &reply);
}

static void
handle_db_find(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  ups_key_t key = {0};
  ups_record_t rec = {0};
  bool send_key = false;

  Txn *txn = 0;
  Db *db = 0;
  Cursor *cursor = 0;

  if (request->db_find_request.txn_handle) {
    txn = session->server->transactions.get(request->db_find_request.txn_handle);
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request.cursor_handle) {
    cursor = session->server->cursors.get(request->db_find_request.cursor_handle);
    if (unlikely(!cursor))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0 && request->db_find_request.db_handle)) {
    db = session->server->databases.get(request->db_find_request.db_handle);
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
    key.data = (void *)request->db_find_request.key.data.value;
    key.size = (uint16_t)request->db_find_request.key.data.size;
    key.flags = request->db_find_request.key.flags
                  & (~UPS_KEY_USER_ALLOC);

    if (request->db_find_request.has_record) {
      rec.data = (void *)request->db_find_request.record.data.value;
      rec.size = (uint32_t)request->db_find_request.record.data.size;
      rec.flags = request->db_find_request.record.flags
                    & (~UPS_RECORD_USER_ALLOC);
    }

    if (cursor)
      st = ups_cursor_find((ups_cursor_t *)cursor, &key,
                    request->db_find_request.has_record
                        ? &rec
                        : 0,
                    request->db_find_request.flags);
    else
      st = ups_db_find((ups_db_t *)db, (ups_txn_t *)txn, &key,
                    &rec, request->db_find_request.flags);
    if (st == 0) {
      /* approx matching: key->_flags was modified! */
      if (key._flags)
        send_key = true;
    }
  }

  SerializedWrapper reply;
  reply.id = kDbFindReply;
  reply.db_find_reply.status = st;
  if (likely(st == 0)) {
    if (send_key) {
      reply.db_find_reply.has_key = true;
      reply.db_find_reply.key.has_data = true;
      reply.db_find_reply.key.data.size = key.size;
      reply.db_find_reply.key.data.value = (uint8_t *)key.data;
      reply.db_find_reply.key.flags = key.flags;
      reply.db_find_reply.key.intflags = key._flags;
    }
    reply.db_find_reply.has_record = true;
    reply.db_find_reply.record.has_data = true;
    reply.db_find_reply.record.data.size = rec.size;
    reply.db_find_reply.record.data.value = (uint8_t *)rec.data;
    reply.db_find_reply.record.flags = rec.flags;
  }

  send_wrapper(session, &reply);
}

static void
handle_db_erase(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_db_erase_request());

  Txn *txn = 0;
  Db *db = 0;

  if (request->db_erase_request().txn_handle()) {
    txn = session->server->transactions.get(request->db_erase_request().txn_handle());
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
    db = session->server->databases.get(request->db_erase_request().db_handle());
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else {
      ups_key_t key = {0};

      key.data = (void *)&request->db_erase_request().key().data()[0];
      key.size = (uint16_t)request->db_erase_request().key().data().size();
      key.flags = request->db_erase_request().key().flags()
                  & (~UPS_KEY_USER_ALLOC);

      st = ups_db_erase((ups_db_t *)db, (ups_txn_t *)txn, &key,
              request->db_erase_request().flags());
    }
  }

  Protocol reply(Protocol::DB_ERASE_REPLY);
  reply.mutable_db_erase_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_db_erase(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  Txn *txn = 0;
  Db *db = 0;

  if (request->db_erase_request.txn_handle) {
    txn = session->server->transactions.get(request->db_erase_request.txn_handle);
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  if (likely(st == 0)) {
    db = session->server->databases.get(request->db_erase_request.db_handle);
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else {
      ups_key_t key = {0};
      key.data = (void *)request->db_erase_request.key.data.value;
      key.size = (uint16_t)request->db_erase_request.key.data.size;
      key.flags = request->db_erase_request.key.flags
                    & (~UPS_KEY_USER_ALLOC);

      st = ups_db_erase((ups_db_t *)db, (ups_txn_t *)txn, &key,
                request->db_erase_request.flags);
    }
  }

  SerializedWrapper reply;
  reply.id = kDbEraseReply;
  reply.db_erase_reply.status = st;
  send_wrapper(session, &reply);
}

static void
handle_db_bulk_operations(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_db_bulk_operations_request());

  Txn *txn = 0;
  Db *db = 0;

  if (request->db_bulk_operations_request().txn_handle()) {
    txn = session->server->transactions.get(request->db_bulk_operations_request().txn_handle());
    if (unlikely(!txn))
      st = UPS_INV_PARAMETER;
  }

  std::vector<ups_operation_t> ops;

  if (likely(st == 0)) {
    db = session->server->databases.get(request->db_bulk_operations_request().db_handle());
    if (unlikely(!db))
      st = UPS_INV_PARAMETER;
    else {
      for (int i = 0;
              i < request->db_bulk_operations_request().operations().size();
              i++) {
        const Operation &p = request->db_bulk_operations_request().operations(i);
        ups_operation_t op = {0};
        op.type = p.type();
        op.flags = p.flags();
        op.key.data = (void *)&p.key().data()[0];
        op.key.size = p.key().data().size();
        if (p.has_record()) {
          op.record.data = (void *)&p.record().data()[0];
          op.record.size = p.record().data().size();
        }

        ops.push_back(op);
      }

      st = ups_db_bulk_operations((ups_db_t *)db, (ups_txn_t *)txn,
                      ops.data(), ops.size(),
                      request->db_bulk_operations_request().flags());
    }
  }

  Protocol reply(Protocol::DB_BULK_OPERATIONS_REPLY);
  reply.mutable_db_bulk_operations_reply()->set_status(st);

  if (likely(st == 0)) {
    for (std::vector<ups_operation_t>::iterator it = ops.begin();
               it != ops.end();
               it++) {
      Operation *op = reply.mutable_db_bulk_operations_reply()->add_operations();
      op->set_type(it->type);
      op->set_flags(it->flags);
      op->set_result(it->result);

      bool send_key = false;
      bool send_record = false;
      if (it->type == UPS_OP_INSERT
          && ISSETANY(((Db *)db)->flags(),
                  UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))
        send_key = true;
      else if (it->type == UPS_OP_FIND) {
        if (ISSETANY(ups_key_get_intflags(&it->key), BtreeKey::kApproximate))
          send_key = true;
        send_record = true;
      }

      if (send_key)
        Protocol::assign_key(op->mutable_key(), &it->key);
      if (send_record)
        Protocol::assign_record(op->mutable_record(), &it->record);
    }
  }

  send_wrapper(session, &reply);
}

static void
handle_txn_begin(Session *session, Protocol *request)
{
  ups_txn_t *txn;
  ups_status_t st = 0;
  uint64_t txn_handle = 0;

  assert(request != 0);
  assert(request->has_txn_begin_request());

  Env *env = session->server->environments.get(request->txn_begin_request().env_handle());

  st = ups_txn_begin(&txn, (ups_env_t *)env,
            request->txn_begin_request().has_name()
              ? request->txn_begin_request().name().c_str()
              : 0,
            0, request->txn_begin_request().flags());

  if (likely(st == 0))
    txn_handle = session->server->transactions.allocate((Txn *)txn);

  Protocol reply(Protocol::TXN_BEGIN_REPLY);
  reply.mutable_txn_begin_reply()->set_status(st);
  reply.mutable_txn_begin_reply()->set_txn_handle(txn_handle);

  send_wrapper(session, &reply);
}

static void
handle_txn_begin(Session *session, SerializedWrapper *request)
{
  ups_txn_t *txn;
  ups_status_t st = 0;
  uint64_t txn_handle = 0;

  Env *env = session->server->environments.get(request->txn_begin_request.env_handle);

  st = ups_txn_begin(&txn, (ups_env_t *)env,
            (const char *)request->txn_begin_request.name.value,
            0, request->txn_begin_request.flags);

  if (likely(st == 0))
    txn_handle = session->server->transactions.allocate((Txn *)txn);

  SerializedWrapper reply;
  reply.id = kTxnBeginReply;
  reply.txn_begin_reply.status = st;
  reply.txn_begin_reply.txn_handle = txn_handle;

  send_wrapper(session, &reply);
}

static void
handle_txn_commit(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_txn_commit_request());

  Txn *txn = session->server->transactions.get(request->txn_commit_request().txn_handle());
  if (unlikely(!txn)) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_commit((ups_txn_t *)txn,
            request->txn_commit_request().flags());
    if (likely(st == 0)) {
      /* remove the handle from the Env wrapper structure */
      session->server->transactions.remove(request->txn_commit_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_COMMIT_REPLY);
  reply.mutable_txn_commit_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_txn_commit(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;

  Txn *txn = session->server->transactions.get(request->txn_commit_request.txn_handle);
  if (unlikely(!txn)) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_commit((ups_txn_t *)txn, request->txn_commit_request.flags);
    if (likely(st == 0)) {
      /* remove the handle from the Env wrapper structure */
      session->server->transactions.remove(request->txn_commit_request.txn_handle);
    }
  }

  SerializedWrapper reply;
  reply.id = kTxnCommitReply;
  reply.txn_commit_reply.status = st;

  send_wrapper(session, &reply);
}

static void
handle_txn_abort(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_txn_abort_request());

  Txn *txn = session->server->transactions.get(request->txn_abort_request().txn_handle());
  if (unlikely(!txn)) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_abort((ups_txn_t *)txn,
            request->txn_abort_request().flags());
    if (likely(st == 0)) {
      /* remove the handle from the Env wrapper structure */
      session->server->transactions.remove(request->txn_abort_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_ABORT_REPLY);
  reply.mutable_txn_abort_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_txn_abort(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;

  Txn *txn = session->server->transactions.get(request->txn_abort_request.txn_handle);
  if (unlikely(!txn)) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_abort((ups_txn_t *)txn, request->txn_abort_request.flags);
    if (likely(st == 0)) {
      /* remove the handle from the Env wrapper structure */
      session->server->transactions.remove(request->txn_abort_request.txn_handle);
    }
  }

  SerializedWrapper reply;
  reply.id = kTxnAbortReply;
  reply.txn_abort_reply.status = st;

  send_wrapper(session, &reply);
}

static void
handle_cursor_create(Session *session, Protocol *request)
{
  ups_cursor_t *cursor;
  ups_status_t st = 0;
  uint64_t handle = 0;

  assert(request != 0);
  assert(request->has_cursor_create_request());

  Txn *txn = 0;
  Db *db = 0;

  if (request->cursor_create_request().txn_handle()) {
    txn = session->server->transactions.get(request->cursor_create_request().txn_handle());
    if (unlikely(!txn)) {
      st = UPS_INV_PARAMETER;
      goto bail;
    }
  }

  db = session->server->databases.get(request->cursor_create_request().db_handle());
  if (unlikely(!db)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ups_cursor_create(&cursor, (ups_db_t *)db, (ups_txn_t *)txn,
                request->cursor_create_request().flags());

  if (likely(st == 0)) {
    /* allocate a new handle in the Env wrapper structure */
    handle = session->server->cursors.allocate((Cursor *)cursor);
  }

bail:
  Protocol reply(Protocol::CURSOR_CREATE_REPLY);
  reply.mutable_cursor_create_reply()->set_status(st);
  reply.mutable_cursor_create_reply()->set_cursor_handle(handle);

  send_wrapper(session, &reply);
}

static void
handle_cursor_create(Session *session, SerializedWrapper *request)
{
  ups_cursor_t *cursor;
  ups_status_t st = 0;
  uint64_t handle = 0;
  Txn *txn = 0;
  Db *db = 0;

  if (request->cursor_create_request.txn_handle) {
    txn = session->server->transactions.get(request->cursor_create_request.txn_handle);
    if (unlikely(!txn)) {
      st = UPS_INV_PARAMETER;
      goto bail;
    }
  }

  db = session->server->databases.get(request->cursor_create_request.db_handle);
  if (unlikely(!db)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ups_cursor_create(&cursor, (ups_db_t *)db, (ups_txn_t *)txn,
            request->cursor_create_request.flags);

  if (likely(st == 0)) {
    /* allocate a new handle in the Env wrapper structure */
    handle = session->server->cursors.allocate((Cursor *)cursor);
  }

bail:
  SerializedWrapper reply;
  reply.id = kCursorCreateReply;
  reply.cursor_create_reply.status = st;
  reply.cursor_create_reply.cursor_handle = handle;

  send_wrapper(session, &reply);
}

static void
handle_cursor_clone(Session *session, Protocol *request)
{
  ups_cursor_t *dest;
  ups_status_t st = 0;
  uint64_t handle = 0;

  assert(request != 0);
  assert(request->has_cursor_clone_request());

  Cursor *src = session->server->cursors.get(request->cursor_clone_request().cursor_handle());
  if (unlikely(!src))
    st = UPS_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ups_cursor_clone((ups_cursor_t *)src, &dest);
    if (likely(st == 0)) {
      /* allocate a new handle in the Env wrapper structure */
      handle = session->server->cursors.allocate((Cursor *)dest);
    }
  }

  Protocol reply(Protocol::CURSOR_CLONE_REPLY);
  reply.mutable_cursor_clone_reply()->set_status(st);
  reply.mutable_cursor_clone_reply()->set_cursor_handle(handle);

  send_wrapper(session, &reply);
}

static void
handle_cursor_clone(Session *session, SerializedWrapper *request)
{
  ups_cursor_t *dest;
  ups_status_t st = 0;
  uint64_t handle = 0;

  Cursor *src = session->server->cursors.get(request->cursor_clone_request.cursor_handle);
  if (unlikely(!src))
    st = UPS_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ups_cursor_clone((ups_cursor_t *)src, &dest);
    if (likely(st == 0)) {
      /* allocate a new handle in the Env wrapper structure */
      handle = session->server->cursors.allocate((Cursor *)dest);
    }
  }

  SerializedWrapper reply;
  reply.id = kCursorCloneReply;
  reply.cursor_clone_reply.status = st;
  reply.cursor_clone_reply.cursor_handle = handle;

  send_wrapper(session, &reply);
}

static void
handle_cursor_insert(Session *session, Protocol *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key = false;

  assert(request != 0);
  assert(request->has_cursor_insert_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_insert_request().cursor_handle());
  if (unlikely(!cursor)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  if (request->cursor_insert_request().has_key()) {
    key.size = (uint16_t)request->cursor_insert_request().key().data().size();
    if (key.size)
      key.data = (void *)&request->cursor_insert_request().key().data()[0];
    key.flags = request->cursor_insert_request().key().flags()
                & (~UPS_KEY_USER_ALLOC);
  }

  if (likely(request->cursor_insert_request().has_record())) {
    rec.size = (uint32_t)request->cursor_insert_request().record().data().size();
    if (likely(rec.size))
      rec.data = (void *)&request->cursor_insert_request().record().data()[0];
    rec.flags = request->cursor_insert_request().record().flags()
                & (~UPS_RECORD_USER_ALLOC);
  }

  send_key = request->cursor_insert_request().send_key();

  st = ups_cursor_insert((ups_cursor_t *)cursor, &key, &rec,
            request->cursor_insert_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_INSERT_REPLY);
  reply.mutable_cursor_insert_reply()->set_status(st);
  if (st == 0 && send_key)
    Protocol::assign_key(reply.mutable_cursor_insert_reply()->mutable_key(),
                &key);
  send_wrapper(session, &reply);
}

static void
handle_cursor_insert(Session *session, SerializedWrapper *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key;

  Cursor *cursor = session->server->cursors.get(request->cursor_insert_request.cursor_handle);
  if (unlikely(!cursor)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  if (request->cursor_insert_request.has_key) {
    key.size = (uint16_t)request->cursor_insert_request.key.data.size;
    if (key.size)
      key.data = request->cursor_insert_request.key.data.value;
    key.flags = request->cursor_insert_request.key.flags
                & (~UPS_KEY_USER_ALLOC);
  }

  if (likely(request->cursor_insert_request.has_record)) {
    rec.size = (uint32_t)request->cursor_insert_request.record.data.size;
    if (likely(rec.size))
      rec.data = request->cursor_insert_request.record.data.value;
    rec.flags = request->cursor_insert_request.record.flags
                & (~UPS_RECORD_USER_ALLOC);
  }

  st = ups_cursor_insert((ups_cursor_t *)cursor, &key, &rec,
            request->cursor_insert_request.flags);

  send_key = request->cursor_insert_request.send_key;

bail:
  SerializedWrapper reply;
  reply.id = kCursorInsertReply;
  reply.cursor_insert_reply.status = st;
  if (st == 0 && send_key) {
    reply.cursor_insert_reply.has_key = true;
    reply.cursor_insert_reply.key.has_data = true;
    reply.cursor_insert_reply.key.data.size = key.size;
    reply.cursor_insert_reply.key.data.value = (uint8_t *)key.data;
    reply.cursor_insert_reply.key.flags = key.flags;
    reply.cursor_insert_reply.key.intflags = key._flags;
  }

  send_wrapper(session, &reply);
}

static void
handle_cursor_erase(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_cursor_erase_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_erase_request().cursor_handle());
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_erase((ups_cursor_t *)cursor,
            request->cursor_erase_request().flags());

  Protocol reply(Protocol::CURSOR_ERASE_REPLY);
  reply.mutable_cursor_erase_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_cursor_erase(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_erase_request.cursor_handle);
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_erase((ups_cursor_t *)cursor,
                request->cursor_erase_request.flags);

  SerializedWrapper reply;
  reply.id = kCursorEraseReply;
  reply.cursor_erase_reply.status = st;

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_record_count(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  uint32_t count = 0;

  assert(request != 0);
  assert(request->has_cursor_get_record_count_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_get_record_count_request().cursor_handle());
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_count((ups_cursor_t *)cursor, &count,
            request->cursor_get_record_count_request().flags());

  Protocol reply(Protocol::CURSOR_GET_RECORD_COUNT_REPLY);
  reply.mutable_cursor_get_record_count_reply()->set_status(st);
  reply.mutable_cursor_get_record_count_reply()->set_count(count);

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_record_count(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint32_t count = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_get_record_count_request.cursor_handle);
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_count((ups_cursor_t *)cursor, &count,
            request->cursor_get_record_count_request.flags);

  SerializedWrapper reply;
  reply.id = kCursorGetRecordCountReply;
  reply.cursor_get_record_count_reply.status = st;
  reply.cursor_get_record_count_reply.count = count;

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_record_size(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  uint32_t size = 0;

  assert(request != 0);
  assert(request->has_cursor_get_record_size_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_get_record_size_request().cursor_handle());
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_record_size((ups_cursor_t *)cursor, &size);

  Protocol reply(Protocol::CURSOR_GET_RECORD_SIZE_REPLY);
  reply.mutable_cursor_get_record_size_reply()->set_status(st);
  reply.mutable_cursor_get_record_size_reply()->set_size(size);

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_record_size(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint32_t size = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_get_record_size_request.cursor_handle);
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_record_size((ups_cursor_t *)cursor, &size);

  SerializedWrapper reply;
  reply.id = kCursorGetRecordSizeReply;
  reply.cursor_get_record_size_reply.status = st;
  reply.cursor_get_record_size_reply.size = size;

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_duplicate_position(Session *session, Protocol *request)
{
  ups_status_t st = 0;
  uint32_t position = 0;

  assert(request != 0);
  assert(request->has_cursor_get_duplicate_position_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_get_duplicate_position_request().cursor_handle());
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_position((ups_cursor_t *)cursor, &position);

  Protocol reply(Protocol::CURSOR_GET_DUPLICATE_POSITION_REPLY);
  reply.mutable_cursor_get_duplicate_position_reply()->set_status(st);
  reply.mutable_cursor_get_duplicate_position_reply()->set_position(position);

  send_wrapper(session, &reply);
}

static void
handle_cursor_get_duplicate_position(Session *session,
            SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint32_t position = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_get_duplicate_position_request.cursor_handle);
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_position((ups_cursor_t *)cursor, &position);

  SerializedWrapper reply;
  reply.id = kCursorGetDuplicatePositionReply;
  reply.cursor_get_duplicate_position_reply.status = st;
  reply.cursor_get_duplicate_position_reply.position = position;

  send_wrapper(session, &reply);
}

static void
handle_cursor_overwrite(Session *session,
            Protocol *request)
{
  ups_record_t rec = {0};
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_cursor_overwrite_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_overwrite_request().cursor_handle());
  if (unlikely(!cursor)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  rec.data = (void *)&request->cursor_overwrite_request().record().data()[0];
  rec.size = (uint32_t)request->cursor_overwrite_request().record().data().size();
  rec.flags = request->cursor_overwrite_request().record().flags()
              & (~UPS_RECORD_USER_ALLOC);

  st = ups_cursor_overwrite((ups_cursor_t *)cursor, &rec,
            request->cursor_overwrite_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_OVERWRITE_REPLY);
  reply.mutable_cursor_overwrite_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_cursor_overwrite(Session *session, SerializedWrapper *request)
{
  ups_record_t rec = {0};
  ups_status_t st = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_overwrite_request.cursor_handle);
  if (unlikely(!cursor)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  rec.data = request->cursor_overwrite_request.record.data.value;
  rec.size = (uint32_t)request->cursor_overwrite_request.record.data.size;
  rec.flags = request->cursor_overwrite_request.record.flags
              & (~UPS_RECORD_USER_ALLOC);

  st = ups_cursor_overwrite((ups_cursor_t *)cursor, &rec,
            request->cursor_overwrite_request.flags);

bail:
  SerializedWrapper reply;
  reply.id = kCursorOverwriteReply;
  reply.cursor_overwrite_reply.status = st;

  send_wrapper(session, &reply);
}

static void
handle_cursor_move(Session *session, Protocol *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key = false;
  bool send_rec = false;

  assert(request != 0);
  assert(request->has_cursor_move_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_move_request().cursor_handle());
  if (unlikely(!cursor)) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  if (request->cursor_move_request().has_key()) {
    send_key = true;

    key.data = (void *)&request->cursor_move_request().key().data()[0];
    key.size = (uint16_t)request->cursor_move_request().key().data().size();
    key.flags = request->cursor_move_request().key().flags()
                & (~UPS_KEY_USER_ALLOC);
  }

  if (request->cursor_move_request().has_record()) {
    send_rec = true;

    rec.data = (void *)&request->cursor_move_request().record().data()[0];
    rec.size = (uint32_t)request->cursor_move_request().record().data().size();
    rec.flags = request->cursor_move_request().record().flags()
                & (~UPS_RECORD_USER_ALLOC);
  }

  st = ups_cursor_move((ups_cursor_t *)cursor,
                        send_key ? &key : 0,
                        send_rec ? &rec : 0,
                        request->cursor_move_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_MOVE_REPLY);
  reply.mutable_cursor_move_reply()->set_status(st);
  if (likely(st == 0)) {
    if (send_key)
      Protocol::assign_key(reply.mutable_cursor_move_reply()->mutable_key(),
                    &key);
    if (send_rec)
      Protocol::assign_record(reply.mutable_cursor_move_reply()->mutable_record(),
                    &rec);
  }

  send_wrapper(session, &reply);
}

static void
handle_cursor_close(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_cursor_close_request());

  Cursor *cursor = session->server->cursors.get(request->cursor_close_request().cursor_handle());
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_close((ups_cursor_t *)cursor);

  if (likely(st == 0)) {
    /* remove the handle from the Env wrapper structure */
    session->server->cursors.remove(request->cursor_close_request().cursor_handle());
  }

  Protocol reply(Protocol::CURSOR_CLOSE_REPLY);
  reply.mutable_cursor_close_reply()->set_status(st);

  send_wrapper(session, &reply);
}

static void
handle_cursor_close(Session *session, SerializedWrapper *request)
{
  ups_status_t st = 0;

  Cursor *cursor = session->server->cursors.get(request->cursor_close_request.cursor_handle);
  if (unlikely(!cursor))
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_close((ups_cursor_t *)cursor);

  if (likely(st == 0)) {
    /* remove the handle from the Env wrapper structure */
    session->server->cursors.remove(request->cursor_close_request.cursor_handle);
  }

  SerializedWrapper reply;
  reply.id = kCursorCloseReply;
  reply.cursor_close_reply.status = st;

  send_wrapper(session, &reply);
}

static void
handle_select_range(Session *session, Protocol *request)
{
  ups_status_t st = 0;

  assert(request != 0);
  assert(request->has_select_range_request());

  Cursor *begin = 0;
  if (request->select_range_request().begin_cursor_handle())
    begin = session->server->cursors.get(request->select_range_request().begin_cursor_handle());
  Cursor *end = 0;
  if (request->select_range_request().end_cursor_handle())
    end = session->server->cursors.get(request->select_range_request().end_cursor_handle());

  Env *env = session->server->environments.get(request->select_range_request().env_handle());
  const char *query = request->select_range_request().query().c_str();

  uqi_result_t *result = 0;
  st = uqi_select_range((ups_env_t *)env, query, (ups_cursor_t *)begin,
                        (ups_cursor_t *)end, &result);

  Result *r = (Result *)result;
  Protocol reply(Protocol::SELECT_RANGE_REPLY);
  reply.mutable_select_range_reply()->set_status(st);
  if (unlikely(st != 0)) {
    reply.mutable_select_range_reply()->set_row_count(0);
    reply.mutable_select_range_reply()->set_key_type(0);
    reply.mutable_select_range_reply()->set_record_type(0);
    send_wrapper(session, &reply);
    return;
  }
  reply.mutable_select_range_reply()->set_row_count(r->row_count);
  reply.mutable_select_range_reply()->set_key_type(r->key_type);
  reply.mutable_select_range_reply()->set_key_data(r->key_data.data(),
                r->key_data.size());
  reply.mutable_select_range_reply()->set_record_type(r->record_type);
  reply.mutable_select_range_reply()->set_record_data(r->record_data.data(),
                r->record_data.size());

  reply.mutable_select_range_reply()->mutable_key_offsets()->Reserve(r->row_count);
  for (uint32_t i = 0; i < r->row_count; i++)
    reply.mutable_select_range_reply()->add_key_offsets(r->key_offsets[i]);

  reply.mutable_select_range_reply()->mutable_record_offsets()->Reserve(r->row_count);
  for (uint32_t i = 0; i < r->row_count; i++)
    reply.mutable_select_range_reply()->add_record_offsets(r->record_offsets[i]);

  uqi_result_close(result);

  send_wrapper(session, &reply);
}

// returns false if client should be closed, otherwise true
static bool
dispatch(Session *session, uint32_t magic, uint8_t *data, uint32_t size)
{
  if (magic == UPS_TRANSFER_MAGIC_V2) {
    SerializedWrapper request;
    int size_left = (int)size;
    request.deserialize(&data, &size_left);
    assert(size_left == 0);

    switch (request.id) {
      case kDbInsertRequest:
        handle_db_insert(session, &request);
        break;
      case kDbEraseRequest:
        handle_db_erase(session, &request);
        break;
      case kDbFindRequest:
        handle_db_find(session, &request);
        break;
      case kDbGetKeyCountRequest:
        handle_db_count(session, &request);
        break;
      case kCursorCreateRequest:
        handle_cursor_create(session, &request);
        break;
      case kCursorCloneRequest:
        handle_cursor_clone(session, &request);
        break;
      case kCursorCloseRequest:
        handle_cursor_close(session, &request);
        break;
      case kCursorInsertRequest:
        handle_cursor_insert(session, &request);
        break;
      case kCursorEraseRequest:
        handle_cursor_erase(session, &request);
        break;
      case kCursorGetRecordCountRequest:
        handle_cursor_get_record_count(session, &request);
        break;
      case kCursorGetRecordSizeRequest:
        handle_cursor_get_record_size(session, &request);
        break;
      case kCursorGetDuplicatePositionRequest:
        handle_cursor_get_duplicate_position(session, &request);
        break;
      case kCursorOverwriteRequest:
        handle_cursor_overwrite(session, &request);
        break;
      case kTxnBeginRequest:
        handle_txn_begin(session, &request);
        break;
      case kTxnAbortRequest:
        handle_txn_abort(session, &request);
        break;
      case kTxnCommitRequest:
        handle_txn_commit(session, &request);
        break;
      default:
        ups_trace(("ignoring unknown request"));
        break;
    }
    return true;
  }

  // Protocol buffer requests are handled here
  Protocol *wrapper = Protocol::unpack(data, size);
  if (unlikely(!wrapper)) {
    ups_trace(("failed to unpack wrapper (%d bytes)\n", size));
    return false;
  }

  bool rv = true;

  switch (wrapper->type()) {
    case ProtoWrapper_Type_CONNECT_REQUEST:
      handle_connect(session, wrapper);
      break;
    case ProtoWrapper_Type_DISCONNECT_REQUEST:
      handle_disconnect(session, wrapper);
      rv = false;
      break;
    case ProtoWrapper_Type_ENV_GET_PARAMETERS_REQUEST:
      handle_env_get_parameters(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_GET_DATABASE_NAMES_REQUEST:
      handle_env_get_database_names(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_FLUSH_REQUEST:
      handle_env_flush(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_RENAME_REQUEST:
      handle_env_rename(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_CREATE_DB_REQUEST:
      handle_env_create_db(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_OPEN_DB_REQUEST:
      handle_env_open_db(session, wrapper);
      break;
    case ProtoWrapper_Type_ENV_ERASE_DB_REQUEST:
      handle_env_erase_db(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_CLOSE_REQUEST:
      handle_db_close(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_GET_PARAMETERS_REQUEST:
      handle_db_get_parameters(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_CHECK_INTEGRITY_REQUEST:
      handle_db_check_integrity(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_GET_KEY_COUNT_REQUEST:
      handle_db_count(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_INSERT_REQUEST:
      handle_db_insert(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_FIND_REQUEST:
      handle_db_find(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_ERASE_REQUEST:
      handle_db_erase(session, wrapper);
      break;
    case ProtoWrapper_Type_DB_BULK_OPERATIONS_REQUEST:
      handle_db_bulk_operations(session, wrapper);
      break;
    case ProtoWrapper_Type_TXN_BEGIN_REQUEST:
      handle_txn_begin(session, wrapper);
      break;
    case ProtoWrapper_Type_TXN_COMMIT_REQUEST:
      handle_txn_commit(session, wrapper);
      break;
    case ProtoWrapper_Type_TXN_ABORT_REQUEST:
      handle_txn_abort(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CREATE_REQUEST:
      handle_cursor_create(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CLONE_REQUEST:
      handle_cursor_clone(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_INSERT_REQUEST:
      handle_cursor_insert(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_ERASE_REQUEST:
      handle_cursor_erase(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_RECORD_COUNT_REQUEST:
      handle_cursor_get_record_count(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_RECORD_SIZE_REQUEST:
      handle_cursor_get_record_size(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_DUPLICATE_POSITION_REQUEST:
      handle_cursor_get_duplicate_position(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_OVERWRITE_REQUEST:
      handle_cursor_overwrite(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_MOVE_REQUEST:
      handle_cursor_move(session, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CLOSE_REQUEST:
      handle_cursor_close(session, wrapper);
      break;
    case ProtoWrapper_Type_SELECT_RANGE_REQUEST:
      handle_select_range(session, wrapper);
      break;
    default:
      ups_trace(("ignoring unknown request"));
      break;
  }

  delete wrapper;

  return rv;
}

void
Session::handle_read(const boost::system::error_code &error,
                size_t bytes_transferred) {
  if (unlikely(error != boost::system::errc::errc_t::success) || bytes_transferred == 0) {
    delete this;
    return;
  }

  bool close_client = false;

  if (current_position != 0) {
    bytes_transferred += current_position;
    current_position = 0;
  }

  // each request is prepended with a header:
  //   4 byte magic
  //   4 byte size  (without those 8 bytes)
  // for each full package in the buffer...
  uint8_t *p = buffer_in.data();

  while (bytes_transferred > 8) {
    uint32_t magic = *(uint32_t *)(p + 0);
    uint32_t size = *(uint32_t *)(p + 4);
    if (magic == UPS_TRANSFER_MAGIC_V1)
      size += 8;
    // still not enough data? then return immediately
    if (unlikely(bytes_transferred < size)) {
      current_position = bytes_transferred;
      break;
    }

    // otherwise dispatch the message
    close_client = !dispatch(this, magic, p, size);

    // more data left? if not then leave
    if (bytes_transferred == size) {
      current_position = 0;
      break;
    }

    // otherwise remove the data that was handled
    uint32_t new_size = bytes_transferred - size;
    ::memmove(p, p + size, new_size);
    bytes_transferred = new_size;

    // repeat the loop
  }

  // done? then either close the socket or continue reading
  if (unlikely(close_client)) {
    delete this;
    return;
  }

  // resize the buffer?
  if (buffer_in.capacity() - current_position < 1024)
    buffer_in.resize(buffer_in.capacity() * 2);
  socket.async_read_some(boost::asio::buffer(buffer_in.data() + current_position,
                            buffer_in.capacity() - current_position),
        boost::bind(&Session::handle_read, this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
}

} // namespace upscaledb

// global namespace is below

using namespace upscaledb;

ups_status_t
ups_srv_init(ups_srv_config_t *config, ups_srv_t **psrv)
{
  *psrv = 0;
  Server *srv = 0;

  try {
    if (!config->bind_addr || !*config->bind_addr)
      srv = new Server(config->port);
    else
      srv = new Server(config->bind_addr, config->port);
    srv->run();
  }
  catch (std::exception &e) {
    ups_log(("failed to start server at port %d", config->port)); 
    delete srv;
    return UPS_IO_ERROR;
  }

  *psrv = (ups_srv_t *)srv;
  return 0;
}

ups_status_t
ups_srv_add_env(ups_srv_t *hsrv, ups_env_t *env, const char *urlname)
{
  Server *srv = (Server *)hsrv;
  if (unlikely(!srv || !env || !urlname)) {
    ups_log(("parameters srv, env, urlname must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  ScopedLock lock(srv->open_env_mutex);
  srv->open_envs[urlname] = (Env *)env;
  return 0;
}

ups_status_t
ups_srv_remove_env(ups_srv_t *hsrv, ups_env_t *env)
{
  Server *srv = (Server *)hsrv;
  if (unlikely(!srv || !env)) {
    ups_log(("parameters srv and env must not be NULL"));
    return UPS_INV_PARAMETER;
  }

  ScopedLock lock(srv->open_env_mutex);
  for (EnvironmentMap::iterator it = srv->open_envs.begin();
                  it != srv->open_envs.end();
                  ++it) {
    if (it->second == (Env *)env) {
      srv->open_envs.erase(it);
      return 0;
    }
  }
  return 0;
}

void
ups_srv_close(ups_srv_t *hsrv)
{
  Server *srv = (Server *)hsrv;
  if (unlikely(!srv))
    return;

  delete srv;

  // free libprotocol static data
  Protocol::shutdown();
}

