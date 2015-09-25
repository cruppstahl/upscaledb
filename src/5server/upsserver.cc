/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include <string.h>

// winsock2.h is required for libuv
#ifdef WIN32
#  include <winsock2.h>
#endif

// include this BEFORE os.h!
#include <uv.h>

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1mem/mem.h"
#include "2protobuf/protocol.h"
#include "2protoserde/messages.h"
#include "4env/env.h"
#include "4db/db_local.h"
#include "5server/upsserver.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

static void
on_write_cb(uv_write_t *req, int status)
{
  Memory::release(req->data);
  delete req;
};

static void
on_write_cb2(uv_write_t *req, int status)
{
  delete req;
};

static void
send_wrapper(ServerContext *srv, uv_stream_t *tcp, Protocol *reply)
{
  uint8_t *data;
  uint32_t data_size;

  if (!reply->pack(&data, &data_size))
    return;

  // |req| needs to exist till the request was finished asynchronously;
  // therefore it must be allocated on the heap
  uv_write_t *req = new uv_write_t();
  uv_buf_t buf = uv_buf_init((char *)data, data_size);
  req->data = data;
  // |req| and |data| are freed in on_write_cb()
  uv_write(req, (uv_stream_t *)tcp, &buf, 1, on_write_cb);
}

static void
send_wrapper(ServerContext *srv, uv_stream_t *tcp, SerializedWrapper *reply)
{
  int size_left = (int)reply->get_size();
  int reply_size = size_left;
  reply->magic = UPS_TRANSFER_MAGIC_V2;
  reply->size = size_left;
  srv->buffer.resize(size_left);
  uint8_t *ptr = (uint8_t *)srv->buffer.get_ptr();

  reply->serialize(&ptr, &size_left);
  ups_assert(size_left == 0);

  // |req| needs to exist till the request was finished asynchronously;
  // therefore it must be allocated on the heap
  uv_write_t *req = new uv_write_t();
  uv_buf_t buf = uv_buf_init((char *)srv->buffer.get_ptr(), reply_size);
  req->data = (uint8_t *)srv->buffer.get_ptr();

  // |req| is freed in on_write_cb()
  uv_write(req, (uv_stream_t *)tcp, &buf, 1, on_write_cb2);
}

static void
handle_connect(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_assert(request != 0);
  Environment *env = srv->open_envs[request->connect_request().path()];

  if (ErrorInducer::is_active()) {
    if (ErrorInducer::get_instance()->induce(ErrorInducer::kServerConnect)) {
#ifdef WIN32
      Sleep(5);
#else
      sleep(5);
#endif
      ErrorInducer::activate(false);
    }
  }

  Protocol reply(Protocol::CONNECT_REPLY);
  if (!env) {
    reply.mutable_connect_reply()->set_status(UPS_FILE_NOT_FOUND);
  }
  else {
    reply.mutable_connect_reply()->set_status(0);
    reply.mutable_connect_reply()->set_env_flags(
            ((Environment *)env)->get_flags());
    reply.mutable_connect_reply()->set_env_handle(srv->allocate_handle(env));
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_disconnect(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_assert(request != 0);
  srv->remove_env_handle(request->disconnect_request().env_handle());

  Protocol reply(Protocol::DISCONNECT_REPLY);
  reply.mutable_disconnect_reply()->set_status(0);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_get_parameters(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  uint32_t i;
  ups_status_t st = 0;
  Environment *env = 0;
  ups_parameter_t params[100]; /* 100 should be enough... */

  ups_assert(request != 0);
  ups_assert(request->has_env_get_parameters_request());

  /* initialize the ups_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (i = 0;
      i < (uint32_t)request->env_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_env_get_parameters_request()->mutable_names()->mutable_data()[i];

  Protocol reply(Protocol::ENV_GET_PARAMETERS_REPLY);

  env = srv->get_env(request->env_get_parameters_request().env_handle());

  /* and request the parameters from the Environment */
  st = ups_env_get_parameters((ups_env_t *)env, &params[0]);
  reply.mutable_env_get_parameters_reply()->set_status(st);
  if (st) {
    send_wrapper(srv, tcp, &reply);
    return;
  }

  /* initialize the reply package */
  for (i = 0;
      i < (uint32_t)request->env_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
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
              (const char *)(U64_TO_PTR(params[i].value)));
      break;
    default:
      ups_trace(("unsupported parameter %u", (unsigned)params[i].name));
      break;
    }
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_get_database_names(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  uint32_t num_names = 1024;
  uint16_t names[1024]; /* should be enough */
  Environment *env = 0;

  ups_assert(request != 0);
  ups_assert(request->has_env_get_database_names_request());

  env = srv->get_env(request->env_get_database_names_request().env_handle());

  /* request the database names from the Environment */
  ups_status_t st = ups_env_get_database_names((ups_env_t *)env,
          &names[0], &num_names);

  Protocol reply(Protocol::ENV_GET_DATABASE_NAMES_REPLY);
  reply.mutable_env_get_database_names_reply()->set_status(st);
  for (uint32_t i = 0; i < num_names; i++)
    reply.mutable_env_get_database_names_reply()->add_names(names[i]);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_flush(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  Environment *env = 0;

  ups_assert(request != 0);
  ups_assert(request->has_env_flush_request());

  env = srv->get_env(request->env_flush_request().env_handle());

  /* request the database names from the Environment */
  Protocol reply(Protocol::ENV_FLUSH_REPLY);
  reply.mutable_env_flush_reply()->set_status(ups_env_flush((ups_env_t *)env,
            request->env_flush_request().flags()));

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_rename(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  Environment *env = 0;

  ups_assert(request != 0);
  ups_assert(request->has_env_rename_request());

  env = srv->get_env(request->env_rename_request().env_handle());

  /* rename the databases */
  ups_status_t st = ups_env_rename_db((ups_env_t *)env,
          request->env_rename_request().oldname(),
          request->env_rename_request().newname(),
          request->env_rename_request().flags());

  Protocol reply(Protocol::ENV_RENAME_REPLY);
  reply.mutable_env_rename_reply()->set_status(st);
  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_create_db(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_db_t *db;
  Environment *env;
  ups_status_t st = 0;
  uint64_t db_handle = 0;
  ups_parameter_t params[100] = {{0, 0}};

  ups_assert(request != 0);
  ups_assert(request->has_env_create_db_request());

  env = srv->get_env(request->env_create_db_request().env_handle());

  /* convert parameters */
  ups_assert(request->env_create_db_request().param_names().size() < 100);
  for (uint32_t i = 0;
      i < (uint32_t)request->env_create_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_create_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_create_db_request()->mutable_param_values()->data()[i];
  }

  /* create the database */
  st = ups_env_create_db((ups_env_t *)env, &db,
            request->env_create_db_request().dbname(),
            request->env_create_db_request().flags(), &params[0]);

  if (st == 0) {
    /* allocate a new database handle in the Env wrapper structure */
    db_handle = srv->allocate_handle((Database *)db);
  }

  Protocol reply(Protocol::ENV_CREATE_DB_REPLY);
  reply.mutable_env_create_db_reply()->set_status(st);
  if (db_handle) {
    reply.mutable_env_create_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_create_db_reply()->set_db_flags(
        ((Database *)db)->config().flags);
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_open_db(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_db_t *db = 0;
  uint64_t db_handle = 0;
  ups_status_t st = 0;
  uint16_t dbname = request->env_open_db_request().dbname();
  ups_parameter_t params[100] = {{0, 0}};

  ups_assert(request != 0);
  ups_assert(request->has_env_open_db_request());

  Environment *env = srv->get_env(request->env_open_db_request().env_handle());

  /* convert parameters */
  ups_assert(request->env_open_db_request().param_names().size() < 100);
  for (uint32_t i = 0;
      i < (uint32_t)request->env_open_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_open_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_open_db_request()->mutable_param_values()->data()[i];
  }

  /* check if the database is already open */
  Handle<Database> handle = srv->get_db_by_name(dbname);
  db = (ups_db_t *)handle.object;
  db_handle = handle.index;

  if (!db) {
    /* still not found: open the database */
    st = ups_env_open_db((ups_env_t *)env, &db, dbname,
                request->env_open_db_request().flags(), &params[0]);

    if (st == 0)
      db_handle = srv->allocate_handle((Database *)db);
  }

  Protocol reply(Protocol::ENV_OPEN_DB_REPLY);
  reply.mutable_env_open_db_reply()->set_status(st);
  if (st == 0) {
    reply.mutable_env_open_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_open_db_reply()->set_db_flags(
        ((Database *)db)->config().flags);
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_erase_db(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_assert(request != 0);
  ups_assert(request->has_env_erase_db_request());

  Environment *env = srv->get_env(request->env_erase_db_request().env_handle());

  ups_status_t st = ups_env_erase_db((ups_env_t *)env,
            request->env_erase_db_request().name(),
            request->env_erase_db_request().flags());

  Protocol reply(Protocol::ENV_ERASE_DB_REPLY);
  reply.mutable_env_erase_db_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_close(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_db_close_request());

  Database *db = srv->get_db(request->db_close_request().db_handle());
  if (!db) {
    /* accept this - most likely the database was already closed by
     * another process */
    st = 0;
    srv->remove_db_handle(request->db_close_request().db_handle());
  }
  else {
    st = ups_db_close((ups_db_t *)db, request->db_close_request().flags());
    if (st == 0)
      srv->remove_db_handle(request->db_close_request().db_handle());
  }

  Protocol reply(Protocol::DB_CLOSE_REPLY);
  reply.mutable_db_close_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_get_parameters(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_status_t st = 0;
  ups_parameter_t params[100]; /* 100 should be enough... */

  ups_assert(request != 0);
  ups_assert(request->has_db_get_parameters_request());

  /* initialize the ups_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (uint32_t i = 0;
      i < (uint32_t)request->db_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_db_get_parameters_request()->mutable_names()->mutable_data()[i];

  /* and request the parameters from the Database */
  Database *db = srv->get_db(request->db_get_parameters_request().db_handle());
  if (!db)
    st = UPS_INV_PARAMETER;
  else
    st = ups_db_get_parameters((ups_db_t *)db, &params[0]);

  Protocol reply(Protocol::DB_GET_PARAMETERS_REPLY);
  reply.mutable_db_get_parameters_reply()->set_status(st);

  if (st) {
    send_wrapper(srv, tcp, &reply);
    return;
  }

  /* initialize the reply package */
  for (uint32_t i = 0;
      i < (uint32_t)request->db_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case 0:
      continue;
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

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_check_integrity(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_db_check_integrity_request());

  Database *db = 0;

  uint32_t flags = request->db_check_integrity_request().flags();

  if (st == 0) {
    db = srv->get_db(request->db_check_integrity_request().db_handle());
    if (!db)
      st = UPS_INV_PARAMETER;
    else
      st = ups_db_check_integrity((ups_db_t *)db, flags);
  }

  Protocol reply(Protocol::DB_CHECK_INTEGRITY_REPLY);
  reply.mutable_db_check_integrity_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_count(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_status_t st = 0;
  uint64_t keycount;

  ups_assert(request != 0);
  ups_assert(request->has_db_count_request());

  Transaction *txn = 0;
  Database *db = 0;
  
  if (request->db_count_request().txn_handle()) {
    txn = srv->get_txn(request->db_count_request().txn_handle());
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
   db = srv->get_db(request->db_count_request().db_handle());
    if (!db)
      st = UPS_INV_PARAMETER;
    else
      st = ups_db_get_key_count((ups_db_t *)db, (ups_txn_t *)txn,
                request->db_count_request().distinct(), &keycount);
  }

  Protocol reply(Protocol::DB_GET_KEY_COUNT_REPLY);
  reply.mutable_db_count_reply()->set_status(st);
  if (st == 0)
    reply.mutable_db_count_reply()->set_keycount(keycount);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_count(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint64_t keycount;

  Transaction *txn = 0;
  Database *db = 0;
  
  if (request->db_count_request.txn_handle) {
    txn = srv->get_txn(request->db_count_request.txn_handle);
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
   db = srv->get_db(request->db_count_request.db_handle);
    if (!db)
      st = UPS_INV_PARAMETER;
    else
      st = ups_db_get_key_count((ups_db_t *)db, (ups_txn_t *)txn,
                request->db_count_request.distinct, &keycount);
  }

  SerializedWrapper reply;
  reply.id = kDbGetKeyCountReply;
  reply.db_count_reply.status = st;
  reply.db_count_reply.keycount = keycount;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_insert(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_status_t st = 0;
  bool send_key = false;
  ups_key_t key = {0};
  ups_record_t rec = {0};

  ups_assert(request != 0);
  ups_assert(request->has_db_insert_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_insert_request().txn_handle()) {
    txn = srv->get_txn(request->db_insert_request().txn_handle());
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_insert_request().db_handle());
    if (!db)
      st = UPS_INV_PARAMETER;
    else {
      if (request->db_insert_request().has_key()) {
        key.size = (uint16_t)request->db_insert_request().key().data().size();
        if (key.size)
          key.data = (void *)&request->db_insert_request().key().data()[0];
        key.flags = request->db_insert_request().key().flags()
                    & (~UPS_KEY_USER_ALLOC);
      }

      if (request->db_insert_request().has_record()) {
        rec.size = (uint32_t)request->db_insert_request().record().data().size();
        if (rec.size)
          rec.data = (void *)&request->db_insert_request().record().data()[0];
        rec.partial_size = request->db_insert_request().record().partial_size();
        rec.partial_offset = request->db_insert_request().record().partial_offset();
        rec.flags = request->db_insert_request().record().flags()
                    & (~UPS_RECORD_USER_ALLOC);
      }
      st = ups_db_insert((ups_db_t *)db, (ups_txn_t *)txn, &key, &rec,
                    request->db_insert_request().flags());

      /* recno: return the modified key */
      if ((st == 0)
          && (((Database *)db)->get_flags()
                  & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
        send_key = true;
      }
    }
  }

  Protocol reply(Protocol::DB_INSERT_REPLY);
  reply.mutable_db_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_db_insert_reply()->mutable_key(), &key);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_insert(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;
  bool send_key = false;
  ups_key_t key = {0};
  ups_record_t rec = {0};

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_insert_request.txn_handle) {
    txn = srv->get_txn(request->db_insert_request.txn_handle);
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_insert_request.db_handle);
    if (!db)
      st = UPS_INV_PARAMETER;
    else {
      if (request->db_insert_request.has_key) {
        key.size = (uint16_t)request->db_insert_request.key.data.size;
        if (key.size)
          key.data = (void *)request->db_insert_request.key.data.value;
        key.flags = request->db_insert_request.key.flags
                        & (~UPS_KEY_USER_ALLOC);
      }

      if (request->db_insert_request.has_record) {
        rec.size = (uint32_t)request->db_insert_request.record.data.size;
        if (rec.size)
          rec.data = (void *)request->db_insert_request.record.data.value;
        rec.partial_size = request->db_insert_request.record.partial_size;
        rec.partial_offset = request->db_insert_request.record.partial_offset;
        rec.flags = request->db_insert_request.record.flags
                        & (~UPS_RECORD_USER_ALLOC);
      }
      st = ups_db_insert((ups_db_t *)db, (ups_txn_t *)txn, &key, &rec,
                    request->db_insert_request.flags);

      /* recno: return the modified key */
      if ((st == 0)
          && (((Database *)db)->get_flags()
                  & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
        send_key = true;
      }
    }
  }

  SerializedWrapper reply;
  reply.id = kDbInsertReply;
  reply.db_insert_reply.status = st;
  if (send_key) {
    reply.db_insert_reply.has_key = true;
    reply.db_insert_reply.key.has_data = true;
    reply.db_insert_reply.key.data.size = key.size;
    reply.db_insert_reply.key.data.value = (uint8_t *)key.data;
    reply.db_insert_reply.key.flags = key.flags;
    reply.db_insert_reply.key.intflags = key._flags;
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_find(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ups_status_t st = 0;
  ups_key_t key = {0};
  ups_record_t rec = {0};
  bool send_key = false;

  ups_assert(request != 0);
  ups_assert(request->has_db_find_request());

  Transaction *txn = 0;
  Database *db = 0;
  Cursor *cursor = 0;

  if (request->db_find_request().txn_handle()) {
    txn = srv->get_txn(request->db_find_request().txn_handle());
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request().cursor_handle()) {
    cursor = srv->get_cursor(request->db_find_request().cursor_handle());
    if (!cursor)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request().db_handle()) {
    db = srv->get_db(request->db_find_request().db_handle());
    if (!db)
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
      rec.partial_size = request->db_find_request().record().partial_size();
      rec.partial_offset = request->db_find_request().record().partial_offset();
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
  if (send_key)
    Protocol::assign_key(reply.mutable_db_find_reply()->mutable_key(), &key);
  Protocol::assign_record(reply.mutable_db_find_reply()->mutable_record(),
                &rec);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_find(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;
  ups_key_t key = {0};
  ups_record_t rec = {0};
  bool send_key = false;

  Transaction *txn = 0;
  Database *db = 0;
  Cursor *cursor = 0;

  if (request->db_find_request.txn_handle) {
    txn = srv->get_txn(request->db_find_request.txn_handle);
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request.cursor_handle) {
    cursor = srv->get_cursor(request->db_find_request.cursor_handle);
    if (!cursor)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0 && request->db_find_request.db_handle) {
    db = srv->get_db(request->db_find_request.db_handle);
    if (!db)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    key.data = (void *)request->db_find_request.key.data.value;
    key.size = (uint16_t)request->db_find_request.key.data.size;
    key.flags = request->db_find_request.key.flags
                  & (~UPS_KEY_USER_ALLOC);

    if (request->db_find_request.has_record) {
      rec.data = (void *)request->db_find_request.record.data.value;
      rec.size = (uint32_t)request->db_find_request.record.data.size;
      rec.partial_size = request->db_find_request.record.partial_size;
      rec.partial_offset = request->db_find_request.record.partial_offset;
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
  reply.db_find_reply.record.partial_offset = rec.partial_offset;
  reply.db_find_reply.record.partial_size = rec.partial_size;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_erase(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_db_erase_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_erase_request().txn_handle()) {
    txn = srv->get_txn(request->db_erase_request().txn_handle());
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_erase_request().db_handle());
    if (!db)
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

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_erase(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;
  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_erase_request.txn_handle) {
    txn = srv->get_txn(request->db_erase_request.txn_handle);
    if (!txn)
      st = UPS_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_erase_request.db_handle);
    if (!db)
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
  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_begin(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_txn_t *txn;
  ups_status_t st = 0;
  uint64_t txn_handle = 0;

  ups_assert(request != 0);
  ups_assert(request->has_txn_begin_request());

  Environment *env = srv->get_env(request->txn_begin_request().env_handle());

  st = ups_txn_begin(&txn, (ups_env_t *)env,
            request->txn_begin_request().has_name()
              ? request->txn_begin_request().name().c_str()
              : 0,
            0, request->txn_begin_request().flags());

  if (st == 0)
    txn_handle = srv->allocate_handle((Transaction *)txn);

  Protocol reply(Protocol::TXN_BEGIN_REPLY);
  reply.mutable_txn_begin_reply()->set_status(st);
  reply.mutable_txn_begin_reply()->set_txn_handle(txn_handle);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_begin(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_txn_t *txn;
  ups_status_t st = 0;
  uint64_t txn_handle = 0;

  Environment *env = srv->get_env(request->txn_begin_request.env_handle);

  st = ups_txn_begin(&txn, (ups_env_t *)env,
            (const char *)request->txn_begin_request.name.value,
            0, request->txn_begin_request.flags);

  if (st == 0)
    txn_handle = srv->allocate_handle((Transaction *)txn);

  SerializedWrapper reply;
  reply.id = kTxnBeginReply;
  reply.txn_begin_reply.status = st;
  reply.txn_begin_reply.txn_handle = txn_handle;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_commit(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_txn_commit_request());

  Transaction *txn = srv->get_txn(request->txn_commit_request().txn_handle());
  if (!txn) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_commit((ups_txn_t *)txn,
            request->txn_commit_request().flags());
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      srv->remove_txn_handle(request->txn_commit_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_COMMIT_REPLY);
  reply.mutable_txn_commit_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_commit(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;

  Transaction *txn = srv->get_txn(request->txn_commit_request.txn_handle);
  if (!txn) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_commit((ups_txn_t *)txn, request->txn_commit_request.flags);
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      srv->remove_txn_handle(request->txn_commit_request.txn_handle);
    }
  }

  SerializedWrapper reply;
  reply.id = kTxnCommitReply;
  reply.txn_commit_reply.status = st;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_abort(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_txn_abort_request());

  Transaction *txn = srv->get_txn(request->txn_abort_request().txn_handle());
  if (!txn) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_abort((ups_txn_t *)txn,
            request->txn_abort_request().flags());
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      srv->remove_txn_handle(request->txn_abort_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_ABORT_REPLY);
  reply.mutable_txn_abort_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_abort(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;

  Transaction *txn = srv->get_txn(request->txn_abort_request.txn_handle);
  if (!txn) {
    st = UPS_INV_PARAMETER;
  }
  else {
    st = ups_txn_abort((ups_txn_t *)txn, request->txn_abort_request.flags);
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      srv->remove_txn_handle(request->txn_abort_request.txn_handle);
    }
  }

  SerializedWrapper reply;
  reply.id = kTxnAbortReply;
  reply.txn_abort_reply.status = st;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_create(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_cursor_t *cursor;
  ups_status_t st = 0;
  uint64_t handle = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_create_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->cursor_create_request().txn_handle()) {
    txn = srv->get_txn(request->cursor_create_request().txn_handle());
    if (!txn) {
      st = UPS_INV_PARAMETER;
      goto bail;
    }
  }

  db = srv->get_db(request->cursor_create_request().db_handle());
  if (!db) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ups_cursor_create(&cursor, (ups_db_t *)db, (ups_txn_t *)txn,
        request->cursor_create_request().flags());

  if (st == 0) {
    /* allocate a new handle in the Env wrapper structure */
    handle = srv->allocate_handle((Cursor *)cursor);
  }

bail:
  Protocol reply(Protocol::CURSOR_CREATE_REPLY);
  reply.mutable_cursor_create_reply()->set_status(st);
  reply.mutable_cursor_create_reply()->set_cursor_handle(handle);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_create(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_cursor_t *cursor;
  ups_status_t st = 0;
  uint64_t handle = 0;
  Transaction *txn = 0;
  Database *db = 0;

  if (request->cursor_create_request.txn_handle) {
    txn = srv->get_txn(request->cursor_create_request.txn_handle);
    if (!txn) {
      st = UPS_INV_PARAMETER;
      goto bail;
    }
  }

  db = srv->get_db(request->cursor_create_request.db_handle);
  if (!db) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ups_cursor_create(&cursor, (ups_db_t *)db, (ups_txn_t *)txn,
            request->cursor_create_request.flags);

  if (st == 0) {
    /* allocate a new handle in the Env wrapper structure */
    handle = srv->allocate_handle((Cursor *)cursor);
  }

bail:
  SerializedWrapper reply;
  reply.id = kCursorCreateReply;
  reply.cursor_create_reply.status = st;
  reply.cursor_create_reply.cursor_handle = handle;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_clone(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_cursor_t *dest;
  ups_status_t st = 0;
  uint64_t handle = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_clone_request());

  Cursor *src = srv->get_cursor(request->cursor_clone_request().cursor_handle());
  if (!src)
    st = UPS_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ups_cursor_clone((ups_cursor_t *)src, &dest);
    if (st == 0) {
      /* allocate a new handle in the Env wrapper structure */
      handle = srv->allocate_handle((Cursor *)dest);
    }
  }

  Protocol reply(Protocol::CURSOR_CLONE_REPLY);
  reply.mutable_cursor_clone_reply()->set_status(st);
  reply.mutable_cursor_clone_reply()->set_cursor_handle(handle);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_clone(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_cursor_t *dest;
  ups_status_t st = 0;
  uint64_t handle = 0;

  Cursor *src = srv->get_cursor(request->cursor_clone_request.cursor_handle);
  if (!src)
    st = UPS_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ups_cursor_clone((ups_cursor_t *)src, &dest);
    if (st == 0) {
      /* allocate a new handle in the Env wrapper structure */
      handle = srv->allocate_handle((Cursor *)dest);
    }
  }

  SerializedWrapper reply;
  reply.id = kCursorCloneReply;
  reply.cursor_clone_reply.status = st;
  reply.cursor_clone_reply.cursor_handle = handle;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_insert(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_insert_request());

  Cursor *cursor = srv->get_cursor(request->cursor_insert_request().cursor_handle());
  if (!cursor) {
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

  if (request->cursor_insert_request().has_record()) {
    rec.size = (uint32_t)request->cursor_insert_request().record().data().size();
    if (rec.size)
      rec.data = (void *)&request->cursor_insert_request().record().data()[0];
    rec.partial_size = request->cursor_insert_request().record().partial_size();
    rec.partial_offset = request->cursor_insert_request().record().partial_offset();
    rec.flags = request->cursor_insert_request().record().flags()
                & (~UPS_RECORD_USER_ALLOC);
  }

  send_key = request->cursor_insert_request().send_key();

  st = ups_cursor_insert((ups_cursor_t *)cursor, &key, &rec,
            request->cursor_insert_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_INSERT_REPLY);
  reply.mutable_cursor_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_cursor_insert_reply()->mutable_key(),
                &key);
  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_insert(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key;

  Cursor *cursor = srv->get_cursor(request->cursor_insert_request.cursor_handle);
  if (!cursor) {
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

  if (request->cursor_insert_request.has_record) {
    rec.size = (uint32_t)request->cursor_insert_request.record.data.size;
    if (rec.size)
      rec.data = request->cursor_insert_request.record.data.value;
    rec.partial_size = request->cursor_insert_request.record.partial_size;
    rec.partial_offset = request->cursor_insert_request.record.partial_offset;
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
  if (send_key) {
    reply.cursor_insert_reply.has_key = true;
    reply.cursor_insert_reply.key.has_data = true;
    reply.cursor_insert_reply.key.data.size = key.size;
    reply.cursor_insert_reply.key.data.value = (uint8_t *)key.data;
    reply.cursor_insert_reply.key.flags = key.flags;
    reply.cursor_insert_reply.key.intflags = key._flags;
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_erase(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_erase_request());

  Cursor *cursor = srv->get_cursor(request->cursor_erase_request().cursor_handle());
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_erase((ups_cursor_t *)cursor,
            request->cursor_erase_request().flags());

  Protocol reply(Protocol::CURSOR_ERASE_REPLY);
  reply.mutable_cursor_erase_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_erase(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_erase_request.cursor_handle);
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_erase((ups_cursor_t *)cursor,
                request->cursor_erase_request.flags);

  SerializedWrapper reply;
  reply.id = kCursorEraseReply;
  reply.cursor_erase_reply.status = st;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_record_count(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ups_status_t st = 0;
  uint32_t count = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_get_record_count_request());

  Cursor *cursor = srv->get_cursor(request->cursor_get_record_count_request().cursor_handle());
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_count((ups_cursor_t *)cursor, &count,
            request->cursor_get_record_count_request().flags());

  Protocol reply(Protocol::CURSOR_GET_RECORD_COUNT_REPLY);
  reply.mutable_cursor_get_record_count_reply()->set_status(st);
  reply.mutable_cursor_get_record_count_reply()->set_count(count);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_record_count(ServerContext *srv, uv_stream_t *tcp,
            SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint32_t count = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_get_record_count_request.cursor_handle);
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_count((ups_cursor_t *)cursor, &count,
            request->cursor_get_record_count_request.flags);

  SerializedWrapper reply;
  reply.id = kCursorGetRecordCountReply;
  reply.cursor_get_record_count_reply.status = st;
  reply.cursor_get_record_count_reply.count = count;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_record_size(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ups_status_t st = 0;
  uint64_t size = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_get_record_size_request());

  Cursor *cursor = srv->get_cursor(request->cursor_get_record_size_request().cursor_handle());
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_record_size((ups_cursor_t *)cursor, &size);

  Protocol reply(Protocol::CURSOR_GET_RECORD_SIZE_REPLY);
  reply.mutable_cursor_get_record_size_reply()->set_status(st);
  reply.mutable_cursor_get_record_size_reply()->set_size(size);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_record_size(ServerContext *srv, uv_stream_t *tcp,
            SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint64_t size = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_get_record_size_request.cursor_handle);
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_record_size((ups_cursor_t *)cursor, &size);

  SerializedWrapper reply;
  reply.id = kCursorGetRecordSizeReply;
  reply.cursor_get_record_size_reply.status = st;
  reply.cursor_get_record_size_reply.size = size;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_duplicate_position(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ups_status_t st = 0;
  uint32_t position = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_get_duplicate_position_request());

  Cursor *cursor = srv->get_cursor(request->cursor_get_duplicate_position_request().cursor_handle());
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_position((ups_cursor_t *)cursor, &position);

  Protocol reply(Protocol::CURSOR_GET_DUPLICATE_POSITION_REPLY);
  reply.mutable_cursor_get_duplicate_position_reply()->set_status(st);
  reply.mutable_cursor_get_duplicate_position_reply()->set_position(position);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_duplicate_position(ServerContext *srv, uv_stream_t *tcp,
            SerializedWrapper *request)
{
  ups_status_t st = 0;
  uint32_t position = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_get_duplicate_position_request.cursor_handle);
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_get_duplicate_position((ups_cursor_t *)cursor, &position);

  SerializedWrapper reply;
  reply.id = kCursorGetDuplicatePositionReply;
  reply.cursor_get_duplicate_position_reply.status = st;
  reply.cursor_get_duplicate_position_reply.position = position;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_overwrite(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ups_record_t rec = {0};
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_overwrite_request());

  Cursor *cursor = srv->get_cursor(request->cursor_overwrite_request().cursor_handle());
  if (!cursor) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  rec.data = (void *)&request->cursor_overwrite_request().record().data()[0];
  rec.size = (uint32_t)request->cursor_overwrite_request().record().data().size();
  rec.partial_size = request->cursor_overwrite_request().record().partial_size();
  rec.partial_offset = request->cursor_overwrite_request().record().partial_offset();
  rec.flags = request->cursor_overwrite_request().record().flags()
              & (~UPS_RECORD_USER_ALLOC);

  st = ups_cursor_overwrite((ups_cursor_t *)cursor, &rec,
            request->cursor_overwrite_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_OVERWRITE_REPLY);
  reply.mutable_cursor_overwrite_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_overwrite(ServerContext *srv, uv_stream_t *tcp,
            SerializedWrapper *request)
{
  ups_record_t rec = {0};
  ups_status_t st = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_overwrite_request.cursor_handle);
  if (!cursor) {
    st = UPS_INV_PARAMETER;
    goto bail;
  }

  rec.data = request->cursor_overwrite_request.record.data.value;
  rec.size = (uint32_t)request->cursor_overwrite_request.record.data.size;
  rec.partial_size = request->cursor_overwrite_request.record.partial_size;
  rec.partial_offset = request->cursor_overwrite_request.record.partial_offset;
  rec.flags = request->cursor_overwrite_request.record.flags
              & (~UPS_RECORD_USER_ALLOC);

  st = ups_cursor_overwrite((ups_cursor_t *)cursor, &rec,
            request->cursor_overwrite_request.flags);

bail:
  SerializedWrapper reply;
  reply.id = kCursorOverwriteReply;
  reply.cursor_overwrite_reply.status = st;

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_move(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_key_t key = {0};
  ups_record_t rec = {0};
  ups_status_t st = 0;
  bool send_key = false;
  bool send_rec = false;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_move_request());

  Cursor *cursor = srv->get_cursor(request->cursor_move_request().cursor_handle());
  if (!cursor) {
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
    rec.partial_size = request->cursor_move_request().record().partial_size();
    rec.partial_offset = request->cursor_move_request().record().partial_offset();
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
  if (send_key)
    Protocol::assign_key(reply.mutable_cursor_move_reply()->mutable_key(),
                    &key);
  if (send_rec)
    Protocol::assign_record(reply.mutable_cursor_move_reply()->mutable_record(),
                    &rec);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_close(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ups_status_t st = 0;

  ups_assert(request != 0);
  ups_assert(request->has_cursor_close_request());

  Cursor *cursor = srv->get_cursor(request->cursor_close_request().cursor_handle());
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_close((ups_cursor_t *)cursor);

  if (st==0) {
    /* remove the handle from the Env wrapper structure */
    srv->remove_cursor_handle(request->cursor_close_request().cursor_handle());
  }

  Protocol reply(Protocol::CURSOR_CLOSE_REPLY);
  reply.mutable_cursor_close_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_close(ServerContext *srv, uv_stream_t *tcp,
                SerializedWrapper *request)
{
  ups_status_t st = 0;

  Cursor *cursor = srv->get_cursor(request->cursor_close_request.cursor_handle);
  if (!cursor)
    st = UPS_INV_PARAMETER;
  else
    st = ups_cursor_close((ups_cursor_t *)cursor);

  if (st==0) {
    /* remove the handle from the Env wrapper structure */
    srv->remove_cursor_handle(request->cursor_close_request.cursor_handle);
  }

  SerializedWrapper reply;
  reply.id = kCursorCloseReply;
  reply.cursor_close_reply.status = st;

  send_wrapper(srv, tcp, &reply);
}

// returns false if client should be closed, otherwise true
static bool
dispatch(ServerContext *srv, uv_stream_t *tcp, uint32_t magic,
                uint8_t *data, uint32_t size)
{
  if (magic == UPS_TRANSFER_MAGIC_V2) {
    SerializedWrapper request;
    int size_left = (int)size;
    request.deserialize(&data, &size_left);
    ups_assert(size_left == 0);

    switch (request.id) {
      case kDbInsertRequest:
        handle_db_insert(srv, tcp, &request);
        break;
      case kDbEraseRequest:
        handle_db_erase(srv, tcp, &request);
        break;
      case kDbFindRequest:
        handle_db_find(srv, tcp, &request);
        break;
      case kDbGetKeyCountRequest:
        handle_db_count(srv, tcp, &request);
        break;
      case kCursorCreateRequest:
        handle_cursor_create(srv, tcp, &request);
        break;
      case kCursorCloneRequest:
        handle_cursor_clone(srv, tcp, &request);
        break;
      case kCursorCloseRequest:
        handle_cursor_close(srv, tcp, &request);
        break;
      case kCursorInsertRequest:
        handle_cursor_insert(srv, tcp, &request);
        break;
      case kCursorEraseRequest:
        handle_cursor_erase(srv, tcp, &request);
        break;
      case kCursorGetRecordCountRequest:
        handle_cursor_get_record_count(srv, tcp, &request);
        break;
      case kCursorGetRecordSizeRequest:
        handle_cursor_get_record_size(srv, tcp, &request);
        break;
      case kCursorGetDuplicatePositionRequest:
        handle_cursor_get_duplicate_position(srv, tcp, &request);
        break;
      case kCursorOverwriteRequest:
        handle_cursor_overwrite(srv, tcp, &request);
        break;
      case kTxnBeginRequest:
        handle_txn_begin(srv, tcp, &request);
        break;
      case kTxnAbortRequest:
        handle_txn_abort(srv, tcp, &request);
        break;
      case kTxnCommitRequest:
        handle_txn_commit(srv, tcp, &request);
        break;
      default:
        ups_trace(("ignoring unknown request"));
        break;
    }
    return (true);
  }

  // Protocol buffer requests are handled here
  Protocol *wrapper = Protocol::unpack(data, size);
  if (!wrapper) {
    ups_trace(("failed to unpack wrapper (%d bytes)\n", size));
    return (false);
  }

  switch (wrapper->type()) {
    case ProtoWrapper_Type_CONNECT_REQUEST:
      handle_connect(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DISCONNECT_REQUEST:
      handle_disconnect(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_GET_PARAMETERS_REQUEST:
      handle_env_get_parameters(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_GET_DATABASE_NAMES_REQUEST:
      handle_env_get_database_names(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_FLUSH_REQUEST:
      handle_env_flush(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_RENAME_REQUEST:
      handle_env_rename(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_CREATE_DB_REQUEST:
      handle_env_create_db(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_OPEN_DB_REQUEST:
      handle_env_open_db(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_ENV_ERASE_DB_REQUEST:
      handle_env_erase_db(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_CLOSE_REQUEST:
      handle_db_close(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_GET_PARAMETERS_REQUEST:
      handle_db_get_parameters(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_CHECK_INTEGRITY_REQUEST:
      handle_db_check_integrity(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_GET_KEY_COUNT_REQUEST:
      handle_db_count(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_INSERT_REQUEST:
      handle_db_insert(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_FIND_REQUEST:
      handle_db_find(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_DB_ERASE_REQUEST:
      handle_db_erase(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_TXN_BEGIN_REQUEST:
      handle_txn_begin(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_TXN_COMMIT_REQUEST:
      handle_txn_commit(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_TXN_ABORT_REQUEST:
      handle_txn_abort(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CREATE_REQUEST:
      handle_cursor_create(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CLONE_REQUEST:
      handle_cursor_clone(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_INSERT_REQUEST:
      handle_cursor_insert(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_ERASE_REQUEST:
      handle_cursor_erase(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_RECORD_COUNT_REQUEST:
      handle_cursor_get_record_count(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_RECORD_SIZE_REQUEST:
      handle_cursor_get_record_size(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_DUPLICATE_POSITION_REQUEST:
      handle_cursor_get_duplicate_position(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_OVERWRITE_REQUEST:
      handle_cursor_overwrite(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_MOVE_REQUEST:
      handle_cursor_move(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_CLOSE_REQUEST:
      handle_cursor_close(srv, tcp, wrapper);
      break;
    default:
      ups_trace(("ignoring unknown request"));
      break;
  }

  delete wrapper;

  return (true);
}

static void
on_close_connection(uv_handle_t *handle)
{
  delete (ClientContext *)handle->data;
  Memory::release(handle);
}

#if UV_VERSION_MINOR >= 11
static void
on_alloc_buffer(uv_handle_t *handle, size_t size, uv_buf_t *buf)
{
  buf->base = Memory::allocate<char>(size);
  buf->len = size;
}
#else
static uv_buf_t
on_alloc_buffer(uv_handle_t *handle, size_t size)
{
  return (uv_buf_init(Memory::allocate<char>(size), size));
}
#endif

// TODO
// this routine uses a ByteArray to allocate memory; should we directly use
// |buf| instead, and then re-use |buf| instead of releasing the memory?
static void
#if UV_VERSION_MINOR >= 11
on_read_data(uv_stream_t *tcp, ssize_t nread, const uv_buf_t *buf)
{
#else
on_read_data(uv_stream_t *tcp, ssize_t nread, uv_buf_t buf_struct)
{
  uv_buf_t *buf = &buf_struct;
#endif
  ups_assert(tcp != 0);
  uint32_t size = 0;
  uint32_t magic = 0;
  bool close_client = false;
  ClientContext *context = (ClientContext *)tcp->data;
  ByteArray *buffer = &context->buffer;

  // each request is prepended with a header:
  //   4 byte magic
  //   4 byte size  (without those 8 bytes)
  if (nread >= 0) {
    // if we already started buffering data: append the data to the buffer
    if (!buffer->is_empty()) {
      buffer->append((uint8_t *)buf->base, nread);

      // for each full package in the buffer...
      while (buffer->get_size() > 8) {
        uint8_t *p = (uint8_t *)buffer->get_ptr();
        magic = *(uint32_t *)(p + 0);
        size = *(uint32_t *)(p + 4);
        if (magic == UPS_TRANSFER_MAGIC_V1)
          size += 8;
        // still not enough data? then return immediately
        if (buffer->get_size() < size)
          goto bail;
        // otherwise dispatch the message
        close_client = !dispatch(context->srv, tcp, magic, p, size);
        // and move the remaining data to "the left"
        if (buffer->get_size() == size) {
          buffer->clear();
          goto bail;
        }
        else {
          uint32_t new_size = buffer->get_size() - size;
          memmove(p, p + size, new_size);
          buffer->set_size(new_size);
          // fall through and repeat the loop
        }
      }
      goto bail;
    }

    // we have not buffered data from previous calls; try to dispatch the
    // current network packet
    uint8_t *p = (uint8_t *)buf->base;
    while (p < (uint8_t *)buf->base + nread) {
      magic = *(uint32_t *)(p + 0);
      size = *(uint32_t *)(p + 4);
      if (magic == UPS_TRANSFER_MAGIC_V1)
        size += 8;
      if (size <= (uint32_t)nread) {
        close_client = !dispatch(context->srv, tcp, magic, p, size);
        if (close_client)
          goto bail;
        nread -= size;
        p += size;
        continue;
      }
      // not enough data? then cache it in the buffer
      else {
        ups_assert(buffer->is_empty());
        buffer->append(p, nread);
        goto bail;
      }
    }
  }

bail:
  if (close_client || nread < 0)
    uv_close((uv_handle_t *)tcp, on_close_connection);
  Memory::release(buf->base);
  //buf->base = 0;
}

static void
on_new_connection(uv_stream_t *server, int status)
{
  if (status == -1)
    return;

  ServerContext *srv = (ServerContext *)server->data;

  uv_tcp_t *client = Memory::allocate<uv_tcp_t>(sizeof(uv_tcp_t));
  client->data = new ClientContext(srv);

#if UV_VERSION_MINOR >= 11
  uv_tcp_init(&srv->loop, client);
#else
  uv_tcp_init(srv->loop, client);
#endif
  if (uv_accept(server, (uv_stream_t *)client) == 0)
    uv_read_start((uv_stream_t *)client, on_alloc_buffer, on_read_data);
  else
    uv_close((uv_handle_t *)client, on_close_connection);
}

static void
on_run_thread(void *loop)
{
  uv_run((uv_loop_t *)loop, UV_RUN_DEFAULT);
}

static void
#if UV_VERSION_PATCH <= 22
on_async_cb(uv_async_t *handle, int status)
#else
on_async_cb(uv_async_t *handle)
#endif
{
  ServerContext *srv = (ServerContext *)handle->data;

  // simply copy the Environments in the queue to the map of opened
  // Environments
  ScopedLock lock(srv->open_queue_mutex);
  for (EnvironmentMap::iterator it = srv->open_queue.begin();
          it != srv->open_queue.end(); it++) {
    srv->open_envs[it->first] = it->second;
  }
}

} // namespace hamsterdb

// global namespace is below

ups_status_t
ups_srv_init(ups_srv_config_t *config, ups_srv_t **psrv)
{
  ServerContext *srv = new ServerContext();
  struct sockaddr_in bind_addr;

#if UV_VERSION_MINOR >= 11
  uv_loop_init(&srv->loop);
  uv_tcp_init(&srv->loop, &srv->server);
  uv_ip4_addr("0.0.0.0", config->port, &bind_addr);
  uv_tcp_bind(&srv->server, (sockaddr *)&bind_addr, 0);
#else
  srv->loop = uv_loop_new();
  uv_tcp_init(srv->loop, &srv->server);
  bind_addr = uv_ip4_addr("0.0.0.0", config->port);
  uv_tcp_bind(&srv->server, bind_addr);
#endif

  srv->server.data = srv;
  int r = uv_listen((uv_stream_t *)&srv->server, 128,
            hamsterdb::on_new_connection);
  if (r) {
    ups_log(("failed to listen to port %d", config->port)); 
    return (UPS_IO_ERROR);
  }

  srv->async.data = srv;
#if UV_VERSION_MINOR >= 11
  uv_async_init(&srv->loop, &srv->async, on_async_cb);
  uv_thread_create(&srv->thread_id, on_run_thread, &srv->loop);
#else
  uv_async_init(srv->loop, &srv->async, on_async_cb);
  uv_thread_create(&srv->thread_id, on_run_thread, srv->loop);
#endif

  *psrv = (ups_srv_t *)srv;
  return (UPS_SUCCESS);
}

ups_status_t
ups_srv_add_env(ups_srv_t *hsrv, ups_env_t *env, const char *urlname)
{
  ServerContext *srv = (ServerContext *)hsrv;
  if (!srv || !env || !urlname) {
    ups_log(("parameters srv, env, urlname must not be NULL"));
    return (UPS_INV_PARAMETER);
  }

  {
    ScopedLock lock(srv->open_queue_mutex);
    srv->open_queue[urlname] = (Environment *)env;
  }

  uv_async_send(&srv->async);
  return (UPS_SUCCESS);
}

void
ups_srv_close(ups_srv_t *hsrv)
{
  ServerContext *srv = (ServerContext *)hsrv;
  if (!srv)
    return;

  uv_unref((uv_handle_t *)&srv->server);
  uv_unref((uv_handle_t *)&srv->async);

  // TODO clean up all allocated objects and handles

  /* stop the event loop */
#if UV_VERSION_MINOR >= 11
  uv_stop(&srv->loop);
#else
  uv_stop(srv->loop);
#endif
  uv_async_send(&srv->async);

  /* join the libuv thread */
  (void)uv_thread_join(&srv->thread_id);

  /* close the async handle and the server socket */
  uv_close((uv_handle_t *)&srv->async, 0);
  uv_close((uv_handle_t *)&srv->server, 0);

  /* clean up libuv */
#if UV_VERSION_MINOR >= 11
  uv_loop_close(&srv->loop);
#else
  uv_loop_delete(srv->loop);
#endif

  delete srv;

  /* free libprotocol static data */
  Protocol::shutdown();
}

