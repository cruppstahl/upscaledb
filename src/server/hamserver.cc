/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */


#include <stdio.h> /* needed for mongoose.h */
#include <malloc.h>
#include <string.h>

#include <mongoose/mongoose.h>

#include <ham/types.h>
#include <ham/hamsterdb_srv.h>
#include "../protocol/protocol.h"
#include "os.h"
#include "db.h"
#include "error.h"
#include "assert.h"
#include "cursor.h"
#include "mem.h"
#include "env.h"

namespace ham {

/* max. number of open hamsterdb Environments - if you change this, also change
 * MAX_CALLBACKS in 3rdparty/mongoose/mongoose.c! */
#define MAX_ENVIRONMENTS  128
#define MAX_DATABASES     512

static const char *standard_reply = "HTTP/1.1 200 OK\r\n"
                  "Content-Type: text/plain\r\n"
                  "Connection: close\r\n\r\n";

#define HANDLE_TYPE_DATABASE 1
#define HANDLE_TYPE_TRANSACTION 2
#define HANDLE_TYPE_CURSOR 3

typedef struct srv_handle_t
{
  void *ptr;
  int type;
  ham_u64_t handle;
} srv_handle_t;

struct env_t {
  ::ham_env_t *env;
  os_critsec_t cs;
  char *urlname;
  srv_handle_t *handles;
  ham_u32_t handles_ctr;
  ham_u32_t handles_size;
} env_t;

static ham_u64_t
__store_handle(struct env_t *envh, void *ptr, int type)
{
  unsigned i;
  ham_u64_t ret;

  for (i = 0; i < envh->handles_size; i++) {
    if (envh->handles[i].ptr == 0) {
      break;
    }
  }

  if (i == envh->handles_size) {
    envh->handles_size += 10;
    envh->handles=(srv_handle_t *)realloc(envh->handles,
            sizeof(srv_handle_t)*envh->handles_size);
    if (!envh->handles)
      return 0; /* not so nice, but if we're out of memory then
             * it does not make sense to go on... */
    memset(&envh->handles[envh->handles_size - 10], 0,
            sizeof(srv_handle_t) * 10);
  }

  ret = ++envh->handles_ctr;
  ret = ret << 32;

  envh->handles[i].ptr = ptr;
  envh->handles[i].handle = ret | i;
  envh->handles[i].type = type;

  return (envh->handles[i].handle);
}

static void *
__get_handle(struct env_t *envh, ham_u64_t handle)
{
  srv_handle_t *h = &envh->handles[handle & 0xffffffff];
  if (h->handle != handle)
    return (0);
  return h->ptr;
}

static void
__remove_handle(struct env_t *envh, ham_u64_t handle)
{
  srv_handle_t *h = &envh->handles[handle & 0xffffffff];
  ham_assert(h->handle == handle);
  if (h->handle != handle)
    return;
  memset(h, 0, sizeof(*h));
}

static void
send_wrapper(ham_env_t *henv, struct mg_connection *conn, Protocol *wrapper)
{
  ham_u8_t *data;
  ham_size_t data_size;
  Environment *env = (Environment *)henv;

  if (!wrapper->pack(env->get_allocator(), &data, &data_size))
    return;

  ham_trace(("type %u: sending %d bytes", wrapper->type(), data_size));
  mg_printf(conn, "%s", standard_reply);
  mg_write(conn, data, data_size);

  env->get_allocator()->free(data);
}

static void
handle_connect(ham_env_t *env, struct mg_connection *conn,
                const struct mg_request_info *ri, Protocol *request)
{
  ham_assert(request!=0);

  Protocol *reply = new Protocol(Protocol::CONNECT_REPLY);
  reply->mutable_connect_reply()->set_status(0);
  reply->mutable_connect_reply()->set_env_flags(
          ((Environment *)env)->get_flags());

  send_wrapper(env, conn, reply);
  delete reply;
}

static void
handle_env_get_parameters(ham_env_t *env, struct mg_connection *conn,
                const struct mg_request_info *ri, Protocol *request)
{
  ham_size_t i;
  ham_status_t st = 0;
  ham_parameter_t params[100]; /* 100 should be enough... */

  ham_assert(request != 0);
  ham_assert(request->has_env_get_parameters_request());

  /* initialize the ham_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (i = 0;
      i < (ham_size_t)request->env_get_parameters_request().names().size()
      && i < 100; i++)
    params[i].name = request->mutable_env_get_parameters_request()->mutable_names()->mutable_data()[i];

  Protocol reply(Protocol::ENV_GET_PARAMETERS_REPLY);

  /* and request the parameters from the Environment */
  st = ham_env_get_parameters(env, &params[0]);
  reply.mutable_env_get_parameters_reply()->set_status(st);
  if (st) {
    send_wrapper(env, conn, &reply);
    return;
  }

  /* initialize the reply package */
  for (i = 0;
      i < (ham_size_t)request->env_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case HAM_PARAM_CACHESIZE:
      reply.mutable_env_get_parameters_reply()->set_cachesize(
              (int)params[i].value);
      break;
    case HAM_PARAM_PAGESIZE:
      reply.mutable_env_get_parameters_reply()->set_pagesize(
              (int)params[i].value);
      break;
    case HAM_PARAM_MAX_DATABASES:
      reply.mutable_env_get_parameters_reply()->set_max_env_databases(
              (int)params[i].value);
      break;
    case HAM_PARAM_FLAGS:
      reply.mutable_env_get_parameters_reply()->set_flags(
              (int)params[i].value);
      break;
    case HAM_PARAM_FILEMODE:
      reply.mutable_env_get_parameters_reply()->set_filemode(
              (int)params[i].value);
      break;
    case HAM_PARAM_FILENAME:
      if (params[i].value)
        reply.mutable_env_get_parameters_reply()->set_filename(
              (const char *)(U64_TO_PTR(params[i].value)));
      break;
    default:
      ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
      break;
    }
  }

  send_wrapper(env, conn, &reply);
}

static void
handle_db_get_parameters(struct env_t *envh, struct mg_connection *conn,
                const struct mg_request_info *ri, Protocol *request)
{
  ham_env_t *env = envh->env;
  ham_db_t *db;
  ham_status_t st = 0;
  ham_parameter_t params[100]; /* 100 should be enough... */

  ham_assert(request != 0);
  ham_assert(request->has_db_get_parameters_request());

  /* initialize the ham_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (ham_size_t i = 0;
      i < (ham_size_t)request->db_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_db_get_parameters_request()->mutable_names()->mutable_data()[i];

  /* and request the parameters from the Environment */
  db = (ham_db_t *)__get_handle(envh,
                request->db_get_parameters_request().db_handle());
  if (!db)
    st = HAM_INV_PARAMETER;
  else
    st = ham_db_get_parameters((ham_db_t *)db, &params[0]);

  Protocol reply(Protocol::DB_GET_PARAMETERS_REPLY);
  reply.mutable_db_get_parameters_reply()->set_status(st);

  if (st) {
    send_wrapper(env, conn, &reply);
    return;
  }

  /* initialize the reply package */
  for (ham_size_t i = 0;
      i < (ham_size_t)request->db_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case 0:
      continue;
    case HAM_PARAM_FLAGS:
      reply.mutable_db_get_parameters_reply()->set_flags(
              (int)params[i].value);
      break;
    case HAM_PARAM_KEYSIZE:
      reply.mutable_db_get_parameters_reply()->set_keysize(
              (int)params[i].value);
      break;
    case HAM_PARAM_DATABASE_NAME:
      reply.mutable_db_get_parameters_reply()->set_dbname(
              (int)params[i].value);
      break;
    case HAM_PARAM_MAX_KEYS_PER_PAGE:
      reply.mutable_db_get_parameters_reply()->set_keys_per_page(
              (int)params[i].value);
      break;
    default:
      ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
      break;
    }
  }

  send_wrapper(env, conn, &reply);
}

static void
handle_env_get_database_names(ham_env_t *env, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_size_t num_names = 1024;
  ham_u16_t names[1024]; /* should be enough */

  ham_assert(request != 0);
  ham_assert(request->has_env_get_database_names_request());

  /* request the database names from the Environment */
  ham_status_t st = ham_env_get_database_names(env, &names[0], &num_names);

  Protocol reply(Protocol::ENV_GET_DATABASE_NAMES_REPLY);
  reply.mutable_env_get_database_names_reply()->set_status(st);
  for (ham_u32_t i = 0; i < num_names; i++)
    reply.mutable_env_get_database_names_reply()->add_names(names[i]);

  send_wrapper(env, conn, &reply);
}

static void
handle_env_flush(ham_env_t *env, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_assert(request != 0);
  ham_assert(request->has_env_flush_request());

  /* request the database names from the Environment */
  Protocol reply(Protocol::ENV_FLUSH_REPLY);
  reply.mutable_env_flush_reply()->set_status(ham_env_flush(env,
        request->env_flush_request().flags()));

  send_wrapper(env, conn, &reply);
}

static void
handle_env_rename(ham_env_t *env, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_assert(request != 0);
  ham_assert(request->has_env_rename_request());

  /* rename the databases */
  ham_status_t st = ham_env_rename_db(env,
          request->env_rename_request().oldname(),
          request->env_rename_request().newname(),
          request->env_rename_request().flags());

  Protocol reply(Protocol::ENV_RENAME_REPLY);
  reply.mutable_env_rename_reply()->set_status(st);
  send_wrapper(env, conn, &reply);
}

static void
handle_env_create_db(struct env_t *envh, ham_env_t *env,
            struct mg_connection *conn, const struct mg_request_info *ri,
            Protocol *request)
{
  ham_db_t *db;
  ham_status_t st = 0;
  ham_u64_t db_handle = 0;
  ham_parameter_t params[100] = {{0, 0}};

  ham_assert(request != 0);
  ham_assert(request->has_env_create_db_request());

  /* convert parameters */
  ham_assert(request->env_create_db_request().param_names().size() < 100);
  for (ham_size_t i = 0;
      i < (ham_size_t)request->env_create_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_create_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_create_db_request()->mutable_param_values()->data()[i];
  }

  /* create the database */
  st = ham_env_create_db(env, &db,
            request->env_create_db_request().dbname(),
            request->env_create_db_request().flags(), &params[0]);

  if (st == 0) {
    /* allocate a new database handle in the Env wrapper structure */
    db_handle = __store_handle(envh, db, HANDLE_TYPE_DATABASE);
  }

  Protocol reply(Protocol::ENV_CREATE_DB_REPLY);
  reply.mutable_env_create_db_reply()->set_status(st);
  if (db_handle) {
    reply.mutable_env_create_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_create_db_reply()->set_db_flags(
        ((Database *)db)->get_rt_flags(true));
  }

  send_wrapper(env, conn, &reply);
}

static void
handle_env_open_db(struct env_t *envh, ham_env_t *env,
            struct mg_connection *conn, const struct mg_request_info *ri,
            Protocol *request)
{
  ham_db_t *db = 0;
  ham_u64_t db_handle = 0;
  ham_status_t st = 0;
  ham_u16_t dbname = request->env_open_db_request().dbname();
  ham_parameter_t params[100] = {{0, 0}};

  ham_assert(request != 0);
  ham_assert(request->has_env_open_db_request());

  /* convert parameters */
  ham_assert(request->env_open_db_request().param_names().size() < 100);
  for (ham_size_t i = 0;
      i < (ham_size_t)request->env_open_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_open_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_open_db_request()->mutable_param_values()->data()[i];
  }

  /* check if the database is already open */
  for (ham_size_t i = 0; i < envh->handles_size; i++) {
    if (envh->handles[i].ptr != 0) {
      if (envh->handles[i].type == HANDLE_TYPE_DATABASE) {
        db = (ham_db_t *)envh->handles[i].ptr;
        if (((Database *)db)->get_name() == dbname) {
          db_handle = envh->handles[i].handle;
          break;
        }
        else
          db = 0;
      }
    }
  }

  /* if not found: open the database */
  if (!db) {
    st = ham_env_open_db(env, &db, dbname,
                request->env_open_db_request().flags(), &params[0]);

    if (st == 0) {
      /* allocate a new database handle in the Env wrapper structure */
      db_handle = __store_handle(envh, db, HANDLE_TYPE_DATABASE);
    }
  }

  Protocol reply(Protocol::ENV_OPEN_DB_REPLY);
  reply.mutable_env_open_db_reply()->set_status(st);
  if (st == 0) {
    reply.mutable_env_open_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_open_db_reply()->set_db_flags(
        ((Database *)db)->get_rt_flags(true));
  }

  send_wrapper(env, conn, &reply);
}

static void
handle_env_erase_db(ham_env_t *env, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_assert(request != 0);
  ham_assert(request->has_env_erase_db_request());

  ham_status_t st = ham_env_erase_db(env,
            request->env_erase_db_request().name(),
            request->env_erase_db_request().flags());

  Protocol reply(Protocol::ENV_ERASE_DB_REPLY);
  reply.mutable_env_erase_db_reply()->set_status(st);

  send_wrapper(env, conn, &reply);
}

static void
handle_db_close(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_db_t *db;
  ham_status_t st = 0;
  ham_env_t *env = envh->env;

  ham_assert(request != 0);
  ham_assert(request->has_db_close_request());

  db = (ham_db_t *)__get_handle(envh, request->db_close_request().db_handle());
  if (!db) {
    /* accept this - most likely the database was already closed by
     * another process */
    st = 0;
  }
  else {
    st = ham_db_close(db, request->db_close_request().flags());
    if (st == 0)
      __remove_handle(envh, request->db_close_request().db_handle());
  }

  Protocol reply(Protocol::DB_CLOSE_REPLY);
  reply.mutable_db_close_reply()->set_status(st);

  send_wrapper(env, conn, &reply);
}

static void
handle_txn_begin(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn;
  ham_status_t st = 0;
  ham_u64_t handle = 0;
  ham_env_t *env = envh->env;

  ham_assert(request != 0);
  ham_assert(request->has_txn_begin_request());

  st = ham_txn_begin(&txn, env, request->txn_begin_request().has_name()
              ? request->txn_begin_request().name().c_str()
              : 0,
            0, request->txn_begin_request().flags());

  if (st == 0)
    handle = __store_handle(envh, txn, HANDLE_TYPE_TRANSACTION);

  Protocol reply(Protocol::TXN_BEGIN_REPLY);
  reply.mutable_txn_begin_reply()->set_status(st);
  reply.mutable_txn_begin_reply()->set_txn_handle(handle);

  send_wrapper(env, conn, &reply);
}

static void
handle_txn_commit(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn;
  ham_env_t *env = envh->env;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_txn_commit_request());

  txn = (ham_txn_t *)__get_handle(envh,
            request->txn_commit_request().txn_handle());
  if (!txn) {
    st = HAM_INV_PARAMETER;
  }
  else {
    st = ham_txn_commit(txn, request->txn_commit_request().flags());
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      __remove_handle(envh,
            request->txn_commit_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_COMMIT_REPLY);
  reply.mutable_txn_commit_reply()->set_status(st);

  send_wrapper(env, conn, &reply);
}

static void
handle_txn_abort(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn;
  ham_status_t st = 0;
  ham_env_t *env = envh->env;

  ham_assert(request != 0);
  ham_assert(request->has_txn_abort_request());

  txn = (ham_txn_t *)__get_handle(envh,
            request->txn_abort_request().txn_handle());
  if (!txn) {
    st = HAM_INV_PARAMETER;
  }
  else {
    st = ham_txn_abort(txn, request->txn_abort_request().flags());
    if (st == 0) {
      /* remove the handle from the Env wrapper structure */
      __remove_handle(envh, request->txn_abort_request().txn_handle());
    }
  }

  Protocol reply(Protocol::TXN_ABORT_REPLY);
  reply.mutable_txn_abort_reply()->set_status(st);

  send_wrapper(env, conn, &reply);
}

static void
handle_db_check_integrity(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_db_check_integrity_request());

  if (request->db_check_integrity_request().txn_handle()) {
    txn=(ham_txn_t *)__get_handle(envh,
            request->db_check_integrity_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = (ham_db_t *)__get_handle(envh,
            request->db_check_integrity_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else
      st = ham_check_integrity(db, txn);
  }

  Protocol reply(Protocol::DB_CHECK_INTEGRITY_REPLY);
  reply.mutable_db_check_integrity_reply()->set_status(st);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_db_get_key_count(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_status_t st = 0;
  ham_u64_t keycount;

  ham_assert(request!=0);
  ham_assert(request->has_db_get_key_count_request());

  if (request->db_get_key_count_request().txn_handle()) {
    txn = (ham_txn_t *)__get_handle(envh,
            request->db_get_key_count_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = (ham_db_t *)__get_handle(envh,
                request->db_get_key_count_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else
      st = ham_db_get_key_count(db, txn,
                request->db_get_key_count_request().flags(), &keycount);
  }

  Protocol reply(Protocol::DB_GET_KEY_COUNT_REPLY);
  reply.mutable_db_get_key_count_reply()->set_status(st);
  if (st == 0)
    reply.mutable_db_get_key_count_reply()->set_keycount(keycount);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_db_insert(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_status_t st = 0;
  ham_bool_t send_key = HAM_FALSE;
  ham_key_t key;
  ham_record_t rec;

  ham_assert(request != 0);
  ham_assert(request->has_db_insert_request());

  if (request->db_insert_request().txn_handle()) {
    txn = (ham_txn_t *)__get_handle(envh,
            request->db_insert_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = (ham_db_t *)__get_handle(envh,
        request->db_insert_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      memset(&key, 0, sizeof(key));
      if (request->db_insert_request().has_key()) {
        key.size = request->db_insert_request().key().data().size();
        if (key.size)
          key.data = (void *)&request->db_insert_request().key().data()[0];
        key.flags = request->db_insert_request().key().flags()
                    & (~HAM_KEY_USER_ALLOC);
      }

      memset(&rec, 0, sizeof(rec));
      if (request->db_insert_request().has_record()) {
        rec.size = request->db_insert_request().record().data().size();
        if (rec.size)
          rec.data = (void *)&request->db_insert_request().record().data()[0];
        rec.partial_size = request->db_insert_request().record().partial_size();
        rec.partial_offset = request->db_insert_request().record().partial_offset();
        rec.flags = request->db_insert_request().record().flags()
                    & (~HAM_RECORD_USER_ALLOC);
      }
      st = ham_db_insert(db, txn, &key, &rec,
                    request->db_insert_request().flags());

      /* recno: return the modified key */
      if ((st == 0)
          && (((Database *)db)->get_rt_flags(true) & HAM_RECORD_NUMBER)) {
        ham_assert(key.size == sizeof(ham_offset_t));
        send_key = HAM_TRUE;
      }
    }
  }

  Protocol reply(Protocol::DB_INSERT_REPLY);
  reply.mutable_db_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_db_insert_reply()->mutable_key(), &key);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_db_find(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_status_t st = 0;
  ham_key_t key;
  ham_record_t rec;
  ham_bool_t send_key = HAM_FALSE;

  ham_assert(request != 0);
  ham_assert(request->has_db_find_request());

  if (request->db_find_request().txn_handle()) {
    txn = (ham_txn_t *)__get_handle(envh,
                request->db_find_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = (ham_db_t *)__get_handle(envh,
                request->db_find_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      memset(&key, 0, sizeof(key));
      key.data = (void *)&request->db_find_request().key().data()[0];
      key.size = request->db_find_request().key().data().size();
      key.flags = request->db_find_request().key().flags()
                  & (~HAM_KEY_USER_ALLOC);

      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)&request->db_find_request().record().data()[0];
      rec.size = request->db_find_request().record().data().size();
      rec.partial_size = request->db_find_request().record().partial_size();
      rec.partial_offset = request->db_find_request().record().partial_offset();
      rec.flags = request->db_find_request().record().flags()
                  & (~HAM_RECORD_USER_ALLOC);

      st = ham_db_find(db, txn, &key, &rec, request->db_find_request().flags());
      if (st == 0) {
        /* approx matching: key->_flags was modified! */
        if (key._flags)
          send_key = HAM_TRUE;
      }
    }
  }

  Protocol reply(Protocol::DB_FIND_REPLY);
  reply.mutable_db_find_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_db_find_reply()->mutable_key(), &key);
  Protocol::assign_record(reply.mutable_db_find_reply()->mutable_record(),
                &rec);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_db_erase(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_db_erase_request());

  if (request->db_erase_request().txn_handle()) {
    txn = (ham_txn_t *)__get_handle(envh,
                request->db_erase_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = (ham_db_t *)__get_handle(envh,
                request->db_erase_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      ham_key_t key;

      memset(&key, 0, sizeof(key));
      key.data = (void *)&request->db_erase_request().key().data()[0];
      key.size = request->db_erase_request().key().data().size();
      key.flags = request->db_erase_request().key().flags()
                  & (~HAM_KEY_USER_ALLOC);

      st = ham_db_erase(db, txn, &key, request->db_erase_request().flags());
    }
  }

  Protocol reply(Protocol::DB_ERASE_REPLY);
  reply.mutable_db_erase_reply()->set_status(st);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_create(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_txn_t *txn = 0;
  ham_db_t *db;
  ham_cursor_t *cursor;
  ham_status_t st = 0;
  ham_u64_t handle = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_create_request());

  if (request->cursor_create_request().txn_handle()) {
    txn = (ham_txn_t *)__get_handle(envh,
                request->cursor_create_request().txn_handle());
    if (!txn) {
      st = HAM_INV_PARAMETER;
      goto bail;
    }
  }

  db = (ham_db_t *)__get_handle(envh,
            request->cursor_create_request().db_handle());
  if (!db) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ham_cursor_create(db, txn,
        request->cursor_create_request().flags(), &cursor);

  if (st == 0) {
    /* allocate a new handle in the Env wrapper structure */
    handle = __store_handle(envh, cursor, HANDLE_TYPE_CURSOR);
  }

bail:
  Protocol reply(Protocol::CURSOR_CREATE_REPLY);
  reply.mutable_cursor_create_reply()->set_status(st);
  reply.mutable_cursor_create_reply()->set_cursor_handle(handle);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_clone(struct env_t *envh, struct mg_connection *conn,
        const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *src;
  ham_cursor_t *dest;
  ham_status_t st = 0;
  ham_u64_t handle = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_clone_request());

  src = (ham_cursor_t *)__get_handle(envh,
            request->cursor_clone_request().cursor_handle());
  if (!src)
    st = HAM_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ham_cursor_clone(src, &dest);
    if (st == 0) {
      /* allocate a new handle in the Env wrapper structure */
      handle = __store_handle(envh, dest, HANDLE_TYPE_CURSOR);
    }
  }

  Protocol reply(Protocol::CURSOR_CLONE_REPLY);
  reply.mutable_cursor_clone_reply()->set_status(st);
  reply.mutable_cursor_clone_reply()->set_cursor_handle(handle);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_insert(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  bool send_key = false;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_insert_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
        request->cursor_insert_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&key, 0, sizeof(key));
  if (request->cursor_insert_request().has_key()) {
    key.size = request->cursor_insert_request().key().data().size();
    if (key.size)
      key.data = (void *)&request->cursor_insert_request().key().data()[0];
    key.flags = request->cursor_insert_request().key().flags()
                & (~HAM_KEY_USER_ALLOC);
  }

  memset(&rec, 0, sizeof(rec));
  if (request->cursor_insert_request().has_record()) {
    rec.size = request->cursor_insert_request().record().data().size();
    if (rec.size)
      rec.data = (void *)&request->cursor_insert_request().record().data()[0];
    rec.partial_size = request->cursor_insert_request().record().partial_size();
    rec.partial_offset = request->cursor_insert_request().record().partial_offset();
    rec.flags = request->cursor_insert_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_insert(cursor, &key, &rec,
            request->cursor_insert_request().flags());

  /* recno: return the modified key */
  if (st == 0) {
    Cursor *c = (Cursor *)cursor;
    if (c->get_db()->get_rt_flags(true) & HAM_RECORD_NUMBER) {
      ham_assert(key.size == sizeof(ham_offset_t));
      send_key = true;
    }
  }

bail:
  Protocol reply(Protocol::CURSOR_INSERT_REPLY);
  reply.mutable_cursor_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_cursor_insert_reply()->mutable_key(),
                &key);
  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_erase(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_erase_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
            request->cursor_erase_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_erase(cursor, request->cursor_erase_request().flags());

  Protocol reply(Protocol::CURSOR_ERASE_REPLY);
  reply.mutable_cursor_erase_reply()->set_status(st);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_find(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  ham_bool_t send_key = HAM_FALSE;
  ham_bool_t send_rec = HAM_FALSE;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_find_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
            request->cursor_find_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&key, 0, sizeof(key));
  key.data = (void *)&request->cursor_find_request().key().data()[0];
  key.size = request->cursor_find_request().key().data().size();
  key.flags = request->cursor_find_request().key().flags()
              & (~HAM_KEY_USER_ALLOC);

  if (request->cursor_find_request().has_record()) {
    send_rec = HAM_TRUE;

    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)&request->cursor_find_request().record().data()[0];
    rec.size = request->cursor_find_request().record().data().size();
    rec.partial_size = request->cursor_find_request().record().partial_size();
    rec.partial_offset = request->cursor_find_request().record().partial_offset();
    rec.flags = request->cursor_find_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_find(cursor, &key, send_rec ? &rec : 0,
                request->cursor_find_request().flags());
  if (st==0) {
    /* approx matching: key->_flags was modified! */
    if (key._flags)
      send_key = HAM_TRUE;
  }

bail:
  Protocol reply(Protocol::CURSOR_FIND_REPLY);
  reply.mutable_cursor_find_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_cursor_find_reply()->mutable_key(),
                    &key);
  if (send_rec)
    Protocol::assign_record(reply.mutable_cursor_find_reply()->mutable_record(),
                    &rec);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_get_duplicate_count(struct env_t *envh,
            struct mg_connection *conn, const struct mg_request_info *ri,
            Protocol *request)
{
  ham_cursor_t *cursor;
  ham_status_t st = 0;
  ham_size_t count = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_get_duplicate_count_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
            request->cursor_get_duplicate_count_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_get_duplicate_count(cursor, &count,
            request->cursor_get_duplicate_count_request().flags());

  Protocol reply(Protocol::CURSOR_GET_DUPLICATE_COUNT_REPLY);
  reply.mutable_cursor_get_duplicate_count_reply()->set_status(st);
  reply.mutable_cursor_get_duplicate_count_reply()->set_count(count);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_overwrite(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_record_t rec;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_overwrite_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
            request->cursor_overwrite_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&rec, 0, sizeof(rec));
  rec.data = (void *)&request->cursor_overwrite_request().record().data()[0];
  rec.size = request->cursor_overwrite_request().record().data().size();
  rec.partial_size = request->cursor_overwrite_request().record().partial_size();
  rec.partial_offset = request->cursor_overwrite_request().record().partial_offset();
  rec.flags = request->cursor_overwrite_request().record().flags()
              & (~HAM_RECORD_USER_ALLOC);

  st = ham_cursor_overwrite(cursor, &rec,
            request->cursor_overwrite_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_OVERWRITE_REPLY);
  reply.mutable_cursor_overwrite_reply()->set_status(st);

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_move(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  ham_bool_t send_key = HAM_FALSE;
  ham_bool_t send_rec = HAM_FALSE;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_move_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
            request->cursor_move_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  if (request->cursor_move_request().has_key()) {
    send_key = HAM_TRUE;

    memset(&key, 0, sizeof(key));
    key.data = (void *)&request->cursor_move_request().key().data()[0];
    key.size = request->cursor_move_request().key().data().size();
    key.flags = request->cursor_move_request().key().flags()
                & (~HAM_KEY_USER_ALLOC);
  }

  if (request->cursor_move_request().has_record()) {
    send_rec = HAM_TRUE;

    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)&request->cursor_move_request().record().data()[0];
    rec.size = request->cursor_move_request().record().data().size();
    rec.partial_size = request->cursor_move_request().record().partial_size();
    rec.partial_offset = request->cursor_move_request().record().partial_offset();
    rec.flags = request->cursor_move_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_move(cursor, send_key ? &key : 0, send_rec ? &rec : 0,
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

  send_wrapper(envh->env, conn, &reply);
}

static void
handle_cursor_close(struct env_t *envh, struct mg_connection *conn,
            const struct mg_request_info *ri, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_close_request());

  cursor = (ham_cursor_t *)__get_handle(envh,
                request->cursor_close_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_close(cursor);

  if (st==0) {
    /* remove the handle from the Env wrapper structure */
    __remove_handle(envh, request->cursor_close_request().cursor_handle());
  }

  Protocol reply(Protocol::CURSOR_CLOSE_REPLY);
  reply.mutable_cursor_close_reply()->set_status(st);

  send_wrapper(envh->env, conn, &reply);
}

static void
request_handler(struct mg_connection *conn, const struct mg_request_info *ri,
            void *user_data)
{
  Protocol *wrapper = 0;
  struct env_t *env = (struct env_t *)user_data;

  mg_authorize(conn);

  os_critsec_enter(&env->cs);

  wrapper = Protocol::unpack((ham_u8_t *)ri->post_data, ri->post_data_len);
  if (!wrapper) {
    ham_trace(("failed to unpack wrapper (%d bytes)\n", ri->post_data_len));
    goto bail;
  }

  switch (wrapper->type()) {
  case ProtoWrapper_Type_CONNECT_REQUEST:
    ham_trace(("connect request"));
    handle_connect(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_GET_PARAMETERS_REQUEST:
    ham_trace(("env_get_parameters request"));
    handle_env_get_parameters(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_GET_DATABASE_NAMES_REQUEST:
    ham_trace(("env_get_database_names request"));
    handle_env_get_database_names(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_FLUSH_REQUEST:
    ham_trace(("env_flush request"));
    handle_env_flush(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_RENAME_REQUEST:
    ham_trace(("env_rename request"));
    handle_env_rename(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_CREATE_DB_REQUEST:
    ham_trace(("env_create_db request"));
    handle_env_create_db(env, env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_OPEN_DB_REQUEST:
    ham_trace(("env_open_db request"));
    handle_env_open_db(env, env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_ENV_ERASE_DB_REQUEST:
    ham_trace(("env_erase_db request"));
    handle_env_erase_db(env->env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_CLOSE_REQUEST:
    ham_trace(("db_close request"));
    handle_db_close(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_GET_PARAMETERS_REQUEST:
    ham_trace(("db_get_parameters request"));
    handle_db_get_parameters(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_TXN_BEGIN_REQUEST:
    ham_trace(("txn_begin request"));
    handle_txn_begin(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_TXN_COMMIT_REQUEST:
    ham_trace(("txn_commit request"));
    handle_txn_commit(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_TXN_ABORT_REQUEST:
    ham_trace(("txn_abort request"));
    handle_txn_abort(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_CHECK_INTEGRITY_REQUEST:
    ham_trace(("db_check_integrity request"));
    handle_db_check_integrity(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_GET_KEY_COUNT_REQUEST:
    ham_trace(("db_get_key_count request"));
    handle_db_get_key_count(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_INSERT_REQUEST:
    ham_trace(("db_insert request"));
    handle_db_insert(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_FIND_REQUEST:
    ham_trace(("db_find request"));
    handle_db_find(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_DB_ERASE_REQUEST:
    ham_trace(("db_erase request"));
    handle_db_erase(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_CREATE_REQUEST:
    ham_trace(("cursor_create request"));
    handle_cursor_create(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_CLONE_REQUEST:
    ham_trace(("cursor_clone request"));
    handle_cursor_clone(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_INSERT_REQUEST:
    ham_trace(("cursor_insert request"));
    handle_cursor_insert(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_ERASE_REQUEST:
    ham_trace(("cursor_erase request"));
    handle_cursor_erase(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_FIND_REQUEST:
    ham_trace(("cursor_find request"));
    handle_cursor_find(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_GET_DUPLICATE_COUNT_REQUEST:
    ham_trace(("cursor_get_duplicate_count request"));
    handle_cursor_get_duplicate_count(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_OVERWRITE_REQUEST:
    ham_trace(("cursor_overwrite request"));
    handle_cursor_overwrite(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_MOVE_REQUEST:
    ham_trace(("cursor_move request"));
    handle_cursor_move(env, conn, ri, wrapper);
    break;
  case ProtoWrapper_Type_CURSOR_CLOSE_REQUEST:
    ham_trace(("cursor_close request"));
    handle_cursor_close(env, conn, ri, wrapper);
    break;
  default:
    ham_trace(("ignoring unknown request"));
    break;
  }

#if 0
    printf("Method: [%s]\n", ri->request_method);
    printf("URI: [%s]\n", ri->uri);
    printf("HTTP version: [%d.%d]\n", ri->http_version_major,
      ri->http_version_minor);

    for (i = 0; i < ri->num_headers; i++)
        printf("HTTP header [%s]: [%s]\n",
             ri->http_headers[i].name,
             ri->http_headers[i].value);

    printf("Query string: [%s]\n",
            ri->query_string ? ri->query_string: "");
    printf("POST data: [%.*s]\n",
            ri->post_data_len, ri->post_data);
    printf("Remote IP: [%lu]\n", ri->remote_ip);
    printf("Remote port: [%d]\n", ri->remote_port);
    printf("Remote user: [%s]\n",
            ri->remote_user ? ri->remote_user : "");
    printf("Hamsterdb url: [%s]\n", env->urlname);
#endif

bail:
  delete wrapper;

  os_critsec_leave(&env->cs);
}

} // namespace ham

// global namespace is below

struct ham_srv_t {
  /* the mongoose context structure */
  struct mg_context *mg_ctxt;

  /* handlers for each Environment */
  struct ham::env_t environments[MAX_ENVIRONMENTS];
};

ham_status_t
ham_srv_init(ham_srv_config_t *config, ham_srv_t **psrv)
{
  ham_srv_t *srv;
  char buf[32];
  sprintf(buf, "%d", (int)config->port);

  srv = (ham_srv_t *)malloc(sizeof(ham_srv_t));
  if (!srv)
    return (HAM_OUT_OF_MEMORY);
  memset(srv, 0, sizeof(*srv));

  srv->mg_ctxt = mg_start();
  mg_set_option(srv->mg_ctxt, "ports", buf);
  mg_set_option(srv->mg_ctxt, "dir_list", "no");
  if (config->access_log_path) {
    if (!mg_set_option(srv->mg_ctxt, "access_log", config->access_log_path)) {
      ham_log(("failed to write access log file '%s'",
            config->access_log_path));
      mg_stop(srv->mg_ctxt);
      free(srv);
      return (HAM_IO_ERROR);
    }
  }
  if (config->error_log_path) {
    if (!mg_set_option(srv->mg_ctxt, "error_log", config->error_log_path)) {
      ham_log(("failed to write access log'%s'", config->access_log_path));
      mg_stop(srv->mg_ctxt);
      free(srv);
      return (HAM_IO_ERROR);
    }
  }

  *psrv = srv;
  return (HAM_SUCCESS);
}

ham_status_t
ham_srv_add_env(ham_srv_t *srv, ham_env_t *env, const char *urlname)
{
  int i;

  /* search for a free handler */
  for (i = 0; i < MAX_ENVIRONMENTS; i++) {
    if (!srv->environments[i].env) {
      srv->environments[i].env = env;
      srv->environments[i].urlname = strdup(urlname);
      os_critsec_init(&srv->environments[i].cs);
      break;
    }
  }

  if (i == MAX_ENVIRONMENTS)
    return (HAM_LIMITS_REACHED);

  mg_set_uri_callback(srv->mg_ctxt, urlname,
            request_handler, &srv->environments[i]);
  return (HAM_SUCCESS);
}

void
ham_srv_close(ham_srv_t *srv)
{
  int i;

  /* clean up Environment handlers */
  for (i = 0; i < MAX_ENVIRONMENTS; i++) {
    if (srv->environments[i].env) {
      if (srv->environments[i].urlname)
        free(srv->environments[i].urlname);
      if (srv->environments[i].handles)
        free(srv->environments[i].handles);
      os_critsec_close(&srv->environments[i].cs);
      /* env will be closed by the caller */
      srv->environments[i].env = 0;
    }
  }

  mg_stop(srv->mg_ctxt);
  free(srv);

  /* free libprotocol static data */
  Protocol::shutdown();
}

