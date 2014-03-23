/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <string.h>

#include "../protocol/protocol.h"
#include "os.h"
#include "error.h"
#include "errorinducer.h"
#include "mem.h"
#include "env.h"
#include "hamserver.h"

#define INDUCE(id)                                                  \
  while (srv->m_inducer) {                                          \
    ham_status_t st = srv->m_inducer->induce(id);                   \
    if (st)                                                         \
      return;                                                       \
    break;                                                          \
  }

namespace hamsterdb {

static void
on_write_cb(uv_write_t *req, int status)
{
  Memory::release(req->data);
  delete req;
};

static void
send_wrapper(ServerContext *srv, uv_stream_t *tcp, Protocol *reply)
{
  ham_u8_t *data;
  ham_u32_t data_size;

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
handle_connect(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_assert(request != 0);
  Environment *env = srv->open_envs[request->connect_request().path()];

  INDUCE(ErrorInducer::kServerConnect);

  Protocol reply(Protocol::CONNECT_REPLY);
  if (!env) {
    reply.mutable_connect_reply()->set_status(HAM_FILE_NOT_FOUND);
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
  ham_assert(request != 0);
  srv->remove_env_handle(request->disconnect_request().env_handle());

  Protocol reply(Protocol::DISCONNECT_REPLY);
  reply.mutable_disconnect_reply()->set_status(0);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_get_parameters(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_u32_t i;
  ham_status_t st = 0;
  Environment *env = 0;
  ham_parameter_t params[100]; /* 100 should be enough... */

  ham_assert(request != 0);
  ham_assert(request->has_env_get_parameters_request());

  /* initialize the ham_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (i = 0;
      i < (ham_u32_t)request->env_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_env_get_parameters_request()->mutable_names()->mutable_data()[i];

  Protocol reply(Protocol::ENV_GET_PARAMETERS_REPLY);

  env = srv->get_env(request->env_get_parameters_request().env_handle());

  /* and request the parameters from the Environment */
  st = ham_env_get_parameters((ham_env_t *)env, &params[0]);
  reply.mutable_env_get_parameters_reply()->set_status(st);
  if (st) {
    send_wrapper(srv, tcp, &reply);
    return;
  }

  /* initialize the reply package */
  for (i = 0;
      i < (ham_u32_t)request->env_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case HAM_PARAM_CACHESIZE:
      reply.mutable_env_get_parameters_reply()->set_cache_size(
              (int)params[i].value);
      break;
    case HAM_PARAM_PAGESIZE:
      reply.mutable_env_get_parameters_reply()->set_page_size(
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

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_get_database_names(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_u32_t num_names = 1024;
  ham_u16_t names[1024]; /* should be enough */
  Environment *env = 0;

  ham_assert(request != 0);
  ham_assert(request->has_env_get_database_names_request());

  env = srv->get_env(request->env_get_database_names_request().env_handle());

  /* request the database names from the Environment */
  ham_status_t st = ham_env_get_database_names((ham_env_t *)env,
          &names[0], &num_names);

  Protocol reply(Protocol::ENV_GET_DATABASE_NAMES_REPLY);
  reply.mutable_env_get_database_names_reply()->set_status(st);
  for (ham_u32_t i = 0; i < num_names; i++)
    reply.mutable_env_get_database_names_reply()->add_names(names[i]);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_flush(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  Environment *env = 0;

  ham_assert(request != 0);
  ham_assert(request->has_env_flush_request());

  env = srv->get_env(request->env_flush_request().env_handle());

  /* request the database names from the Environment */
  Protocol reply(Protocol::ENV_FLUSH_REPLY);
  reply.mutable_env_flush_reply()->set_status(ham_env_flush((ham_env_t *)env,
            request->env_flush_request().flags()));

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_rename(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  Environment *env = 0;

  ham_assert(request != 0);
  ham_assert(request->has_env_rename_request());

  env = srv->get_env(request->env_rename_request().env_handle());

  /* rename the databases */
  ham_status_t st = ham_env_rename_db((ham_env_t *)env,
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
  ham_db_t *db;
  Environment *env;
  ham_status_t st = 0;
  ham_u64_t db_handle = 0;
  ham_parameter_t params[100] = {{0, 0}};

  ham_assert(request != 0);
  ham_assert(request->has_env_create_db_request());

  env = srv->get_env(request->env_create_db_request().env_handle());

  /* convert parameters */
  ham_assert(request->env_create_db_request().param_names().size() < 100);
  for (ham_u32_t i = 0;
      i < (ham_u32_t)request->env_create_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_create_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_create_db_request()->mutable_param_values()->data()[i];
  }

  /* create the database */
  st = ham_env_create_db((ham_env_t *)env, &db,
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
        ((Database *)db)->get_rt_flags(true));
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_open_db(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_db_t *db = 0;
  ham_u64_t db_handle = 0;
  ham_status_t st = 0;
  ham_u16_t dbname = request->env_open_db_request().dbname();
  ham_parameter_t params[100] = {{0, 0}};

  ham_assert(request != 0);
  ham_assert(request->has_env_open_db_request());

  Environment *env = srv->get_env(request->env_open_db_request().env_handle());

  /* convert parameters */
  ham_assert(request->env_open_db_request().param_names().size() < 100);
  for (ham_u32_t i = 0;
      i < (ham_u32_t)request->env_open_db_request().param_names().size();
      i++) {
    params[i].name  = request->mutable_env_open_db_request()->mutable_param_names()->data()[i];
    params[i].value = request->mutable_env_open_db_request()->mutable_param_values()->data()[i];
  }

  /* check if the database is already open */
  Handle<Database> handle = srv->get_db_by_name(dbname);
  db = (ham_db_t *)handle.object;
  db_handle = handle.index;

  /* if not found: check if the Database was opened by another
   * connection
   * TODO this is not thread safe! - not really a problem because the server
   * is currently single-threaded, though
   */
  if (!db) {
    Environment::DatabaseMap::iterator it
            = env->get_database_map().find(dbname);
    if (it != env->get_database_map().end())
      db = (ham_db_t *)it->second;
    db_handle = srv->allocate_handle((Database *)db);
  }

  if (!db) {
    /* still not found: open the database */
    st = ham_env_open_db((ham_env_t *)env, &db, dbname,
                request->env_open_db_request().flags(), &params[0]);

    if (st == 0)
      db_handle = srv->allocate_handle((Database *)db);
  }

  Protocol reply(Protocol::ENV_OPEN_DB_REPLY);
  reply.mutable_env_open_db_reply()->set_status(st);
  if (st == 0) {
    reply.mutable_env_open_db_reply()->set_db_handle(db_handle);
    reply.mutable_env_open_db_reply()->set_db_flags(
        ((Database *)db)->get_rt_flags(true));
  }

  send_wrapper(srv, tcp, &reply);
}

static void
handle_env_erase_db(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_assert(request != 0);
  ham_assert(request->has_env_erase_db_request());

  Environment *env = srv->get_env(request->env_erase_db_request().env_handle());

  ham_status_t st = ham_env_erase_db((ham_env_t *)env,
            request->env_erase_db_request().name(),
            request->env_erase_db_request().flags());

  Protocol reply(Protocol::ENV_ERASE_DB_REPLY);
  reply.mutable_env_erase_db_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_close(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_db_close_request());

  Database *db = srv->get_db(request->db_close_request().db_handle());
  if (!db) {
    /* accept this - most likely the database was already closed by
     * another process */
    st = 0;
    srv->remove_db_handle(request->db_close_request().db_handle());
  }
  else {
    st = ham_db_close((ham_db_t *)db, request->db_close_request().flags());
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
  ham_status_t st = 0;
  ham_parameter_t params[100]; /* 100 should be enough... */

  ham_assert(request != 0);
  ham_assert(request->has_db_get_parameters_request());

  /* initialize the ham_parameters_t array */
  memset(&params[0], 0, sizeof(params));
  for (ham_u32_t i = 0;
      i < (ham_u32_t)request->db_get_parameters_request().names().size()
        && i < 100; i++)
    params[i].name = request->mutable_db_get_parameters_request()->mutable_names()->mutable_data()[i];

  /* and request the parameters from the Database */
  Database *db = srv->get_db(request->db_get_parameters_request().db_handle());
  if (!db)
    st = HAM_INV_PARAMETER;
  else
    st = ham_db_get_parameters((ham_db_t *)db, &params[0]);

  Protocol reply(Protocol::DB_GET_PARAMETERS_REPLY);
  reply.mutable_db_get_parameters_reply()->set_status(st);

  if (st) {
    send_wrapper(srv, tcp, &reply);
    return;
  }

  /* initialize the reply package */
  for (ham_u32_t i = 0;
      i < (ham_u32_t)request->db_get_parameters_request().names().size();
      i++) {
    switch (params[i].name) {
    case 0:
      continue;
    case HAM_PARAM_FLAGS:
      reply.mutable_db_get_parameters_reply()->set_flags(
              (int)params[i].value);
      break;
    case HAM_PARAM_KEY_SIZE:
      reply.mutable_db_get_parameters_reply()->set_key_size(
              (int)params[i].value);
      break;
    case HAM_PARAM_RECORD_SIZE:
      reply.mutable_db_get_parameters_reply()->set_record_size(
              (int)params[i].value);
      break;
    case HAM_PARAM_KEY_TYPE:
      reply.mutable_db_get_parameters_reply()->set_key_type(
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

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_check_integrity(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_db_check_integrity_request());

  Database *db = 0;

  ham_u32_t flags = request->db_check_integrity_request().flags();

  if (st == 0) {
    db = srv->get_db(request->db_check_integrity_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else
      st = ham_db_check_integrity((ham_db_t *)db, flags);
  }

  Protocol reply(Protocol::DB_CHECK_INTEGRITY_REPLY);
  reply.mutable_db_check_integrity_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_get_key_count(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_status_t st = 0;
  ham_u64_t keycount;

  ham_assert(request!=0);
  ham_assert(request->has_db_get_key_count_request());

  Transaction *txn = 0;
  Database *db = 0;
  
  if (request->db_get_key_count_request().txn_handle()) {
    txn = srv->get_txn(request->db_get_key_count_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
   db = srv->get_db(request->db_get_key_count_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else
      st = ham_db_get_key_count((ham_db_t *)db, (ham_txn_t *)txn,
                request->db_get_key_count_request().flags(), &keycount);
  }

  Protocol reply(Protocol::DB_GET_KEY_COUNT_REPLY);
  reply.mutable_db_get_key_count_reply()->set_status(st);
  if (st == 0)
    reply.mutable_db_get_key_count_reply()->set_keycount(keycount);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_db_insert(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_status_t st = 0;
  bool send_key = false;
  ham_key_t key;
  ham_record_t rec;

  ham_assert(request != 0);
  ham_assert(request->has_db_insert_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_insert_request().txn_handle()) {
    txn = srv->get_txn(request->db_insert_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_insert_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      memset(&key, 0, sizeof(key));
      if (request->db_insert_request().has_key()) {
        key.size = (ham_u16_t)request->db_insert_request().key().data().size();
        if (key.size)
          key.data = (void *)&request->db_insert_request().key().data()[0];
        key.flags = request->db_insert_request().key().flags()
                    & (~HAM_KEY_USER_ALLOC);
      }

      memset(&rec, 0, sizeof(rec));
      if (request->db_insert_request().has_record()) {
        rec.size = (ham_u32_t)request->db_insert_request().record().data().size();
        if (rec.size)
          rec.data = (void *)&request->db_insert_request().record().data()[0];
        rec.partial_size = request->db_insert_request().record().partial_size();
        rec.partial_offset = request->db_insert_request().record().partial_offset();
        rec.flags = request->db_insert_request().record().flags()
                    & (~HAM_RECORD_USER_ALLOC);
      }
      st = ham_db_insert((ham_db_t *)db, (ham_txn_t *)txn, &key, &rec,
                    request->db_insert_request().flags());

      /* recno: return the modified key */
      if ((st == 0)
          && (((Database *)db)->get_rt_flags(true) & HAM_RECORD_NUMBER)) {
        ham_assert(key.size == sizeof(ham_u64_t));
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
handle_db_find(ServerContext *srv, uv_stream_t *tcp,
                Protocol *request)
{
  ham_status_t st = 0;
  ham_key_t key;
  ham_record_t rec = {0};
  bool send_key = false;

  ham_assert(request != 0);
  ham_assert(request->has_db_find_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_find_request().txn_handle()) {
    txn = srv->get_txn(request->db_find_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_find_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      memset(&key, 0, sizeof(key));
      key.data = (void *)&request->db_find_request().key().data()[0];
      key.size = (ham_u16_t)request->db_find_request().key().data().size();
      key.flags = request->db_find_request().key().flags()
                  & (~HAM_KEY_USER_ALLOC);

      rec.data = (void *)&request->db_find_request().record().data()[0];
      rec.size = (ham_u32_t)request->db_find_request().record().data().size();
      rec.partial_size = request->db_find_request().record().partial_size();
      rec.partial_offset = request->db_find_request().record().partial_offset();
      rec.flags = request->db_find_request().record().flags()
                  & (~HAM_RECORD_USER_ALLOC);

      st = ham_db_find((ham_db_t *)db, (ham_txn_t *)txn, &key,
              &rec, request->db_find_request().flags());
      if (st == 0) {
        /* approx matching: key->_flags was modified! */
        if (key._flags)
          send_key = true;
      }
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
handle_db_erase(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_db_erase_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->db_erase_request().txn_handle()) {
    txn = srv->get_txn(request->db_erase_request().txn_handle());
    if (!txn)
      st = HAM_INV_PARAMETER;
  }

  if (st == 0) {
    db = srv->get_db(request->db_erase_request().db_handle());
    if (!db)
      st = HAM_INV_PARAMETER;
    else {
      ham_key_t key;

      memset(&key, 0, sizeof(key));
      key.data = (void *)&request->db_erase_request().key().data()[0];
      key.size = (ham_u16_t)request->db_erase_request().key().data().size();
      key.flags = request->db_erase_request().key().flags()
                  & (~HAM_KEY_USER_ALLOC);

      st = ham_db_erase((ham_db_t *)db, (ham_txn_t *)txn, &key,
              request->db_erase_request().flags());
    }
  }

  Protocol reply(Protocol::DB_ERASE_REPLY);
  reply.mutable_db_erase_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_txn_begin(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_txn_t *txn;
  ham_status_t st = 0;
  ham_u64_t txn_handle = 0;

  ham_assert(request != 0);
  ham_assert(request->has_txn_begin_request());

  Environment *env = srv->get_env(request->txn_begin_request().env_handle());

  st = ham_txn_begin(&txn, (ham_env_t *)env,
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
handle_txn_commit(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_txn_commit_request());

  Transaction *txn = srv->get_txn(request->txn_commit_request().txn_handle());
  if (!txn) {
    st = HAM_INV_PARAMETER;
  }
  else {
    st = ham_txn_commit((ham_txn_t *)txn,
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
handle_txn_abort(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_txn_abort_request());

  Transaction *txn = srv->get_txn(request->txn_abort_request().txn_handle());
  if (!txn) {
    st = HAM_INV_PARAMETER;
  }
  else {
    st = ham_txn_abort((ham_txn_t *)txn,
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
handle_cursor_create(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_cursor_t *cursor;
  ham_status_t st = 0;
  ham_u64_t handle = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_create_request());

  Transaction *txn = 0;
  Database *db = 0;

  if (request->cursor_create_request().txn_handle()) {
    txn = srv->get_txn(request->cursor_create_request().txn_handle());
    if (!txn) {
      st = HAM_INV_PARAMETER;
      goto bail;
    }
  }

  db = srv->get_db(request->cursor_create_request().db_handle());
  if (!db) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  /* create the cursor */
  st = ham_cursor_create(&cursor, (ham_db_t *)db, (ham_txn_t *)txn,
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
handle_cursor_clone(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_cursor_t *dest;
  ham_status_t st = 0;
  ham_u64_t handle = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_clone_request());

  Cursor *src = srv->get_cursor(request->cursor_clone_request().cursor_handle());
  if (!src)
    st = HAM_INV_PARAMETER;
  else {
    /* clone the cursor */
    st = ham_cursor_clone((ham_cursor_t *)src, &dest);
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
handle_cursor_insert(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  bool send_key = false;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_insert_request());

  Cursor *cursor = srv->get_cursor(request->cursor_insert_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&key, 0, sizeof(key));
  if (request->cursor_insert_request().has_key()) {
    key.size = (ham_u16_t)request->cursor_insert_request().key().data().size();
    if (key.size)
      key.data = (void *)&request->cursor_insert_request().key().data()[0];
    key.flags = request->cursor_insert_request().key().flags()
                & (~HAM_KEY_USER_ALLOC);
  }

  memset(&rec, 0, sizeof(rec));
  if (request->cursor_insert_request().has_record()) {
    rec.size = (ham_u32_t)request->cursor_insert_request().record().data().size();
    if (rec.size)
      rec.data = (void *)&request->cursor_insert_request().record().data()[0];
    rec.partial_size = request->cursor_insert_request().record().partial_size();
    rec.partial_offset = request->cursor_insert_request().record().partial_offset();
    rec.flags = request->cursor_insert_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_insert((ham_cursor_t *)cursor, &key, &rec,
            request->cursor_insert_request().flags());

  /* recno: return the modified key */
  if (st == 0) {
    if (cursor->get_db()->get_rt_flags(true) & HAM_RECORD_NUMBER) {
      ham_assert(key.size == sizeof(ham_u64_t));
      send_key = true;
    }
  }

bail:
  Protocol reply(Protocol::CURSOR_INSERT_REPLY);
  reply.mutable_cursor_insert_reply()->set_status(st);
  if (send_key)
    Protocol::assign_key(reply.mutable_cursor_insert_reply()->mutable_key(),
                &key);
  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_erase(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_erase_request());

  Cursor *cursor = srv->get_cursor(request->cursor_erase_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_erase((ham_cursor_t *)cursor,
            request->cursor_erase_request().flags());

  Protocol reply(Protocol::CURSOR_ERASE_REPLY);
  reply.mutable_cursor_erase_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_find(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  bool send_key = false;
  bool send_rec = false;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_find_request());

  Cursor *cursor = srv->get_cursor(request->cursor_find_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&key, 0, sizeof(key));
  key.data = (void *)&request->cursor_find_request().key().data()[0];
  key.size = (ham_u16_t)request->cursor_find_request().key().data().size();
  key.flags = request->cursor_find_request().key().flags()
              & (~HAM_KEY_USER_ALLOC);

  if (request->cursor_find_request().has_record()) {
    send_rec = true;

    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)&request->cursor_find_request().record().data()[0];
    rec.size = (ham_u32_t)request->cursor_find_request().record().data().size();
    rec.partial_size = request->cursor_find_request().record().partial_size();
    rec.partial_offset = request->cursor_find_request().record().partial_offset();
    rec.flags = request->cursor_find_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_find((ham_cursor_t *)cursor, &key, send_rec ? &rec : 0,
                request->cursor_find_request().flags());
  if (st==0) {
    /* approx matching: key->_flags was modified! */
    if (key._flags)
      send_key = true;
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

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_get_record_count(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ham_status_t st = 0;
  ham_u32_t count = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_get_record_count_request());

  Cursor *cursor = srv->get_cursor(request->cursor_get_record_count_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_get_duplicate_count((ham_cursor_t *)cursor, &count,
            request->cursor_get_record_count_request().flags());

  Protocol reply(Protocol::CURSOR_GET_RECORD_COUNT_REPLY);
  reply.mutable_cursor_get_record_count_reply()->set_status(st);
  reply.mutable_cursor_get_record_count_reply()->set_count(count);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_overwrite(ServerContext *srv, uv_stream_t *tcp,
            Protocol *request)
{
  ham_record_t rec;
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_overwrite_request());

  Cursor *cursor = srv->get_cursor(request->cursor_overwrite_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  memset(&rec, 0, sizeof(rec));
  rec.data = (void *)&request->cursor_overwrite_request().record().data()[0];
  rec.size = (ham_u32_t)request->cursor_overwrite_request().record().data().size();
  rec.partial_size = request->cursor_overwrite_request().record().partial_size();
  rec.partial_offset = request->cursor_overwrite_request().record().partial_offset();
  rec.flags = request->cursor_overwrite_request().record().flags()
              & (~HAM_RECORD_USER_ALLOC);

  st = ham_cursor_overwrite((ham_cursor_t *)cursor, &rec,
            request->cursor_overwrite_request().flags());

bail:
  Protocol reply(Protocol::CURSOR_OVERWRITE_REPLY);
  reply.mutable_cursor_overwrite_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static void
handle_cursor_move(ServerContext *srv, uv_stream_t *tcp, Protocol *request)
{
  ham_key_t key;
  ham_record_t rec;
  ham_status_t st = 0;
  bool send_key = false;
  bool send_rec = false;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_move_request());

  Cursor *cursor = srv->get_cursor(request->cursor_move_request().cursor_handle());
  if (!cursor) {
    st = HAM_INV_PARAMETER;
    goto bail;
  }

  if (request->cursor_move_request().has_key()) {
    send_key = true;

    memset(&key, 0, sizeof(key));
    key.data = (void *)&request->cursor_move_request().key().data()[0];
    key.size = (ham_u16_t)request->cursor_move_request().key().data().size();
    key.flags = request->cursor_move_request().key().flags()
                & (~HAM_KEY_USER_ALLOC);
  }

  if (request->cursor_move_request().has_record()) {
    send_rec = true;

    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)&request->cursor_move_request().record().data()[0];
    rec.size = (ham_u32_t)request->cursor_move_request().record().data().size();
    rec.partial_size = request->cursor_move_request().record().partial_size();
    rec.partial_offset = request->cursor_move_request().record().partial_offset();
    rec.flags = request->cursor_move_request().record().flags()
                & (~HAM_RECORD_USER_ALLOC);
  }

  st = ham_cursor_move((ham_cursor_t *)cursor,
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
  ham_status_t st = 0;

  ham_assert(request != 0);
  ham_assert(request->has_cursor_close_request());

  Cursor *cursor = srv->get_cursor(request->cursor_close_request().cursor_handle());
  if (!cursor)
    st = HAM_INV_PARAMETER;
  else
    st = ham_cursor_close((ham_cursor_t *)cursor);

  if (st==0) {
    /* remove the handle from the Env wrapper structure */
    srv->remove_cursor_handle(request->cursor_close_request().cursor_handle());
  }

  Protocol reply(Protocol::CURSOR_CLOSE_REPLY);
  reply.mutable_cursor_close_reply()->set_status(st);

  send_wrapper(srv, tcp, &reply);
}

static bool
dispatch(ServerContext *srv, uv_stream_t *tcp, ham_u8_t *data, ham_u32_t size)
{
  // returns false if client should be closed, otherwise true
  Protocol *wrapper = Protocol::unpack(data, size);
  if (!wrapper) {
    ham_trace(("failed to unpack wrapper (%d bytes)\n", size));
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
      handle_db_get_key_count(srv, tcp, wrapper);
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
    case ProtoWrapper_Type_CURSOR_FIND_REQUEST:
      handle_cursor_find(srv, tcp, wrapper);
      break;
    case ProtoWrapper_Type_CURSOR_GET_RECORD_COUNT_REQUEST:
      handle_cursor_get_record_count(srv, tcp, wrapper);
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
      ham_trace(("ignoring unknown request"));
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

static uv_buf_t
on_alloc_buffer(uv_handle_t *handle, size_t size)
{
  return (uv_buf_init(Memory::allocate<char>(size), size));
}

static void
on_read_data(uv_stream_t *tcp, ssize_t nread, uv_buf_t buf)
{
  ham_assert(tcp != 0);
  ham_u32_t size = 0;
  bool close_client = false;
  ClientContext *context = (ClientContext *)tcp->data;
  ByteArray *buffer = &context->buffer;

  // each request is prepended with a header:
  //   4 byte magic
  //   4 byte size  (without those 8 bytes)
  if (nread >= 0) {
    // if we already started buffering data: append the data to the buffer
    if (!buffer->is_empty()) {
      buffer->append(buf.base, nread);

      // for each full package in the buffer...
      while (buffer->get_size() > 8) {
        ham_u8_t *p = (ham_u8_t *)buffer->get_ptr();
        size = 8 + *(ham_u32_t *)(p + 4);
        // still not enough data? then return immediately
        if (buffer->get_size() < size)
          goto bail;
        // otherwise dispatch the message
        close_client = !dispatch(context->srv, tcp, p, size);
        // and move the remaining data to "the left"
        if (buffer->get_size() == size) {
          buffer->clear();
          goto bail;
        }
        else {
          ham_u32_t new_size = buffer->get_size() - size;
          memmove(p, p + size, new_size);
          buffer->set_size(new_size);
          // fall through and repeat the loop
        }
      }
      goto bail;
    }

    // we have not buffered data from previous calls; try to dispatch the
    // current network packet
    ham_u8_t *p = (ham_u8_t *)buf.base;
    while (p < (ham_u8_t *)buf.base + nread) {
      size = 8 + *(ham_u32_t *)(p + 4);
      if (size <= nread) {
        close_client = !dispatch(context->srv, tcp, p, size);
        if (close_client)
          goto bail;
        nread -= size;
        p += size;
        continue;
      }
      // not enough data? then cache it in the buffer
      else {
        ham_assert(buffer->is_empty());
        buffer->append(p, nread);
        goto bail;
      }
    }
  }

bail:
  if (close_client || nread < 0)
    uv_close((uv_handle_t *)tcp, on_close_connection);
  Memory::release(buf.base);
}

static void
on_new_connection(uv_stream_t *server, int status)
{
  if (status == -1)
    return;

  ServerContext *srv = (ServerContext *)server->data;

  uv_tcp_t *client = Memory::allocate<uv_tcp_t>(sizeof(uv_tcp_t));
  client->data = new ClientContext(srv);

  uv_tcp_init(srv->loop, client);
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
on_async_cb(uv_async_t *handle, int status)
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

ham_status_t
ham_srv_init(ham_srv_config_t *config, ham_srv_t **psrv)
{
  ServerContext *srv = new ServerContext();

  srv->loop = uv_loop_new();

  uv_tcp_init(srv->loop, &srv->server);

  struct sockaddr_in bind_addr = uv_ip4_addr("0.0.0.0", config->port);
  uv_tcp_bind(&srv->server, bind_addr);
  srv->server.data = srv;
  int r = uv_listen((uv_stream_t *)&srv->server, 128,
          hamsterdb::on_new_connection);
  if (r) {
    ham_log(("failed to listen to port %d", config->port)); 
    return (HAM_IO_ERROR);
  }

  srv->async.data = srv;
  uv_async_init(srv->loop, &srv->async, on_async_cb);

  uv_thread_create(&srv->thread_id, on_run_thread, srv->loop);

  *psrv = (ham_srv_t *)srv;
  return (HAM_SUCCESS);
}

ham_status_t
ham_srv_add_env(ham_srv_t *hsrv, ham_env_t *env, const char *urlname)
{
  ServerContext *srv = (ServerContext *)hsrv;
  if (!srv || !env || !urlname) {
    ham_log(("parameters srv, env, urlname must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  {
    ScopedLock lock(srv->open_queue_mutex);
    srv->open_queue[urlname] = (Environment *)env;
  }

  uv_async_send(&srv->async);
  return (HAM_SUCCESS);
}

void
ham_srv_close(ham_srv_t *hsrv)
{
  ServerContext *srv = (ServerContext *)hsrv;
  if (!srv)
    return;

  uv_unref((uv_handle_t *)&srv->server);
  uv_unref((uv_handle_t *)&srv->async);

  // TODO clean up all allocated objects and handles

  /* stop the event loop */
  uv_stop(srv->loop);
  uv_async_send(&srv->async);

  /* join the libuv thread */
  (void)uv_thread_join(&srv->thread_id);

  /* close the async handle and the server socket */
  uv_close((uv_handle_t *)&srv->async, 0);
  uv_close((uv_handle_t *)&srv->server, 0);

  /* clean up libuv */
  uv_loop_delete(srv->loop);

  delete srv;

  /* free libprotocol static data */
  Protocol::shutdown();
}

