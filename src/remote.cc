/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "txn.h"
#include "env.h"
#include "mem.h"
#include "cursor.h"

#if HAM_ENABLE_REMOTE

#define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#include <curl/curl.h>
#include <curl/easy.h>

#include "protocol/protocol.h"

namespace ham {

typedef struct curl_buffer_t
{
  ham_size_t packed_size;
  ham_u8_t *packed_data;
  ham_size_t offset;
  Protocol *wrapper;
  Allocator *alloc;
} curl_buffer_t;

static size_t
__writefunc(void *buffer, size_t size, size_t nmemb, void *ptr)
{
  curl_buffer_t *buf = (curl_buffer_t *)ptr;
  char *cbuf = (char *)buffer;
  ham_size_t payload_size = 0;

  if (buf->offset == 0) {
    if (*(ham_u32_t *)&cbuf[0] != ham_db2h32(HAM_TRANSFER_MAGIC_V1)) {
      ham_trace(("invalid protocol version"));
      return (0);
    }
    payload_size = ham_h2db32(*(ham_u32_t *)&cbuf[4]);

    /* did we receive the whole data in this packet? */
    if (payload_size + 8 == size * nmemb) {
      buf->wrapper = Protocol::unpack((ham_u8_t *)&cbuf[0],
                (ham_size_t)(size * nmemb));
      if (!buf->wrapper)
        return (0);
      return (size * nmemb);
    }

    /* otherwise we have to buffer the received data */
    buf->packed_size = payload_size + 8;
    buf->packed_data = (ham_u8_t *)buf->alloc->alloc(buf->packed_size);
    if (!buf->packed_data)
      return (0);
    memcpy(buf->packed_data, &cbuf[0], size * nmemb);
    buf->offset += (ham_size_t)(size * nmemb);
  }
  /* append to an existing buffer? */
  else {
    memcpy(buf->packed_data + buf->offset, &cbuf[0], size * nmemb);
    buf->offset += (ham_size_t)(size * nmemb);
  }

  /* check if we've received the whole data */
  if (buf->offset == buf->packed_size) {
    buf->wrapper = Protocol::unpack(buf->packed_data, buf->packed_size);
    if (!buf->wrapper)
      return (0);
    buf->alloc->free(buf->packed_data);
    if (!buf->wrapper)
      return 0;
  }

  return (size * nmemb);
}

static size_t
__readfunc(char *buffer, size_t size, size_t nmemb, void *ptr)
{
  curl_buffer_t *buf = (curl_buffer_t *)ptr;
  size_t remaining = buf->packed_size-buf->offset;

  if (remaining == 0)
    return (0);

  if (nmemb > remaining)
    nmemb = remaining;

  memcpy(buffer, buf->packed_data + buf->offset, nmemb);
  buf->offset += (ham_size_t)nmemb;
  return (nmemb);
}

#define SETOPT(curl, opt, val)                                      \
          if ((cc = curl_easy_setopt(curl, opt, val))) {            \
            ham_log(("curl_easy_setopt failed: %d/%s", cc,          \
                     curl_easy_strerror(cc)));                      \
            return (HAM_INTERNAL_ERROR);                            \
          }

static ham_status_t
_perform_request(Environment *env, CURL *handle, Protocol *request,
                Protocol **reply)
{
  CURLcode cc;
  long response = 0;
  char header[128];
  curl_buffer_t rbuf = {0};
  curl_buffer_t wbuf = {0};
  struct curl_slist *slist = 0;

  wbuf.alloc = env->get_allocator();

  *reply = 0;

  if (!request->pack(wbuf.alloc, &rbuf.packed_data, &rbuf.packed_size)) {
    ham_log(("protoype Protocol::pack failed"));
    return (HAM_INTERNAL_ERROR);
  }

  sprintf(header, "Content-Length: %u", rbuf.packed_size);
  slist = curl_slist_append(slist, header);
  slist = curl_slist_append(slist, "Transfer-Encoding:");
  slist = curl_slist_append(slist, "Expect:");

#ifdef HAM_DEBUG
  SETOPT(handle, CURLOPT_VERBOSE, 1);
#endif
  SETOPT(handle, CURLOPT_URL, env->get_filename().c_str());
  SETOPT(handle, CURLOPT_READFUNCTION, __readfunc);
  SETOPT(handle, CURLOPT_READDATA, &rbuf);
  SETOPT(handle, CURLOPT_UPLOAD, 1);
  SETOPT(handle, CURLOPT_PUT, 1);
  SETOPT(handle, CURLOPT_WRITEFUNCTION, __writefunc);
  SETOPT(handle, CURLOPT_WRITEDATA, &wbuf);
  SETOPT(handle, CURLOPT_HTTPHEADER, slist);

  cc = curl_easy_perform(handle);

  if (rbuf.packed_data)
    env->get_allocator()->free(rbuf.packed_data);
  curl_slist_free_all(slist);

  if (cc) {
    ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
    return (HAM_NETWORK_ERROR);
  }

  cc = curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response);
  if (cc) {
    ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
    return (HAM_NETWORK_ERROR);
  }

  if (response != 200) {
    ham_trace(("server returned error %u", response));
    return (HAM_NETWORK_ERROR);
  }

  *reply = wbuf.wrapper;

  return (0);
}

static ham_status_t
_remote_fun_create(Environment *env, const char *filename, ham_u32_t flags,
                ham_u32_t mode, const ham_parameter_t *param)
{
  ham_status_t st;
  Protocol *reply = 0;
  CURL *handle = curl_easy_init();

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  st = _perform_request(env, handle, &request, &reply);
  if (st) {
    curl_easy_cleanup(handle);
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->type() == Protocol::CONNECT_REPLY);

  st = reply->connect_reply().status();
  if (st == 0) {
    env->set_curl(handle);
    env->set_flags(env->get_flags() | reply->connect_reply().env_flags());
  }

  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_open(Environment *env, const char *filename, ham_u32_t flags,
                const ham_parameter_t *param)
{
  ham_status_t st;
  Protocol *reply = 0;
  CURL *handle = curl_easy_init();

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  st = _perform_request(env, handle, &request, &reply);
  if (st) {
    curl_easy_cleanup(handle);
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->type() == Protocol::CONNECT_REPLY);

  st = reply->connect_reply().status();
  if (st == 0) {
    env->set_curl(handle);
    env->set_flags(env->get_flags() | reply->connect_reply().env_flags());
  }

  delete reply;

  return (st);
}

static ham_status_t
_remote_fun_rename_db(Environment *env, ham_u16_t oldname,
                ham_u16_t newname, ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_RENAME_REQUEST);
  request.mutable_env_rename_request()->set_oldname(oldname);
  request.mutable_env_rename_request()->set_newname(newname);
  request.mutable_env_rename_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_rename_reply());

  st = reply->env_rename_reply().status();

  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_erase_db(Environment *env, ham_u16_t name, ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_ERASE_DB_REQUEST);
  request.mutable_env_erase_db_request()->set_name(name);
  request.mutable_env_erase_db_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_erase_db_reply());

  st = reply->env_erase_db_reply().status();

  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_get_database_names(Environment *env, ham_u16_t *names,
            ham_size_t *count)
{
  ham_status_t st;
  ham_size_t i;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_GET_DATABASE_NAMES_REQUEST);
  request.mutable_env_get_database_names_request();

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_get_database_names_reply());

  st = reply->env_get_database_names_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  /* copy the retrieved names */
  for (i = 0;
      i < (ham_size_t)reply->env_get_database_names_reply().names_size()
        && i < *count;
      i++) {
    names[i] = (ham_u16_t)*(reply->mutable_env_get_database_names_reply()->mutable_names()->mutable_data() + i);
  }

  *count = i;

  delete reply;
  return (0);
}

static ham_status_t
_remote_fun_env_get_parameters(Environment *env, ham_parameter_t *param)
{
  static char filename[1024];
  Protocol *reply = 0;
  ham_parameter_t *p = param;

  if (!param)
    return (HAM_INV_PARAMETER);

  Protocol request(Protocol::ENV_GET_PARAMETERS_REQUEST);
  while (p && p->name != 0) {
    request.mutable_env_get_parameters_request()->add_names(p->name);
    p++;
  }

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_get_parameters_reply());

  st = reply->env_get_parameters_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  p = param;
  while (p && p->name) {
    switch (p->name) {
    case HAM_PARAM_CACHESIZE:
      ham_assert(reply->env_get_parameters_reply().has_cachesize());
      p->value = reply->env_get_parameters_reply().cachesize();
      break;
    case HAM_PARAM_PAGESIZE:
      ham_assert(reply->env_get_parameters_reply().has_pagesize());
      p->value = reply->env_get_parameters_reply().pagesize();
      break;
    case HAM_PARAM_MAX_ENV_DATABASES:
      ham_assert(reply->env_get_parameters_reply().has_max_env_databases());
      p->value = reply->env_get_parameters_reply().max_env_databases();
      break;
    case HAM_PARAM_GET_FLAGS:
      ham_assert(reply->env_get_parameters_reply().has_flags());
      p->value = reply->env_get_parameters_reply().flags();
      break;
    case HAM_PARAM_GET_FILEMODE:
      ham_assert(reply->env_get_parameters_reply().has_filemode());
      p->value = reply->env_get_parameters_reply().filemode();
      break;
    case HAM_PARAM_GET_FILENAME:
      if (reply->env_get_parameters_reply().has_filename()) {
        strncpy(filename, reply->env_get_parameters_reply().filename().c_str(),
              sizeof(filename));
        p->value = (ham_u64_t)(&filename[0]);
      }
      break;
    default:
      ham_trace(("unknown parameter %d", (int)p->name));
      break;
    }
    p++;
  }

  delete reply;
  return (0);
}

static ham_status_t
_remote_fun_env_flush(Environment *env, ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_FLUSH_REQUEST);
  request.mutable_env_flush_request()->set_flags(flags);

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_flush_reply());

  st = reply->env_flush_reply().status();
  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_create_db(Environment *env, Database *db, ham_u16_t dbname,
                ham_u32_t flags, const ham_parameter_t *param)
{
  Protocol *reply = 0;
  const ham_parameter_t *p;

  Protocol request(Protocol::ENV_CREATE_DB_REQUEST);
  request.mutable_env_create_db_request()->set_dbname(dbname);
  request.mutable_env_create_db_request()->set_flags(flags);

  p = param;
  if (p) {
    for (; p->name; p++) {
      request.mutable_env_create_db_request()->add_param_names(p->name);
      request.mutable_env_create_db_request()->add_param_values(p->value);
    }
  }

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_create_db_reply());

  st = reply->env_create_db_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  db->set_remote_handle(reply->env_create_db_reply().db_handle());
  db->set_rt_flags(reply->env_create_db_reply().db_flags());

  /* store the env pointer in the database */
  db->set_env(env);

  delete reply;

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  db->set_next(env->get_databases());
  env->set_databases(db);

  /* initialize the remaining function pointers in Database */
  return (db->initialize_remote());
}

static ham_status_t
_remote_fun_open_db(Environment *env, Database *db, ham_u16_t dbname,
                ham_u32_t flags, const ham_parameter_t *param)
{
  Protocol *reply = 0;
  const ham_parameter_t *p;

  Protocol request(Protocol::ENV_OPEN_DB_REQUEST);
  request.mutable_env_open_db_request()->set_dbname(dbname);
  request.mutable_env_open_db_request()->set_flags(flags);

  p = param;
  if (p) {
    for (; p->name; p++) {
      request.mutable_env_open_db_request()->add_param_names(p->name);
      request.mutable_env_open_db_request()->add_param_values(p->value);
    }
  }

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_env_open_db_reply());

  st = reply->env_open_db_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  /* store the env pointer in the database */
  db->set_env(env);
  db->set_remote_handle(reply->env_open_db_reply().db_handle());
  db->set_rt_flags(reply->env_open_db_reply().db_flags());

  delete reply;

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  db->set_next(env->get_databases());
  env->set_databases(db);

  /* initialize the remaining function pointers in Database */
  return (db->initialize_remote());
}

static ham_status_t
_remote_fun_env_close(Environment *env, ham_u32_t flags)
{
  (void)flags;

  if (env->get_curl()) {
    curl_easy_cleanup(env->get_curl());
    env->set_curl(0);
  }

  return (0);
}

static ham_status_t
_remote_fun_txn_begin(Environment *env, Transaction **txn, const char *name,
                ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_BEGIN_REQUEST);
  request.mutable_txn_begin_request()->set_flags(flags);
  if (name)
    request.mutable_txn_begin_request()->set_name(name);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_txn_begin_reply());

  st = reply->txn_begin_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  st = txn_begin(txn, env, name, flags);
  if (st)
    *txn = 0;
  else
    txn_set_remote_handle(*txn, reply->txn_begin_reply().txn_handle());

  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_txn_commit(Environment *env, Transaction *txn, ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_COMMIT_REQUEST);
  request.mutable_txn_commit_request()->set_txn_handle(txn_get_remote_handle(txn));
  request.mutable_txn_commit_request()->set_flags(flags);

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_txn_commit_reply());

  st = reply->txn_commit_reply().status();
  if (st == 0) {
    env_remove_txn(env, txn);
    txn_free(txn);
  }

  delete reply;
  return (st);
}

static ham_status_t
_remote_fun_txn_abort(Environment *env, Transaction *txn, ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_ABORT_REQUEST);
  request.mutable_txn_abort_request()->set_txn_handle(txn_get_remote_handle(txn));
  request.mutable_txn_abort_request()->set_flags(flags);

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_txn_abort_reply());

  st = reply->txn_abort_reply().status();
  if (st == 0) {
    env_remove_txn(env, txn);
    txn_free(txn);
  }

  delete reply;
  return (st);
}


ham_status_t
env_initialize_remote(Environment *env)
{
  env->_fun_create             =_remote_fun_create;
  env->_fun_open               =_remote_fun_open;
  env->_fun_rename_db          =_remote_fun_rename_db;
  env->_fun_erase_db           =_remote_fun_erase_db;
  env->_fun_get_database_names =_remote_fun_get_database_names;
  env->_fun_get_parameters     =_remote_fun_env_get_parameters;
  env->_fun_flush              =_remote_fun_env_flush;
  env->_fun_create_db          =_remote_fun_create_db;
  env->_fun_open_db            =_remote_fun_open_db;
  env->_fun_close              =_remote_fun_env_close;
  env->_fun_txn_begin          =_remote_fun_txn_begin;
  env->_fun_txn_commit         =_remote_fun_txn_commit;
  env->_fun_txn_abort          =_remote_fun_txn_abort;

  env->set_flags(env->get_flags() | DB_IS_REMOTE);

  return (HAM_SUCCESS);
}


ham_status_t
DatabaseImplementationRemote::get_parameters(ham_parameter_t *param)
{
  static char filename[1024];
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;
  ham_parameter_t *p;

  Protocol request(Protocol::DB_GET_PARAMETERS_REQUEST);
  request.mutable_db_get_parameters_request()->set_db_handle(m_db->get_remote_handle());

  p = param;
  if (p) {
    for (; p->name; p++)
      request.mutable_db_get_parameters_request()->add_names(p->name);
  }

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_get_parameters_reply());

  st = reply->db_get_parameters_reply().status();
  if (st) {
    delete reply;
    return (st);
  }

  p = param;
  while (p && p->name) {
    switch (p->name) {
    case HAM_PARAM_CACHESIZE:
      ham_assert(reply->db_get_parameters_reply().has_cachesize());
      p->value = reply->db_get_parameters_reply().cachesize();
      break;
    case HAM_PARAM_PAGESIZE:
      ham_assert(reply->db_get_parameters_reply().has_pagesize());
      p->value = reply->db_get_parameters_reply().pagesize();
      break;
    case HAM_PARAM_MAX_ENV_DATABASES:
      ham_assert(reply->db_get_parameters_reply().has_max_env_databases());
      p->value = reply->db_get_parameters_reply().max_env_databases();
      break;
    case HAM_PARAM_GET_FLAGS:
      ham_assert(reply->db_get_parameters_reply().has_flags());
      p->value = reply->db_get_parameters_reply().flags();
      break;
    case HAM_PARAM_GET_FILEMODE:
      ham_assert(reply->db_get_parameters_reply().has_filemode());
      p->value = reply->db_get_parameters_reply().filemode();
      break;
    case HAM_PARAM_GET_FILENAME:
      ham_assert(reply->db_get_parameters_reply().has_filename());
      strncpy(filename, reply->db_get_parameters_reply().filename().c_str(),
            sizeof(filename));
      p->value = (ham_u64_t)(&filename[0]);
      break;
    case HAM_PARAM_KEYSIZE:
      ham_assert(reply->db_get_parameters_reply().has_keysize());
      p->value = reply->db_get_parameters_reply().keysize();
      break;
    case HAM_PARAM_GET_DATABASE_NAME:
      ham_assert(reply->db_get_parameters_reply().has_dbname());
      p->value = reply->db_get_parameters_reply().dbname();
      break;
    case HAM_PARAM_GET_KEYS_PER_PAGE:
      ham_assert(reply->db_get_parameters_reply().has_keys_per_page());
      p->value = reply->db_get_parameters_reply().keys_per_page();
      break;
    case HAM_PARAM_GET_DATA_ACCESS_MODE:
      ham_assert(reply->db_get_parameters_reply().has_dam());
      p->value = reply->db_get_parameters_reply().dam();
      break;
    default:
      ham_trace(("unknown parameter %d", (int)p->name));
      break;
    }
    p++;
  }

  delete reply;
  return (0);
}

ham_status_t
DatabaseImplementationRemote::check_integrity(Transaction *txn)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::DB_CHECK_INTEGRITY_REQUEST);
  request.mutable_db_check_integrity_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_check_integrity_request()->set_txn_handle(txn ? txn_get_remote_handle(txn) : 0);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_check_integrity_reply());

  st = reply->db_check_integrity_reply().status();

  delete reply;
  return (st);
}


ham_status_t
DatabaseImplementationRemote::get_key_count(Transaction *txn, ham_u32_t flags,
              ham_offset_t *keycount)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::DB_GET_KEY_COUNT_REQUEST);
  request.mutable_db_get_key_count_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_get_key_count_request()->set_txn_handle(txn ? txn_get_remote_handle(txn) : 0);
  request.mutable_db_get_key_count_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_get_key_count_reply());

  st = reply->db_get_key_count_reply().status();
  if (!st)
    *keycount = reply->db_get_key_count_reply().keycount();

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::insert(Transaction *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  ByteArray *arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_key_arena()
                : &txn->get_key_arena();

  /* recno: do not send the key */
  if (m_db->get_rt_flags() & HAM_RECORD_NUMBER) {
    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }
  }

  Protocol request(Protocol::DB_INSERT_REQUEST);
  request.mutable_db_insert_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_insert_request()->set_txn_handle(txn
                ? txn_get_remote_handle(txn)
                : 0);
  request.mutable_db_insert_request()->set_flags(flags);
  if (key && !(m_db->get_rt_flags() & HAM_RECORD_NUMBER))
    Protocol::assign_key(request.mutable_db_insert_request()->mutable_key(),
                    key);
  if (record)
    Protocol::assign_record(request.mutable_db_insert_request()->mutable_record(),
                    record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_insert_reply() != 0);

  st = reply->db_insert_reply().status();

  /* recno: the key was modified! */
  if (st == 0 && reply->db_insert_reply().has_key()) {
    if (reply->db_insert_reply().key().data().size() == sizeof(ham_offset_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_offset_t));
      memcpy(key->data, &reply->db_insert_reply().key().data()[0],
            sizeof(ham_offset_t));
    }
  }

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::erase(Transaction *txn, ham_key_t *key,
            ham_u32_t flags)
{
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::DB_ERASE_REQUEST);
  request.mutable_db_erase_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_erase_request()->set_txn_handle(txn
                ? txn_get_remote_handle(txn)
                : 0);
  request.mutable_db_erase_request()->set_flags(flags);
  Protocol::assign_key(request.mutable_db_erase_request()->mutable_key(), key);

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_erase_reply() != 0);

  st = reply->db_erase_reply().status();

  delete reply;
  return (st);
}


ham_status_t
DatabaseImplementationRemote::find(Transaction *txn, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::DB_FIND_REQUEST);
  request.mutable_db_find_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_find_request()->set_txn_handle(txn
                ? txn_get_remote_handle(txn)
                : 0);
  request.mutable_db_find_request()->set_flags(flags);

  if (key)
    Protocol::assign_key(request.mutable_db_find_request()->mutable_key(), key);
  if (record)
    Protocol::assign_record(request.mutable_db_find_request()->mutable_record(), record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    if (reply)
      delete reply;
    return (st);
  }

  ByteArray *key_arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_record_arena()
                : &txn->get_record_arena();

  ham_assert(reply != 0);
  ham_assert(reply->has_db_find_reply() != 0);

  st = reply->db_find_reply().status();
  if (st == 0) {
    /* approx. matching: need to copy the _flags and the key data! */
    if (reply->db_find_reply().has_key()) {
      ham_assert(key);
      key->_flags = reply->db_find_reply().key().intflags();
      key->size = reply->db_find_reply().key().data().size();
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key_arena->resize(key->size);
        key->data = key_arena->get_ptr();
      }
      memcpy(key->data, (void *)&reply->db_find_reply().key().data()[0],
            key->size);
    }
    if (reply->db_find_reply().has_record()) {
      record->size = reply->db_find_reply().record().data().size();
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        rec_arena->resize(record->size);
        record->data = rec_arena->get_ptr();
      }
      memcpy(record->data, (void *)&reply->db_find_reply().record().data()[0],
            record->size);
    }
  }

  delete reply;
  return (st);
}

Cursor *
DatabaseImplementationRemote::cursor_create(Transaction *txn, ham_u32_t flags)
{
  Environment *env = m_db->get_env();
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CREATE_REQUEST);
  request.mutable_cursor_create_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_cursor_create_request()->set_txn_handle(txn
                    ? txn_get_remote_handle(txn)
                    : 0);
  request.mutable_cursor_create_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (0);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_create_reply() != 0);

  st = reply->cursor_create_reply().status();
  if (st) {
    delete reply;
    return (0);
  }

  Cursor *c = new Cursor(m_db);
  c->set_remote_handle(reply->cursor_create_reply().cursor_handle());

  delete reply;
  return (c);
}

Cursor *
DatabaseImplementationRemote::cursor_clone(Cursor *src)
{
  Environment *env = src->get_db()->get_env();
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CLONE_REQUEST);
  request.mutable_cursor_clone_request()->set_cursor_handle(src->get_remote_handle());

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (0);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_clone_reply() != 0);

  st = reply->cursor_clone_reply().status();
  if (st) {
    delete reply;
    return (0);
  }

  Cursor *c = new Cursor(src->get_db());
  c->set_remote_handle(reply->cursor_clone_reply().cursor_handle());

  delete reply;
  return (c);
}

ham_status_t
DatabaseImplementationRemote::cursor_insert(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;
  bool send_key = true;
  Transaction *txn = cursor->get_txn();

  ByteArray *arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_key_arena()
                : &txn->get_key_arena();

  /* recno: do not send the key */
  if (m_db->get_rt_flags() & HAM_RECORD_NUMBER) {
    send_key = false;

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }
  }

  Protocol request(Protocol::CURSOR_INSERT_REQUEST);
  request.mutable_cursor_insert_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_insert_request()->set_flags(flags);
  if (send_key)
    Protocol::assign_key(request.mutable_cursor_insert_request()->mutable_key(),
              key);
  Protocol::assign_record(request.mutable_cursor_insert_request()->mutable_record(),
              record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_insert_reply() != 0);

  st = reply->cursor_insert_reply().status();

  /* recno: the key was modified! */
  if (st == 0 && reply->cursor_insert_reply().has_key()) {
    if (reply->cursor_insert_reply().key().data().size()
        == sizeof(ham_offset_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_offset_t));
      memcpy(key->data, (void *)&reply->cursor_insert_reply().key().data()[0],
            sizeof(ham_offset_t));
    }
  }

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_ERASE_REQUEST);
  request.mutable_cursor_erase_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_erase_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_erase_reply() != 0);

  st = reply->cursor_erase_reply().status();

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::cursor_find(Cursor *cursor, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_FIND_REQUEST);
  request.mutable_cursor_find_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_find_request()->set_flags(flags);
  if (key)
    Protocol::assign_key(request.mutable_cursor_find_request()->mutable_key(),
                key);
  if (record)
    Protocol::assign_record(request.mutable_cursor_find_request()->mutable_record(),
                record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  Transaction *txn = cursor->get_txn();

  ByteArray *arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_record_arena()
                : &txn->get_record_arena();

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_find_reply() != 0);

  st = reply->cursor_find_reply().status();
  if (st == 0) {
    /* approx. matching: need to copy the _flags! */
    if (reply->cursor_find_reply().has_key())
      key->_flags = reply->cursor_find_reply().key().intflags();
    if (reply->cursor_find_reply().has_record()) {
      ham_assert(record);
      record->size = reply->cursor_find_reply().record().data().size();
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        arena->resize(record->size);
        record->data = arena->get_ptr();
      }
      memcpy(record->data, (void *)&reply->cursor_find_reply().record().data()[0],
              record->size);
    }
  }

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::cursor_get_duplicate_count(Cursor *cursor,
                ham_size_t *count, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_GET_DUPLICATE_COUNT_REQUEST);
  request.mutable_cursor_get_duplicate_count_request()->set_cursor_handle(
                  cursor->get_remote_handle());
  request.mutable_cursor_get_duplicate_count_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_get_duplicate_count_reply() != 0);

  st = reply->cursor_get_duplicate_count_reply().status();
  if (st == 0)
    *count = reply->cursor_get_duplicate_count_reply().count();

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::cursor_get_record_size(Cursor *cursor,
            ham_offset_t *size)
{
  (void)cursor;
  (void)size;
  /* need this? send me a mail and i will implement it */
  return (HAM_NOT_IMPLEMENTED);
}

ham_status_t
DatabaseImplementationRemote::cursor_overwrite(Cursor *cursor,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_OVERWRITE_REQUEST);
  request.mutable_cursor_overwrite_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_overwrite_request()->set_flags(flags);
  Protocol::assign_record(request.mutable_cursor_overwrite_request()->mutable_record(),
                    record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_overwrite_reply() != 0);

  st = reply->cursor_overwrite_reply().status();

  delete reply;
  return (st);
}

ham_status_t
DatabaseImplementationRemote::cursor_move(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  Transaction *txn = cursor->get_txn();
  ByteArray *key_arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn_get_flags(txn) & HAM_TXN_TEMPORARY))
                ? &m_db->get_record_arena()
                : &txn->get_record_arena();

  Protocol request(Protocol::CURSOR_MOVE_REQUEST);
  request.mutable_cursor_move_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_move_request()->set_flags(flags);
  if (key)
    Protocol::assign_key(request.mutable_cursor_move_request()->mutable_key(),
                  key);
  if (record)
    Protocol::assign_record(request.mutable_cursor_move_request()->mutable_record(),
                  record);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_move_reply() != 0);

  st = reply->cursor_move_reply().status();
  if (st)
    goto bail;

  /* modify key/record, but make sure that USER_ALLOC is respected! */
  if (reply->cursor_move_reply().has_key()) {
    ham_assert(key);
    key->_flags = reply->cursor_move_reply().key().intflags();
    key->size = reply->cursor_move_reply().key().data().size();
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
  delete reply;
  return (st);
}

void
DatabaseImplementationRemote::cursor_close(Cursor *cursor)
{
  Environment *env = cursor->get_db()->get_env();
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CLOSE_REQUEST);
  request.mutable_cursor_close_request()->set_cursor_handle(cursor->get_remote_handle());

  ham_status_t st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return;
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_close_reply() != 0);

  delete reply;
}

ham_status_t
DatabaseImplementationRemote::close(ham_u32_t flags)
{
  ham_status_t st;
  Environment *env = m_db->get_env();
  Protocol *reply = 0;

  /* auto-cleanup cursors?  */
  if (flags & HAM_AUTO_CLEANUP) {
    Cursor *cursor = m_db->get_cursors();
    while ((cursor = m_db->get_cursors()))
      m_db->close_cursor(cursor);
  }
  else if (m_db->get_cursors())
    return (HAM_CURSOR_STILL_OPEN);

  Protocol request(Protocol::DB_CLOSE_REQUEST);
  request.mutable_db_close_request()->set_db_handle(m_db->get_remote_handle());
  request.mutable_db_close_request()->set_flags(flags);

  st = _perform_request(env, env->get_curl(), &request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  /* free cached memory */
  m_db->get_key_arena().clear();
  m_db->get_record_arena().clear();

  ham_assert(reply != 0);
  ham_assert(reply->has_db_close_reply());

  st = reply->db_close_reply().status();
  if (st == 0)
    m_db->set_remote_handle(0);

  delete reply;
  return (st);
}

} // namespace ham

#else // HAM_ENABLE_REMOTE

namespace ham {

ham_status_t
env_initialize_remote(Environment *env)
{
	return HAM_NOT_IMPLEMENTED;
}

} // namespace ham

#endif // HAM_ENABLE_REMOTE

