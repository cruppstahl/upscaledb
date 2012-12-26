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

#ifdef HAM_ENABLE_REMOTE

#define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#include <curl/curl.h>
#include <curl/easy.h>

#include "protocol/protocol.h"

namespace hamsterdb {

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

ham_status_t
RemoteEnvironment::perform_request(Protocol *request, Protocol **reply)
{
  CURLcode cc;
  long response = 0;
  char header[128];
  curl_buffer_t rbuf = {0};
  curl_buffer_t wbuf = {0};
  struct curl_slist *slist = 0;

  wbuf.alloc = get_allocator();

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
  SETOPT(m_curl, CURLOPT_VERBOSE, 1);
#endif
  SETOPT(m_curl, CURLOPT_URL, get_filename().c_str());
  SETOPT(m_curl, CURLOPT_READFUNCTION, __readfunc);
  SETOPT(m_curl, CURLOPT_READDATA, &rbuf);
  SETOPT(m_curl, CURLOPT_UPLOAD, 1);
  SETOPT(m_curl, CURLOPT_PUT, 1);
  SETOPT(m_curl, CURLOPT_WRITEFUNCTION, __writefunc);
  SETOPT(m_curl, CURLOPT_WRITEDATA, &wbuf);
  SETOPT(m_curl, CURLOPT_HTTPHEADER, slist);

  cc = curl_easy_perform(m_curl);

  if (rbuf.packed_data)
    get_allocator()->free(rbuf.packed_data);
  curl_slist_free_all(slist);

  if (cc) {
    ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
    return (HAM_NETWORK_ERROR);
  }

  cc = curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &response);
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

ham_status_t
RemoteEnvironment::create(const char *filename, ham_u32_t flags,
        ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
        ham_u16_t maxdbs)
{
  ham_status_t st;
  Protocol *reply = 0;
  m_curl = curl_easy_init();

  set_flags(flags);
  if (filename)
    set_filename(filename);

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  st = perform_request(&request, &reply);
  if (st) {
    curl_easy_cleanup(m_curl);
    m_curl = 0;
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->type() == Protocol::CONNECT_REPLY);

  st = reply->connect_reply().status();
  if (st == 0)
    set_flags(get_flags() | reply->connect_reply().env_flags());

  delete reply;
  return (st);
}

ham_status_t
RemoteEnvironment::open(const char *filename, ham_u32_t flags,
        ham_size_t cachesize)
{
  ham_status_t st;
  Protocol *reply = 0;
  m_curl = curl_easy_init();

  set_flags(flags);
  if (filename)
    set_filename(filename);

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  st = perform_request(&request, &reply);
  if (st) {
    curl_easy_cleanup(m_curl);
    m_curl = 0;
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->type() == Protocol::CONNECT_REPLY);

  st = reply->connect_reply().status();
  if (st == 0)
    set_flags(get_flags() | reply->connect_reply().env_flags());

  delete reply;

  return (st);
}

ham_status_t
RemoteEnvironment::rename_db( ham_u16_t oldname, ham_u16_t newname,
        ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_RENAME_REQUEST);
  request.mutable_env_rename_request()->set_oldname(oldname);
  request.mutable_env_rename_request()->set_newname(newname);
  request.mutable_env_rename_request()->set_flags(flags);

  st = perform_request(&request, &reply);
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

ham_status_t
RemoteEnvironment::erase_db(ham_u16_t name, ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_ERASE_DB_REQUEST);
  request.mutable_env_erase_db_request()->set_name(name);
  request.mutable_env_erase_db_request()->set_flags(flags);

  st = perform_request(&request, &reply);
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

ham_status_t
RemoteEnvironment::get_database_names(ham_u16_t *names, ham_size_t *count)
{
  ham_status_t st;
  ham_size_t i;
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_GET_DATABASE_NAMES_REQUEST);
  request.mutable_env_get_database_names_request();

  st = perform_request(&request, &reply);
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

ham_status_t
RemoteEnvironment::get_parameters(ham_parameter_t *param)
{
  static char filename[1024]; // TODO not threadsafe!!
  Protocol *reply = 0;
  ham_parameter_t *p = param;

  if (!param)
    return (HAM_INV_PARAMETER);

  Protocol request(Protocol::ENV_GET_PARAMETERS_REQUEST);
  while (p && p->name != 0) {
    request.mutable_env_get_parameters_request()->add_names(p->name);
    p++;
  }

  ham_status_t st = perform_request(&request, &reply);
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
    case HAM_PARAM_MAX_DATABASES:
      ham_assert(reply->env_get_parameters_reply().has_max_env_databases());
      p->value = reply->env_get_parameters_reply().max_env_databases();
      break;
    case HAM_PARAM_FLAGS:
      ham_assert(reply->env_get_parameters_reply().has_flags());
      p->value = reply->env_get_parameters_reply().flags();
      break;
    case HAM_PARAM_FILEMODE:
      ham_assert(reply->env_get_parameters_reply().has_filemode());
      p->value = reply->env_get_parameters_reply().filemode();
      break;
    case HAM_PARAM_FILENAME:
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

ham_status_t
RemoteEnvironment::flush(ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::ENV_FLUSH_REQUEST);
  request.mutable_env_flush_request()->set_flags(flags);

  ham_status_t st = perform_request(&request, &reply);
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

ham_status_t
RemoteEnvironment::create_db(Database **pdb, ham_u16_t dbname, ham_u32_t flags,
        const ham_parameter_t *param)
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

  ham_status_t st = perform_request(&request, &reply);
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

  RemoteDatabase *rdb = new RemoteDatabase(this, dbname,
          reply->env_create_db_reply().db_flags());

  rdb->set_remote_handle(reply->env_create_db_reply().db_handle());
  *pdb = rdb;

  delete reply;

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  get_database_map()[dbname] = *pdb;

  return (0);
}

ham_status_t
RemoteEnvironment::open_db(Database **pdb, ham_u16_t dbname, ham_u32_t flags,
        const ham_parameter_t *param)
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

  ham_status_t st = perform_request(&request, &reply);
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

  RemoteDatabase *rdb = new RemoteDatabase(this, dbname,
          reply->env_open_db_reply().db_flags());
  rdb->set_remote_handle(reply->env_open_db_reply().db_handle());
  *pdb = rdb;

  delete reply;

  /*
   * on success: store the open database in the environment's list of
   * opened databases
   */
  get_database_map()[dbname] = *pdb;

  return (0);
}

ham_status_t
RemoteEnvironment::close(ham_u32_t flags)
{
  (void)flags;

  if (m_curl) {
    curl_easy_cleanup(m_curl);
    m_curl = 0;
  }

  return (0);
}

ham_status_t
RemoteEnvironment::txn_begin(Transaction **txn, const char *name,
                ham_u32_t flags)
{
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_BEGIN_REQUEST);
  request.mutable_txn_begin_request()->set_flags(flags);
  if (name)
    request.mutable_txn_begin_request()->set_name(name);

  st = perform_request(&request, &reply);
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

  *txn = new Transaction(this, name, flags);
  (*txn)->set_remote_handle(reply->txn_begin_reply().txn_handle());

  delete reply;
  return (0);
}

ham_status_t
RemoteEnvironment::txn_commit(Transaction *txn, ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_COMMIT_REQUEST);
  request.mutable_txn_commit_request()->set_txn_handle(txn->get_remote_handle());
  request.mutable_txn_commit_request()->set_flags(flags);

  ham_status_t st = perform_request(&request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_txn_commit_reply());

  st = reply->txn_commit_reply().status();
  if (st == 0) {
    remove_txn(txn);
    delete txn;
  }

  delete reply;
  return (st);
}

ham_status_t
RemoteEnvironment::txn_abort(Transaction *txn, ham_u32_t flags)
{
  Protocol *reply = 0;

  Protocol request(Protocol::TXN_ABORT_REQUEST);
  request.mutable_txn_abort_request()->set_txn_handle(txn->get_remote_handle());
  request.mutable_txn_abort_request()->set_flags(flags);

  ham_status_t st = perform_request(&request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_txn_abort_reply());

  st = reply->txn_abort_reply().status();
  if (st == 0) {
    remove_txn(txn);
    delete txn;
  }

  delete reply;
  return (st);
}


ham_status_t
RemoteDatabase::get_parameters(ham_parameter_t *param)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;
  ham_parameter_t *p;

  Protocol request(Protocol::DB_GET_PARAMETERS_REQUEST);
  request.mutable_db_get_parameters_request()->set_db_handle(get_remote_handle());

  p = param;
  if (p) {
    for (; p->name; p++)
      request.mutable_db_get_parameters_request()->add_names(p->name);
  }

  st = env->perform_request(&request, &reply);
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
    case HAM_PARAM_FLAGS:
      ham_assert(reply->db_get_parameters_reply().has_flags());
      p->value = reply->db_get_parameters_reply().flags();
      break;
    case HAM_PARAM_KEYSIZE:
      ham_assert(reply->db_get_parameters_reply().has_keysize());
      p->value = reply->db_get_parameters_reply().keysize();
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

  delete reply;
  return (0);
}

ham_status_t
RemoteDatabase::check_integrity(Transaction *txn)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::DB_CHECK_INTEGRITY_REQUEST);
  request.mutable_db_check_integrity_request()->set_db_handle(get_remote_handle());
  request.mutable_db_check_integrity_request()->set_txn_handle(txn
            ? txn->get_remote_handle()
            : 0);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::get_key_count(Transaction *txn, ham_u32_t flags,
              ham_u64_t *keycount)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::DB_GET_KEY_COUNT_REQUEST);
  request.mutable_db_get_key_count_request()->set_db_handle(get_remote_handle());
  request.mutable_db_get_key_count_request()->set_txn_handle(txn
            ? txn->get_remote_handle()
            : 0);
  request.mutable_db_get_key_count_request()->set_flags(flags);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::insert(Transaction *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

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

  Protocol request(Protocol::DB_INSERT_REQUEST);
  request.mutable_db_insert_request()->set_db_handle(get_remote_handle());
  request.mutable_db_insert_request()->set_txn_handle(txn
                ? txn->get_remote_handle()
                : 0);
  request.mutable_db_insert_request()->set_flags(flags);
  if (key && !(get_rt_flags() & HAM_RECORD_NUMBER))
    Protocol::assign_key(request.mutable_db_insert_request()->mutable_key(),
                    key);
  if (record)
    Protocol::assign_record(request.mutable_db_insert_request()->mutable_record(),
                    record);

  st = env->perform_request(&request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_insert_reply() != 0);

  st = reply->db_insert_reply().status();

  /* recno: the key was modified! */
  if (st == 0 && reply->db_insert_reply().has_key()) {
    if (reply->db_insert_reply().key().data().size() == sizeof(ham_u64_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_u64_t));
      memcpy(key->data, &reply->db_insert_reply().key().data()[0],
            sizeof(ham_u64_t));
    }
  }

  delete reply;
  return (st);
}

ham_status_t
RemoteDatabase::erase(Transaction *txn, ham_key_t *key,
            ham_u32_t flags)
{
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::DB_ERASE_REQUEST);
  request.mutable_db_erase_request()->set_db_handle(get_remote_handle());
  request.mutable_db_erase_request()->set_txn_handle(txn
                ? txn->get_remote_handle()
                : 0);
  request.mutable_db_erase_request()->set_flags(flags);
  Protocol::assign_key(request.mutable_db_erase_request()->mutable_key(), key);

  ham_status_t st = env->perform_request(&request, &reply);
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
RemoteDatabase::find(Transaction *txn, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::DB_FIND_REQUEST);
  request.mutable_db_find_request()->set_db_handle(get_remote_handle());
  request.mutable_db_find_request()->set_txn_handle(txn
                ? txn->get_remote_handle()
                : 0);
  request.mutable_db_find_request()->set_flags(flags);

  if (key)
    Protocol::assign_key(request.mutable_db_find_request()->mutable_key(), key);
  if (record)
    Protocol::assign_record(request.mutable_db_find_request()->mutable_record(), record);

  st = env->perform_request(&request, &reply);
  if (st) {
    if (reply)
      delete reply;
    return (st);
  }

  ByteArray *key_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
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
RemoteDatabase::cursor_create(Transaction *txn, ham_u32_t flags)
{
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CREATE_REQUEST);
  request.mutable_cursor_create_request()->set_db_handle(get_remote_handle());
  request.mutable_cursor_create_request()->set_txn_handle(txn
                    ? txn->get_remote_handle()
                    : 0);
  request.mutable_cursor_create_request()->set_flags(flags);

  st = env->perform_request(&request, &reply);
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

  Cursor *c = new Cursor(this);
  c->set_remote_handle(reply->cursor_create_reply().cursor_handle());

  delete reply;
  return (c);
}

Cursor *
RemoteDatabase::cursor_clone_impl(Cursor *src)
{
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>
                                    (src->get_db()->get_env());
  ham_status_t st;
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CLONE_REQUEST);
  request.mutable_cursor_clone_request()->set_cursor_handle(src->get_remote_handle());

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::cursor_insert(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;
  bool send_key = true;
  Transaction *txn = cursor->get_txn();

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

  Protocol request(Protocol::CURSOR_INSERT_REQUEST);
  request.mutable_cursor_insert_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_insert_request()->set_flags(flags);
  if (send_key)
    Protocol::assign_key(request.mutable_cursor_insert_request()->mutable_key(),
              key);
  Protocol::assign_record(request.mutable_cursor_insert_request()->mutable_record(),
              record);

  st = env->perform_request(&request, &reply);
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
        == sizeof(ham_u64_t)) {
      ham_assert(key->data != 0);
      ham_assert(key->size == sizeof(ham_u64_t));
      memcpy(key->data, (void *)&reply->cursor_insert_reply().key().data()[0],
            sizeof(ham_u64_t));
    }
  }

  delete reply;
  return (st);
}

ham_status_t
RemoteDatabase::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_ERASE_REQUEST);
  request.mutable_cursor_erase_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_erase_request()->set_flags(flags);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::cursor_find(Cursor *cursor, ham_key_t *key,
              ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
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

  st = env->perform_request(&request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  Transaction *txn = cursor->get_txn();

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
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
RemoteDatabase::cursor_get_duplicate_count(Cursor *cursor,
                ham_size_t *count, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_GET_DUPLICATE_COUNT_REQUEST);
  request.mutable_cursor_get_duplicate_count_request()->set_cursor_handle(
                  cursor->get_remote_handle());
  request.mutable_cursor_get_duplicate_count_request()->set_flags(flags);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::cursor_get_record_size(Cursor *cursor,
            ham_u64_t *size)
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
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_OVERWRITE_REQUEST);
  request.mutable_cursor_overwrite_request()->set_cursor_handle(cursor->get_remote_handle());
  request.mutable_cursor_overwrite_request()->set_flags(flags);
  Protocol::assign_record(request.mutable_cursor_overwrite_request()->mutable_record(),
                    record);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Transaction *txn = cursor->get_txn();
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
                  key);
  if (record)
    Protocol::assign_record(request.mutable_cursor_move_request()->mutable_record(),
                  record);

  st = env->perform_request(&request, &reply);
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
RemoteDatabase::cursor_close_impl(Cursor *cursor)
{
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>
                                (cursor->get_db()->get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::CURSOR_CLOSE_REQUEST);
  request.mutable_cursor_close_request()->set_cursor_handle(cursor->get_remote_handle());

  ham_status_t st = env->perform_request(&request, &reply);
  if (st) {
    delete reply;
    return;
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_cursor_close_reply() != 0);

  delete reply;
}

ham_status_t
RemoteDatabase::close_impl(ham_u32_t flags)
{
  ham_status_t st;
  RemoteEnvironment *env = dynamic_cast<RemoteEnvironment *>(get_env());
  Protocol *reply = 0;

  Protocol request(Protocol::DB_CLOSE_REQUEST);
  request.mutable_db_close_request()->set_db_handle(get_remote_handle());
  request.mutable_db_close_request()->set_flags(flags);

  st = env->perform_request(&request, &reply);
  if (st) {
    delete reply;
    return (st);
  }

  ham_assert(reply != 0);
  ham_assert(reply->has_db_close_reply());

  st = reply->db_close_reply().status();
  if (st == 0)
    set_remote_handle(0);

  delete reply;
  return (st);
}

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

