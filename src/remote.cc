/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include "db_remote.h"
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
    buf->packed_data = Memory::allocate<ham_u8_t>(buf->packed_size);
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
    Memory::release(buf->packed_data);
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

  *reply = 0;

  if (!request->pack(&rbuf.packed_data, &rbuf.packed_size)) {
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

  Memory::release(rbuf.packed_data);
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

  /* make sure that this database is not yet open */
  if (get_database_map().find(dbname) !=  get_database_map().end())
    return (HAM_DATABASE_ALREADY_OPEN);

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
  ham_status_t st = 0;
  (void)flags;

  /* close all databases */
  Environment::DatabaseMap::iterator it = get_database_map().begin();
  while (it != get_database_map().end()) {
    Environment::DatabaseMap::iterator it2 = it; it++;
    Database *db = it2->second;
    if (flags & HAM_AUTO_CLEANUP)
      st = ham_db_close((ham_db_t *)db, flags | HAM_DONT_LOCK);
    else
      st = db->close(flags);
    if (st)
      return (st);
  }

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

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

