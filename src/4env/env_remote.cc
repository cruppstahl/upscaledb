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

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"
#include "2protobuf/protocol.h"
#include "4cursor/cursor.h"
#include "4db/db_remote.h"
#include "4env/env_remote.h"
#include "4txn/txn_remote.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

RemoteEnvironment::RemoteEnvironment()
  : Environment(), m_remote_handle(0), m_buffer(1024 * 4), m_timeout(0)
{
}

Protocol *
RemoteEnvironment::perform_request(Protocol *request)
{
  // use ByteArray to avoid frequent reallocs!
  m_buffer.clear();

  if (!request->pack(&m_buffer)) {
    ham_log(("protoype Protocol::pack failed"));
    throw Exception(HAM_INTERNAL_ERROR);
  }

  m_socket.send((ham_u8_t *)m_buffer.get_ptr(), m_buffer.get_size());

  // now block and wait for the reply; first read the header, then the
  // remaining data
  m_socket.recv((ham_u8_t *)m_buffer.get_ptr(), 8);

  // no need to check the magic; it's verified in Protocol::unpack
  ham_u32_t size = *(ham_u32_t *)((char *)m_buffer.get_ptr() + 4);
  m_buffer.resize(size + 8);
  m_socket.recv((ham_u8_t *)m_buffer.get_ptr() + 8, size);

  return (Protocol::unpack((const ham_u8_t *)m_buffer.get_ptr(), size + 8));
}

void
RemoteEnvironment::perform_request(SerializedWrapper *request,
                SerializedWrapper *reply)
{
  int size_left = (int)request->get_size();
  request->size = size_left;
  request->magic = HAM_TRANSFER_MAGIC_V2;
  m_buffer.resize(request->size);

  ham_u8_t *ptr = (ham_u8_t *)m_buffer.get_ptr();
  request->serialize(&ptr, &size_left);
  ham_assert(size_left == 0);

  m_socket.send((ham_u8_t *)m_buffer.get_ptr(), request->size);

  // now block and wait for the reply; first read the header, then the
  // remaining data
  m_socket.recv((ham_u8_t *)m_buffer.get_ptr(), 8);

  // now check the magic and receive the remaining data
  ham_u32_t magic = *(ham_u32_t *)((char *)m_buffer.get_ptr() + 0);
  if (magic != HAM_TRANSFER_MAGIC_V2)
    throw Exception(HAM_INTERNAL_ERROR);
  // TODO check the magic
  int size = (int)*(ham_u32_t *)((char *)m_buffer.get_ptr() + 4);
  m_buffer.resize(size);
  m_socket.recv((ham_u8_t *)m_buffer.get_ptr() + 8, size - 8);

  ptr = (ham_u8_t *)m_buffer.get_ptr();
  reply->deserialize(&ptr, &size);
  ham_assert(size == 0);
}

ham_status_t
RemoteEnvironment::create(const char *url, ham_u32_t flags,
        ham_u32_t mode, size_t page_size, ham_u64_t cache_size,
        ham_u16_t maxdbs, ham_u64_t file_size_limit)
{
  // the 'create' operation is identical to 'open'
  return (open(url, flags, cache_size, file_size_limit));
}

ham_status_t
RemoteEnvironment::open(const char *url, ham_u32_t flags,
        ham_u64_t cache_size, ham_u64_t file_size_limit)
{
  m_socket.close();

  ham_assert(url != 0);
  ham_assert(::strstr(url, "ham://") == url);
  const char *ip = url + 6;
  const char *port_str = strstr(ip, ":");
  if (!port_str) {
    ham_trace(("remote uri does not include port - expected "
                "`ham://<ip>:<port>`"));
    return (HAM_INV_PARAMETER);
  }
  ham_u16_t port = (ham_u16_t)atoi(port_str + 1);
  if (!port) {
    ham_trace(("remote uri includes invalid port - expected "
                "`ham://<ip>:<port>`"));
    return (HAM_INV_PARAMETER);
  }

  const char *filename = strstr(port_str, "/");

  std::string hostname(ip, port_str);
  m_socket.connect(hostname.c_str(), port, m_timeout);

  Protocol request(Protocol::CONNECT_REQUEST);
  request.mutable_connect_request()->set_path(filename);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->type() == Protocol::CONNECT_REPLY);

  ham_status_t st = reply->connect_reply().status();
  if (st == 0) {
    m_filename = url;
    set_flags(flags | reply->connect_reply().env_flags());
    m_remote_handle = reply->connect_reply().env_handle();

    if (get_flags() & HAM_ENABLE_TRANSACTIONS)
      m_txn_manager.reset(new RemoteTransactionManager(this));
  }

  return (st);
}

ham_status_t
RemoteEnvironment::rename_db( ham_u16_t oldname, ham_u16_t newname,
        ham_u32_t flags)
{
  Protocol request(Protocol::ENV_RENAME_REQUEST);
  request.mutable_env_rename_request()->set_env_handle(m_remote_handle);
  request.mutable_env_rename_request()->set_oldname(oldname);
  request.mutable_env_rename_request()->set_newname(newname);
  request.mutable_env_rename_request()->set_flags(flags);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_rename_reply());

  return (reply->env_rename_reply().status());
}

ham_status_t
RemoteEnvironment::erase_db(ham_u16_t name, ham_u32_t flags)
{
  Protocol request(Protocol::ENV_ERASE_DB_REQUEST);
  request.mutable_env_erase_db_request()->set_env_handle(m_remote_handle);
  request.mutable_env_erase_db_request()->set_name(name);
  request.mutable_env_erase_db_request()->set_flags(flags);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_erase_db_reply());

  return (reply->env_erase_db_reply().status());
}

ham_status_t
RemoteEnvironment::get_database_names(ham_u16_t *names, ham_u32_t *count)
{
  Protocol request(Protocol::ENV_GET_DATABASE_NAMES_REQUEST);
  request.mutable_env_get_database_names_request();
  request.mutable_env_get_database_names_request()->set_env_handle(m_remote_handle);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_get_database_names_reply());

  ham_status_t st = reply->env_get_database_names_reply().status();
  if (st)
    return (st);

  /* copy the retrieved names */
  ham_u32_t i;
  for (i = 0;
      i < (ham_u32_t)reply->env_get_database_names_reply().names_size()
        && i < *count;
      i++) {
    names[i] = (ham_u16_t)*(reply->mutable_env_get_database_names_reply()->mutable_names()->mutable_data() + i);
  }

  *count = i;

  return (0);
}

ham_status_t
RemoteEnvironment::get_parameters(ham_parameter_t *param)
{
  static char filename[1024]; // TODO not threadsafe!!
  ham_parameter_t *p = param;

  if (!param)
    return (HAM_INV_PARAMETER);

  Protocol request(Protocol::ENV_GET_PARAMETERS_REQUEST);
  request.mutable_env_get_parameters_request()->set_env_handle(m_remote_handle);
  while (p && p->name != 0) {
    request.mutable_env_get_parameters_request()->add_names(p->name);
    p++;
  }

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_get_parameters_reply());

  ham_status_t st = reply->env_get_parameters_reply().status();
  if (st)
    return (st);

  p = param;
  while (p && p->name) {
    switch (p->name) {
    case HAM_PARAM_CACHESIZE:
      ham_assert(reply->env_get_parameters_reply().has_cache_size());
      p->value = reply->env_get_parameters_reply().cache_size();
      break;
    case HAM_PARAM_PAGESIZE:
      ham_assert(reply->env_get_parameters_reply().has_page_size());
      p->value = reply->env_get_parameters_reply().page_size();
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
              sizeof(filename) - 1);
        filename[sizeof(filename) - 1] = 0;
        p->value = (ham_u64_t)(&filename[0]);
      }
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
RemoteEnvironment::flush(ham_u32_t flags)
{
  Protocol request(Protocol::ENV_FLUSH_REQUEST);
  request.mutable_env_flush_request()->set_flags(flags);
  request.mutable_env_flush_request()->set_env_handle(m_remote_handle);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_flush_reply());

  return (reply->env_flush_reply().status());
}

ham_status_t
RemoteEnvironment::create_db(Database **pdb, ham_u16_t dbname, ham_u32_t flags,
        const ham_parameter_t *param)
{
  const ham_parameter_t *p;

  Protocol request(Protocol::ENV_CREATE_DB_REQUEST);
  request.mutable_env_create_db_request()->set_env_handle(m_remote_handle);
  request.mutable_env_create_db_request()->set_dbname(dbname);
  request.mutable_env_create_db_request()->set_flags(flags);

  p = param;
  if (p) {
    for (; p->name; p++) {
      request.mutable_env_create_db_request()->add_param_names(p->name);
      request.mutable_env_create_db_request()->add_param_values(p->value);
    }
  }

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_create_db_reply());

  ham_status_t st = reply->env_create_db_reply().status();
  if (st)
    return (st);

  RemoteDatabase *rdb = new RemoteDatabase(this, dbname,
          reply->env_create_db_reply().db_flags());

  rdb->set_remote_handle(reply->env_create_db_reply().db_handle());
  *pdb = rdb;

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
  const ham_parameter_t *p;

  /* make sure that this database is not yet open */
  if (get_database_map().find(dbname) !=  get_database_map().end())
    return (HAM_DATABASE_ALREADY_OPEN);

  Protocol request(Protocol::ENV_OPEN_DB_REQUEST);
  request.mutable_env_open_db_request()->set_env_handle(m_remote_handle);
  request.mutable_env_open_db_request()->set_dbname(dbname);
  request.mutable_env_open_db_request()->set_flags(flags);

  p = param;
  if (p) {
    for (; p->name; p++) {
      request.mutable_env_open_db_request()->add_param_names(p->name);
      request.mutable_env_open_db_request()->add_param_values(p->value);
    }
  }

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->has_env_open_db_reply());

  ham_status_t st = reply->env_open_db_reply().status();
  if (st)
    return (st);

  RemoteDatabase *rdb = new RemoteDatabase(this, dbname,
          reply->env_open_db_reply().db_flags());
  rdb->set_remote_handle(reply->env_open_db_reply().db_handle());
  *pdb = rdb;

  // on success: store the open database in the environment's list of
  // opened databases
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

  Protocol request(Protocol::DISCONNECT_REQUEST);
  request.mutable_disconnect_request()->set_env_handle(m_remote_handle);

  std::auto_ptr<Protocol> reply(perform_request(&request));

  ham_assert(reply->type() == Protocol::DISCONNECT_REPLY);

  st = reply->disconnect_reply().status();
  if (st == 0) {
    m_socket.close();
    m_remote_handle = 0;
  }

  return (st);
}

Transaction *
RemoteEnvironment::txn_begin(const char *name, ham_u32_t flags)
{
  return (m_txn_manager->begin(name, flags));
}

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

