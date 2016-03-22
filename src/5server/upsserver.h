/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_UPSSERVER_H
#define UPS_UPSSERVER_H

#include "0root/root.h"

#include <vector>

#include <uv.h>

#include "ups/upscaledb.h"
#include "ups/types.h"
#include "ups/upscaledb_srv.h"

#include "1base/mutex.h"
#include "4db/db.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

struct ups_srv_t {
  bool dummy;
};

namespace upscaledb {

template<typename T>
struct Handle {
  Handle(uint64_t _index, T *_object)
    : index(_index), object(_object) {
  }

  uint64_t index;
  T *object;
};

typedef std::vector< Handle<Env> > EnvironmentVector;
typedef std::vector< Handle<Db> > DatabaseVector;
typedef std::vector< Handle<Cursor> > CursorVector;
typedef std::vector< Handle<Txn> > TxnVector;
typedef std::map<std::string, Env *> EnvironmentMap;

struct ServerContext {
  ServerContext()
    : thread_id(0), _handle_counter(1) {
    ::memset(&server, 0, sizeof(server));
    ::memset(&async, 0, sizeof(async));
  }

  // allocates a new handle
  // TODO the allocate_handle methods have lots of duplicate code;
  // try to find a generic solution!
  uint64_t allocate_handle(Env *env) {
    uint64_t c = 0;
    for (EnvironmentVector::iterator it = _environments.begin();
            it != _environments.end(); it++, c++) {
      if (it->index == 0) {
        c |= _handle_counter << 32;
        _handle_counter++;
        it->index = c;
        it->object = env;
        return c;
      }
    }

    c = _environments.size() | _handle_counter << 32;
    _handle_counter++;
    _environments.push_back(Handle<Env>(c, env));
    return c;
  }

  uint64_t allocate_handle(Db *db) {
    uint64_t c = 0;
    for (DatabaseVector::iterator it = _databases.begin();
            it != _databases.end(); it++, c++) {
      if (it->index == 0) {
        c |= _handle_counter << 32;
        _handle_counter++;
        it->index = c;
        it->object = db;
        return c;
      }
    }

    c = _databases.size() | _handle_counter << 32;
    _handle_counter++;
    _databases.push_back(Handle<Db>(c, db));
    return c;
  }

  uint64_t allocate_handle(Txn *txn) {
    uint64_t c = 0;
    for (TxnVector::iterator it = _transactions.begin();
            it != _transactions.end(); it++, c++) {
      if (it->index == 0) {
        c |= _handle_counter << 32;
        _handle_counter++;
        it->index = c;
        it->object = txn;
        return c;
      }
    }

    c = _transactions.size() | _handle_counter << 32;
    _handle_counter++;
    _transactions.push_back(Handle<Txn>(c, txn));
    return c;
  }

  uint64_t allocate_handle(Cursor *cursor) {
    uint64_t c = 0;
    for (CursorVector::iterator it = _cursors.begin();
            it != _cursors.end(); it++, c++) {
      if (it->index == 0) {
        c |= _handle_counter << 32;
        _handle_counter++;
        it->index = c;
        it->object = cursor;
        return c;
      }
    }

    c = _cursors.size() | _handle_counter << 32;
    _handle_counter++;
    _cursors.push_back(Handle<Cursor>(c, cursor));
    return c;
  }

  void remove_env_handle(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    //assert(index < _environments.size());
    if (unlikely(index >= _environments.size()))
      return;
    EnvironmentVector::iterator it = _environments.begin() + index;
    // assert(it->index == handle);
    if (unlikely(it->index != handle))
      return;
    it->index = 0;
    it->object = 0;
  }

  void remove_db_handle(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _databases.size());
    if (unlikely(index >= _databases.size()))
      return;
    DatabaseVector::iterator it = _databases.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return;
    it->index = 0;
    it->object = 0;
  }

  void remove_txn_handle(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _transactions.size());
    if (unlikely(index >= _transactions.size()))
      return;
    TxnVector::iterator it = _transactions.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return;
    it->index = 0;
    it->object = 0;
  }

  void remove_cursor_handle(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _cursors.size());
    if (unlikely(index >= _cursors.size()))
      return;
    CursorVector::iterator it = _cursors.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return;
    it->index = 0;
    it->object = 0;
  }

  Env *get_env(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _environments.size());
    if (unlikely(index >= _environments.size()))
      return 0;
    EnvironmentVector::iterator it = _environments.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return 0;
    return it->object;
  }

  Db *get_db(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _databases.size());
    if (unlikely(index >= _databases.size()))
      return 0;
    DatabaseVector::iterator it = _databases.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return 0;
    return it->object;
  }

  Txn *get_txn(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _transactions.size());
    if (unlikely(index >= _transactions.size()))
      return 0;
    TxnVector::iterator it = _transactions.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return 0;
    return it->object;
  }

  Cursor *get_cursor(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < _cursors.size());
    if (unlikely(index >= _cursors.size()))
      return 0;
    CursorVector::iterator it = _cursors.begin() + index;
    assert(it->index == handle);
    if (unlikely(it->index != handle))
      return 0;
    return it->object;
  }

  Handle<Db> get_db_by_name(uint16_t dbname) {
    for (size_t i = 0; i < _databases.size(); i++) {
      Db *db = _databases[i].object;
      if (db && db->name() == dbname)
        return _databases[i];
    }
    return Handle<Db>(0, 0);
  }

  uv_tcp_t server;
  uv_thread_t thread_id;
  uv_async_t async;
#if UV_VERSION_MINOR >= 11
	uv_loop_t loop;
#else
	uv_loop_t *loop;
#endif
  EnvironmentMap open_envs;

  Mutex open_queue_mutex;
  EnvironmentMap open_queue;
  ByteArray buffer;

  EnvironmentVector _environments;
  DatabaseVector _databases;
  CursorVector _cursors;
  TxnVector _transactions;
  uint64_t _handle_counter;
};

struct ClientContext {
  ClientContext(ServerContext *_srv)
    : buffer(0), srv(_srv) {
    assert(srv != 0);
  }

  ByteArray buffer;
  ServerContext *srv;
};

} // namespace upscaledb

#endif /* UPS_UPSSERVER_H */
