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

#include <boost/bind.hpp>
#include <boost/asio.hpp>

#include "ups/upscaledb.h"
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

typedef std::map<std::string, Env *> EnvironmentMap;

struct Server;

using boost::asio::ip::tcp;

template<typename T>
struct Handle {
  Handle(uint64_t _index, T *_object)
    : index(_index), object(_object) {
  }

  uint64_t index;
  T *object;
};

template<typename T>
struct HandleVector {
  typedef std::vector< Handle<T> > Vector;

  HandleVector()
    : handle_counter(1) {
  }

  size_t size() const {
    return data.size();
  }

  Handle<T> at(size_t index) {
    return data[index];
  }

  uint64_t allocate(T *t) {
    uint64_t c = 0;
    for (typename Vector::iterator it = data.begin();
                    it != data.end(); it++, c++) {
      if (it->index == 0) {
        c |= handle_counter << 32;
        handle_counter++;
        it->index = c;
        it->object = t;
        return c;
      }
    }

    c = data.size() | handle_counter << 32;
    handle_counter++;
    data.push_back(Handle<T>(c, t));
    return c;
  }

  void remove(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    if (unlikely(index >= data.size()))
      return;
    typename Vector::iterator it = data.begin() + index;
    if (unlikely(it->index != handle))
      return;
    it->index = 0;
    it->object = 0;
  }

  T *get(uint64_t handle) {
    uint32_t index = handle & 0xffffffff;
    assert(index < data.size());
    if (unlikely(index >= data.size()))
      return 0;
    typename Vector::iterator it = data.begin() + index;
    if (unlikely(it->index != handle))
      return 0;
    return it->object;
  }

  Vector data;
  uint64_t handle_counter;
};

struct Session {
  Session(Server *server_, boost::asio::io_service &io_service)
    : server(server_), socket(io_service), buffer_in(1024),
      current_position(0) {
  }

  void start() {
    socket.async_read_some(boost::asio::buffer(buffer_in.data() + current_position,
                            buffer_in.capacity() - current_position),
      boost::bind(&Session::handle_read, this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
  }

  void send(const uint8_t *data, size_t data_size) {
    // TODO should we send asynchronously??
    socket.send(boost::asio::buffer(data, data_size));
  }

  void handle_write(const boost::system::error_code &error,
                  size_t bytes_transferred) {
    /* nop */
  }

  void handle_read(const boost::system::error_code &error,
                  size_t bytes_transferred);

  Server *server;
  boost::asio::ip::tcp::socket socket;
  std::vector<uint8_t> buffer_in;
  int current_position;
};

struct Server {
  Server(short port)
    : acceptor(io_service, boost::asio::ip::tcp::endpoint(
                            boost::asio::ip::tcp::v4(), port)) {
    start_accept();
  }

  Server(std::string bind_url, short port)
    : acceptor(io_service, boost::asio::ip::tcp::endpoint(
                            boost::asio::ip::address::from_string(bind_url),
                            port)) {
    start_accept();
  }

  ~Server() {
    io_service.stop();
    thread->join();
  }

  void run() {
    thread.reset(new boost::thread(boost::bind(&boost::asio::io_service::run,
                                    &io_service)));
  }

  void start_accept() {
    Session *new_session = new Session(this, io_service);
    acceptor.async_accept(new_session->socket,
                    boost::bind(&Server::handle_accept, this, new_session,
                            boost::asio::placeholders::error));
  }

  void handle_accept(Session *new_session,
                  const boost::system::error_code &error) {
    if (!error)
      new_session->start();
    else
      delete new_session;

    start_accept();
  }

  Handle<Db> get_db_by_name(uint16_t dbname) {
    for (size_t i = 0; i < databases.size(); i++) {
      Db *db = databases.at(i).object;
      if (db && db->name() == dbname)
        return databases.at(i);
    }
    return Handle<Db>(0, 0);
  }

  boost::asio::io_service io_service;
  boost::asio::ip::tcp::acceptor acceptor;
  ScopedPtr<boost::thread> thread;

  EnvironmentMap open_envs;
  Mutex open_env_mutex;

  HandleVector<Env> environments;
  HandleVector<Db> databases;
  HandleVector<Cursor> cursors;
  HandleVector<Txn> transactions;
};

} // namespace upscaledb

#endif // UPS_UPSSERVER_H
