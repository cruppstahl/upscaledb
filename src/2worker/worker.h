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

/*
 * The worker thread
 */

#ifndef UPS_WORKER_H
#define UPS_WORKER_H

#include "0root/root.h"

#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>

// Always verify that a file of level N does not include headers > N!
#include "2worker/workitem.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct WorkerPool;
 
// our worker thread objects
struct WorkerThread {
  WorkerThread(WorkerPool &s)
    : pool(s) {
  }

  void operator()();

  WorkerPool &pool; 
};
 
// the actual thread pool
struct WorkerPool {
  // the constructor just launches some amount of workers
  WorkerPool(size_t num_threads)
    : working(service), strand(service) {
    for (size_t i = 0; i < num_threads; ++i)
      workers.push_back(new boost::thread(WorkerThread(*this)));
  }

  // Add a new work item to the pool
  template<typename F>
  void enqueue(F &f) {
    strand.post(f);
  }

  // the destructor joins all threads
  ~WorkerPool() {
    service.stop();

    for (size_t i = 0; i < workers.size(); ++i) {
      workers[i]->join();
      delete workers[i];
    }
  }

  // keep track of the threads so we can join them
  std::vector<boost::thread *> workers;
   
  // the io_service we are wrapping
  boost::asio::io_service service;
  boost::asio::io_service::work working;
  boost::asio::strand strand;
};

inline void
WorkerThread::operator()() {
  pool.service.run();
}
 
} // namespace upscaledb

#endif // UPS_WORKER_H
