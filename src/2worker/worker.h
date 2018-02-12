/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#if BOOST_VERSION < 106600
  boost::asio::strand strand;
#else
  boost::asio::io_context::strand strand;
#endif
};

inline void
WorkerThread::operator()() {
  pool.service.run();
}
 
} // namespace upscaledb

#endif // UPS_WORKER_H
