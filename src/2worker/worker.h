/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * The worker thread
 */

#ifndef HAM_WORKER_H
#define HAM_WORKER_H

// let root.h include boost/asio.hpp (WIN32 only!)
#define USE_ASIO 1

#include "0root/root.h"

// other platforms need to include boost/asio.hpp directly
#ifndef WIN32
#  include <boost/asio.hpp>
#  include <boost/thread/thread.hpp>
#endif

// Always verify that a file of level N does not include headers > N!
#include "2worker/workitem.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

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
 
} // namespace hamsterdb

#endif // HAM_WORKER_H
