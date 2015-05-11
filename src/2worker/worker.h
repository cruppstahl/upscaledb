/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * The worker thread. Asynchronously purges the cache. Thread will start as
 * soon as it's constructed.
 */

#ifndef HAM_WORKER_H
#define HAM_WORKER_H

#include "0root/root.h"

#include <boost/thread.hpp>

// Always verify that a file of level N does not include headers > N!
#include "2queue/queue.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Worker
{
  public:
    Worker()
      : m_stop_requested(false), m_thread(&Worker::run, this) {
    }

    void add_to_queue(MessageBase *message) {
      m_queue.push(message);

      ScopedLock lock(m_mutex);
      m_cond.notify_one();
    }

    void add_to_queue_blocking(BlockingMessageBase *message) {
      add_to_queue(message);
      message->wait();
    }

    void stop_and_join() {
      {
        ScopedLock lock(m_mutex);
        m_stop_requested = true;
        m_cond.notify_one();
      }
      m_thread.join();
    }

  private:
    // The thread function
    void run() {
      MessageBase *message = 0;

      while (true) {
        {
          ScopedLock lock(m_mutex);
          if (m_stop_requested)
            break;
          message = m_queue.pop();
          if (!message) {
            m_cond.wait(lock); // will unlock m_mutex while waiting
            message = m_queue.pop();
          }
        }

        do {
          if (message) {
            // it's possible that handle_message() causes the main thread to
            // delete the message, if kDontDelete is set. Therefore the flags
            // are copied to a local variable.
            uint32_t flags = message->flags;
            handle_message(message);
            if ((flags & MessageBase::kDontDelete) == false)
              delete message;
          }
        } while ((message = m_queue.pop()));
      }

      // pick up remaining messages
      while ((message = m_queue.pop())) {
        // see comment above
        uint32_t flags = message->flags;
        handle_message(message);
        if ((flags & MessageBase::kDontDelete) == false)
          delete message;
      }
    }

    // The message handler - has to be overridden
    virtual void handle_message(MessageBase *message) = 0;

    // A queue for storing messages
    Queue m_queue;

    // true if the Environment is closed
    bool m_stop_requested;

    // A mutex for protecting |m_cond|
    boost::mutex m_mutex;

    // A condition to wait for
    boost::condition_variable m_cond;

    // The actual thread
    boost::thread m_thread;
};

} // namespace hamsterdb

#endif // HAM_WORKER_H
