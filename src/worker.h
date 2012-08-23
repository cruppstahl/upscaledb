/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief The worker thread which flushes committed Transactions to disk
 *
 */

#ifndef HAM_WORKER_H__
#define HAM_WORKER_H__

#include "internal_fwd_decl.h"

namespace ham {

/**
 * the Worker thread
 */
class Worker
{
  public:
    /** default constructor starts the new thread */
    Worker(Environment *env)
      : m_env(env), m_exit_requested(false), m_last_error(0) {
      m_thread = boost::thread(boost::bind(&Worker::run, this));
    }

    /** retrieve the last error */
    ham_status_t get_last_error(bool reset = false) {
      ScopedLock lock(m_last_error_mutex);
      ham_status_t st = m_last_error;
      if (reset)
        m_last_error = 0;
      return (st);
    }

    /** worker function for the background thread */
    void run() {
      ScopedLock lock(m_mutex);

      while (!m_exit_requested) {
        m_cond.wait(lock);
        if (m_exit_requested)
          return;
        ham_status_t st = m_env->flush_committed_txns(false);
        if (st) {
          ScopedLock lock2(m_last_error_mutex);
          m_last_error = st;
        }
      }
    }

    /**
     * signal a commit of a transaction; this will lock the Environment
     * and start flushing to disk
     */
    void signal_commit() {
      m_cond.notify_all();
    }

    /**
     * ask the thread to exit and join the parent thread; this will again
     * flush all committed transactions, but it will not lock the Environment
     * since it was already locked in ham_env_close
     */
    void join() {
      m_exit_requested = true;
      m_cond.notify_all();
      m_thread.join();
    }

  private:
    /** a mutex to protect the thread members */
    Mutex m_mutex;

    /** and another one for the last_error variable */
    Mutex m_last_error_mutex;

    /** a condition for signalling */
    boost::condition m_cond;

    /** the Environment */
    Environment *m_env;

    /** the actual thread object */
    boost::thread m_thread;

    /** ask async thread to exit */
    bool m_exit_requested;

    /** last seen error */
    ham_status_t m_last_error;
};

} // namespace ham

#endif /* HAM_WORKER_H__ */
