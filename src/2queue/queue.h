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
 * A thread-safe message queue. Producers can insert at the front, Consumers
 * pick messages from the tail.
 *
 * The queue uses a Spinlock for synchronization, but locks it only very,
 * very briefly.
 */

#ifndef HAM_QUEUE_H
#define HAM_QUEUE_H

#include "0root/root.h"

#include <ham/types.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/spinlock.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

// The Message. Other messages can derive from it and append their own
// payload.
struct MessageBase
{
  // Message flags
  enum {
    // Message is mandatory and must not be skipped
    kIsMandatory = 0,

    // Do NOT delete the message after it was processed
    kDontDelete  = 1,
  };

  MessageBase(int type_, int flags_ = kIsMandatory)
    : type(type_), flags(flags_), previous(0), next(0) {
  }

  virtual ~MessageBase() {
  }

  int type;
  int flags;
  MessageBase *previous;
  MessageBase *next;
};

struct BlockingMessageBase : public MessageBase
{
  BlockingMessageBase(int type_, int flags_ = kIsMandatory)
    : MessageBase(type_, flags_ | MessageBase::kDontDelete), completed(false) {
  }

  // wake up the waiting thread
  void notify() {
    ScopedLock lock(mutex);
    completed = true;
    cond.notify_all();
  }

  // lets the caller wait till the operation is completed
  void wait() {
    ScopedLock lock(mutex);
    while (!completed)
      cond.wait(lock);
  }

  Mutex mutex;      // to protect |cond| and |completed|
  Condition cond;
  bool completed;
};


class Queue
{
  public:
    template<typename T>
    struct Message : public MessageBase
    {
      Message(int type, int flags)
        : MessageBase(type, flags) {
      }

      T payload;
    };

    Queue()
      : m_head(0), m_tail(0) {
    }

    // Pushes a |message| object to the queue
    void push(MessageBase *message) {
      ScopedSpinlock lock(m_mutex);
      if (!m_tail) {
        ham_assert(m_head == 0);
        m_head = m_tail = message;
      }
      else if (m_tail == m_head) {
        m_tail->previous = message;
        message->next = m_tail;
        m_head = message;
      }
      else {
        message->next = m_head;
        m_head->previous = message;
        m_head = message;
      }
    }

    // Pops a message from the tail of the queue. Returns null if the queue
    // is empty.
    MessageBase *pop() {
      ScopedSpinlock lock(m_mutex);
      if (!m_tail) {
        ham_assert(m_head == 0);
        return (0);
      }

      MessageBase *msg = m_tail;
      if (m_tail == m_head)
        m_head = m_tail = 0;
      else
        m_tail = m_tail->previous;
      return (msg);
    }

  private:
    // For synchronization
    Spinlock m_mutex;

    // The head of the linked list (and newest MessageBase)
    MessageBase *m_head;

    // The tail of the linked list (and oldest MessageBase)
    MessageBase *m_tail;
};

} // namespace hamsterdb

#endif // HAM_QUEUE_H
