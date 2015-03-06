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
    kIsMandatory = 0
  };

  MessageBase(int type_, int flags_)
    : type(type_), flags(flags_), previous(0), next(0) {
  }

  virtual ~MessageBase() {
  }

  int type;
  int flags;
  MessageBase *previous;
  MessageBase *next;
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
