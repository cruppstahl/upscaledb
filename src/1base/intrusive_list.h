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
 * An intrusive list
 *
 * usage:
 *
 *   struct P {
 *     IntrusiveListNode<P> list_node; // don't change that name!
 *   };
 *
 *   IntrusiveList<P> list;
 *
 *   P p;
 *   list.put(&p);
 *   list.del(&p);
 *   assert(list.has(&p));
 *
 * If |P| should be a member in multiple lists:
 *
 *   struct P {
 *     IntrusiveListNode<P, 3> list_node; // for 3 lists
 *   };
 *
 *   IntrusiveList<P, 0> list1;
 *   IntrusiveList<P, 1> list2;
 *   IntrusiveList<P, 2> list3;
 */

#ifndef UPS_INTRUSIVE_LIST_H
#define UPS_INTRUSIVE_LIST_H

#include "0root/root.h"
#include "1base/uncopyable.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename T, int I = 1>
struct IntrusiveListNode : public Uncopyable {
  IntrusiveListNode() {
    for (int i = 0; i < I; i++)
      previous[i] = next[i] = 0;
  }

  T *previous[I];
  T *next[I];
};

template<typename T, int I = 0>
struct IntrusiveList {
  IntrusiveList() {
    clear();
  }

  T *head() const {
    return head_;
  }

  T *tail() const {
    return tail_;
  }

  bool is_empty() const {
    return size_ == 0;
  }

  size_t size() const {
    return size_;
  }

  void put(T *t) {
    t->list_node.previous[I] = 0;
    t->list_node.next[I] = 0;
    if (head_) {
      t->list_node.next[I] = head_;
      head_->list_node.previous[I] = t;
    }
    head_ = t;
    if (!tail_)
      tail_ = t;
    size_++;
  }

  void append(T *t) {
    t->list_node.previous[I] = 0;
    t->list_node.next[I] = 0;
    if (!head_) {
      assert(tail_ == 0);
      head_ = t;
      tail_ = t;
    }
    else {
      tail_->list_node.next[I] = t;
      tail_ = t;
      if (!head_)
        head_ = t;
    }
    size_++;
  }

  void del(T *t) {
    assert(has(t));

    if (t == tail_)
      tail_ = t->list_node.previous[I];
    if (t == head_) {
      T *next = head_->list_node.next[I];
      if (next)
        next->list_node.previous[I] = 0;
      head_ = next;
    }
    else {
      T *next = t->list_node.next[I];
      T *prev = t->list_node.previous[I];
      if (prev)
        prev->list_node.next[I] = next;
      if (next)
        next->list_node.previous[I] = prev;
    }
    t->list_node.next[I] = 0;
    t->list_node.previous[I] = 0;
    size_--;
  }

  bool has(const T *t) const {
    return t->list_node.previous[I] != 0
            || t->list_node.next[I] != 0
            || t == head_;
  }

  void clear() {
    head_ = 0;
    tail_ = 0;
    size_ = 0;
  }

  T *head_;
  T *tail_;
  size_t size_;
};

} // namespace upscaledb

#endif // UPS_INTRUSIVE_LIST_H
