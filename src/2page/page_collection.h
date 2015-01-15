/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef HAM_PAGE_COLLECTION_H
#define HAM_PAGE_COLLECTION_H

#include <string.h>

#include <boost/atomic.hpp>

#include "1mem/mem.h"
#include "2page/page.h"

namespace hamsterdb {

/*
 * The PageCollection class
 */
class PageCollection {
  public:
    // Default constructor
    PageCollection(int list_id)
      : m_head(0), m_tail(0), m_size(0), m_id(list_id) {
    }

    // Destructor
    ~PageCollection() {
      clear();
    }

    bool is_empty() const {
      return (m_size == 0);
    }

    int size() const {
      return (m_size);
    }

    // Returns the first page where |predicate()| returns true. Starts at
    // the tail.
    template<typename Predicate>
    Page *find_first_reverse(Predicate &predicate) {
      for (Page *p = m_tail; p != 0; p = p->get_previous(m_id)) {
        if (predicate(p))
          return (p);
      }
      return (0);
    }

    // Atomically applies the |visitor()| to each page
    template<typename Visitor>
    void for_each(Visitor &visitor) {
      for (Page *p = m_head; p != 0; p = p->get_next(m_id)) {
        visitor(p);
      }
    }

    // Same as |for_each()|, but removes the page if |visitor()| returns true
    template<typename Visitor>
    void extract(Visitor &visitor) {
      visitor.prepare(m_size);

      Page *page = m_head;
      while (page) {
        Page *next = page->get_next(m_id);
        if (visitor(page)) {
          remove_impl(page);
        }
        page = next;
      }
    }

    // Clears the collection.
    void clear() {
      Page *page = m_head;
      while (page) {
        Page *next = page->get_next(m_id);
        remove_impl(page);
        page = next;
      }

      ham_assert(m_head == 0);
      ham_assert(m_tail == 0);
      ham_assert(m_size == 0);
    }

    // Returns a page from the collection
    Page *get(uint64_t address) const {
      for (Page *p = m_head; p != 0; p = p->get_next(m_id)) {
        if (p->get_address() == address)
          return (p);
      }
      return (0);
    }

    // Returns the head
    Page *head() const {
      return (m_head);
    }

    // Returns the tail
    Page *tail() const {
      return (m_tail);
    }

    // Removes a page from the collection
    void remove(uint64_t address) {
      Page *page = get(address);
      if (page) {
        remove_impl(page);
      }
    }

    // Removes a page from the collection. Returns true if the page was removed,
    // otherwise false (if the page was not in the list)
    bool remove(Page *page) {
      if (contains(page)) {
        remove_impl(page);
        return (true);
      }
      return (false);
    }

    // Adds a new page at the head of the list. Returns true if the page was
    // added, otherwise false (that's the case if the page is already part of
    // the list)
    bool add(Page *page) {
      if (!contains(page)) {
        m_head = page->list_insert(m_head, m_id);
        if (!m_tail)
          m_tail = page;
        ++m_size;
        return (true);
      }
      return (false);
    }

    // Returns true if a page with the |address| is already stored.
    bool contains(uint64_t address) const {
      return (get(address) != 0);
    }

    // Returns true if the |page| is already stored. This is much faster
    // than contains(uint64_t address).
    bool contains(Page *page) const {
      return (page->is_in_list(m_head, m_id));
    }
    
  private:
    void remove_impl(Page *page) {
      m_head = page->list_remove(m_head, m_id);
      if (m_tail == page)
        m_tail = page->get_previous(m_id);
      ham_assert(m_size > 0);
      --m_size;
    }

    // The head of the linked list
    Page *m_head;

    // The tail of the linked list
    Page *m_tail;

    // Number of elements in the list
    int m_size;

    // The list ID
    int m_id;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_COLLECTION_H */
