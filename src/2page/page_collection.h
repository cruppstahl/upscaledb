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

    // Atomically applies the |visitor()| to each page
    template<typename Visitor>
    void for_each(Visitor &visitor) {
      for (Page *p = m_head; p != 0; p = p->get_next(m_id)) {
        if (!visitor(p))
          break;
      }
    }

    // Atomically applies the |visitor()| to each page; starts at the tail
    template<typename Visitor>
    void for_each_reverse(Visitor &visitor) {
      for (Page *p = m_tail; p != 0; p = p->get_previous(m_id)) {
        if (!visitor(p))
          break;
      }
    }

    // Same as |for_each()|, but removes the page if |visitor()| returns true
    template<typename Visitor>
    void extract(Visitor &visitor) {
      Page *page = m_head;
      while (page) {
        Page *next = page->get_next(m_id);
        if (visitor(page)) {
          del_impl(page);
        }
        page = next;
      }
    }

    // Clears the collection.
    void clear() {
      Page *page = m_head;
      while (page) {
        Page *next = page->get_next(m_id);
        del_impl(page);
        page = next;
      }

      ham_assert(m_head == 0);
      ham_assert(m_tail == 0);
      ham_assert(m_size == 0);
    }

    // Returns the list's id
    int id() const {
      return (m_id);
    }

    // Returns the head
    Page *head() const {
      return (m_head);
    }

    // Returns the tail
    Page *tail() const {
      return (m_tail);
    }

    // Returns a page from the collection
    Page *get(uint64_t address) const {
      for (Page *p = m_head; p != 0; p = p->get_next(m_id)) {
        if (p->get_address() == address)
          return (p);
      }
      return (0);
    }

    // Removes a page from the collection. Returns true if the page was removed,
    // otherwise false (if the page was not in the list)
    bool del(Page *page) {
      if (has(page)) {
        del_impl(page);
        return (true);
      }
      return (false);
    }

    // Adds a new page at the head of the list. Returns true if the page was
    // added, otherwise false (that's the case if the page is already part of
    // the list)
    bool put(Page *page) {
      if (!has(page)) {
        m_head = page->list_insert(m_head, m_id);
        if (!m_tail)
          m_tail = page;
        ++m_size;
        return (true);
      }
      return (false);
    }

    // Returns true if a page with the |address| is already stored.
    bool has(uint64_t address) const {
      return (get(address) != 0);
    }

    // Returns true if the |page| is already stored. This is much faster
    // than has(uint64_t address).
    bool has(Page *page) const {
      return (page->is_in_list(m_head, m_id));
    }
    
  private:
    void del_impl(Page *page) {
      // First update the tail because Page::list_remove() will change the
      // pointers!
      if (m_tail == page)
        m_tail = page->get_previous(m_id);
      m_head = page->list_remove(m_head, m_id);
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
