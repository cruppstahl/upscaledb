/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef UPS_PAGE_COLLECTION_H
#define UPS_PAGE_COLLECTION_H

#include <string.h>

#include <boost/atomic.hpp>

#include "1base/intrusive_list.h"
#include "1mem/mem.h"
#include "2page/page.h"

namespace upscaledb {

/*
 * The PageCollection class
 */
template<int ID>
class PageCollection {
  public:
    // Destructor
    ~PageCollection() {
      clear();
    }

    // Returns the list's id
    int id() const {
      return ID;
    }

    bool is_empty() const {
      return list.is_empty();
    }

    int size() const {
      return list.size();
    }

    // Returns the head
    Page *head() const {
      return list.head();
    }

    // Returns the tail
    Page *tail() const {
      return list.tail();
    }

    // Atomically applies the |visitor()| to each page
    template<typename Visitor>
    void for_each(Visitor &visitor) {
      for (Page *p = head(); p != 0; p = p->next(ID)) {
        if (!visitor(p))
          break;
      }
    }

    // Atomically applies the |visitor()| to each page; starts at the tail
    template<typename Visitor>
    void for_each_reverse(Visitor &visitor) {
      for (Page *p = tail(); p != 0; p = p->previous(ID)) {
        if (!visitor(p))
          break;
      }
    }

    // Same as |for_each()|, but removes the page if |visitor()| returns true
    template<typename Visitor>
    void extract(Visitor &visitor) {
      Page *page = head();
      while (page) {
        Page *next = page->next(ID);
        if (visitor(page)) {
          list.del(page);
        }
        page = next;
      }
    }

    // Clears the collection.
    void clear() {
      Page *page = head();
      while (page) {
        Page *next = page->next(ID);
        list.del(page);
        page = next;
      }

      assert(is_empty() == true);
    }

    // Returns a page from the collection; this is expensive!
    Page *get(uint64_t address) const {
      for (Page *p = head(); p != 0; p = p->next(ID)) {
        if (p->address() == address)
          return (p);
      }
      return 0;
    }

    // Removes a page from the collection. Returns true if the page was removed,
    // otherwise false (if the page was not in the list)
    bool del(Page *page) {
      if (has(page)) {
        list.del(page);
        return true;
      }
      return false;
    }

    // Adds a new page at the head of the list. Returns true if the page was
    // added, otherwise false (that's the case if the page is already part of
    // the list)
    bool put(Page *page) {
      if (!list.has(page)) {
        list.put(page);
        return true;
      }
      return false;
    }

    // Returns true if a page with the |address| is already stored.
    // This is expensive! TODO can we remove this?
    bool has(uint64_t address) const {
      return get(address) != 0;
    }

    // Returns true if the |page| is already stored. This is much faster
    // than has(uint64_t address).
    bool has(Page *page) const {
      return list.has(page);
    }
    
  private:
    // The linked list
    IntrusiveList<Page, ID> list;
};

} // namespace upscaledb

#endif /* UPS_PAGE_COLLECTION_H */
