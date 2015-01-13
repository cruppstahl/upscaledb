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
 * @thread_safe: yes
 */

#ifndef HAM_PAGE_COLLECTION_H
#define HAM_PAGE_COLLECTION_H

#include <string.h>

#include <boost/atomic.hpp>

#include "1base/spinlock.h"
#include "1mem/mem.h"
#include "2page/page.h"

namespace hamsterdb {

/*
 * The PageCollection class
 *
 * Stores Page lists in serial memory. Uses CAS (w/ hazard pointers)
 * for writes. Reads never block and can be performed even while a writer
 * is active. Only one writer at a time.
 */
class PageCollection {
    enum {
      /* default capacity of the builtin storage */
      kDefaultCapacity = 32,

      /* address replacement for zero */
      kAddressZero = 0xffffffffffffffff
    };

  public:
    /* maps page id to Page object */
    class Entry {
      public:
        bool is_in_use() const {
          return (m_address != 0);
        }

        // The page address is used as an indicator whether this
        // Entry is used (0) or not (!= 0). But since page address of 0 is
        // valid, 0 is rewritten as -1 (and vice versa).
        uint64_t get_address() const {
          uint64_t address = m_address;
          return (address == kAddressZero ? 0 : address);
        }

        void set_address(uint64_t address) {
          m_address = address ? address : kAddressZero;
        }

        Page *get_page() const {
          return (m_page);
        }

        void set_page(Page *page) {
          m_page = page;
        }

      private:
        boost::atomic<uint64_t> m_address;
        boost::atomic<Page *> m_page;
    };

    // Default constructor
    PageCollection()
      : m_entries(&m_entry_storage[0]), m_entries_used(0),
        m_entries_capacity(kDefaultCapacity) {
      ::memset(m_entry_storage, 0, sizeof(m_entry_storage));
    }

    // Destructor - releases allocated memory and resources
    ~PageCollection() {
      ScopedSpinlock lock(m_writer_mutex);
      if (m_entries != &m_entry_storage[0]) {
        Memory::release(m_entries);
        m_entries = 0;
        m_entries_capacity = 0;
        m_entries_used = 0;
      }
    }

    bool is_empty() const {
      return (m_entries_used.load(boost::memory_order_relaxed) == 0);
    }

    // Clears the collection.
    void clear() {
      ScopedSpinlock lock(m_writer_mutex);
      clear_nolock();
    }

    // Returns a page from the collection
    //
    // No lock required.
    Page *get_page(uint64_t address) const {
      for (int i = 0; i < m_entries_capacity; i++) {
        if (m_entries[i].get_address() == address)
          return (m_entries[i].get_page());
      }
      return (0);
    }

    // Removes a page from the collection
    //
    // No lock required. Sets the stored page address to zero.
    void remove_page(uint64_t address) {
      for (int i = 0; i < m_entries_capacity; i++) {
        if (m_entries[i].get_address() == address) {
          m_entries[i].set_address(0);
          --m_entries_used;
          return;
        }
      }
    }

    // Adds a new page to the collection
    //
    // !!
    // Readers will always check the address of a slot first, therefore
    // store new pages in reverse order (first store store the |page| pointer,
    // THEN the address).
    void add_page(Page *page) {
      ScopedSpinlock lock(m_writer_mutex);

      if (m_entries_used == m_entries_capacity - 1)
        grow_buffer();

      // fast path: try to append at the end
      if (m_entries[m_entries_used].is_in_use() == false) {
        m_entries[m_entries_used].set_page(page);
        m_entries[m_entries_used].set_address(page->get_address());
        ++m_entries_used;
        return;
      }

      // otherwise search for a free slot
      for (int i = 0; i < m_entries_capacity; i++) {
        if (m_entries[i].is_in_use() == false) {
          m_entries[i].set_page(page);
          m_entries[i].set_address(page->get_address());
          ++m_entries_used;
          return;
        }
      }

      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Returns true if a page with the |address| is already stored.
    bool contains(uint64_t address) const {
      if (address == 0)
        address = kAddressZero;

      for (int i = 0; i < m_entries_capacity; i++) {
        if (m_entries[i].get_address() == address)
          return (true);
      }
      return (false);
    }

    // Atomically moves the data to the |destination|, which must be
    // an unused and empty PageCollection. Calls clear() afterwards.
    void move(PageCollection &destination) {
      ScopedSpinlock lock1(m_writer_mutex);
      ScopedSpinlock lock2(destination.m_writer_mutex);
      
      ham_assert(destination.is_empty());
      ham_assert(destination.m_entries == &destination.m_entry_storage[0]);

      // this collection uses allocated storage
      if (m_entries != &m_entry_storage[0]) {
        destination.m_entries = m_entries.load(boost::memory_order_relaxed);
        destination.m_entries_used = m_entries_used.load(boost::memory_order_relaxed);
        destination.m_entries_capacity = m_entries_capacity;

        m_entries = &m_entry_storage[0];
        m_entries_used = 0;
        m_entries_capacity = kDefaultCapacity;
        return;
      }

      // this collection uses builtin storage - copy it
      ::memcpy(&destination.m_entry_storage[0], &m_entry_storage[0],
                      sizeof(Entry) * kDefaultCapacity);
      destination.m_entries_used = m_entries_used.load(boost::memory_order_relaxed);

      clear_nolock();
    }

    /* Returns a pointer to the first element */
    Entry *begin() const {
      return (&m_entries[0]);
    }

    /* Returns a pointer *after* the last element */
    Entry *end() const {
      return (&m_entries[m_entries_capacity]);
    }

  private:
    // Clears the collection.
    void clear_nolock() {
      Entry *p;
      if (m_entries != &m_entry_storage[0]) {
        p = &m_entry_storage[0];
        m_entries_capacity = kDefaultCapacity;
      }
      else {
        p = Memory::allocate<Entry>(sizeof(Entry) * m_entries_capacity);
      }

      ::memset(p, 0, sizeof(Entry) * m_entries_capacity);
      
      Entry *old = m_entries.exchange(p, boost::memory_order_acquire);
      m_entries_used = 0;

      // TODO clean up in background
      if (old != &m_entry_storage[0])
        Memory::release(old);
    }

    // Grows the underlying buffer
    //
    // First allocate a new buffer and copy the data.
    // Then atomically replace the existing pointer. The old pointer
    // will be hazard and garbage-collected after a while.
    //
    // !!
    // Always call in locked context!
    void grow_buffer() {
      Entry *p = Memory::allocate<Entry>(sizeof(Entry) * m_entries_capacity * 2);
      ::memcpy(p, m_entries, sizeof(Entry) * m_entries_capacity);
      ::memset(p + m_entries_capacity, 0, sizeof(Entry) * m_entries_capacity);
      
      Entry *old = m_entries.exchange(p, boost::memory_order_acquire);
      m_entries_capacity *= 2;

      // TODO clean up in background
      if (old != &m_entry_storage[0])
        Memory::release(old);
    }

    // A fast mutex for write access
    Spinlock m_writer_mutex;

    // Pointer to the stored entries
    boost::atomic<Entry *>m_entries;

    // Actually used entries. This number is pessimistic; deletes are not
    // immediately substracted, but new pages cause atomic increments.
    boost::atomic<int> m_entries_used;

    // Capacity of m_entries
    int m_entries_capacity;

    // Builtin storage; used to avoid allocations
    Entry m_entry_storage[kDefaultCapacity];
};

} // namespace hamsterdb

#endif /* HAM_PAGE_COLLECTION_H */
