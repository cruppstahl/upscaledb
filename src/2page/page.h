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

#ifndef UPS_PAGE_H
#define UPS_PAGE_H

#include <string.h>
#include <stdint.h>
#include <boost/atomic.hpp>

#include "1base/error.h"
#include "1base/spinlock.h"
#include "1mem/mem.h"

namespace upscaledb {

class Device;
class BtreeCursor;
class BtreeNodeProxy;
class LocalDatabase;

#include "1base/packstart.h"

/*
 * This header is only available if the (non-persistent) flag
 * kNpersNoHeader is not set! Blob pages do not have this header.
 */
typedef UPS_PACK_0 struct UPS_PACK_1 PPageHeader {
  // flags of this page - currently only used for the Page::kType* codes
  uint32_t flags;

  // crc32
  uint32_t crc32;

  // the lsn of the last operation
  uint64_t lsn;

  // the persistent data blob
  uint8_t payload[1];

} UPS_PACK_2 PPageHeader;

#include "1base/packstop.h"

#include "1base/packstart.h"

/*
 * A union combining the page header and a pointer to the raw page data.
 *
 * This structure definition is present outside of @ref Page scope
 * to allow compile-time OFFSETOF macros to correctly judge the size,
 * depending on platform and compiler settings.
 */
typedef UPS_PACK_0 union UPS_PACK_1 PPageData {
  // the persistent header
  struct PPageHeader header;

  // a char pointer to the allocated storage on disk
  uint8_t payload[1];

} UPS_PACK_2 PPageData;

#include "1base/packstop.h"

/*
 * The Page class
 *
 * Each Page instance is a node in several linked lists.
 * In order to avoid multiple memory allocations, the previous/next pointers
 * are part of the Page class (m_prev and m_next). Both fields are arrays
 * of pointers and can be used i.e. with m_prev[Page::kListBucket] etc.
 * (or with the methods defined below).
 */

class Page {
  public:
    // A wrapper around the persisted page data
    struct PersistedData {
      PersistedData()
        : address(0), size(0), is_dirty(false), is_allocated(false),
          is_without_header(false), raw_data(0) {
      }

      PersistedData(const PersistedData &other)
        : address(other.address), size(other.size), is_dirty(other.is_dirty),
          is_allocated(other.is_allocated),
          is_without_header(other.is_without_header), raw_data(other.raw_data) {
      }

      ~PersistedData() {
#ifdef UPS_DEBUG
        mutex.safe_unlock();
#endif
        if (is_allocated)
          Memory::release(raw_data);
        raw_data = 0;
      }

      // The spinlock is locked if the page is in use or written to disk
      Spinlock mutex;

      // address of this page - the absolute offset in the file
      uint64_t address;

      // the size of this page
      uint32_t size;

      // is this page dirty and needs to be flushed to disk?
      bool is_dirty;

      // Page buffer was allocated with malloc() (if not then it was mapped
      // with mmap)
      bool is_allocated;

      // True if page has no persistent header
      bool is_without_header;

      // the persistent data of this page
      PPageData *raw_data;
    };

    // Misc. enums
    enum {
      // sizeof the persistent page header
      kSizeofPersistentHeader = sizeof(PPageHeader) - 1,

      // instruct Page::alloc() to reset the page with zeroes
      kInitializeWithZeroes,
    };

    // The various linked lists (indices in m_prev, m_next)
    enum {
      // list of all cached pages
      kListCache              = 0,

      // list of all pages in a changeset
      kListChangeset          = 1,

      // a bucket in the hash table of the cache
      kListBucket             = 2,

      // array limit
      kListMax                = 3
    };

    // non-persistent page flags
    enum {
      // page->m_data was allocated with malloc, not mmap
      kNpersMalloc            = 1,

      // page has no header (i.e. it's part of a large blob)
      kNpersNoHeader          = 2
    };

    // Page types
    //
    // When large BLOBs span multiple pages, only their initial page
    // will have a valid type code; subsequent pages of this blog will store
    // the data as-is, so as to provide one continuous storage space
    enum {
      // unidentified db page type
      kTypeUnknown            =  0x00000000,

      // the header page: this is the first page in the environment (offset 0)
      kTypeHeader             =  0x10000000,

      // a B+tree root page
      kTypeBroot              =  0x20000000,

      // a B+tree node page
      kTypeBindex             =  0x30000000,

      // a page storing the state of the PageManager
      kTypePageManager        =  0x40000000,

      // a page which stores blobs
      kTypeBlob               =  0x50000000
    };

    // Default constructor
    Page(Device *device, LocalDatabase *db = 0);

    // Destructor - releases allocated memory and resources, but neither
    // flushes dirty pages to disk nor moves them to the freelist!
    // Asserts that no cursors are attached.
    ~Page();

    // Returns the size of the usable persistent payload of a page
    // (page_size minus the overhead of the page header)
    uint32_t usable_page_size();

    // Returns the spinlock
    Spinlock &mutex() {
      return persisted_data.mutex;
    }

    // Returns the database which manages this page; can be NULL if this
    // page belongs to the Environment (i.e. for freelist-pages)
    LocalDatabase *db() {
      return db_;
    }

    // Sets the database to which this Page belongs
    void set_db(LocalDatabase *db) {
      db_ = db;
    }

    // Returns the address of this page
    uint64_t address() const {
      return persisted_data.address;
    }

    // Sets the address of this page
    void set_address(uint64_t address) {
      persisted_data.address = address;
    }

    // Returns the page's type (kType*)
    uint32_t type() const {
      return persisted_data.raw_data->header.flags;
    }

    // Sets the page's type (kType*)
    void set_type(uint32_t type) {
      persisted_data.raw_data->header.flags = type;
    }

    // Returns the crc32
    uint32_t crc32() const {
      return persisted_data.raw_data->header.crc32;
    }

    // Sets the crc32
    void set_crc32(uint32_t crc32) {
      persisted_data.raw_data->header.crc32 = crc32;
    }

    // Returns the lsn
    uint64_t lsn() const {
      return persisted_data.raw_data->header.lsn;
    }

    // Sets the lsn
    void set_lsn(uint64_t lsn) {
      persisted_data.raw_data->header.lsn = lsn;
    }

    // Returns the pointer to the persistent data
    PPageData *data() {
      return persisted_data.raw_data;
    }

    // Sets the pointer to the persistent data
    void set_data(PPageData *data) {
      persisted_data.raw_data = data;
    }

    // Returns the persistent payload (after the header!)
    uint8_t *payload() {
      return persisted_data.raw_data->header.payload;
    }
    
    // Returns the persistent payload (including the header!)
    uint8_t *raw_payload() {
      return persisted_data.raw_data->payload;
    }

    // Returns true if this is the header page of the Environment
    bool is_header() const {
      return persisted_data.address == 0;
    }

    // Returns true if this page is dirty (and needs to be flushed to disk)
    bool is_dirty() const {
      return persisted_data.is_dirty;
    }

    // Sets this page dirty/not dirty
    void set_dirty(bool dirty) {
      persisted_data.is_dirty = dirty;
    }

    // Returns true if the page's buffer was allocated with malloc
    bool is_allocated() const {
      return persisted_data.is_allocated;
    }

    // Returns true if the page has no persistent header
    bool is_without_header() const {
      return persisted_data.is_without_header;
    }

    // Sets the flag whether this page has a persistent header or not
    void set_without_header(bool is_without_header) {
      persisted_data.is_without_header = is_without_header;
    }

    // Assign a buffer which was allocated with malloc()
    void assign_allocated_buffer(void *buffer, uint64_t address) {
      free();
      persisted_data.raw_data = (PPageData *)buffer;
      persisted_data.is_allocated = true;
      persisted_data.address = address;
    }

    // Assign a buffer from mmapped storage
    void assign_mapped_buffer(void *buffer, uint64_t address) {
      free();
      persisted_data.raw_data = (PPageData *)buffer;
      persisted_data.is_allocated = false;
      persisted_data.address = address;
    }

    // Free resources associated with the buffer
    void free();

    // Returns the linked list of coupled cursors (can be NULL)
    BtreeCursor *cursor_list() {
      return cursor_list_;
    }

    // Sets the (head of the) linked list of cursors
    void set_cursor_list(BtreeCursor *cursor) {
      cursor_list_ = cursor;
    }

    // Allocates a new page from the device
    // |flags|: either 0 or kInitializeWithZeroes
    void alloc(uint32_t type, uint32_t flags = 0);

    // Reads a page from the device
    void fetch(uint64_t address);

    // Flushes the page to disk, clears the "dirty" flag
    void flush();

    // Returns true if this page is in a linked list
    bool is_in_list(Page *list_head, int list) {
      if (get_next(list) != 0)
        return true;
      if (get_previous(list) != 0)
        return true;
      return list_head == this;
    }

    // Inserts this page at the beginning of a list and returns the
    // new head of the list
    Page *list_insert(Page *list_head, int list) {
      set_next(list, 0);
      set_previous(list, 0);

      if (!list_head)
        return this;

      set_next(list, list_head);
      list_head->set_previous(list, this);
      return this;
    }

    // Removes this page from a list and returns the new head of the list
    Page *list_remove(Page *list_head, int list) {
      Page *n, *p;

      if (this == list_head) {
        n = get_next(list);
        if (n)
          n->set_previous(list, 0);
        set_next(list, 0);
        set_previous(list, 0);
        return n;
      }

      n = get_next(list);
      p = get_previous(list);
      if (p)
        p->set_next(list, n);
      if (n)
        n->set_previous(list, p);
      set_next(list, 0);
      set_previous(list, 0);
      return list_head;
    }

    // Returns the next page in a linked list
    Page *get_next(int list) {
      return m_next[list];
    }

    // Returns the previous page of a linked list
    Page *get_previous(int list) {
      return m_prev[list];
    }

    // Returns the cached BtreeNodeProxy
    BtreeNodeProxy *node_proxy() {
      return node_proxy_;
    }

    // Sets the cached BtreeNodeProxy
    void set_node_proxy(BtreeNodeProxy *proxy) {
      node_proxy_ = proxy;
    }

    // tracks number of flushed pages
    static uint64_t ms_page_count_flushed;

    // the persistent data of this page
    PersistedData persisted_data;

  private:
    friend class PageCollection;

    // Sets the previous page of a linked list
    void set_previous(int list, Page *other) {
      m_prev[list] = other;
    }

    // Sets the next page in a linked list
    void set_next(int list, Page *other) {
      m_next[list] = other;
    }

    // the Device for allocating storage
    Device *device_;

    // the Database handle (can be NULL)
    LocalDatabase *db_;

    // linked list of all cursors which are coupled to that page
    BtreeCursor *cursor_list_;

    // linked lists of pages - see comments above
    // TODO hide behind implementation class
    Page *m_prev[Page::kListMax];
    Page *m_next[Page::kListMax];

    // the cached BtreeNodeProxy object
    BtreeNodeProxy *node_proxy_;
};

} // namespace upscaledb

#endif /* UPS_PAGE_H */
