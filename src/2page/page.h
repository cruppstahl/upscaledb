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

#ifndef UPS_PAGE_H
#define UPS_PAGE_H

#include <string.h>
#include <stdint.h>

#include "1base/error.h"
#include "1base/spinlock.h"
#include "1mem/mem.h"
#include "1base/intrusive_list.h"
#include "3btree/btree_cursor.h"

namespace upscaledb {

struct Device;
struct BtreeCursor;
struct BtreeNodeProxy;
struct LocalDb;

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
#ifdef NDEBUG
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
    Page(Device *device, LocalDb *db = 0);

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
    LocalDb *db() {
      return db_;
    }

    // Sets the database to which this Page belongs
    void set_db(LocalDb *db) {
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
      free_buffer();
      persisted_data.raw_data = (PPageData *)buffer;
      persisted_data.is_allocated = true;
      persisted_data.address = address;
    }

    // Assign a buffer from mmapped storage
    void assign_mapped_buffer(void *buffer, uint64_t address) {
      free_buffer();
      persisted_data.raw_data = (PPageData *)buffer;
      persisted_data.is_allocated = false;
      persisted_data.address = address;
    }

    // Free resources associated with the buffer
    void free_buffer();

    // Allocates a new page from the device
    // |flags|: either 0 or kInitializeWithZeroes
    void alloc(uint32_t type, uint32_t flags = 0);

    // Reads a page from the device
    void fetch(uint64_t address);

    // Flushes the page to disk, clears the "dirty" flag
    void flush();

    // Returns the cached BtreeNodeProxy
    BtreeNodeProxy *node_proxy() {
      return node_proxy_;
    }

    // Sets the cached BtreeNodeProxy
    void set_node_proxy(BtreeNodeProxy *proxy) {
      node_proxy_ = proxy;
    }

    // Returns the next page in a linked list
    Page *next(int list) {
      return list_node.next[list];
    }

    // Returns the previous page of a linked list
    Page *previous(int list) {
      return list_node.previous[list];
    }

    // tracks number of flushed pages
    static uint64_t ms_page_count_flushed;

    // the persistent data of this page
    PersistedData persisted_data;

    // Intrusive linked lists
    IntrusiveListNode<Page, Page::kListMax> list_node;

    // Intrusive linked btree cursors
    IntrusiveList<BtreeCursor> cursor_list;

  private:
    // the Device for allocating storage
    Device *device_;

    // the Database handle (can be NULL)
    LocalDb *db_;

    // the cached BtreeNodeProxy object
    BtreeNodeProxy *node_proxy_;
};

} // namespace upscaledb

#endif /* UPS_PAGE_H */
