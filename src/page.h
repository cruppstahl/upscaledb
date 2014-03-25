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

#ifndef HAM_PAGE_H__
#define HAM_PAGE_H__

#include <string.h>

#include "endianswap.h"
#include "error.h"
#include "mem.h"

namespace hamsterdb {

class Device;
class BtreeCursor;
class BtreeNodeProxy;
class LocalDatabase;
class LocalEnvironment;

#include "packstart.h"

/*
 * This header is only available if the (non-persistent) flag
 * kNpersNoHeader is not set! Blob pages do not have this header.
 *
 * !!
 * if this structure is changed, env->get_usable_page_size has
 * to be changed as well!
 */
typedef HAM_PACK_0 struct HAM_PACK_1 PPageHeader {
  // flags of this page - currently only used for the Page::kType* codes
  ham_u32_t _flags;

  // the lsn of the last operation
  ham_u64_t _lsn;

  // the persistent data blob
  ham_u8_t _payload[1];

} HAM_PACK_2 PPageHeader;

#include "packstop.h"

#include "packstart.h"

/*
 * A union combining the page header and a pointer to the raw page data.
 *
 * This structure definition is present outside of @ref Page scope
 * to allow compile-time OFFSETOF macros to correctly judge the size,
 * depending on platform and compiler settings.
 */
typedef HAM_PACK_0 union HAM_PACK_1 PPageData {
  // the persistent header
  struct PPageHeader _s;

  // a char pointer to the allocated storage on disk
  ham_u8_t _p[1];

} HAM_PACK_2 PPageData;

#include "packstop.h"

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
    // Misc. enums
    enum {
      // sizeof the persistent page header
      kSizeofPersistentHeader = sizeof(PPageHeader) - 1,

      // instruct Page::allocate() to reset the page with zeroes
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
    Page(LocalEnvironment *env = 0, LocalDatabase *db = 0);

    // Destructor - releases allocated memory and resources, but neither
    // flushes dirty pages to disk nor moves them to the freelist!
    // Asserts that no cursors are attached.
    ~Page();

    // Returns the Environment
    LocalEnvironment *get_env() {
      return (m_env);
    }

    // Returns the database which manages this page; can be NULL if this
    // page belongs to the Environment (i.e. for freelist-pages)
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Sets the database to which this Page belongs
    void set_db(LocalDatabase *db) {
      m_db = db;
    }

    // Returns true if this is the header page of the Environment
    bool is_header() const {
      return (m_address == 0);
    }

    // Returns the address of this page
    ham_u64_t get_address() const {
      return (m_address);
    }

    // Sets the address of this page
    void set_address(ham_u64_t address) {
      m_address = address;
    }

    // Returns the non-persistent page flags
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    // Sets the non-persistent page flags
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    // Returns true if this page is dirty (and needs to be flushed to disk)
    bool is_dirty() const {
      return (m_dirty);
    }

    // Sets this page dirty/not dirty
    void set_dirty(bool dirty) {
      m_dirty = dirty;
    }

    // Returns the linked list of coupled cursors (can be NULL)
    BtreeCursor *get_cursor_list() {
      return (m_cursor_list);
    }

    // Sets the (head of the) linked list of cursors
    void set_cursor_list(BtreeCursor *cursor) {
      m_cursor_list = cursor;
    }

    // Returns the page's type (kType*)
    ham_u32_t get_type() const {
      return (ham_db2h32(m_data->_s._flags));
    }

    // Sets the page's type (kType*)
    void set_type(ham_u32_t type) {
      m_data->_s._flags = ham_h2db32(type);
    }

    // Returns the lsn of the last modification
    ham_u64_t get_lsn() const {
      return (ham_db2h64(m_data->_s._lsn));
    }

    // Sets the lsn of the last modification
    void set_lsn(ham_u64_t lsn) {
      m_data->_s._lsn = ham_h2db64(lsn);
    }

    // Sets the pointer to the persistent data
    void set_data(PPageData *data) {
      m_data = data;
    }

    // Returns the pointer to the persistent data
    PPageData *get_data() {
      return (m_data);
    }

    // Returns the persistent payload (after the header!)
    ham_u8_t *get_payload() {
      return (m_data->_s._payload);
    }
    
    // Returns the persistent payload (after the header!)
    const ham_u8_t *get_payload() const {
      return (m_data->_s._payload);
    }

    // Returns the persistent payload (including the header!)
    ham_u8_t *get_raw_payload() {
      return (m_data->_p);
    }

    // Returns the persistent payload (including the header!)
    const ham_u8_t *get_raw_payload() const {
      return (m_data->_p);
    }

    // Allocates a new page from the device
    // |flags|: either 0 or kInitializeWithZeroes
    void allocate(ham_u32_t type, ham_u32_t flags = 0);

    // Reads a page from the device
    void fetch(ham_u64_t address);

    // Writes the page to the device
    void flush();

    // Returns true if this page is in a linked list
    bool is_in_list(Page *list_head, int which) {
      if (get_next(which))
        return (true);
      if (get_previous(which))
        return (true);
      if (list_head == this)
        return (true);
      return (false);
    }

    // Inserts this page at the beginning of a list and returns the
    // new head of the list
    Page *list_insert(Page *list_head, int which) {
      set_next(which, 0);
      set_previous(which, 0);

      if (!list_head)
        return (this);

      set_next(which, list_head);
      list_head->set_previous(which, this);
      return (this);
    }

    // Removes this page from a list and returns the new head of the list
    Page *list_remove(Page *list_head, int which) {
      Page *n, *p;

      if (this == list_head) {
        n = get_next(which);
        if (n)
          n->set_previous(which, 0);
        set_next(which, 0);
        set_previous(which, 0);
        return (n);
      }

      n = get_next(which);
      p = get_previous(which);
      if (p)
        p->set_next(which, n);
      if (n)
        n->set_previous(which, p);
      set_next(which, 0);
      set_previous(which, 0);
      return (list_head);
    }

    // Returns the next page in a linked list
    Page *get_next(int which) {
      return (m_next[which]);
    }

    // Returns the previous page of a linked list
    Page *get_previous(int which) {
      return (m_prev[which]);
    }

    // Returns the cached BtreeNodeProxy
    BtreeNodeProxy *get_node_proxy() {
      return (m_node_proxy);
    }

    // Sets the cached BtreeNodeProxy
    void set_node_proxy(BtreeNodeProxy *proxy) {
      m_node_proxy = proxy;
    }

  private:
    // Sets the previous page of a linked list
    void set_previous(int which, Page *other) {
      m_prev[which] = other;
    }

    // Sets the next page in a linked list
    void set_next(int which, Page *other) {
      m_next[which] = other;
    }

    // the Environment handle
    LocalEnvironment *m_env;

    // the Database handle (can be NULL)
    LocalDatabase *m_db;

    // address of this page
    ham_u64_t m_address;

    // non-persistent flags
    ham_u32_t m_flags;

    // is this page dirty and needs to be flushed to disk?
    bool m_dirty;

    // linked list of all cursors which point to that page
    BtreeCursor *m_cursor_list;

    // linked lists of pages - see comments above
    Page *m_prev[Page::kListMax];
    Page *m_next[Page::kListMax];

    // the cached BtreeNodeProxy object
    BtreeNodeProxy *m_node_proxy;

    // from here on everything will be written to disk
    PPageData *m_data;
};

} // namespace hamsterdb

#endif /* HAM_PAGE_H__ */
