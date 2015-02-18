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

#ifndef HAM_BLOB_MANAGER_DISK_H
#define HAM_BLOB_MANAGER_DISK_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

#include "1base/packstart.h"

/*
 * The header of a blob page
 *
 * Contains a fixed length freelist and a couter for the number of free
 * bytes
 */
HAM_PACK_0 class HAM_PACK_1 PBlobPageHeader
{
  public:
    void initialize() {
      memset(this, 0, sizeof(PBlobPageHeader));
    }

    // Returns a PBlobPageHeader from a page
    static PBlobPageHeader *from_page(Page *page) {
      return (PBlobPageHeader *)&page->get_payload()[0];
    }

    // Returns the number of pages which are all managed by this header
    uint32_t get_num_pages() const {
      return (m_num_pages);
    }

    // Sets the number of pages which are all managed by this header
    void set_num_pages(uint32_t num_pages) {
      m_num_pages = num_pages;
    }

    // Returns the "free bytes" counter
    uint32_t get_free_bytes() const {
      return (m_free_bytes);
    }

    // Sets the "free bytes" counter
    void set_free_bytes(uint32_t free_bytes) {
      m_free_bytes = free_bytes;
    }

    // Returns the total number of freelist entries
    uint8_t get_freelist_entries() const {
      return (32);
    }

    // Returns the offset of freelist entry |i|
    uint32_t get_freelist_offset(uint32_t i) const {
      return (m_freelist[i].offset);
    }

    // Sets the offset of freelist entry |i|
    void set_freelist_offset(uint32_t i, uint32_t offset) {
      m_freelist[i].offset = offset;
    }

    // Returns the size of freelist entry |i|
    uint32_t get_freelist_size(uint32_t i) const {
      return (m_freelist[i].size);
    }

    // Sets the size of freelist entry |i|
    void set_freelist_size(uint32_t i, uint32_t size) {
      m_freelist[i].size = size;
    }

  private:
    // Number of "regular" pages for this blob; used for blobs exceeding
    // a page size
    uint32_t m_num_pages;

    // Number of free bytes in this page
    uint32_t m_free_bytes;

    struct FreelistEntry {
      uint32_t offset;
      uint32_t size;
    };

    // The freelist - offset/size pairs in this page
    FreelistEntry m_freelist[32];
} HAM_PACK_2;

#include "1base/packstop.h"


/*
 * A BlobManager for disk-based databases
 */
class DiskBlobManager : public BlobManager
{
  enum {
    // Overhead per page
    kPageOverhead = Page::kSizeofPersistentHeader + sizeof(PBlobPageHeader)
  };

  public:
    DiskBlobManager(LocalEnvironment *env)
      : BlobManager(env) {
    }

  protected:
    // allocate/create a blob
    // returns the blob-id (the start address of the blob header)
    virtual uint64_t do_allocate(Context *context, ham_record_t *record,
                    uint32_t flags);

    // reads a blob and stores the data in |record|. The pointer |record.data|
    // is backed by the |arena|, unless |HAM_RECORD_USER_ALLOC| is set.
    // flags: either 0 or HAM_DIRECT_ACCESS
    virtual void do_read(Context *context, uint64_t blobid,
                    ham_record_t *record, uint32_t flags,
                    ByteArray *arena);

    // retrieves the size of a blob
    virtual uint64_t do_get_blob_size(Context *context, uint64_t blobid);

    // overwrite an existing blob
    //
    // will return an error if the blob does not exist
    // returns the blob-id (the start address of the blob header) in |blobid|
    virtual uint64_t do_overwrite(Context *context, uint64_t old_blobid,
                    ham_record_t *record, uint32_t flags);

    // delete an existing blob
    virtual void do_erase(Context *context, uint64_t blobid,
                    Page *page = 0, uint32_t flags = 0);

  private:
    friend class DuplicateManager;
    friend struct BlobManagerFixture;

    // write a series of data chunks to storage at file offset 'addr'.
    //
    // The chunks are assumed to be stored in sequential order, adjacent
    // to each other, i.e. as one long data strip.
    void write_chunks(Context *context, Page *page, uint64_t addr,
                    uint8_t **chunk_data, uint32_t *chunk_size,
                    uint32_t chunks);

    // Same as above, but for reading chunks from the file. The data
    // is copied to |data|.
    void copy_chunk(Context *context, Page *page, Page **fpage,
                    uint64_t addr, uint8_t *data, uint32_t size,
                    bool fetch_read_only);

    // Same as |copy_chunk|, but does not copy the data
    uint8_t *read_chunk(Context *context, Page *page, Page **fpage,
                    uint64_t addr, bool fetch_read_only);

    // adds a free chunk to the freelist
    void add_to_freelist(PBlobPageHeader *header, uint32_t offset,
                    uint32_t size);

    // searches the freelist for a free chunk; if available, returns |true|
    // and stores the offset in |poffset|.
    bool alloc_from_freelist(PBlobPageHeader *header, uint32_t size,
                    uint64_t *poffset);

    // verifies the integrity of the freelist
    bool check_integrity(PBlobPageHeader *header) const;
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_DISK_H */
