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

#include "3rdparty/catch/catch.hpp"

#include "os.hpp"
#include "fixture.hpp"

#include "3page_manager/page_manager.h"
#include "3btree/btree_flags.h"
#include "3blob_manager/blob_manager_disk.h"
#include "4db/db_local.h"
#include "4context/context.h"

namespace upscaledb {

struct BlobManagerProxy {
  BlobManagerProxy(LocalEnv *lenv)
    : blob_manager(lenv->blob_manager.get()) {
  }

  uint64_t allocate(Context *context, std::vector<uint8_t> &buffer) {
    ups_record_t rec = ups_make_record(buffer.data(), (uint32_t)buffer.size());
    uint64_t blobid = blob_manager->allocate(context, &rec, 0);
    REQUIRE(blobid != 0);
    return blobid;
  }

  BlobManagerProxy &require_read(Context *context, uint64_t blobid,
                  std::vector<uint8_t> &buffer, ByteArray *arena) {
    ups_record_t record = {0};
    blob_manager->read(context, blobid, &record, 0, arena);
    REQUIRE(record.size == buffer.size());
    REQUIRE(0 == ::memcmp(buffer.data(), record.data, record.size));
    return *this;
  }

  BlobManagerProxy &require_erase(Context *context, uint64_t blobid) {
    blob_manager->erase(context, blobid, 0);
    return *this;
  }

  BlobManagerProxy &require_overwrite(Context *context, uint64_t blobid,
                  std::vector<uint8_t> &buffer) {
    overwrite(context, blobid, buffer);
    return *this;
  }

  uint64_t overwrite(Context *context, uint64_t blobid,
                  std::vector<uint8_t> &buffer) {
    ups_record_t rec = ups_make_record(buffer.data(), (uint32_t)buffer.size());
    blobid = blob_manager->overwrite(context, blobid, &rec, 0);
    REQUIRE(blobid != 0ull);
    return blobid;
  }

  BlobManager *blob_manager;
};

struct BlobManagerFixture : BaseFixture {

  ScopedPtr<Context> context;

  BlobManagerFixture(uint32_t flags = 0, uint32_t cache_size = 0,
                  uint32_t page_size = 0) {
    ups_parameter_t params[3] = {
      { UPS_PARAM_CACHESIZE, cache_size },
      { UPS_PARAM_PAGESIZE, (page_size ? page_size : 4096) },
      { 0, 0 }
    };

    require_create(flags, params);

    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~BlobManagerFixture() {
    context->changeset.clear();
  }

  PBlobPageHeader *blob_page_header(uint64_t blobid) {
    Page *page = lenv()->page_manager->fetch(context.get(),
                    (blobid / lenv()->config.page_size_bytes)
                          * lenv()->config.page_size_bytes);
    return PBlobPageHeader::from_page(page);
  }

  void overwriteMappedBlob() {
    std::vector<uint8_t> buffer(128);
    std::fill(buffer.begin(), buffer.end(), 0x12);
    ups_key_t key = {0};

    DbProxy dbp(db);
    dbp.require_insert(&key, buffer);

    // reopen the file
    context->changeset.clear();
    close();
    require_open();

    std::fill(buffer.begin(), buffer.end(), 0x13);
    dbp = DbProxy(db);
    dbp.require_overwrite(&key, buffer)
       .require_find(&key, buffer);
  }

  void allocReadFreeTest() {
    std::vector<uint8_t> buffer(64);
    std::fill(buffer.begin(), buffer.end(), 0x12);

    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer);
    ByteArray *arena = &ldb()->record_arena(0);
    bmp.require_read(context.get(), blobid, buffer, arena)
       .require_erase(context.get(), blobid);
  }

  void freeBlobTest() {
    std::vector<uint8_t> buffer(64);
    std::fill(buffer.begin(), buffer.end(), 0x12);

    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer);
    uint64_t page_id = (blobid / lenv()->config.page_size_bytes)
                            * lenv()->config.page_size_bytes;

    PageManager *page_manager = lenv()->page_manager.get();
    REQUIRE(page_manager->state->freelist.has(page_id) == false);

    bmp.require_erase(context.get(), blobid);

    REQUIRE(page_manager->state->freelist.has(page_id) == true);
  }

  void replaceTest() {
    std::vector<uint8_t> buffer1(64);
    std::fill(buffer1.begin(), buffer1.end(), 0x12);
    std::vector<uint8_t> buffer2(64);
    std::fill(buffer2.begin(), buffer2.end(), 0x13);

    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer1);

    ByteArray *arena = &ldb()->record_arena(0);
    bmp.require_read(context.get(), blobid, buffer1, arena)
       .require_overwrite(context.get(), blobid, buffer2)
       .require_read(context.get(), blobid, buffer2, arena)
       .require_erase(context.get(), blobid);
  }

  void replaceWithBigTest() {
    std::vector<uint8_t> buffer1(64);
    std::fill(buffer1.begin(), buffer1.end(), 0x12);
    std::vector<uint8_t> buffer2(128);
    std::fill(buffer2.begin(), buffer2.end(), 0x13);

    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer1);

    ByteArray *arena = &ldb()->record_arena(0);
    bmp.require_read(context.get(), blobid, buffer1, arena);
    blobid = bmp.overwrite(context.get(), blobid, buffer2);
    bmp.require_read(context.get(), blobid, buffer2, arena)
       .require_erase(context.get(), blobid);
  }

  void replaceWithSmallTest() {
    std::vector<uint8_t> buffer1(128);
    std::fill(buffer1.begin(), buffer1.end(), 0x12);
    std::vector<uint8_t> buffer2(64);
    std::fill(buffer2.begin(), buffer2.end(), 0x13);

    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer1);

    ByteArray *arena = &ldb()->record_arena(0);
    bmp.require_read(context.get(), blobid, buffer1, arena);

    // verify the freelist information
    if (!is_in_memory()) {
      PBlobPageHeader *header = blob_page_header(blobid);
      if (lenv()->config.page_size_bytes == 1024 * 16) {
        REQUIRE(header->free_bytes == 3666);
        REQUIRE(header->freelist[0].size == 3666);
      }
      REQUIRE(header->freelist[0].offset == 428);
    }

    // overwrite the blob
    blobid = bmp.overwrite(context.get(), blobid, buffer2);
    bmp.require_read(context.get(), blobid, buffer2, arena);

    // verify the freelist information - free area must have increased
    // by 64 bytes (the size difference between both records)
    if (!is_in_memory()) {
      PBlobPageHeader *header = blob_page_header(blobid);
      if (lenv()->config.page_size_bytes == 1024 * 16) {
        REQUIRE(header->free_bytes == 3666 - 64);
        REQUIRE(header->freelist[0].size == 3666);
      }
    }

    bmp.require_erase(context.get(), blobid);

    // once more check the freelist
    if (!is_in_memory()) {
      PBlobPageHeader *header = blob_page_header(blobid);
      if (lenv()->config.page_size_bytes == 1024 * 16) {
        REQUIRE(header->free_bytes == 3758);
        REQUIRE(header->freelist[0].size == 3666);
      }
    }
  }

  void replaceBiggerAndBiggerTest() {
    const int BLOCKS = 32;
    unsigned page_size = lenv()->config.page_size_bytes;
    std::vector<uint8_t> buffer(page_size * BLOCKS * 2);
    std::fill(buffer.begin(), buffer.end(), 0x13);

    // first: create a big blob and erase it - we want to use the
    // space from the freelist
    BlobManagerProxy bmp(lenv());
    uint64_t blobid = bmp.allocate(context.get(), buffer);

    // verify it, then erase it
    ByteArray *arena = &ldb()->record_arena(0);
    bmp.require_read(context.get(), blobid, buffer, arena)
       .require_erase(context.get(), blobid);

    // now use a loop to allocate the buffer, and make it bigger and
    // bigger
    for (int i = 1; i < 32; i++) {
      std::fill(buffer.begin(), buffer.end(), (uint8_t)i);
      if (i == 1)
        blobid = bmp.allocate(context.get(), buffer);
      else
        blobid = bmp.overwrite(context.get(), blobid, buffer);
      bmp.require_read(context.get(), blobid, buffer, arena);
    }

    bmp.require_erase(context.get(), blobid);
  }

  void loopInsert(int loops, int factor) {
    ups_txn_t *txn = 0; // need a txn object for the blob routines

    std::vector<uint64_t> blobid(loops);
    BlobManagerProxy bmp(lenv());
    ByteArray *arena = &ldb()->record_arena(0);

    if (!is_in_memory() && uses_transactions())
      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));

    for (int i = 0; i < loops; i++) {
      std::vector<uint8_t> buffer((i + 1) * factor);
      std::fill(buffer.begin(), buffer.end(), (uint8_t)i);

      blobid[i] = bmp.allocate(context.get(), buffer);
    }

    for (int i = 0; i < loops; i++) {
      std::vector<uint8_t> buffer((i + 1) * factor);
      std::fill(buffer.begin(), buffer.end(), (uint8_t)i);

      bmp.require_read(context.get(), blobid[i], buffer, arena);
    }

    for (int i = 0; i < loops; i++)
      bmp.require_erase(context.get(), blobid[i]);

    if (!is_in_memory() && uses_transactions())
      REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void multipleAllocReadFreeTest() {
    loopInsert(20, 2048);
  }

  void hugeBlobTest() {
    loopInsert(10, 1024 * 1024 * 4);
  }

  void smallBlobTest() {
    loopInsert(20, 64);
  }
};

TEST_CASE("BlobManager/overwriteMappedBlob", "")
{
  BlobManagerFixture f;
  f.overwriteMappedBlob();
}

TEST_CASE("BlobManager/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/freeBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.freeBlobTest();
}

TEST_CASE("BlobManager/replaceTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.replaceTest();
}

TEST_CASE("BlobManager/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/notxn/allocReadFreeTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/notxn/replaceTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.replaceTest();
}

TEST_CASE("BlobManager/notxn/replaceWithBigTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/notxn/replaceWithSmallTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/notxn/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/notxn/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/notxn/hugeBlobTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/notxn/smallBlobTest", "")
{
  BlobManagerFixture f(0, 1024);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/64k/replaceTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager/64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/64k/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/64k/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 1024 * 64, 1024 * 64);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/nocache/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/nocache/replaceTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.replaceTest();
}

TEST_CASE("BlobManager/nocache/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/nocache/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/nocache/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/nocache/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/nocache/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/nocache/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/nocache-notxn/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/nocache-notxn/replaceTest", "")
{
  BlobManagerFixture f(0, 0);
  f.replaceTest();
}

TEST_CASE("BlobManager/nocache-notxn/replaceWithBigTest", "")
{
  BlobManagerFixture f(0, 0);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/nocache-notxn/replaceWithSmallTest", "")
{
  BlobManagerFixture f(0, 0);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/nocache-notxn/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(0, 0);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/nocache-notxn/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(0, 0);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/nocache-notxn/hugeBlobTest", "")
{
  BlobManagerFixture f(0, 0);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/nocache-notxn/smallBlobTest", "")
{
  BlobManagerFixture f(0, 0);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/nocache-64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/nocache-64k/replaceTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager/nocache-64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/nocache-64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/nocache-64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/nocache-64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/nocache-64k/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/nocache-64k/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_ENABLE_TRANSACTIONS, 0, 1024 * 64);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/inmem/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/inmem/replaceTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.replaceTest();
}

TEST_CASE("BlobManager/inmem/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/inmem/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/inmem/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/inmem/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/inmem/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/inmem/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY);
  f.smallBlobTest();
}


TEST_CASE("BlobManager/inmem-64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/inmem-64k/replaceTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager/inmem-64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/inmem-64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/inmem-64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/inmem-64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/inmem-64k/hugeBlobTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/inmem-64k/smallBlobTest", "")
{
  BlobManagerFixture f(UPS_IN_MEMORY, 0, 1024 * 64);
  f.smallBlobTest();
}

} // namespace upscaledb
