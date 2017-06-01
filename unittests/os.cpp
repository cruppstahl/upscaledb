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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "1os/file.h"

#include "os.hpp"
#include "fixture.hpp"

#ifdef WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

using namespace upscaledb;

struct FileProxy {
  ~FileProxy() {
    f.close();
  }

  FileProxy &require_open(const char *filename, bool read_only,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE_CATCH(f.open(filename, read_only), status);
    }
    else {
      f.open(filename, read_only);
    }
    return *this;
  }

  FileProxy &require_create(const char *filename, uint32_t mode,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE_CATCH(f.create(filename, mode), status);
    }
    else {
      f.create(filename, mode);
    }
    return *this;
  }

  FileProxy &require_mmap(uint64_t position, size_t size, bool readonly,
                    uint8_t **buffer, ups_status_t status = 0) {
    if (status) {
      REQUIRE_CATCH(f.mmap(position, size, readonly, buffer), status);
    }
    else {
      f.mmap(position, size, readonly, buffer);
    }
    return *this;
  }

  FileProxy &require_munmap(void *buffer, size_t size) {
    f.munmap(buffer, size);
    return *this;
  }

  FileProxy &require_pwrite(uint64_t address, const void *data, size_t length,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE_CATCH(f.pwrite(address, data, length), status);
    }
    else {
      f.pwrite(address, data, length);
    }
    return *this;
  }

  FileProxy &require_pread(uint64_t address, void *data, size_t length,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE_CATCH(f.pread(address, data, length), status);
    }
    else {
      f.pread(address, data, length);
    }
    return *this;
  }

  FileProxy &require_seek(uint64_t address, uint32_t flags) {
    f.seek(address, flags);
    return *this;
  }

  FileProxy &require_tell(uint64_t address) {
    REQUIRE(address == f.tell());
    return *this;
  }

  FileProxy &require_truncate(uint64_t address) {
    f.truncate(address);
    return *this;
  }

  FileProxy &require_size(uint64_t size) {
    REQUIRE(size == f.file_size());
    return *this;
  }

  FileProxy &close() {
    f.close();
    return *this;
  }

  File f;
};

TEST_CASE("Os/openClose")
{
  FileProxy fp;
  fp.require_open("Makefile.am", false)
    .close();
}

TEST_CASE("Os/openReadOnlyClose")
{
  const char *p = "# XXXXXXXXX ERROR\n";

  FileProxy fp;
  fp.require_open("data/test", false)
    .require_pwrite(0, p, ::strlen(p), UPS_IO_ERROR)
    .close();
}

TEST_CASE("Os/negativeOpen")
{
  FileProxy fp;
  fp.require_open("__98324kasdlf.blöd", false, UPS_FILE_NOT_FOUND);
}

TEST_CASE("Os/createClose")
{
  FileProxy fp;
  fp.require_create("test.db", 0664);
}

TEST_CASE("Os/createCloseOverwrite")
{
  FileProxy fp;

  for (int i = 0; i < 3; i++) {
    fp.require_create("test.db", 0664)
      .require_seek(0, File::kSeekEnd)
      .require_tell(0)
      .require_truncate(1024)
      .require_seek(0, File::kSeekEnd)
      .require_tell(1024)
      .close();
  }
}

TEST_CASE("Os/openExclusive")
{
  // fails on cygwin - cygwin bug?
#ifndef __CYGWIN__
  FileProxy fp1, fp2;

  fp1.require_create("test.db", 0664)
     .close()
     .require_open("test.db", false);

  fp2.require_open("test.db", false, UPS_WOULD_BLOCK);
  fp1.close();
  fp2.require_open("test.db", false);
  fp2.close();
  fp2.require_open("test.db", false);
  fp2.close();
#endif
}

TEST_CASE("Os/readWrite")
{
  FileProxy fp;
  char buffer[128], orig[128];

  fp.require_create("test.db", 0664);
  for (uint32_t i = 0; i < 10; i++) {
    ::memset(buffer, i, sizeof(buffer));
    fp.require_pwrite(i * sizeof(buffer), buffer, sizeof(buffer));
  }
  for (uint32_t i = 0; i < 10; i++) {
    ::memset(orig, i, sizeof(orig));
    ::memset(buffer, 0, sizeof(buffer));
    fp.require_pread(i * sizeof(buffer), buffer, sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, orig, sizeof(buffer)));
  }
}

TEST_CASE("Os/mmap")
{
  uint32_t page_size = File::granularity();
  std::vector<uint8_t> vec(page_size);

  FileProxy fp;
  fp.require_create("test.db", 0664);

  // append 10 pages
  for (uint8_t i = 0; i < 10; i++) {
    std::fill(vec.begin(), vec.end(), i);
    fp.require_pwrite(i * page_size, vec.data(), page_size);
  }

  for (uint8_t i = 0; i < 10; i++) {
    std::fill(vec.begin(), vec.end(), i);
    uint8_t *mapped;
    fp.require_mmap(i * page_size, page_size, 0, &mapped);
    REQUIRE(0 == ::memcmp(vec.data(), mapped, page_size));
    fp.require_munmap(mapped, page_size);
  }
}

TEST_CASE("Os/mmapAbort")
{
  uint32_t page_size = File::granularity();
  std::vector<uint8_t> vec(page_size, 0x13);
  uint8_t *mapped;

  FileProxy fp;
  fp.require_create("test.db", 0664)
    .require_pwrite(0, vec.data(), page_size)
    .require_mmap(0, page_size, 0, &mapped);

  // modify the page
  ::memset(mapped, 0x42, page_size);

  // unmap, then read again
  fp.require_munmap(mapped, page_size);
  std::fill(vec.begin(), vec.end(), 0);
  fp.require_pread(0, vec.data(), page_size);

  // compare
  for (auto v : vec) {
	REQUIRE(0x13 == v);
  }
}

TEST_CASE("Os/mmapReadOnly")
{
  uint32_t page_size = File::granularity();
  std::vector<uint8_t> vec(page_size);
  uint8_t *mapped;

  FileProxy fp;
  fp.require_create("test.db", 0664);
  for (uint8_t i = 0; i < 10; i++) {
    std::fill(vec.begin(), vec.end(), i);
    fp.require_pwrite(i * page_size, vec.data(), page_size);
  }
  fp.close();

  fp.require_open("test.db", true);
  for (uint8_t i = 0; i < 10; i++) {
    std::fill(vec.begin(), vec.end(), i);
    fp.require_mmap(i * page_size, page_size, true, &mapped);
    REQUIRE(0 == ::memcmp(vec.data(), mapped, page_size));
    fp.require_munmap(mapped, page_size);
  }
}

TEST_CASE("Os/multipleMmap")
{
  uint32_t page_size = File::granularity();
  uint64_t addr = 0;

  FileProxy fp;
  fp.require_create("test.db", 0664);

  for (uint8_t i = 0; i < 5; i++) {
    size_t size = page_size * (i + 1);
    std::vector<uint8_t> v(size, i);
    fp.require_pwrite(addr, v.data(), size);
    addr += size;
  }

  addr = 0;
  for (uint8_t i = 0; i < 5; i++) {
    size_t size = page_size * (i + 1);
    std::vector<uint8_t> v(size, i);
    uint8_t *mapped;
    fp.require_mmap(addr, size, 0, &mapped);
    REQUIRE(0 == ::memcmp(v.data(), mapped, size));
    fp.require_munmap(mapped, size);
    addr += size;
  }
}

TEST_CASE("Os/negativeMmap")
{
  // bad address && page size! - I don't know why this succeeds
  // on MacOS...
#ifndef __MACH__
  FileProxy fp;
  fp.require_create("test.db", 0664);

  uint8_t *mapped;
  fp.require_mmap(33, 66, 0, &mapped, UPS_IO_ERROR);
#endif
}

TEST_CASE("Os/seekTell")
{
  FileProxy fp;
  fp.require_create("test.db", 0664);

  for (uint64_t i = 0; i < 10; i++) {
    fp.require_seek(i, File::kSeekSet)
      .require_tell(i);
  }
}

TEST_CASE("OsTest/truncateTest")
{
  FileProxy fp;
  fp.require_create("test.db", 0664);
  for (uint64_t i = 0; i < 10; i++) {
    fp.require_truncate(i * 128)
      .require_size(i * 128);
  }
}

TEST_CASE("Os/largefile")
{
  uint8_t kb[1024] = {0};

  FileProxy fp;
  fp.require_create("test.db", 0664);
  for (uint64_t i = 0; i < 4 * 1024; i++) {
    fp.require_pwrite(i * sizeof(kb), kb, sizeof(kb));
  }
  fp.close();

  fp.require_open("test.db", false)
    .require_seek(0, File::kSeekEnd)
    .require_tell(1024 * 1024 * 4);
}

