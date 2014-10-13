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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1os/file.h"
#include "os.hpp"

#ifdef WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

using namespace hamsterdb;

TEST_CASE("OsTest/openClose",
           "Tests the operating system functions in os*")
{
  File f;
  f.open("Makefile.am", false);
  f.close();
}

TEST_CASE("OsTest/openReadOnlyClose",
           "Tests the operating system functions in os*")
{
  const char *p = "# XXXXXXXXX ERROR\n";

  File f;
  f.open("Makefile.am", true);
  REQUIRE_CATCH(f.pwrite(0, p, (ham_u32_t)strlen(p)), HAM_IO_ERROR);
}

TEST_CASE("OsTest/negativeOpenTest",
           "Tests the operating system functions in os*")
{
  File f;

  REQUIRE_CATCH(f.open("__98324kasdlf.blöd", false), HAM_FILE_NOT_FOUND);
}

TEST_CASE("OsTest/createCloseTest",
           "Tests the operating system functions in os*")
{
  File f;
  f.create(Utils::opath(".test"), 0664);
}

TEST_CASE("OsTest/createCloseOverwrite",
           "Tests the operating system functions in os*")
{
  File f;

  for (int i = 0; i < 3; i++) {
    f.create(Utils::opath(".test"), 0664);
    f.seek(0, File::kSeekEnd);
    REQUIRE(0ull == f.tell());
    f.truncate(1024);
    f.seek(0, File::kSeekEnd);
    REQUIRE(1024ull == f.tell());
    f.close();
  }
}

TEST_CASE("OsTest/openExclusiveTest",
           "Tests the operating system functions in os*")
{
  /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
  File f1, f2;

  f1.create(Utils::opath(".test"), 0664);
  f1.close();

  f1.open(Utils::opath(".test"), false);
  REQUIRE_CATCH(f2.open(Utils::opath(".test"), false), HAM_WOULD_BLOCK);
  f1.close();
  f2.open(Utils::opath(".test"), false);
  f2.close();
  f2.open(Utils::opath(".test"), false);
  f2.close();
#endif
}

TEST_CASE("OsTest/readWriteTest",
           "Tests the operating system functions in os*")
{
  File f;
  char buffer[128], orig[128];

  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    memset(buffer, i, sizeof(buffer));
    f.pwrite(i * sizeof(buffer), buffer, sizeof(buffer));
  }
  for (int i = 0; i < 10; i++) {
    memset(orig, i, sizeof(orig));
    memset(buffer, 0, sizeof(buffer));
    f.pread(i * sizeof(buffer), buffer, sizeof(buffer));
    REQUIRE(0 == memcmp(buffer, orig, sizeof(buffer)));
  }
}

TEST_CASE("OsTest/mmapTest",
           "Tests the operating system functions in os*")
{
  File f;
  ham_u32_t ps = File::get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    f.pwrite(i * ps, p1, ps);
  }
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    f.mmap(i * ps, ps, 0, &p2);
    REQUIRE(0 == memcmp(p1, p2, ps));
    f.munmap(p2, ps);
  }
  free(p1);
}

TEST_CASE("OsTest/mmapAbortTest",
           "Tests the operating system functions in os*")
{
  File f;
  ham_u32_t ps = File::get_granularity();
  ham_u8_t *page, *mapped;
  page = (ham_u8_t *)malloc(ps);

  f.create(Utils::opath(".test"), 0664);
  memset(page, 0x13, ps);
  f.pwrite(0, page, ps);

  f.mmap(0, ps, 0, &mapped);
  /* modify the page */
  memset(mapped, 0x42, ps);
  /* unmap */
  f.munmap(mapped, ps);
  /* read again */
  memset(page, 0, ps);
  f.pread(0, page, ps);
  /* compare */
  REQUIRE(0x13 == page[0]);

  free(page);
}

TEST_CASE("OsTest/mmapReadOnlyTest",
           "Tests the operating system functions in os*")
{
  int i;
  File f;
  ham_u32_t ps = File::get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  f.create(Utils::opath(".test"), 0664);
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    f.pwrite(i * ps, p1, ps);
  }
  f.close();

  f.open(Utils::opath(".test"), true);
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    f.mmap(i * ps, ps, true, &p2);
    REQUIRE(0 == memcmp(p1, p2, ps));
    f.munmap(p2, ps);
  }
  free(p1);
}

TEST_CASE("OsTest/multipleMmapTest",
           "Tests the operating system functions in os*")
{
  File f;
  ham_u32_t ps = File::get_granularity();
  ham_u8_t *p1, *p2;
  ham_u64_t addr = 0, size;

  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    f.pwrite(addr, p1, (ham_u32_t)size);
    free(p1);
    addr += size;
  }

  addr = 0;
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    f.mmap(addr, (ham_u32_t)size, 0, &p2);
    REQUIRE(0 == memcmp(p1, p2, (size_t)size));
    f.munmap(p2, (ham_u32_t)size);
    free(p1);
    addr += size;
  }
}

TEST_CASE("OsTest/negativeMmapTest",
           "Tests the operating system functions in os*")
{
  File f;
  ham_u8_t *page;

  f.create(Utils::opath(".test"), 0664);
  // bad address && page size! - i don't know why this succeeds
  // on MacOS...
#ifndef __MACH__
  REQUIRE_CATCH(f.mmap(33, 66, 0, &page), HAM_IO_ERROR);
#endif
}

TEST_CASE("OsTest/seekTellTest",
           "Tests the operating system functions in os*")
{
  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    f.seek(i, File::kSeekSet);
    REQUIRE((ham_u64_t)i == f.tell());
  }
}

TEST_CASE("OsTest/truncateTest",
           "Tests the operating system functions in os*")
{
  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    f.truncate(i * 128);
    REQUIRE((ham_u64_t)(i * 128) == f.get_file_size());
  }
}

TEST_CASE("OsTest/largefileTest",
           "Tests the operating system functions in os*")
{
  ham_u8_t kb[1024] = {0};

  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 4 * 1024; i++)
    f.pwrite(i * sizeof(kb), kb, sizeof(kb));
  f.close();

  f.open(Utils::opath(".test"), false);
  f.seek(0, File::kSeekEnd);
  REQUIRE(f.tell() == (ham_u64_t)1024 * 1024 * 4);
  f.close();
}

