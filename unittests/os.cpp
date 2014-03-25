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

#include "../src/config.h"

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/os.h"
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
  ham_fd_t fd = os_open("Makefile.am", 0);
  os_close(fd);
}

TEST_CASE("OsTest/openReadOnlyClose",
           "Tests the operating system functions in os*")
{
  const char *p = "# XXXXXXXXX ERROR\n";

  ham_fd_t fd = os_open("Makefile.am", HAM_READ_ONLY);
  REQUIRE_CATCH(os_pwrite(fd, 0, p, (ham_u32_t)strlen(p)), HAM_IO_ERROR);
  os_close(fd);
}

TEST_CASE("OsTest/negativeOpenTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;

  REQUIRE_CATCH(fd = os_open("__98324kasdlf.blöd", 0), HAM_FILE_NOT_FOUND);
  (void)fd; // avoid gcc warnign
}

TEST_CASE("OsTest/createCloseTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd = os_create(Globals::opath(".test"), 0, 0664);
  os_close(fd);
}

TEST_CASE("OsTest/createCloseOverwrite",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;

  for (int i = 0; i < 3; i++) {
    fd = os_create(Globals::opath(".test"), 0, 0664);
    os_seek(fd, 0, HAM_OS_SEEK_END);
    REQUIRE(0ull == os_tell(fd));
    os_truncate(fd, 1024);
    os_seek(fd, 0, HAM_OS_SEEK_END);
    REQUIRE(1024ull == os_tell(fd));
    os_close(fd);
  }
}

TEST_CASE("OsTest/closeTest",
           "Tests the operating system functions in os*")
{
#ifndef WIN32  // crashs in ntdll.dll
  REQUIRE_CATCH(os_close((ham_fd_t)0x12345), HAM_IO_ERROR);
#endif
}

TEST_CASE("OsTest/openExclusiveTest",
           "Tests the operating system functions in os*")
{
  /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
  ham_fd_t fd, fd2;

  fd = os_create(Globals::opath(".test"), 0, 0664);
  os_close(fd);

  fd = os_open(Globals::opath(".test"), 0);
  REQUIRE_CATCH(fd2 = os_open(Globals::opath(".test"), 0), HAM_WOULD_BLOCK);
  os_close(fd);
  fd2 = os_open(Globals::opath(".test"), 0);
  os_close(fd2);
  fd2 = os_open(Globals::opath(".test"), 0);
  os_close(fd2);
#endif
}

TEST_CASE("OsTest/readWriteTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  char buffer[128], orig[128];

  fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 10; i++) {
    memset(buffer, i, sizeof(buffer));
    os_pwrite(fd, i * sizeof(buffer), buffer, sizeof(buffer));
  }
  for (int i = 0; i < 10; i++) {
    memset(orig, i, sizeof(orig));
    memset(buffer, 0, sizeof(buffer));
    os_pread(fd, i * sizeof(buffer), buffer, sizeof(buffer));
    REQUIRE(0 == memcmp(buffer, orig, sizeof(buffer)));
  }
  os_close(fd);
}

TEST_CASE("OsTest/mmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_u32_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    os_pwrite(fd, i * ps, p1, ps);
  }
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    os_mmap(fd, &mmaph, i * ps, ps, 0, &p2);
    REQUIRE(0 == memcmp(p1, p2, ps));
    os_munmap(&mmaph, p2, ps);
  }
  os_close(fd);
  free(p1);
}

TEST_CASE("OsTest/mmapAbortTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_u32_t ps = os_get_granularity();
  ham_u8_t *page, *mapped;
  page = (ham_u8_t *)malloc(ps);

  fd = os_create(Globals::opath(".test"), 0, 0664);
  memset(page, 0x13, ps);
  os_pwrite(fd, 0, page, ps);

  os_mmap(fd, &mmaph, 0, ps, 0, &mapped);
  /* modify the page */
  memset(mapped, 0x42, ps);
  /* unmap */
  os_munmap(&mmaph, mapped, ps);
  /* read again */
  memset(page, 0, ps);
  os_pread(fd, 0, page, ps);
  /* compare */
  REQUIRE(0x13 == page[0]);

  os_close(fd);
  free(page);
}

TEST_CASE("OsTest/mmapReadOnlyTest",
           "Tests the operating system functions in os*")
{
  int i;
  ham_fd_t fd, mmaph;
  ham_u32_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  fd = os_create(Globals::opath(".test"), 0, 0664);
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    os_pwrite(fd, i * ps, p1, ps);
  }
  os_close(fd);

  fd = os_open(Globals::opath(".test"), HAM_READ_ONLY);
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    os_mmap(fd, &mmaph, i * ps, ps, true, &p2);
    REQUIRE(0 == memcmp(p1, p2, ps));
    os_munmap(&mmaph, p2, ps);
  }
  os_close(fd);
  free(p1);
}

TEST_CASE("OsTest/multipleMmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_u32_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  ham_u64_t addr = 0, size;

  fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    os_pwrite(fd, addr, p1, (ham_u32_t)size);
    free(p1);
    addr += size;
  }

  addr = 0;
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    os_mmap(fd, &mmaph, addr, (ham_u32_t)size, 0, &p2);
    REQUIRE(0 == memcmp(p1, p2, (size_t)size));
    os_munmap(&mmaph, p2, (ham_u32_t)size);
    free(p1);
    addr += size;
  }
  os_close(fd);
}

TEST_CASE("OsTest/negativeMmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_u8_t *page;

  fd = os_create(Globals::opath(".test"), 0, 0664);
  // bad address && page size! - i don't know why this succeeds
  // on MacOS...
#ifndef __MACH__
  REQUIRE_CATCH(os_mmap(fd, &mmaph, 33, 66, 0, &page), HAM_IO_ERROR);
#endif
  os_close(fd);
}

TEST_CASE("OsTest/seekTellTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 10; i++) {
    os_seek(fd, i, HAM_OS_SEEK_SET);
    REQUIRE((ham_u64_t)i == os_tell(fd));
  }
  os_close(fd);
}

TEST_CASE("OsTest/negativeSeekTest",
           "Tests the operating system functions in os*")
{
  REQUIRE_CATCH(os_seek((ham_fd_t)0x12345, 0, HAM_OS_SEEK_SET), HAM_IO_ERROR);
}

TEST_CASE("OsTest/truncateTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 10; i++) {
    os_truncate(fd, i * 128);
    REQUIRE((ham_u64_t)(i * 128) == os_get_file_size(fd));
  }
  os_close(fd);
}

TEST_CASE("OsTest/largefileTest",
           "Tests the operating system functions in os*")
{
  ham_u8_t kb[1024] = {0};

  ham_fd_t fd = os_create(Globals::opath(".test"), 0, 0664);
  for (int i = 0; i < 4 * 1024; i++)
    os_pwrite(fd, i * sizeof(kb), kb, sizeof(kb));
  os_close(fd);

  fd = os_open(Globals::opath(".test"), 0);
  os_seek(fd, 0, HAM_OS_SEEK_END);
  REQUIRE(os_tell(fd) == (ham_u64_t)1024 * 1024 * 4);
  os_close(fd);
}

