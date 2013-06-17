/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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
  ham_fd_t fd;

  REQUIRE(0 == os_open("Makefile.am", 0, &fd));
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/openReadOnlyClose",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  const char *p = "# XXXXXXXXX ERROR\n";

  REQUIRE(0 == os_open("Makefile.am", HAM_READ_ONLY, &fd));
  REQUIRE(HAM_IO_ERROR == os_pwrite(fd, 0, p, (ham_size_t)strlen(p)));
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/negativeOpenTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;

  REQUIRE(HAM_FILE_NOT_FOUND == os_open("__98324kasdlf.blöd", 0, &fd));
}

TEST_CASE("OsTest/createCloseTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/createCloseOverwrite",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  ham_u64_t filesize;

  for (int i = 0; i < 3; i++) {
    REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
    REQUIRE(0 == os_seek(fd, 0, HAM_OS_SEEK_END));
    REQUIRE(0 == os_tell(fd, &filesize));
    REQUIRE(0ull == filesize);
    REQUIRE(0 == os_truncate(fd, 1024));
    REQUIRE(0 == os_seek(fd, 0, HAM_OS_SEEK_END));
    REQUIRE(0 == os_tell(fd, &filesize));
    REQUIRE(1024ull == filesize);
    REQUIRE(0 == os_close(fd));
  }
}

TEST_CASE("OsTest/closeTest",
           "Tests the operating system functions in os*")
{
#ifndef WIN32  // crashs in ntdll.dll
  REQUIRE(HAM_IO_ERROR == os_close((ham_fd_t)0x12345));
#endif
}

TEST_CASE("OsTest/openExclusiveTest",
           "Tests the operating system functions in os*")
{
  /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
  ham_fd_t fd, fd2;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  REQUIRE(0 == os_close(fd));

  REQUIRE(0 == os_open(Globals::opath(".test"), 0, &fd));
  REQUIRE(HAM_WOULD_BLOCK == os_open(Globals::opath(".test"), 0, &fd2));
  REQUIRE(0 == os_close(fd));
  REQUIRE(0 == os_open(Globals::opath(".test"), 0, &fd2));
  REQUIRE(0 == os_close(fd2));
  REQUIRE(0 == os_open(Globals::opath(".test"), 0, &fd2));
  REQUIRE(0 == os_close(fd2));
#endif
}

TEST_CASE("OsTest/readWriteTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  char buffer[128], orig[128];

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 10; i++) {
    memset(buffer, i, sizeof(buffer));
    REQUIRE(0 ==
          os_pwrite(fd, i * sizeof(buffer), buffer, sizeof(buffer)));
  }
  for (int i = 0; i < 10; i++) {
    memset(orig, i, sizeof(orig));
    memset(buffer, 0, sizeof(buffer));
    REQUIRE(0 ==
          os_pread(fd, i * sizeof(buffer), buffer, sizeof(buffer)));
    REQUIRE(0 == memcmp(buffer, orig, sizeof(buffer)));
  }
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/mmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_size_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    REQUIRE(0 == os_pwrite(fd, i * ps, p1, ps));
  }
  for (int i = 0; i < 10; i++) {
    memset(p1, i, ps);
    REQUIRE(0 == os_mmap(fd, &mmaph, i * ps, ps, 0, &p2));
    REQUIRE(0 == memcmp(p1, p2, ps));
    REQUIRE(0 == os_munmap(&mmaph, p2, ps));
  }
  REQUIRE(0 == os_close(fd));
  free(p1);
}

TEST_CASE("OsTest/mmapAbortTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_size_t ps = os_get_granularity();
  ham_u8_t *page, *mapped;
  page = (ham_u8_t *)malloc(ps);

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  memset(page, 0x13, ps);
  REQUIRE(0 == os_pwrite(fd, 0, page, ps));

  REQUIRE(0 == os_mmap(fd, &mmaph, 0, ps, 0, &mapped));
  /* modify the page */
  memset(mapped, 0x42, ps);
  /* unmap */
  REQUIRE(0 == os_munmap(&mmaph, mapped, ps));
  /* read again */
  memset(page, 0, ps);
  REQUIRE(0 == os_pread(fd, 0, page, ps));
  /* compare */
  REQUIRE(0x13 == page[0]);

  REQUIRE(0 == os_close(fd));
  free(page);
}

TEST_CASE("OsTest/mmapReadOnlyTest",
           "Tests the operating system functions in os*")
{
  int i;
  ham_fd_t fd, mmaph;
  ham_size_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  p1 = (ham_u8_t *)malloc(ps);

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    REQUIRE(0 == os_pwrite(fd, i * ps, p1, ps));
  }
  REQUIRE(0 == os_close(fd));

  REQUIRE(0 == os_open(Globals::opath(".test"), HAM_READ_ONLY, &fd));
  for (i = 0; i < 10; i++) {
    memset(p1, i, ps);
    REQUIRE(0 == os_mmap(fd, &mmaph, i * ps, ps, true, &p2));
    REQUIRE(0 == memcmp(p1, p2, ps));
    REQUIRE(0 == os_munmap(&mmaph, p2, ps));
  }
  REQUIRE(0 == os_close(fd));
  free(p1);
}

TEST_CASE("OsTest/multipleMmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_size_t ps = os_get_granularity();
  ham_u8_t *p1, *p2;
  ham_u64_t addr = 0, size;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    REQUIRE(0 == os_pwrite(fd, addr, p1, (ham_size_t)size));
    free(p1);
    addr += size;
  }

  addr = 0;
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (ham_u8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    REQUIRE(0 == os_mmap(fd, &mmaph, addr, (ham_size_t)size, 0, &p2));
    REQUIRE(0 == memcmp(p1, p2, (size_t)size));
    REQUIRE(0 == os_munmap(&mmaph, p2, (ham_size_t)size));
    free(p1);
    addr += size;
  }
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/negativeMmapTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd, mmaph;
  ham_u8_t *page;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  // bad address && page size! - i don't know why this succeeds
  // on MacOS...
#ifndef __MACH__
  REQUIRE(HAM_IO_ERROR == os_mmap(fd, &mmaph, 33, 66, 0, &page));
#endif
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/writevTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  const char *hello = "hello ";
  const char *world = "world!";
  char buffer[128];

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  REQUIRE(0 == os_truncate(fd, 128));

  REQUIRE(0 ==
      os_writev(fd, (void *)hello, strlen(hello),
            (void *)world, strlen(world) + 1));
  memset(buffer, 0, sizeof(buffer));
  REQUIRE(0 == os_pread(fd, 0, buffer, sizeof(buffer)));
  REQUIRE(0 == strcmp("hello world!", buffer));

  REQUIRE(0 == os_seek(fd, 10, HAM_OS_SEEK_SET));
  REQUIRE(0 ==
      os_writev(fd, (void *)hello, strlen(hello),
            (void *)world, strlen(world) + 1));
  REQUIRE(0 == os_pread(fd, 10, buffer, sizeof(buffer)-10));
  REQUIRE(0 == strcmp("hello world!", buffer));

  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/seekTellTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  ham_u64_t tell;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 10; i++) {
    REQUIRE(0 == os_seek(fd, i, HAM_OS_SEEK_SET));
    REQUIRE(0 == os_tell(fd, &tell));
    REQUIRE(tell == (ham_u64_t)i);
  }
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/negativeSeekTest",
           "Tests the operating system functions in os*")
{
  REQUIRE(HAM_IO_ERROR == os_seek((ham_fd_t)0x12345, 0, HAM_OS_SEEK_SET));
}

TEST_CASE("OsTest/truncateTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  ham_u64_t fsize;

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 10; i++) {
    REQUIRE(0 == os_truncate(fd, i * 128));
    REQUIRE(0 == os_get_filesize(fd, &fsize));
    REQUIRE(fsize == (ham_u64_t)(i * 128));
  }
  REQUIRE(0 == os_close(fd));
}

TEST_CASE("OsTest/largefileTest",
           "Tests the operating system functions in os*")
{
  ham_fd_t fd;
  ham_u8_t kb[1024];
  ham_u64_t tell;

  memset(kb, 0, sizeof(kb));

  REQUIRE(0 == os_create(Globals::opath(".test"), 0, 0664, &fd));
  for (int i = 0; i < 4 * 1024; i++)
    REQUIRE(0 == os_pwrite(fd, i * sizeof(kb), kb, sizeof(kb)));
  REQUIRE(0 == os_close(fd));

  REQUIRE(0 == os_open(Globals::opath(".test"), 0, &fd));
  REQUIRE(0 == os_seek(fd, 0, HAM_OS_SEEK_END));
  REQUIRE(0 == os_tell(fd, &tell));
  REQUIRE(tell == (ham_u64_t)1024 * 1024 * 4);
  REQUIRE(0 == os_close(fd));
}

