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

using namespace upscaledb;

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
  REQUIRE_CATCH(f.pwrite(0, p, (uint32_t)strlen(p)), UPS_IO_ERROR);
}

TEST_CASE("OsTest/negativeOpenTest",
           "Tests the operating system functions in os*")
{
  File f;

  REQUIRE_CATCH(f.open("__98324kasdlf.blöd", false), UPS_FILE_NOT_FOUND);
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
  REQUIRE_CATCH(f2.open(Utils::opath(".test"), false), UPS_WOULD_BLOCK);
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
  uint32_t ps = File::granularity();
  uint8_t *p1, *p2;
  p1 = (uint8_t *)malloc(ps);

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
  uint32_t ps = File::granularity();
  uint8_t *page, *mapped;
  page = (uint8_t *)malloc(ps);

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
  uint32_t ps = File::granularity();
  uint8_t *p1, *p2;
  p1 = (uint8_t *)malloc(ps);

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
  uint32_t ps = File::granularity();
  uint8_t *p1, *p2;
  uint64_t addr = 0, size;

  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (uint8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    f.pwrite(addr, p1, (uint32_t)size);
    free(p1);
    addr += size;
  }

  addr = 0;
  for (int i = 0; i < 5; i++) {
    size = ps * (i + 1);

    p1 = (uint8_t *)malloc((size_t)size);
    memset(p1, i, (size_t)size);
    f.mmap(addr, (uint32_t)size, 0, &p2);
    REQUIRE(0 == memcmp(p1, p2, (size_t)size));
    f.munmap(p2, (uint32_t)size);
    free(p1);
    addr += size;
  }
}

TEST_CASE("OsTest/negativeMmapTest",
           "Tests the operating system functions in os*")
{
  File f;
  uint8_t *page;

  f.create(Utils::opath(".test"), 0664);
  // bad address && page size! - i don't know why this succeeds
  // on MacOS...
#ifndef __MACH__
  REQUIRE_CATCH(f.mmap(33, 66, 0, &page), UPS_IO_ERROR);
#endif
}

TEST_CASE("OsTest/seekTellTest",
           "Tests the operating system functions in os*")
{
  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    f.seek(i, File::kSeekSet);
    REQUIRE((uint64_t)i == f.tell());
  }
}

TEST_CASE("OsTest/truncateTest",
           "Tests the operating system functions in os*")
{
  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 10; i++) {
    f.truncate(i * 128);
    REQUIRE((uint64_t)(i * 128) == f.file_size());
  }
}

TEST_CASE("OsTest/largefileTest",
           "Tests the operating system functions in os*")
{
  uint8_t kb[1024] = {0};

  File f;
  f.create(Utils::opath(".test"), 0664);
  for (int i = 0; i < 4 * 1024; i++)
    f.pwrite(i * sizeof(kb), kb, sizeof(kb));
  f.close();

  f.open(Utils::opath(".test"), false);
  f.seek(0, File::kSeekEnd);
  REQUIRE(f.tell() == (uint64_t)1024 * 1024 * 4);
  f.close();
}

