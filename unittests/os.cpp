/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include <ham/hamsterdb.h>
#include "../src/os.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

#ifdef WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

class OsTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  OsTest()
    : hamsterDB_fixture("OsTest") {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(OsTest, openCloseTest);
    BFC_REGISTER_TEST(OsTest, openReadOnlyCloseTest);
    BFC_REGISTER_TEST(OsTest, negativeOpenCloseTest);
    BFC_REGISTER_TEST(OsTest, createCloseTest);
    BFC_REGISTER_TEST(OsTest, createCloseOverwriteTest);
    BFC_REGISTER_TEST(OsTest, closeTest);
    BFC_REGISTER_TEST(OsTest, openExclusiveTest);
    BFC_REGISTER_TEST(OsTest, readWriteTest);
#if HAVE_MMAP
    BFC_REGISTER_TEST(OsTest, mmapTest);
    BFC_REGISTER_TEST(OsTest, mmapAbortTest);
    BFC_REGISTER_TEST(OsTest, mmapReadOnlyTest);
    BFC_REGISTER_TEST(OsTest, multipleMmapTest);
    BFC_REGISTER_TEST(OsTest, negativeMmapTest);
#endif
#if HAVE_WRITEV
    BFC_REGISTER_TEST(OsTest, writevTest);
#endif
    BFC_REGISTER_TEST(OsTest, seekTellTest);
    BFC_REGISTER_TEST(OsTest, negativeSeekTest);
    BFC_REGISTER_TEST(OsTest, truncateTest);
    BFC_REGISTER_TEST(OsTest, largefileTest);
  }

public:
  virtual void teardown() {
    __super::teardown();

    (void)os::unlink(BFC_OPATH(".test"));
  }

  void openCloseTest() {
    ham_fd_t fd;

    BFC_ASSERT_EQUAL(0, os_open("Makefile.am", 0, &fd));
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void openReadOnlyCloseTest() {
    ham_fd_t fd;
    const char *p = "# XXXXXXXXX ERROR\n";

    BFC_ASSERT_EQUAL(0, os_open("Makefile.am", HAM_READ_ONLY, &fd));
    BFC_ASSERT_EQUAL(HAM_IO_ERROR, os_pwrite(fd, 0, p, (ham_size_t)strlen(p)));
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void negativeOpenCloseTest() {
    ham_fd_t fd;

    BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, os_open("__98324kasdlf.blöd", 0, &fd));
  }

  void createCloseTest() {
    ham_fd_t fd;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void createCloseOverwriteTest() {
    ham_fd_t fd;
    ham_u64_t filesize;

    for (int i = 0; i < 3; i++) {
      BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
      BFC_ASSERT_EQUAL(0, os_seek(fd, 0, HAM_OS_SEEK_END));
      BFC_ASSERT_EQUAL(0, os_tell(fd, &filesize));
      BFC_ASSERT_EQUAL(0ull, filesize);
      BFC_ASSERT_EQUAL(0, os_truncate(fd, 1024));
      BFC_ASSERT_EQUAL(0, os_seek(fd, 0, HAM_OS_SEEK_END));
      BFC_ASSERT_EQUAL(0, os_tell(fd, &filesize));
      BFC_ASSERT_EQUAL(1024ull, filesize);
      BFC_ASSERT_EQUAL(0, os_close(fd));
    }
  }

  void closeTest() {
#ifndef WIN32  // crashs in ntdll.dll
    BFC_ASSERT_EQUAL(HAM_IO_ERROR, os_close((ham_fd_t)0x12345));
#endif
  }

  void openExclusiveTest() {
    /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
    ham_fd_t fd, fd2;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    BFC_ASSERT_EQUAL(0, os_close(fd));

    BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
    BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, os_open(BFC_OPATH(".test"), 0, &fd2));
    BFC_ASSERT_EQUAL(0, os_close(fd));
    BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd2));
    BFC_ASSERT_EQUAL(0, os_close(fd2));
    BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd2));
    BFC_ASSERT_EQUAL(0, os_close(fd2));
#endif
  }

  void readWriteTest() {
    ham_fd_t fd;
    char buffer[128], orig[128];

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 10; i++) {
      memset(buffer, i, sizeof(buffer));
      BFC_ASSERT_EQUAL(0,
            os_pwrite(fd, i * sizeof(buffer), buffer, sizeof(buffer)));
    }
    for (int i = 0; i < 10; i++) {
      memset(orig, i, sizeof(orig));
      memset(buffer, 0, sizeof(buffer));
      BFC_ASSERT_EQUAL(0,
            os_pread(fd, i * sizeof(buffer), buffer, sizeof(buffer)));
      BFC_ASSERT_EQUAL(0, memcmp(buffer, orig, sizeof(buffer)));
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void mmapTest() {
    ham_fd_t fd, mmaph;
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *p1, *p2;
    p1 = (ham_u8_t *)malloc(ps);

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 10; i++) {
      memset(p1, i, ps);
      BFC_ASSERT_EQUAL(0, os_pwrite(fd, i*ps, p1, ps));
    }
    for (int i = 0; i < 10; i++) {
      memset(p1, i, ps);
      BFC_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, i*ps, ps, 0, &p2));
      BFC_ASSERT_EQUAL(0, memcmp(p1, p2, ps));
      BFC_ASSERT_EQUAL(0, os_munmap(&mmaph, p2, ps));
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
    free(p1);
  }

  void mmapAbortTest() {
    ham_fd_t fd, mmaph;
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *page, *mapped;
    page = (ham_u8_t *)malloc(ps);

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    memset(page, 0x13, ps);
    BFC_ASSERT_EQUAL(0, os_pwrite(fd, 0, page, ps));

    BFC_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, 0, ps, 0, &mapped));
    /* modify the page */
    memset(mapped, 0x42, ps);
    /* unmap */
    BFC_ASSERT_EQUAL(0, os_munmap(&mmaph, mapped, ps));
    /* read again */
    memset(page, 0, ps);
    BFC_ASSERT_EQUAL(0, os_pread(fd, 0, page, ps));
    /* compare */
    BFC_ASSERT_EQUAL(0x13, page[0]);

    BFC_ASSERT_EQUAL(0, os_close(fd));
    free(page);
  }

  void mmapReadOnlyTest() {
    int i;
    ham_fd_t fd, mmaph;
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *p1, *p2;
    p1 = (ham_u8_t *)malloc(ps);

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (i = 0; i < 10; i++) {
      memset(p1, i, ps);
      BFC_ASSERT_EQUAL(0, os_pwrite(fd, i * ps, p1, ps));
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));

    BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), HAM_READ_ONLY, &fd));
    for (i = 0; i < 10; i++) {
      memset(p1, i, ps);
      BFC_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, i * ps, ps, HAM_READ_ONLY, &p2));
      BFC_ASSERT_EQUAL(0, memcmp(p1, p2, ps));
      BFC_ASSERT_EQUAL(0, os_munmap(&mmaph, p2, ps));
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
    free(p1);
  }

  void multipleMmapTest() {
    ham_fd_t fd, mmaph;
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *p1, *p2;
    ham_u64_t addr = 0, size;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 5; i++) {
      size = ps * (i + 1);

      p1 = (ham_u8_t *)malloc((size_t)size);
      memset(p1, i, (size_t)size);
      BFC_ASSERT_EQUAL(0, os_pwrite(fd, addr, p1, (ham_size_t)size));
      free(p1);
      addr += size;
    }

    addr = 0;
    for (int i = 0; i < 5; i++) {
      size = ps * (i + 1);

      p1 = (ham_u8_t *)malloc((size_t)size);
      memset(p1, i, (size_t)size);
      BFC_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, addr, (ham_size_t)size, 0, &p2));
      BFC_ASSERT_EQUAL(0, memcmp(p1, p2, (size_t)size));
      BFC_ASSERT_EQUAL(0, os_munmap(&mmaph, p2, (ham_size_t)size));
      free(p1);
      addr += size;
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void negativeMmapTest() {
    ham_fd_t fd, mmaph;
    ham_u8_t *page;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    // bad address && page size! - i don't know why this succeeds
    // on MacOS...
#ifndef __MACH__
    BFC_ASSERT_EQUAL(HAM_IO_ERROR, os_mmap(fd, &mmaph, 33, 66, 0, &page));
#endif
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void writevTest() {
    ham_fd_t fd;
    const char *hello = "hello ";
    const char *world = "world!";
    char buffer[128];

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    BFC_ASSERT_EQUAL(0, os_truncate(fd, 128));

    BFC_ASSERT_EQUAL(0,
        os_writev(fd, (void *)hello, strlen(hello),
              (void *)world, strlen(world) + 1));
    memset(buffer, 0, sizeof(buffer));
    BFC_ASSERT_EQUAL(0, os_pread(fd, 0, buffer, sizeof(buffer)));
    BFC_ASSERT_EQUAL(0, strcmp("hello world!", buffer));

    BFC_ASSERT_EQUAL(0, os_seek(fd, 10, HAM_OS_SEEK_SET));
    BFC_ASSERT_EQUAL(0,
        os_writev(fd, (void *)hello, strlen(hello),
              (void *)world, strlen(world) + 1));
    BFC_ASSERT_EQUAL(0, os_pread(fd, 10, buffer, sizeof(buffer)-10));
    BFC_ASSERT_EQUAL(0, strcmp("hello world!", buffer));

    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void seekTellTest() {
    ham_fd_t fd;
    ham_u64_t tell;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, os_seek(fd, i, HAM_OS_SEEK_SET));
      BFC_ASSERT_EQUAL(0, os_tell(fd, &tell));
      BFC_ASSERT_EQUAL(tell, (ham_u64_t)i);
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void negativeSeekTest() {
    BFC_ASSERT_EQUAL(HAM_IO_ERROR,
        os_seek((ham_fd_t)0x12345, 0, HAM_OS_SEEK_SET));
  }

  void truncateTest() {
    ham_fd_t fd;
    ham_u64_t fsize;

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, os_truncate(fd, i * 128));
      BFC_ASSERT_EQUAL(0, os_get_filesize(fd, &fsize));
      BFC_ASSERT_EQUAL(fsize, (ham_u64_t)(i * 128));
    }
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

  void largefileTest() {
    ham_fd_t fd;
    ham_u8_t kb[1024];
    ham_u64_t tell;

    memset(kb, 0, sizeof(kb));

    BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
    for (int i = 0; i < 4 * 1024; i++)
      BFC_ASSERT_EQUAL(0, os_pwrite(fd, i * sizeof(kb), kb, sizeof(kb)));
    BFC_ASSERT_EQUAL(0, os_close(fd));

    BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd));
    BFC_ASSERT_EQUAL(0, os_seek(fd, 0, HAM_OS_SEEK_END));
    BFC_ASSERT_EQUAL(0, os_tell(fd, &tell));
    BFC_ASSERT_EQUAL(tell, (ham_u64_t)1024 * 1024 * 4);
    BFC_ASSERT_EQUAL(0, os_close(fd));
  }

};

BFC_REGISTER_FIXTURE(OsTest);
