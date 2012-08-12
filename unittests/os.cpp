/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
using namespace ham;

#if WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

class OsTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    OsTest()
    :   hamsterDB_fixture("OsTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(OsTest, openCloseTest);
        BFC_REGISTER_TEST(OsTest, openReadOnlyCloseTest);
        BFC_REGISTER_TEST(OsTest, negativeOpenCloseTest);
        BFC_REGISTER_TEST(OsTest, createCloseTest);
        BFC_REGISTER_TEST(OsTest, createCloseOverwriteTest);
        BFC_REGISTER_TEST(OsTest, closeTest);
        BFC_REGISTER_TEST(OsTest, openExclusiveTest);
        BFC_REGISTER_TEST(OsTest, readWriteTest);
        BFC_REGISTER_TEST(OsTest, pagesizeTest);
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
    virtual void teardown() 
    { 
        __super::teardown();

        (void)os::unlink(BFC_OPATH(".test"));
    }

    void openCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_open("Makefile.am", 0, &fd);
        BFC_ASSERT(st==0);
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void openReadOnlyCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;
        const char *p="# XXXXXXXXX ERROR\n";

        st=os_open("Makefile.am", HAM_READ_ONLY, &fd);
        BFC_ASSERT(st==0);
        st=os_pwrite(fd, 0, p, (ham_size_t)strlen(p));
        BFC_ASSERT(st==HAM_IO_ERROR);
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void negativeOpenCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_open("__98324kasdlf.blöd", 0, &fd);
        BFC_ASSERT(st==HAM_FILE_NOT_FOUND);
    }

    void createCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT_EQUAL(0, st);
        st=os_close(fd);
        BFC_ASSERT_EQUAL(0, st);
    }

    void createCloseOverwriteTest()
    {
        ham_fd_t fd;
        ham_offset_t filesize;

        for (int i=0; i<3; i++) {
            BFC_ASSERT(os_create(BFC_OPATH(".test"), 0, 0664, &fd)==HAM_SUCCESS);
            BFC_ASSERT(os_seek(fd, 0, HAM_OS_SEEK_END)==HAM_SUCCESS);
            BFC_ASSERT(os_tell(fd, &filesize)==HAM_SUCCESS);
            BFC_ASSERT(filesize==0);
            BFC_ASSERT(os_truncate(fd, 1024)==HAM_SUCCESS);
            BFC_ASSERT(os_seek(fd, 0, HAM_OS_SEEK_END)==HAM_SUCCESS);
            BFC_ASSERT(os_tell(fd, &filesize)==HAM_SUCCESS);
            BFC_ASSERT(filesize==1024);
            BFC_ASSERT(os_close(fd)==HAM_SUCCESS);
        }
    }

    void closeTest()
    {
#ifndef WIN32  // crashs in ntdll.dll
        ham_status_t st;

        st=os_close((ham_fd_t)0x12345);
        BFC_ASSERT(st==HAM_IO_ERROR);
#endif
    }

    void openExclusiveTest()
    {
        /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
        ham_fd_t fd, fd2;

        BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 
                    HAM_LOCK_EXCLUSIVE, 0664, &fd));
        BFC_ASSERT_EQUAL(0, os_close(fd));
        
        BFC_ASSERT_EQUAL(0, 
                         os_open(BFC_OPATH(".test"), HAM_LOCK_EXCLUSIVE, &fd));
        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
                os_open(BFC_OPATH(".test"), HAM_LOCK_EXCLUSIVE, &fd2));
        BFC_ASSERT_EQUAL(0, os_close(fd));
        BFC_ASSERT_EQUAL(0, 
                         os_open(BFC_OPATH(".test"), HAM_LOCK_EXCLUSIVE, &fd2));
        BFC_ASSERT_EQUAL(0, os_close(fd2));
        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), 0, &fd2));
        BFC_ASSERT_EQUAL(0, os_close(fd2));
#endif
    }

    void readWriteTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        char buffer[128], orig[128];

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<10; i++) {
            memset(buffer, i, sizeof(buffer));
            st=os_pwrite(fd, i*sizeof(buffer), buffer, sizeof(buffer));
            BFC_ASSERT(st==0);
        }
        for (i=0; i<10; i++) {
            memset(orig, i, sizeof(orig));
            memset(buffer, 0, sizeof(buffer));
            st=os_pread(fd, i*sizeof(buffer), buffer, sizeof(buffer));
            BFC_ASSERT(st==0);
            BFC_ASSERT(0==memcmp(buffer, orig, sizeof(buffer)));
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void pagesizeTest()
    {
        ham_size_t ps=os_get_pagesize();
        BFC_ASSERT(ps!=0);
        BFC_ASSERT(ps%1024==0);
    }

    void mmapTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *p1, *p2;
        p1=(ham_u8_t *)malloc(ps);

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            st=os_pwrite(fd, i*ps, p1, ps);
            BFC_ASSERT(st==0);
        }
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            st=os_mmap(fd, &mmaph, i*ps, ps, 0, &p2);
            BFC_ASSERT(st==0);
            BFC_ASSERT(0==memcmp(p1, p2, ps));
            st=os_munmap(&mmaph, p2, ps);
            BFC_ASSERT(st==0);
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);
        free(p1);
    }

    void mmapAbortTest()
    {
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *page, *mapped;
        page=(ham_u8_t *)malloc(ps);

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

    void mmapReadOnlyTest()
    {
        int i;
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *p1, *p2;
        p1=(ham_u8_t *)malloc(ps);

        BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            BFC_ASSERT_EQUAL(0, os_pwrite(fd, i*ps, p1, ps));
        }
        BFC_ASSERT_EQUAL(0, os_close(fd));

        BFC_ASSERT_EQUAL(0, os_open(BFC_OPATH(".test"), HAM_READ_ONLY, &fd));
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            BFC_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, i*ps, ps, 
                    HAM_READ_ONLY, &p2));
            BFC_ASSERT_EQUAL(0, memcmp(p1, p2, ps));
            BFC_ASSERT_EQUAL(0, os_munmap(&mmaph, p2, ps));
        }
        BFC_ASSERT_EQUAL(0, os_close(fd));
        free(p1);
    }

    void multipleMmapTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *p1, *p2;
        ham_offset_t addr=0, size;

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<5; i++) {
            size=ps*(i+1);

            p1=(ham_u8_t *)malloc((size_t)size);
            memset(p1, i, (size_t)size);
            st=os_pwrite(fd, addr, p1, (ham_size_t)size);
            BFC_ASSERT(st==0);
            free(p1);
            addr+=size;
        }

        addr=0;
        for (i=0; i<5; i++) {
            size=ps*(i+1);

            p1=(ham_u8_t *)malloc((size_t)size);
            memset(p1, i, (size_t)size);
            st=os_mmap(fd, &mmaph, addr, (ham_size_t)size, 0, &p2);
            BFC_ASSERT(st==0);
            BFC_ASSERT(0==memcmp(p1, p2, (size_t)size));
            st=os_munmap(&mmaph, p2, (ham_size_t)size);
            BFC_ASSERT(st==0);
            free(p1);
            addr+=size;
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void negativeMmapTest()
    {
        ham_fd_t fd, mmaph;
        ham_u8_t *page;

        BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
        // bad address && page size! - i don't know why this succeeds
        // on MacOS...
#ifndef __MACH__
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                os_mmap(fd, &mmaph, 33, 66, 0, &page));
#endif
        BFC_ASSERT_EQUAL(0, os_close(fd));
    }

    void writevTest()
    {
        ham_fd_t fd;
        const char *hello="hello ";
        const char *world="world!";
        char buffer[128];

        BFC_ASSERT_EQUAL(0, os_create(BFC_OPATH(".test"), 0, 0664, &fd));
        BFC_ASSERT_EQUAL(0, os_truncate(fd, 128));

        BFC_ASSERT_EQUAL(0,
                os_writev(fd, (void *)hello, strlen(hello), 
                            (void *)world, strlen(world)+1));
        memset(buffer, 0, sizeof(buffer));
        BFC_ASSERT_EQUAL(0, os_pread(fd, 0, buffer, sizeof(buffer)));
        BFC_ASSERT_EQUAL(0, strcmp("hello world!", buffer));

        BFC_ASSERT_EQUAL(0, os_seek(fd, 10, HAM_OS_SEEK_SET));
        BFC_ASSERT_EQUAL(0,
                os_writev(fd, (void *)hello, strlen(hello), 
                            (void *)world, strlen(world)+1));
        BFC_ASSERT_EQUAL(0, os_pread(fd, 10, buffer, sizeof(buffer)-10));
        BFC_ASSERT_EQUAL(0, strcmp("hello world!", buffer));

        BFC_ASSERT_EQUAL(0, os_close(fd));
    }

    void seekTellTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_offset_t tell;

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<10; i++) {
            st=os_seek(fd, i, HAM_OS_SEEK_SET);
            BFC_ASSERT(st==0);
            st=os_tell(fd, &tell);
            BFC_ASSERT(st==0);
            BFC_ASSERT(tell==(ham_offset_t)i);
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void negativeSeekTest()
    {
        ham_status_t st;

        st=os_seek((ham_fd_t)0x12345, 0, HAM_OS_SEEK_SET);
        BFC_ASSERT(st==HAM_IO_ERROR);
    }

    void truncateTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_offset_t fsize;

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<10; i++) {
            st=os_truncate(fd, i*128);
            BFC_ASSERT(st==0);
            st=os_get_filesize(fd, &fsize);
            BFC_ASSERT(st==0);
            BFC_ASSERT(fsize==(ham_offset_t)(i*128));
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

    void largefileTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_u8_t kb[1024];
        ham_offset_t tell;

        memset(kb, 0, sizeof(kb));

        st=os_create(BFC_OPATH(".test"), 0, 0664, &fd);
        BFC_ASSERT(st==0);
        for (i=0; i<4*1024; i++) {
            st=os_pwrite(fd, i*sizeof(kb), kb, sizeof(kb));
            BFC_ASSERT(st==0);
        }
        st=os_close(fd);
        BFC_ASSERT(st==0);

        st=os_open(BFC_OPATH(".test"), 0, &fd);
        BFC_ASSERT(st==0);
        st=os_seek(fd, 0, HAM_OS_SEEK_END);
        BFC_ASSERT(st==0);
        st=os_tell(fd, &tell);
        BFC_ASSERT(st==0);
        BFC_ASSERT(tell==(ham_offset_t)1024*1024*4);
        st=os_close(fd);
        BFC_ASSERT(st==0);
    }

};

BFC_REGISTER_FIXTURE(OsTest);
