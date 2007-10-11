/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/os.h"
#include "os.hpp"

#if WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

static void
my_errhandler(const char *message)
{
    (void)message;
}

class OsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(OsTest);
    CPPUNIT_TEST      (openCloseTest);
    CPPUNIT_TEST      (openReadOnlyCloseTest);
    CPPUNIT_TEST      (negativeOpenCloseTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseOverwriteTest);
    CPPUNIT_TEST      (closeTest);
    CPPUNIT_TEST      (openExclusiveTest);
    CPPUNIT_TEST      (readWriteTest);
    CPPUNIT_TEST      (pagesizeTest);
    CPPUNIT_TEST      (mmapTest);
	CPPUNIT_TEST      (mmapReadOnlyTest);
    CPPUNIT_TEST      (multipleMmapTest);
    CPPUNIT_TEST      (negativeMmapTest);
    CPPUNIT_TEST      (seekTellTest);
    CPPUNIT_TEST      (negativeSeekTest);
    CPPUNIT_TEST      (truncateTest);
    CPPUNIT_TEST      (largefileTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()    
    { 
        ham_set_errhandler(my_errhandler);
    }

    void tearDown() 
    { 
        (void)os::unlink(".test");
    }

    void openCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_open("Makefile.am", 0, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void openReadOnlyCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;
        const char *p="# XXXXXXXXX ERROR\n";

        st=os_open("Makefile.am", HAM_READ_ONLY, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_pwrite(fd, 0, p, (ham_size_t)strlen(p));
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void negativeOpenCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_open("__98324kasdlf.blöd", 0, &fd);
        CPPUNIT_ASSERT(st==HAM_FILE_NOT_FOUND);
    }

    void createCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT_EQUAL(0, st);
        st=os_close(fd, 0);
        CPPUNIT_ASSERT_EQUAL(0, st);
    }

    void createCloseOverwriteTest()
    {
        ham_fd_t fd;
        ham_offset_t filesize;

        for (int i=0; i<3; i++) {
            CPPUNIT_ASSERT(os_create(".test", 0, 0664, &fd)==HAM_SUCCESS);
            CPPUNIT_ASSERT(os_seek(fd, 0, HAM_OS_SEEK_END)==HAM_SUCCESS);
            CPPUNIT_ASSERT(os_tell(fd, &filesize)==HAM_SUCCESS);
            CPPUNIT_ASSERT(filesize==0);
            CPPUNIT_ASSERT(os_truncate(fd, 1024)==HAM_SUCCESS);
            CPPUNIT_ASSERT(os_seek(fd, 0, HAM_OS_SEEK_END)==HAM_SUCCESS);
            CPPUNIT_ASSERT(os_tell(fd, &filesize)==HAM_SUCCESS);
            CPPUNIT_ASSERT(filesize==1024);
            CPPUNIT_ASSERT(os_close(fd, 0)==HAM_SUCCESS);
        }
    }

    void closeTest()
    {
#ifndef WIN32  // crashs in ntdll.dll
        ham_status_t st;

        st=os_close((ham_fd_t)0x12345, 0);
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
#endif
    }

    void openExclusiveTest()
    {
        /* fails on cygwin - cygwin bug? */
#ifndef __CYGWIN__
        ham_fd_t fd, fd2;

        CPPUNIT_ASSERT_EQUAL(0, os_create(".test", 
                    HAM_LOCK_EXCLUSIVE, 0664, &fd));
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd, HAM_LOCK_EXCLUSIVE));
        
        CPPUNIT_ASSERT_EQUAL(0, 
                         os_open(".test", HAM_LOCK_EXCLUSIVE, &fd));
        CPPUNIT_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
                os_open(".test", HAM_LOCK_EXCLUSIVE, &fd2));
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd, HAM_LOCK_EXCLUSIVE));
        CPPUNIT_ASSERT_EQUAL(0, 
                         os_open(".test", HAM_LOCK_EXCLUSIVE, &fd2));
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd2, HAM_LOCK_EXCLUSIVE));
        CPPUNIT_ASSERT_EQUAL(0, os_open(".test", 0, &fd2));
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd2, 0));
#endif
    }

    void readWriteTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        char buffer[128], orig[128];

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<10; i++) {
            memset(buffer, i, sizeof(buffer));
            st=os_pwrite(fd, i*sizeof(buffer), buffer, sizeof(buffer));
            CPPUNIT_ASSERT(st==0);
        }
        for (i=0; i<10; i++) {
            memset(orig, i, sizeof(orig));
            memset(buffer, 0, sizeof(buffer));
            st=os_pread(fd, i*sizeof(buffer), buffer, sizeof(buffer));
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(0==memcmp(buffer, orig, sizeof(buffer)));
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void pagesizeTest()
    {
        ham_size_t ps=os_get_pagesize();
        CPPUNIT_ASSERT(ps!=0);
        CPPUNIT_ASSERT(ps%1024==0);
    }

    void mmapTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *p1, *p2;
        p1=(ham_u8_t *)malloc(ps);

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            st=os_pwrite(fd, i*ps, p1, ps);
            CPPUNIT_ASSERT(st==0);
        }
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            st=os_mmap(fd, &mmaph, i*ps, ps, 0, &p2);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(0==memcmp(p1, p2, ps));
            st=os_munmap(&mmaph, p2, ps);
            CPPUNIT_ASSERT(st==0);
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
        free(p1);
    }

    void mmapReadOnlyTest()
    {
        int i;
        ham_fd_t fd, mmaph;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *p1, *p2;
        p1=(ham_u8_t *)malloc(ps);

        CPPUNIT_ASSERT_EQUAL(0, os_create(".test", 0, 0664, &fd));
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            CPPUNIT_ASSERT_EQUAL(0, os_pwrite(fd, i*ps, p1, ps));
        }
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd, 0));

        CPPUNIT_ASSERT_EQUAL(0, os_open(".test", HAM_READ_ONLY, &fd));
        for (i=0; i<10; i++) {
            memset(p1, i, ps);
            CPPUNIT_ASSERT_EQUAL(0, os_mmap(fd, &mmaph, i*ps, ps, 
                    HAM_READ_ONLY, &p2));
            CPPUNIT_ASSERT_EQUAL(0, memcmp(p1, p2, ps));
            CPPUNIT_ASSERT_EQUAL(0, os_munmap(&mmaph, p2, ps));
        }
        CPPUNIT_ASSERT_EQUAL(0, os_close(fd, HAM_READ_ONLY));
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

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<5; i++) {
            size=ps*(i+1);

            p1=(ham_u8_t *)malloc((size_t)size);
            memset(p1, i, (size_t)size);
            st=os_pwrite(fd, addr, p1, (ham_size_t)size);
            CPPUNIT_ASSERT(st==0);
            free(p1);
            addr+=size;
        }

        addr=0;
        for (i=0; i<5; i++) {
            size=ps*(i+1);

            p1=(ham_u8_t *)malloc((size_t)size);
            memset(p1, i, (size_t)size);
            st=os_mmap(fd, &mmaph, addr, (ham_size_t)size, 0, &p2);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(0==memcmp(p1, p2, (size_t)size));
            st=os_munmap(&mmaph, p2, (ham_size_t)size);
            CPPUNIT_ASSERT(st==0);
            free(p1);
            addr+=size;
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void negativeMmapTest()
    {
        ham_status_t st;
        ham_fd_t fd, mmaph;
        ham_u8_t *page;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_mmap(fd, &mmaph, 33, 66, 0, &page); // bad address && page size!
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void seekTellTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_offset_t tell;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<10; i++) {
            st=os_seek(fd, i, HAM_OS_SEEK_SET);
            CPPUNIT_ASSERT(st==0);
            st=os_tell(fd, &tell);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(tell==(ham_offset_t)i);
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void negativeSeekTest()
    {
        ham_status_t st;

        st=os_seek((ham_fd_t)0x12345, 0, HAM_OS_SEEK_SET);
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
    }

    void truncateTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_offset_t fsize;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<10; i++) {
            st=os_truncate(fd, i*128);
            CPPUNIT_ASSERT(st==0);
            st=os_get_filesize(fd, &fsize);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(fsize==(ham_offset_t)(i*128));
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

    void largefileTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
        ham_u8_t kb[1024];
        ham_offset_t tell;

        memset(kb, 0, sizeof(kb));

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<4*1024; i++) {
            st=os_pwrite(fd, i*sizeof(kb), kb, sizeof(kb));
            CPPUNIT_ASSERT(st==0);
        }
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);

        st=os_open(".test", 0, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_seek(fd, 0, HAM_OS_SEEK_END);
        CPPUNIT_ASSERT(st==0);
        st=os_tell(fd, &tell);
        CPPUNIT_ASSERT(st==0);
        CPPUNIT_ASSERT(tell==(ham_offset_t)1024*1024*4);
        st=os_close(fd, 0);
        CPPUNIT_ASSERT(st==0);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(OsTest);
