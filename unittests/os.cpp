/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for endian.h
 *
 */

#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/os.h"

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
    CPPUNIT_TEST      (readWriteTest);
    CPPUNIT_TEST      (pagesizeTest);
    CPPUNIT_TEST      (mmapTest);
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
#if WIN32
        (void)DeleteFile(LPCWSTR(".test"));
#else
        (void)unlink(".test");
#endif
    }

    void openCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;

        st=os_open("Makefile", 0, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_close(fd);
        CPPUNIT_ASSERT(st==0);
    }

    void openReadOnlyCloseTest()
    {
        ham_status_t st;
        ham_fd_t fd;
        const char *p="# XXXXXXXXX ERROR\n";

        st=os_open("Makefile", HAM_READ_ONLY, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_pwrite(fd, 0, p, (ham_size_t)strlen(p));
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
        st=os_close(fd);
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
        st=os_close(fd);
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
            CPPUNIT_ASSERT(os_close(fd)==HAM_SUCCESS);
        }
    }

    void closeTest()
    {
        ham_status_t st;

		st=os_close((ham_fd_t)0x12345);
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
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
        st=os_close(fd);
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
        ham_fd_t fd;
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
            st=os_mmap(fd, 0, i*ps, ps, &p2);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(0==memcmp(p1, p2, ps));
            st=os_munmap(0, p2, ps);
            CPPUNIT_ASSERT(st==0);
        }
        st=os_close(fd);
        CPPUNIT_ASSERT(st==0);
        free(p1);
    }

    void multipleMmapTest()
    {
        int i;
        ham_status_t st;
        ham_fd_t fd;
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
            st=os_mmap(fd, 0, addr, (ham_size_t)size, &p2);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(0==memcmp(p1, p2, (size_t)size));
            st=os_munmap(0, p2, (ham_size_t)size);
            CPPUNIT_ASSERT(st==0);
            free(p1);
            addr+=size;
        }
        st=os_close(fd);
        CPPUNIT_ASSERT(st==0);
    }

    void negativeMmapTest()
    {
        ham_status_t st;
        ham_fd_t fd;
        ham_u8_t *page;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_mmap(fd, 0, 33, 66, &page); // bad address && page size!
        CPPUNIT_ASSERT(st==HAM_IO_ERROR);
        st=os_close(fd);
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
        st=os_close(fd);
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
        ham_offset_t tell;

        st=os_create(".test", 0, 0664, &fd);
        CPPUNIT_ASSERT(st==0);
        for (i=0; i<10; i++) {
            st=os_truncate(fd, i*128);
            CPPUNIT_ASSERT(st==0);
            st=os_seek(fd, 0, HAM_OS_SEEK_END);
            CPPUNIT_ASSERT(st==0);
            st=os_tell(fd, &tell);
            CPPUNIT_ASSERT(st==0);
            CPPUNIT_ASSERT(tell==(ham_offset_t)(i*128));
        }
        st=os_close(fd);
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
        st=os_close(fd);
        CPPUNIT_ASSERT(st==0);

        st=os_open(".test", 0, &fd);
        CPPUNIT_ASSERT(st==0);
        st=os_seek(fd, 0, HAM_OS_SEEK_END);
        CPPUNIT_ASSERT(st==0);
        st=os_tell(fd, &tell);
        CPPUNIT_ASSERT(st==0);
        CPPUNIT_ASSERT(tell==(ham_offset_t)1024*1024*4);
        st=os_close(fd);
        CPPUNIT_ASSERT(st==0);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(OsTest);
