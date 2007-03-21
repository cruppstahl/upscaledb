/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <ham/hamsterdb.h>
#include <ham/types.h>
#include "config.h"

#define _GNU_SOURCE   1 /* for O_LARGEFILE */
#define __USE_XOPEN2K 1 /* for ftruncate() */
#include <stdio.h>
#include <errno.h>
#include <string.h>
#if HAVE_MMAP
#  include <sys/mman.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "error.h"
#include "os.h"

#ifdef HAVE_GETPAGESIZE_INT
extern int getpagesize();
#else
extern size_t getpagesize();
#endif

static void
my_enable_largefile(int fd)
{
    /*
     * not available on cygwin...
     */
#ifdef HAVE_O_LARGEFILE
    int oflag=fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, oflag|O_LARGEFILE);
#endif
}

ham_size_t
os_get_pagesize(void)
{
    return ((ham_size_t)getpagesize());
}

ham_status_t
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_offset_t position,
		ham_size_t size, ham_u8_t **buffer)
{
    (void)mmaph; /* only used on win32-platforms */

#if HAVE_MMAP
    *buffer=mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, position);
    if (*buffer==(void *)-1) {
        *buffer=0;
        ham_log(("mmap failed with status %d (%s)", errno, strerror(errno)));
        return (errno);
    }

    return (HAM_SUCCESS);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t
os_munmap(ham_fd_t mmaph, void *buffer, ham_size_t size)
{
    (void)mmaph; /* only used on win32-platforms */

#if HAVE_MUNMAP
    int r=munmap(buffer, size);
    if (r) {
        ham_log(("munmap failed with status %d (%s)", errno, strerror(errno)));
        return (errno);
    }
    return (HAM_SUCCESS);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif
}

#ifndef HAVE_PREAD
static ham_status_t
my_os_read(ham_fd_t fd, ham_u8_t *buffer, ham_size_t bufferlen)
{
    int r;
    ham_size_t total=0;

    while (total<bufferlen) {
        r=read(fd, &buffer[total], bufferlen-total);
        if (r<0)
            return (errno);
        if (r==0)
            break;
        total+=r;
    }

    return (total==bufferlen ? HAM_SUCCESS : HAM_SHORT_READ);
}
#endif

ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer, 
        ham_size_t bufferlen)
{
#if HAVE_PREAD
    int r;
    ham_size_t total=0;

    while (total<bufferlen) {
        r=pread(fd, buffer+total, bufferlen-total, addr+total);
        if (r<0)
            return (errno);
        if (r==0)
            break;
        total+=r;
    }

    return (total==bufferlen ? HAM_SUCCESS : HAM_SHORT_READ);
#else
    ham_status_t st;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);
    st=my_os_read(fd, buffer, bufferlen);
    return (st);
#endif
}

#ifndef HAVE_PWRITE
static ham_status_t
my_os_write(ham_fd_t fd, const ham_u8_t *buffer, ham_size_t bufferlen)
{
    int w;
    ham_size_t total=0;

    while (total<bufferlen) {
        w=write(fd, &buffer[total], bufferlen-total);
        if (w<0)
            return (errno);
        if (w==0)
            break;
        total+=w;
    }

    return (total==bufferlen ? HAM_SUCCESS : HAM_SHORT_WRITE);
}
#endif

ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer, 
        ham_size_t bufferlen)
{
#if HAVE_PWRITE
    ssize_t s;
    ham_size_t total=0;

    while (total<bufferlen) {
        s=pwrite(fd, buffer, bufferlen, addr+total);
        if (s<0)
            return (errno);
        if (s==0)
            break;
        total+=s;
    }

    return (total==bufferlen ? HAM_SUCCESS : HAM_SHORT_WRITE);
#else
    ham_status_t st;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);
    st=my_os_write(fd, buffer, bufferlen);
    return (st);
#endif
}

ham_status_t
os_seek(ham_fd_t fd, ham_offset_t offset, int whence)
{
    if (lseek(fd, offset, whence)<0)
        return (errno);
    return (HAM_SUCCESS);
}

ham_status_t
os_tell(ham_fd_t fd, ham_offset_t *offset)
{
    *offset=lseek(fd, 0, SEEK_CUR);
    return (*offset==(ham_offset_t)-1 ? errno : HAM_SUCCESS);
}

ham_status_t
os_truncate(ham_fd_t fd, ham_offset_t newsize)
{
    if (ftruncate(fd, newsize))
        return (errno);
    return (HAM_SUCCESS);
}

ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd)
{
    int osflags=O_CREAT|O_RDWR;

    *fd=open(filename, osflags, mode);
    if (*fd<0) {
        ham_log(("os_create of %s failed with status %u (%s)", filename,
                errno, strerror(errno)));
        return (HAM_IO_ERROR);
    }

    /*
     * enable O_LARGEFILE support
     */
    my_enable_largefile(*fd);

    return (HAM_SUCCESS);
}

ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd)
{
    int osflags=0;

    if (flags&HAM_READ_ONLY)
        osflags|=O_RDONLY;
    else
        osflags|=O_RDWR;

    *fd=open(filename, osflags);
    if (*fd<0) {
        ham_log(("os_create of %s failed with status %u (%s)", filename,
                errno, strerror(errno)));
        return (errno==ENOENT ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
    }

    /*
     * enable O_LARGEFILE support
     */
    my_enable_largefile(*fd);

    return (HAM_SUCCESS);
}

ham_status_t
os_close(ham_fd_t fd)
{
    if (close(fd)==-1)
        return (errno);
    return (HAM_SUCCESS);
}
