/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ham/hamsterdb.h>
#include <ham/types.h>
#include "error.h"
#include "os.h"

ham_status_t
os_read(ham_fd_t fd, ham_u8_t *buffer, ham_size_t bufferlen)
{
    ham_size_t r, total=0;

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

ham_status_t
os_write(ham_fd_t fd, const ham_u8_t *buffer, ham_size_t bufferlen)
{
    ham_size_t w, total=0;

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
    return (*offset<0 ? errno : HAM_SUCCESS);
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
    int osflags=O_CREAT|O_RDWR|O_EXCL;

    if (flags&HAM_READ_ONLY)
        osflags|=O_RDONLY;

    *fd=open(filename, osflags, mode);
    if (*fd<0) 
        return (errno);
    return (HAM_SUCCESS);
}

ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd)
{
    int osflags=O_RDWR;

    if (flags&HAM_OPEN_CREATE)
        osflags|=O_CREAT;
    if (flags&HAM_READ_ONLY)
        osflags|=O_RDONLY;
    if (flags&HAM_OPEN_EXCLUSIVELY)
        osflags|=O_EXCL;

    *fd=open(filename, osflags);
    if (*fd<0) 
        return (errno);
    return (HAM_SUCCESS);
}

ham_status_t
os_close(ham_fd_t fd)
{
    if (close(fd)==-1)
        return (errno);
    return (HAM_SUCCESS);
}
