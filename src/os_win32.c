/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/types.h>
#include <windows.h>
#include "error.h"
#include "os.h"

ham_size_t
os_get_pagesize(void)
{
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return ((ham_size_t)info.dwAllocationGranularity);
}

ham_status_t
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_offset_t position, 
        ham_size_t size, ham_u8_t **buffer)
{
    ham_status_t st;
    DWORD hsize, fsize=GetFileSize(fd, &hsize);

    *mmaph=CreateFileMapping(fd, 0, PAGE_READWRITE, hsize, fsize, 0); 
    if (!*mmaph) {
        *buffer=0;
        st=(ham_status_t)GetLastError();
        ham_trace(("CreateFileMapping failed with status %u", st));
        return (HAM_IO_ERROR);
    }

    *buffer=MapViewOfFile(*mmaph, PAGE_READONLY, 
            (unsigned long)(position&0xffffffff00000000),
            (unsigned long)(position&0x00000000ffffffff), size);
    if (!*buffer) {
        st=(ham_status_t)GetLastError();
        ham_trace(("MapViewOfFile failed with status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_munmap(ham_fd_t *mmaph, void *buffer, ham_size_t size)
{
    ham_status_t st;

    if (!UnmapViewOfFile(buffer)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("UnMapViewOfFile failed with status %u", st));
        return (HAM_IO_ERROR);
    }

    if (!CloseHandle(*mmaph)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("CloseHandle failed with status %u", st));
        return (HAM_IO_ERROR);
    }

    *mmaph=0;

    return (HAM_SUCCESS);
}

ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer,
        ham_size_t bufferlen)
{
    ham_status_t st;
    DWORD read=0;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);

    if (!ReadFile((HANDLE)fd, buffer, bufferlen, &read, 0)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("read failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (0);
}

ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer,
        ham_size_t bufferlen)
{
    ham_status_t st;
    DWORD written=0;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);

    if (!WriteFile((HANDLE)fd, buffer, bufferlen, &written, 0)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("write failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (written==bufferlen ? HAM_SUCCESS : HAM_SHORT_WRITE);
}

#ifndef INVALID_SET_FILE_POINTER
#   define INVALID_SET_FILE_POINTER  ((DWORD)-1)
#endif

ham_status_t
os_seek(ham_fd_t fd, ham_offset_t offset, int whence)
{
    DWORD st;
    LARGE_INTEGER i;
    i.QuadPart=offset;
    
    i.LowPart=SetFilePointer((HANDLE)fd, i.LowPart, 
            &i.HighPart, whence);
    if (i.LowPart==INVALID_SET_FILE_POINTER && 
        (st=GetLastError())!=NO_ERROR) {
        ham_trace(("seek failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (0);
}

ham_status_t
os_tell(ham_fd_t fd, ham_offset_t *offset)
{
    DWORD st;
    LARGE_INTEGER i;
    i.QuadPart=0;

    i.LowPart=SetFilePointer((HANDLE)fd, i.LowPart, 
            &i.HighPart, HAM_OS_SEEK_CUR);
    if (i.LowPart==INVALID_SET_FILE_POINTER && 
        (st=GetLastError())!=NO_ERROR) {
        ham_trace(("tell failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    *offset=(ham_offset_t)i.QuadPart;
    return (0);
}

ham_status_t
os_get_filesize(ham_fd_t fd, ham_offset_t *size)
{
    LARGE_INTEGER i;

    if (!GetFileSizeEx(fd, &i)) {
        ham_trace(("GetFileSizeEx failed with OS status %u", 
                    GetLastError()));
        return (HAM_IO_ERROR);
    }

    *size=(ham_offset_t)i.QuadPart;
    return (0);
}

ham_status_t
os_truncate(ham_fd_t fd, ham_offset_t newsize)
{
    ham_status_t st;

    st=os_seek(fd, newsize, HAM_OS_SEEK_SET);
    if (st)
        return (st);

    if (!SetEndOfFile((HANDLE)fd)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("SetEndOfFile failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd)
{
    ham_status_t st;
    DWORD osflags=FILE_FLAG_RANDOM_ACCESS;

    *fd=(ham_fd_t)CreateFileA(filename, GENERIC_READ|GENERIC_WRITE, 
                0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
    if (*fd==INVALID_HANDLE_VALUE) {
        st=(ham_status_t)GetLastError();
        /* this function can return errors even when it succeeds... */
        if (st==ERROR_ALREADY_EXISTS)
            return (0);
        ham_trace(("CreateFile failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_flush(ham_fd_t fd)
{
    ham_status_t st;

    if (!FlushFileBuffers((HANDLE)fd)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("FlushFileBuffers failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd)
{
    ham_status_t st;
    DWORD access =0;
    DWORD share  =0;
    DWORD dispo  =OPEN_EXISTING;
    DWORD osflags=0;

    share=FILE_SHARE_DELETE|FILE_SHARE_READ|FILE_SHARE_WRITE;

    if (flags&HAM_READ_ONLY)
        access|=GENERIC_READ;
    else
        access|=GENERIC_READ|GENERIC_WRITE;

    *fd=(ham_fd_t)CreateFileA(filename, access, share, 0, 
                        dispo, osflags, 0);
    if (*fd==INVALID_HANDLE_VALUE) {
        st=(ham_status_t)GetLastError();
        ham_trace(("CreateFile (open) failed with OS status %u", st));
        return (GetLastError()==ENOENT ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_close(ham_fd_t fd)
{
    ham_status_t st;

    if (!CloseHandle((HANDLE)fd)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("CloseHandle failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}
