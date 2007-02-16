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
    return ((ham_size_t)info.dwPageSize);
}

ham_status_t
os_mmap(ham_fd_t fd, ham_offset_t position, ham_size_t size,
        ham_u8_t **buffer)
{
#if 0
    TODO TODO TODO

    *buffer=mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, position);
    if (*buffer==(void *)-1) {
        *buffer=0;
        ham_log("mmap failed with status %d (%s)", errno, strerror(errno));
        return (errno);
    }
#endif
    return (HAM_SUCCESS);
}

ham_status_t
os_munmap(void *buffer, ham_size_t size)
{
#if 0
    TODO TODO TODO

    int r=munmap(buffer, size);
    if (r) {
        ham_log("munmap failed with status %d (%s)", errno, strerror(errno));
        return (errno);
    }
#endif
    return (HAM_SUCCESS);
}

ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer,
        ham_size_t bufferlen)
{
    ham_status_t st;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);

    if (!ReadFile((HANDLE)fd, buffer, bufferlen, 0, 0)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("read failed with OS status %u", st));
        return (st);
    }

    return (0);
}

ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer,
        ham_size_t bufferlen)
{
    ham_status_t st;
    DWORD written;

    st=os_seek(fd, addr, HAM_OS_SEEK_SET);
    if (st)
        return (st);

    if (!WriteFile((HANDLE)fd, buffer, bufferlen, &written, 0)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("write failed with OS status %u", st));
        return (st);
    }

    return (written==bufferlen ? HAM_SUCCESS : HAM_SHORT_WRITE);
}

ham_status_t
os_seek(ham_fd_t fd, ham_offset_t offset, int whence)
{
    DWORD st;
    LARGE_INTEGER i;
    i.QuadPart=offset;
    
    st=SetFilePointerEx((HANDLE)fd, i, 0, whence);
    if (st==0 && (st=GetLastError())!=NO_ERROR) {
        ham_trace(("seek failed with OS status %u", st));
        return ((ham_status_t)st);
    }

    return (0);
}

ham_status_t
os_tell(ham_fd_t fd, ham_offset_t *offset)
{
    DWORD st;
    LARGE_INTEGER i, d;
    d.QuadPart=0;

    st=SetFilePointerEx((HANDLE)fd, d, &i, HAM_OS_SEEK_CUR);
    if (st==0 && (st=GetLastError())!=NO_ERROR) {
        ham_trace(("tell failed with OS status %u", st));
        return ((ham_status_t)st);
    }

    *offset=(ham_offset_t)i.QuadPart;
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
        return (st);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd)
{
    ham_status_t st;
    DWORD osflags=FILE_FLAG_RANDOM_ACCESS;

    *fd=(ham_fd_t)CreateFile(filename, FILE_ALL_ACCESS, 0, 0,
                CREATE_ALWAYS, osflags, 0);
    if (*fd==0) {
        st=(ham_status_t)GetLastError();
        ham_trace(("CreateFile failed with OS status %u", st));
        return (st);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd)
{
    ham_status_t st;
    DWORD osflags=FILE_FLAG_RANDOM_ACCESS;
    DWORD dispo  =OPEN_EXISTING;
    /* TODO open exclusively? 
    TODO TODO TODO */

    if (flags&HAM_READ_ONLY)
        osflags|=FILE_ATTRIBUTE_READONLY;

    *fd=(ham_fd_t)CreateFile(filename, FILE_ALL_ACCESS, 0, 0, dispo,
                osflags, 0);
    if (*fd==0) {
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
        return (st);
    }

    return (HAM_SUCCESS);
}
