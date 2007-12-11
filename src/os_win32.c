/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <windows.h>

#include <stdio.h>
#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/types.h>

#include "error.h"
#include "os.h"

static void
__utf8_string(const char *filename, wchar_t *wfilename, int wlen)
{
	MultiByteToWideChar(CP_ACP, 0, filename, -1, wfilename, wlen);
}

ham_size_t
os_get_pagesize(void)
{
    return (os_get_granularity());
}

ham_size_t
os_get_granularity(void)
{
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return ((ham_size_t)info.dwAllocationGranularity);
}

ham_status_t
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_offset_t position, 
        ham_size_t size, ham_bool_t readonly, ham_u8_t **buffer)
{
#ifndef UNDER_CE
    ham_status_t st;
    DWORD hsize, fsize=GetFileSize(fd, &hsize);
    DWORD protect=PAGE_WRITECOPY; /* | (readonly ? PAGE_READONLY : PAGE_READWRITE); */
    DWORD access =FILE_MAP_COPY; /* | (readonly ? FILE_MAP_READ : FILE_MAP_WRITE); */

    *mmaph=CreateFileMapping(fd, 0, protect, hsize, fsize, 0); 
    if (!*mmaph) {
	    *buffer=0;
        st=(ham_status_t)GetLastError();
        ham_trace(("CreateFileMapping failed with status %u", st));
        return (HAM_IO_ERROR);
    }

	*buffer=MapViewOfFile(*mmaph, access, 
            (unsigned long)(position&0xffffffff00000000),
			(unsigned long)(position&0x00000000ffffffff), size);
    if (!*buffer) {
        st=(ham_status_t)GetLastError();
        ham_trace(("MapViewOfFile failed with status %u", st));
        return (HAM_IO_ERROR);
    }

#endif /* UNDER_CE */
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

    return (read==bufferlen ? 0 : HAM_IO_ERROR);
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

    return (written==bufferlen ? HAM_SUCCESS : HAM_IO_ERROR);
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
	DWORD upper, lower=GetFileSize(fd, &upper);

	if (lower==(DWORD)-1) {
        ham_trace(("GetFileSizeEx failed with OS status %u", 
                    GetLastError()));
        return (HAM_IO_ERROR);
	}

	/* ugly casts to avoid warnings on MSVC 8 for 64bit */
	*size=(ham_offset_t)((unsigned long long)upper<<32)+lower;
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
    DWORD share  =flags & HAM_LOCK_EXCLUSIVE 
                    ? 0 
                    : (FILE_SHARE_READ|FILE_SHARE_WRITE);
#ifdef UNICODE
	wchar_t *wfilename=malloc(strlen(filename)*3*sizeof(wchar_t));

	/* translate ASCII filename to unicode */
	__utf8_string(filename, wfilename, (int)strlen(filename)*3);
    *fd=(ham_fd_t)CreateFileW(wfilename, GENERIC_READ|GENERIC_WRITE, 
                share, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
	free(wfilename);
#else
    *fd=(ham_fd_t)CreateFileA(filename, GENERIC_READ|GENERIC_WRITE, 
                share, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
#endif

	if (*fd==INVALID_HANDLE_VALUE) {
		*fd=HAM_INVALID_FD;
        st=(ham_status_t)GetLastError();
        /* this function can return errors even when it succeeds... */
        if (st==ERROR_ALREADY_EXISTS)
            return (0);
        if (st==ERROR_SHARING_VIOLATION)
            return (HAM_WOULD_BLOCK);
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
    DWORD share  =flags & HAM_LOCK_EXCLUSIVE 
                    ? 0 
                    : (FILE_SHARE_READ|FILE_SHARE_WRITE);
    DWORD dispo  =OPEN_EXISTING;
    DWORD osflags=0;
#ifdef UNICODE
    wchar_t *wfilename;
#endif

    if (flags&HAM_READ_ONLY)
        access|=GENERIC_READ;
    else
        access|=GENERIC_READ|GENERIC_WRITE;

#ifdef UNICODE
	/* translate ASCII filename to unicode */
	wfilename=malloc(strlen(filename)*3*sizeof(wchar_t));
	__utf8_string(filename, wfilename, (int)strlen(filename)*3);
    *fd=(ham_fd_t)CreateFileW(wfilename, access, share, 0, 
                        dispo, osflags, 0);
	free(wfilename);
#else
    *fd=(ham_fd_t)CreateFileA(filename, access, share, 0, 
                        dispo, osflags, 0);
#endif

    if (*fd==INVALID_HANDLE_VALUE) {
		*fd=HAM_INVALID_FD;
        st=(ham_status_t)GetLastError();
        ham_trace(("CreateFile (open) failed with OS status %u", st));
		if (st==ERROR_SHARING_VIOLATION)
			return (HAM_WOULD_BLOCK);
        return (GetLastError()==ERROR_FILE_NOT_FOUND ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}

ham_status_t
os_close(ham_fd_t fd, ham_u32_t flags)
{
    ham_status_t st;

    (void)flags;

    if (!CloseHandle((HANDLE)fd)) {
        st=(ham_status_t)GetLastError();
        ham_trace(("CloseHandle failed with OS status %u", st));
        return (HAM_IO_ERROR);
    }

    return (HAM_SUCCESS);
}
