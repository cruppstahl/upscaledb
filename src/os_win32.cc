/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "error.h"
#include "os.h"

namespace ham {

static const char *
DisplayError(char* buf, ham_size_t buflen, DWORD errorcode)
{
#ifdef UNDER_CE /* TODO not yet tested */
  ham_size_t len;
  WCHAR tmpbuf[1024];
  LPCWSTR s = tmpbuf;
  char *dst = buf;

  buf[0] = 0;
  FormatMessageW(/* FORMAT_MESSAGE_ALLOCATE_BUFFER | */
          FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
          NULL, errorcode,
          MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
          tmpbuf, 1024, NULL);
  /* safely convert wide character string to multibyte character string */
  for (len = buflen - MB_CUR_MAX; len > 0; ) {
    int cl = *s++;
    if (!cl) {
      *dst = 0;
      break;
    }
    cl = wcstombs(dst, (const wchar_t *)cl, buflen);
    if (cl == (size_t)-1) {
      *dst = '?';
      cl = 1;
    }
    dst += cl;
    len -= cl;
  }
  *dst = 0;
  assert(dst < buf + buflen);

  /* strip trailing whitespace\newlines */
  for (len = strlen(buf); len-- > 0; ) {
    if (!isspace(buf[len]))
      break;
    buf[len] = 0;
  }

  return (buf);
#else
  size_t len;

  buf[0] = 0;
  FormatMessageA(/* FORMAT_MESSAGE_ALLOCATE_BUFFER | */
            FORMAT_MESSAGE_FROM_SYSTEM |
            FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL, errorcode,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPSTR)buf, buflen, NULL);
  buf[buflen - 1] = 0;

  /* strip trailing whitespace\newlines */
  for (len = strlen(buf); len-- > 0; ) {
    if (!isspace(buf[len]))
      break;
    buf[len] = 0;
  }

  return (buf);
#endif
}

/*
 * MS says:
 *
 * Security Alert
 *
 * Using the MultiByteToWideChar function incorrectly can compromise the
 * security of your application. Calling this function can easily cause a
 * buffer overrun because the size of the input buffer indicated by
 * lpMultiByteStr equals the number of bytes in the string, while the size of
 * the output buffer indicated by lpWideCharStr equals the number of WCHAR
 * values.
 *
 * To avoid a buffer overrun, your application must specify a buffer size
 * appropriate for the data type the buffer receives. For more information, see
 * Security Considerations: International Features.
 */
static void
utf8_string(const char *filename, WCHAR *wfilename, int wlen)
{
  MultiByteToWideChar(CP_ACP, 0, filename, -1, wfilename, wlen);
}

static int
calc_wlen4str(const char *str)
{
  /*
   * Since we call MultiByteToWideChar with an input length of -1, the
   * output will include the wchar NUL sentinel as well, so count it
   */
  size_t len = strlen(str) + 1;
  return (int)len;
}

/*
 * The typical pagesize of win32 is 4kb - described in info.dwPageSize.
 * However, pages have to be aligned to dwAllocationGranularity (64k),
 * therefore we're also forced to use this as a pagesize.
 */
ham_size_t
os_get_pagesize(void)
{
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  return (ham_size_t)info.dwAllocationGranularity;
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
            ham_offset_t size, bool readonly, ham_u8_t **buffer)
{
#ifndef UNDER_CE
  ham_status_t st;
  DWORD protect = (readonly ? PAGE_READONLY : PAGE_WRITECOPY);
  DWORD access = FILE_MAP_COPY;
  LARGE_INTEGER i;
  i.QuadPart = position;

  *mmaph = CreateFileMapping(fd, 0, protect, 0, 0, 0);
  if (!*mmaph) {
    char buf[256];
    *buffer = 0;
    st = (ham_status_t)GetLastError();
    ham_log(("CreateFileMapping failed with OS status %u (%s)",
        st, DisplayError(buf, sizeof(buf), st)));
    if (st == ERROR_NOT_ENOUGH_QUOTA) // not enough resources - fallback to r/w
      return (HAM_LIMITS_REACHED);
    return (HAM_IO_ERROR);
  }

  *buffer = (ham_u8_t *)MapViewOfFile(*mmaph, access, i.HighPart, i.LowPart,
                                (SIZE_T)size);
  if (!*buffer) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    /* make sure to release the mapping */
    (void)CloseHandle(*mmaph);
    *mmaph = 0;
    ham_log(("MapViewOfFile failed with OS status %u (%s)",
        st, DisplayError(buf, sizeof(buf), st)));
    if (st == ERROR_NOT_ENOUGH_QUOTA) // not enough resources - fallback to r/w
      return (HAM_LIMITS_REACHED);
    return (HAM_IO_ERROR);
  }
  return (HAM_SUCCESS);
#else
  return (HAM_IO_ERROR);
#endif /* UNDER_CE */
}

ham_status_t
os_munmap(ham_fd_t *mmaph, void *buffer, ham_offset_t size)
{
#ifndef UNDER_CE
  ham_status_t st;

  if (!UnmapViewOfFile(buffer)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("UnMapViewOfFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  if (!CloseHandle(*mmaph)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("CloseHandle failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  *mmaph = 0;

  return (HAM_SUCCESS);
#else
  return (HAM_IO_ERROR);
#endif /* UNDER_CE */
}

ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer, ham_offset_t bufferlen)
{
  ham_status_t st;
  OVERLAPPED ov = { 0 };
  ov.Offset = (DWORD)addr;
  ov.OffsetHigh = addr >> 32;
  DWORD read;
  if (!::ReadFile(fd, buffer, (DWORD)bufferlen, &read, &ov)) {
    if (GetLastError() != ERROR_IO_PENDING) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("ReadFile failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      return (HAM_IO_ERROR);
    }
    if (!::GetOverlappedResult(fd, &ov, &read, TRUE)) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("GetOverlappedResult failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      return (HAM_IO_ERROR);
    }
  }

  return (read == bufferlen ? 0 : HAM_IO_ERROR);
}

ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer,
    ham_offset_t bufferlen)
{
  ham_status_t st;
  OVERLAPPED ov = { 0 };
  ov.Offset = (DWORD)addr;
  ov.OffsetHigh = addr >> 32;
  DWORD written;
  if (!::WriteFile(fd, buffer, (DWORD)bufferlen, &written, &ov)) {
    if (GetLastError() != ERROR_IO_PENDING) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("WriteFile failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      return (HAM_IO_ERROR);
    }
    if (!::GetOverlappedResult(fd, &ov, &written, TRUE)) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("GetOverlappedResult failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      return (HAM_IO_ERROR);
    }
  }

  return (written == bufferlen ? 0 : HAM_IO_ERROR);
}

ham_status_t
os_write(ham_fd_t fd, const void *buffer, ham_offset_t bufferlen)
{
  ham_status_t st;
  DWORD written = 0;

  if (!WriteFile((HANDLE)fd, buffer, (DWORD)bufferlen, &written, 0)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("WriteFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (written == bufferlen ? HAM_SUCCESS : HAM_IO_ERROR);
}

ham_status_t
os_writev(ham_fd_t fd, void *buffer1, ham_offset_t buffer1_len,
            void *buffer2, ham_offset_t buffer2_len,
            void *buffer3, ham_offset_t buffer3_len,
            void *buffer4, ham_offset_t buffer4_len,
            void *buffer5, ham_offset_t buffer5_len)
{
  /*
   * Win32 also has a writev implementation, but it requires the pointers
   * to be memory page aligned
   */
  ham_status_t st;
  ham_offset_t rollback;

  st = os_tell(fd, &rollback);
  if (st)
    return (st);

  st = os_write(fd, buffer1, buffer1_len);
  if (st)
    return (st);
  if (buffer2) {
    st = os_write(fd, buffer2, buffer2_len);
    if (st)
      goto bail;
  }
  if (buffer3) {
    st = os_write(fd, buffer3, buffer3_len);
    if (st)
      goto bail;
  }
  if (buffer4) {
    st = os_write(fd, buffer4, buffer4_len);
    if (st)
      goto bail;
  }
  if (buffer5) {
    st = os_write(fd, buffer5, buffer5_len);
    if (st)
      goto bail;
  }

bail:
  if (st) {
    /* rollback the previous change */
    (void)os_seek(fd, rollback, HAM_OS_SEEK_SET);
  }
  return (st);
}

#ifndef INVALID_SET_FILE_POINTER
#   define INVALID_SET_FILE_POINTER  ((DWORD)-1)
#endif

ham_status_t
os_seek(ham_fd_t fd, ham_offset_t offset, int whence)
{
  DWORD st;
  LARGE_INTEGER i;
  i.QuadPart = offset;

  i.LowPart = SetFilePointer((HANDLE)fd, i.LowPart, &i.HighPart, whence);
  if (i.LowPart == INVALID_SET_FILE_POINTER &&
    (st = GetLastError())!=NO_ERROR) {
    char buf[256];
    ham_log(("SetFilePointer failed with OS status %u (%s)", st,
                DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (0);
}

ham_status_t
os_tell(ham_fd_t fd, ham_offset_t *offset)
{
  DWORD st;
  LARGE_INTEGER i;
  i.QuadPart = 0;

  i.LowPart = SetFilePointer((HANDLE)fd, i.LowPart,
      &i.HighPart, HAM_OS_SEEK_CUR);
  if (i.LowPart == INVALID_SET_FILE_POINTER &&
    (st = GetLastError()) != NO_ERROR) {
    char buf[256];
    ham_log(("SetFilePointer failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  *offset = (ham_offset_t)i.QuadPart;
  return (0);
}

#ifndef INVALID_FILE_SIZE
#   define INVALID_FILE_SIZE   ((DWORD)-1)
#endif

ham_status_t
os_get_filesize(ham_fd_t fd, ham_offset_t *size)
{
  ham_status_t st;
  LARGE_INTEGER i;
  i.QuadPart = 0;
  i.LowPart = GetFileSize(fd, (LPDWORD)&i.HighPart);

  if (i.LowPart == INVALID_FILE_SIZE &&
    (st = GetLastError()) != NO_ERROR) {
    char buf[256];
    ham_log(("GetFileSize failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  *size = (ham_offset_t)i.QuadPart;
  return (0);
}

ham_status_t
os_truncate(ham_fd_t fd, ham_offset_t newsize)
{
  ham_status_t st;

  st = os_seek(fd, newsize, HAM_OS_SEEK_SET);
  if (st)
    return (st);

  if (!SetEndOfFile((HANDLE)fd)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("SetEndOfFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (HAM_SUCCESS);
}

ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd)
{
  ham_status_t st;
  DWORD share = 0; /* 1.1.0: default behaviour is exclusive locking */
  DWORD access = ((flags & HAM_READ_ONLY)
          ? GENERIC_READ
          : (GENERIC_READ | GENERIC_WRITE));

#ifdef UNICODE
  int fnameWlen = calc_wlen4str(filename);
  WCHAR *wfilename = (WCHAR *)malloc(fnameWlen * sizeof(wfilename[0]));
  if (!wfilename)
    return (HAM_OUT_OF_MEMORY);

  /* translate ASCII filename to unicode */
  utf8_string(filename, wfilename, fnameWlen);
  *fd = (ham_fd_t)CreateFileW(wfilename, access,
        share, NULL, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, 0);
  free(wfilename);
#else
  *fd = (ham_fd_t)CreateFileA(filename, access,
        share, NULL, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, 0);
#endif

  if (*fd == INVALID_HANDLE_VALUE) {
    char buf[256];
    *fd = HAM_INVALID_FD;
    st = (ham_status_t)GetLastError();
    if (st == ERROR_SHARING_VIOLATION)
      return (HAM_WOULD_BLOCK);
    ham_log(("CreateFile(%s, %x, %x, ...) (create) failed with OS status "
            "%u (%s)", filename, access, share, st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (HAM_SUCCESS);
}

ham_status_t
os_flush(ham_fd_t fd)
{
  ham_status_t st;

  if (!FlushFileBuffers((HANDLE)fd)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("FlushFileBuffers failed with OS status %u (%s)",
        st, DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (HAM_SUCCESS);
}

ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd)
{
  ham_status_t st;
  DWORD share = 0; /* 1.1.0: default behaviour is exclusive locking */
  DWORD access = ((flags & HAM_READ_ONLY)
          ? GENERIC_READ
          : (GENERIC_READ | GENERIC_WRITE));
  DWORD dispo = OPEN_EXISTING;
  DWORD osflags = 0;


#ifdef UNICODE
  {
    int fnameWlen = calc_wlen4str(filename);
    WCHAR *wfilename = (WCHAR *)malloc(fnameWlen * sizeof(wfilename[0]));
    if (!wfilename)
      return (HAM_OUT_OF_MEMORY);

    /* translate ASCII filename to unicode */
    utf8_string(filename, wfilename, fnameWlen);
    *fd = (ham_fd_t)CreateFileW(wfilename, access, share, NULL,
              dispo, osflags, 0);
    free(wfilename);
  }
#else
  *fd = (ham_fd_t)CreateFileA(filename, access, share, NULL,
            dispo, osflags, 0);
#endif

  if (*fd == INVALID_HANDLE_VALUE) {
    char buf[256];
    *fd = HAM_INVALID_FD;
    st = (ham_status_t)GetLastError();
    ham_log(("CreateFile(%s, %x, %x, ...) (open) failed with OS status "
            "%u (%s)", filename, access, share,
            st, DisplayError(buf, sizeof(buf), st)));
    if (st == ERROR_SHARING_VIOLATION)
      return (HAM_WOULD_BLOCK);
    return (st == ERROR_FILE_NOT_FOUND ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
  }

  return (HAM_SUCCESS);
}

ham_status_t
os_close(ham_fd_t fd)
{
  ham_status_t st;

  if (!CloseHandle((HANDLE)fd)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("CloseHandle failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    return (HAM_IO_ERROR);
  }

  return (HAM_SUCCESS);
}

} // namespace ham
