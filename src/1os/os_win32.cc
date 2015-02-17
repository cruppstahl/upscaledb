/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <winsock2.h>
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1os/file.h"
#include "1os/socket.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

static const char *
DisplayError(char* buf, uint32_t buflen, DWORD errorcode)
{
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
  // Since we call MultiByteToWideChar with an input length of -1, the
  // output will include the wchar NUL sentinel as well, so count it
  return (int)(strlen(str) + 1);
}

size_t
File::get_granularity()
{
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  return ((size_t)info.dwAllocationGranularity);
}

void
File::set_posix_advice(int advice)
{
  // Only available for posix platforms
}

void
File::mmap(uint64_t position, size_t size, bool readonly, uint8_t **buffer)
{
  ham_status_t st;
  DWORD protect = (readonly ? PAGE_READONLY : PAGE_WRITECOPY);
  DWORD access = FILE_MAP_COPY;
  LARGE_INTEGER i;
  i.QuadPart = position;

  m_mmaph = CreateFileMapping(m_fd, 0, protect, 0, 0, 0);
  if (!m_mmaph) {
    char buf[256];
    *buffer = 0;
    st = (ham_status_t)GetLastError();
    ham_log(("CreateFileMapping failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  *buffer = (uint8_t *)MapViewOfFile(m_mmaph, access, i.HighPart, i.LowPart,
                                (SIZE_T)size);
  if (!*buffer) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    /* make sure to release the mapping */
    (void)CloseHandle(m_mmaph);
    m_mmaph = HAM_INVALID_FD;
    ham_log(("MapViewOfFile failed with OS status %u (%s)",
        st, DisplayError(buf, sizeof(buf), st)));
    if (st == ERROR_NOT_ENOUGH_QUOTA) // not enough resources - fallback to r/w
      throw Exception(HAM_LIMITS_REACHED);
    throw Exception(HAM_IO_ERROR);
  }
}

void
File::munmap(void *buffer, size_t size)
{
  ham_status_t st;

  if (!UnmapViewOfFile(buffer)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("UnMapViewOfFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  if (m_mmaph != HAM_INVALID_FD) {
    if (!CloseHandle(m_mmaph)) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("CloseHandle failed with OS status %u (%s)", st,
              DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
  }

  m_mmaph = HAM_INVALID_FD;
}

void
File::pread(uint64_t addr, void *buffer, size_t len)
{
  ham_status_t st;
  OVERLAPPED ov = { 0 };
  ov.Offset = (DWORD)addr;
  ov.OffsetHigh = addr >> 32;
  DWORD read;
  if (!::ReadFile(m_fd, buffer, (DWORD)len, &read, &ov)) {
    if (GetLastError() != ERROR_IO_PENDING) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("ReadFile failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
    if (!::GetOverlappedResult(m_fd, &ov, &read, TRUE)) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("GetOverlappedResult failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
  }

  if (read != len)
    throw Exception(HAM_IO_ERROR);
}

void
File::pwrite(uint64_t addr, const void *buffer, size_t len)
{
  ham_status_t st;
  OVERLAPPED ov = { 0 };
  ov.Offset = (DWORD)addr;
  ov.OffsetHigh = addr >> 32;
  DWORD written;
  if (!::WriteFile(m_fd, buffer, (DWORD)len, &written, &ov)) {
    if (GetLastError() != ERROR_IO_PENDING) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("WriteFile failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
    if (!::GetOverlappedResult(m_fd, &ov, &written, TRUE)) {
      char buf[256];
      st = (ham_status_t)GetLastError();
      ham_log(("GetOverlappedResult failed with OS status %u (%s)",
            st, DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
  }

  if (written != len)
    throw Exception(HAM_IO_ERROR);
}

void
File::write(const void *buffer, size_t len)
{
  ham_status_t st;
  DWORD written = 0;

  if (!WriteFile(m_fd, buffer, (DWORD)len, &written, 0)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("WriteFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  if (written != len)
    throw Exception(HAM_IO_ERROR);
}

#ifndef INVALID_SET_FILE_POINTER
#   define INVALID_SET_FILE_POINTER  ((DWORD)-1)
#endif

void
File::seek(uint64_t offset, int whence)
{
  DWORD st;
  LARGE_INTEGER i;
  i.QuadPart = offset;

  i.LowPart = ::SetFilePointer(m_fd, i.LowPart, &i.HighPart, whence);
  if (i.LowPart == INVALID_SET_FILE_POINTER &&
    (st = GetLastError())!=NO_ERROR) {
    char buf[256];
    ham_log(("SetFilePointer failed with OS status %u (%s)", st,
                DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }
}

uint64_t
File::tell()
{
  DWORD st;
  LARGE_INTEGER i;
  i.QuadPart = 0;

  i.LowPart = SetFilePointer(m_fd, i.LowPart, &i.HighPart, kSeekCur);
  if (i.LowPart == INVALID_SET_FILE_POINTER &&
    (st = GetLastError()) != NO_ERROR) {
    char buf[256];
    ham_log(("SetFilePointer failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  return ((size_t)i.QuadPart);
}

#ifndef INVALID_FILE_SIZE
#   define INVALID_FILE_SIZE   ((DWORD)-1)
#endif

uint64_t
File::get_file_size()
{
  ham_status_t st;
  LARGE_INTEGER i;
  i.QuadPart = 0;
  i.LowPart = GetFileSize(m_fd, (LPDWORD)&i.HighPart);

  if (i.LowPart == INVALID_FILE_SIZE && (st = GetLastError()) != NO_ERROR) {
    char buf[256];
    ham_log(("GetFileSize failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  return ((size_t)i.QuadPart);
}

void
File::truncate(uint64_t newsize)
{
  File::seek(newsize, kSeekSet);

  if (!SetEndOfFile(m_fd)) {
    char buf[256];
    ham_status_t st = (ham_status_t)GetLastError();
    ham_log(("SetEndOfFile failed with OS status %u (%s)", st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
File::create(const char *filename, uint32_t mode)
{
  ham_status_t st;
  DWORD share = 0; /* 1.1.0: default behaviour is exclusive locking */
  DWORD access = GENERIC_READ | GENERIC_WRITE;
  ham_fd_t fd;

#ifdef UNICODE
  int fnameWlen = calc_wlen4str(filename);
  WCHAR *wfilename = (WCHAR *)malloc(fnameWlen * sizeof(wfilename[0]));
  if (!wfilename)
    throw Exception(HAM_OUT_OF_MEMORY);

  /* translate ASCII filename to unicode */
  utf8_string(filename, wfilename, fnameWlen);
  fd = (ham_fd_t)CreateFileW(wfilename, access,
        share, NULL, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, 0);
  free(wfilename);
#else
  fd = (ham_fd_t)CreateFileA(filename, access,
        share, NULL, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, 0);
#endif

  if (fd == INVALID_HANDLE_VALUE) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    if (st == ERROR_SHARING_VIOLATION)
      throw Exception(HAM_WOULD_BLOCK);
    ham_log(("CreateFile(%s, %x, %x, ...) (create) failed with OS status "
            "%u (%s)", filename, access, share, st,
            DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }

  m_fd = fd;
}

void
File::flush()
{
  ham_status_t st;

  if (!FlushFileBuffers(m_fd)) {
    char buf[256];
    st = (ham_status_t)GetLastError();
    ham_log(("FlushFileBuffers failed with OS status %u (%s)",
        st, DisplayError(buf, sizeof(buf), st)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
File::open(const char *filename, bool read_only)
{
  ham_status_t st;
  DWORD share = 0; /* 1.1.0: default behaviour is exclusive locking */
  DWORD access = read_only
          ? GENERIC_READ
          : (GENERIC_READ | GENERIC_WRITE);
  DWORD dispo = OPEN_EXISTING;
  DWORD osflags = 0;
  ham_fd_t fd;

#ifdef UNICODE
  {
    int fnameWlen = calc_wlen4str(filename);
    WCHAR *wfilename = (WCHAR *)malloc(fnameWlen * sizeof(wfilename[0]));
    if (!wfilename)
      throw Exception(HAM_OUT_OF_MEMORY);

    /* translate ASCII filename to unicode */
    utf8_string(filename, wfilename, fnameWlen);
    fd = (ham_fd_t)CreateFileW(wfilename, access, share, NULL,
              dispo, osflags, 0);
    free(wfilename);
  }
#else
  fd = (ham_fd_t)CreateFileA(filename, access, share, NULL,
            dispo, osflags, 0);
#endif

  if (fd == INVALID_HANDLE_VALUE) {
    char buf[256];
    fd = HAM_INVALID_FD;
    st = (ham_status_t)GetLastError();
    ham_log(("CreateFile(%s, %x, %x, ...) (open) failed with OS status "
            "%u (%s)", filename, access, share,
            st, DisplayError(buf, sizeof(buf), st)));
    if (st == ERROR_SHARING_VIOLATION)
      throw Exception(HAM_WOULD_BLOCK);
    throw Exception(st == ERROR_FILE_NOT_FOUND
                        ? HAM_FILE_NOT_FOUND
                        : HAM_IO_ERROR);
  }

  m_fd = fd;
}

void
File::close()
{
  if (m_fd != HAM_INVALID_FD) {
    if (!CloseHandle((HANDLE)m_fd)) {
      char buf[256];
      ham_status_t st = (ham_status_t)GetLastError();
      ham_log(("CloseHandle failed with OS status %u (%s)", st,
              DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
    m_fd = HAM_INVALID_FD;
  }

  if (m_mmaph != HAM_INVALID_FD) {
    if (!CloseHandle((HANDLE)m_mmaph)) {
      char buf[256];
      ham_status_t st = (ham_status_t)GetLastError();
      ham_log(("CloseHandle failed with OS status %u (%s)", st,
              DisplayError(buf, sizeof(buf), st)));
      throw Exception(HAM_IO_ERROR);
    }
    m_mmaph = HAM_INVALID_FD;
  }
}

void
Socket::connect(const char *hostname, uint16_t port, uint32_t timeout_sec)
{
  WORD sockVersion = MAKEWORD(1, 1);
  WSADATA wsaData;
  WSAStartup(sockVersion, &wsaData);

  ham_socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s < 0) {
    ham_log(("failed creating socket: %s", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }

  LPHOSTENT server = ::gethostbyname(hostname);
  if (!server) {
    ham_log(("unable to resolve hostname %s", hostname));
    ::closesocket(s);
    throw Exception(HAM_NETWORK_ERROR);
  }

  SOCKADDR_IN addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr = *((LPIN_ADDR)*server->h_addr_list);
  addr.sin_port = htons(port);
  if (::connect(s, (LPSOCKADDR)&addr, sizeof(addr)) < 0) {
    ham_log(("unable to connect to %s:%d: %s", hostname, (int)port,
                strerror(errno)));
    ::closesocket(s);
    throw Exception(HAM_NETWORK_ERROR);
  }

  if (timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    if (::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv)) < 0) {
      char buf[256];
      ham_log(("unable to set socket timeout to %u sec: %u/%s", timeout_sec,
                  WSAGetLastError(), DisplayError(buf, sizeof(buf),
                  WSAGetLastError())));
      // fall through, this is not critical
    }
  }

  m_socket = s;
}

void
Socket::send(const uint8_t *data, size_t len)
{
  size_t sent = 0;
  char buf[256];
  ham_status_t st;
  
  while (sent != len) {
    int s = ::send(m_socket, (const char *)(data + sent), len - sent, 0);
	if (s <= 0) {
      st = (ham_status_t)GetLastError();
      ham_log(("send failed with OS status %u (%s)", st,
              DisplayError(buf, sizeof(buf), st)));
	  throw Exception(HAM_IO_ERROR);
	}
	sent += s;
  }
}

void
Socket::recv(uint8_t *data, size_t len)
{
  size_t read = 0;
  char buf[256];
  ham_status_t st;
  
  while (read != len) {
    int r = ::recv(m_socket, (char *)(data + read), len - read, 0);
	if (r <= 0) {
      st = (ham_status_t)GetLastError();
      ham_log(("recv failed with OS status %u (%s)", st,
              DisplayError(buf, sizeof(buf), st)));
	  throw Exception(HAM_IO_ERROR);
	}
	read += r;
  }
}

void
Socket::close()
{
  if (m_socket != HAM_INVALID_FD) {
    if (::closesocket(m_socket) == -1)
      throw Exception(HAM_IO_ERROR);
    m_socket = HAM_INVALID_FD;
  }
}

} // namespace hamsterdb
