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

#define _GNU_SOURCE        1 // for O_LARGEFILE
#define _FILE_OFFSET_BITS 64

#include "0root/root.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>
#if HAVE_MMAP
#  include <sys/mman.h>
#endif
#if HAVE_WRITEV
#  include <sys/uio.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1os/file.h"
#include "1os/socket.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

#if 0
#  define os_log(x)      ham_log(x)
#else
#  define os_log(x)
#endif

static void
lock_exclusive(int fd, bool lock)
{
#ifdef HAM_SOLARIS
  // SunOS 5.9 doesn't have LOCK_* unless i include /usr/ucbinclude; but then,
  // mmap behaves strangely (the first write-access to the mmapped buffer
  // leads to a segmentation fault).
  //
  // Tell me if this troubles you/if you have suggestions for fixes.
#else
  int flags;

  if (lock)
    flags = LOCK_EX | LOCK_NB;
  else
    flags = LOCK_UN;

  if (0 != flock(fd, flags)) {
    ham_log(("flock failed with status %u (%s)", errno, strerror(errno)));
    // it seems that linux does not only return EWOULDBLOCK, as stated
    // in the documentation (flock(2)), but also other errors...
    if (errno && lock)
      throw Exception(HAM_WOULD_BLOCK);
    throw Exception(HAM_IO_ERROR);
  }
#endif
}

static void
enable_largefile(int fd)
{
  // not available on cygwin...
#ifdef HAVE_O_LARGEFILE
  int oflag = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, oflag | O_LARGEFILE);
#endif
}

static void
os_read(ham_fd_t fd, uint8_t *buffer, size_t len)
{
  os_log(("os_read: fd=%d, size=%lld", fd, len));

  int r;
  size_t total = 0;

  while (total < len) {
    r = read(fd, &buffer[total], len - total);
    if (r < 0) {
      ham_log(("os_read failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (r == 0)
      break;
    total += r;
  }

  if (total != len) {
    ham_log(("os_read() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

static void
os_write(ham_fd_t fd, const void *buffer, size_t len)
{
  int w;
  size_t total = 0;
  const char *p = (const char *)buffer;

  while (total < len) {
    w = ::write(fd, p + total, len - total);
    if (w < 0) {
      ham_log(("os_write failed with status %u (%s)", errno,
                              strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (w == 0)
      break;
    total += w;
  }

  if (total != len) {
    ham_log(("os_write() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

size_t
File::get_granularity()
{
  return ((size_t)sysconf(_SC_PAGE_SIZE));
}

void
File::set_posix_advice(int advice)
{
  m_posix_advice = advice;
  ham_assert(m_fd != HAM_INVALID_FD);

#if HAVE_POSIX_FADVISE
  if (m_posix_advice == HAM_POSIX_FADVICE_RANDOM) {
    int r = ::posix_fadvise(m_fd, 0, 0, POSIX_FADV_RANDOM);
    if (r != 0) {
      ham_log(("posix_fadvise failed with status %d (%s)",
                              errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
  }
#endif
}

void
File::mmap(uint64_t position, size_t size, bool readonly, uint8_t **buffer)
{
  os_log(("File::mmap: fd=%d, position=%lld, size=%lld", m_fd, position, size));

  int prot = PROT_READ;
  if (!readonly)
    prot |= PROT_WRITE;

#if HAVE_MMAP
  *buffer = (uint8_t *)::mmap(0, size, prot, MAP_PRIVATE, m_fd, position);
  if (*buffer == (void *)-1) {
    *buffer = 0;
    ham_log(("mmap failed with status %d (%s)", errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  throw Exception(HAM_NOT_IMPLEMENTED);
#endif

#if HAVE_MADVISE
  if (m_posix_advice == HAM_POSIX_FADVICE_RANDOM) {
    int r = ::madvise(*buffer, size, MADV_RANDOM);
    if (r != 0) {
      ham_log(("madvise failed with status %d (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
  }
#endif
}

void
File::munmap(void *buffer, size_t size)
{
  os_log(("File::munmap: size=%lld", size));

#if HAVE_MUNMAP
  int r = ::munmap(buffer, size);
  if (r) {
    ham_log(("munmap failed with status %d (%s)", errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  throw Exception(HAM_NOT_IMPLEMENTED);
#endif
}

void
File::pread(uint64_t addr, void *buffer, size_t len)
{
#if HAVE_PREAD
  os_log(("File::pread: fd=%d, address=%lld, size=%lld", m_fd, addr,
                          len));

  int r;
  size_t total = 0;

  while (total < len) {
    r = ::pread(m_fd, (uint8_t *)buffer + total, len - total,
                    addr + total);
    if (r < 0) {
      ham_log(("File::pread failed with status %u (%s)", errno,
                              strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (r == 0)
      break;
    total += r;
  }

  if (total != len) {
    ham_log(("File::pread() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  File::seek(addr, kSeekSet);
  os_read(m_fd, (uint8_t *)buffer, len);
#endif
}

void
File::pwrite(uint64_t addr, const void *buffer, size_t len)
{
  os_log(("File::pwrite: fd=%d, address=%lld, size=%lld", m_fd, addr, len));

#if HAVE_PWRITE
  ssize_t s;
  size_t total = 0;

  while (total < len) {
    s = ::pwrite(m_fd, buffer, len, addr + total);
    if (s < 0) {
      ham_log(("pwrite() failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (s == 0)
      break;
    total += s;
  }

  if (total != len) {
    ham_log(("pwrite() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  seek(addr, kSeekSet);
  write(buffer, len);
#endif
}

void
File::write(const void *buffer, size_t len)
{
  os_log(("File::write: fd=%d, size=%lld", m_fd, len));
  os_write(m_fd, buffer, len);
}

void
File::seek(uint64_t offset, int whence)
{
  os_log(("File::seek: fd=%d, offset=%lld, whence=%d", m_fd, offset, whence));
  if (lseek(m_fd, offset, whence) < 0)
    throw Exception(HAM_IO_ERROR);
}

uint64_t
File::tell()
{
  uint64_t offset = lseek(m_fd, 0, SEEK_CUR);
  os_log(("File::tell: fd=%d, offset=%lld", m_fd, offset));
  if (offset == (uint64_t) - 1)
    throw Exception(HAM_IO_ERROR);
  return (offset);
}

uint64_t
File::get_file_size()
{
  seek(0, kSeekEnd);
  uint64_t size = tell();
  os_log(("File::get_file_size: fd=%d, size=%lld", m_fd, size));
  return (size);
}

void
File::truncate(uint64_t newsize)
{
  os_log(("File::truncate: fd=%d, size=%lld", m_fd, newsize));
  if (ftruncate(m_fd, newsize))
    throw Exception(HAM_IO_ERROR);
}

void
File::create(const char *filename, uint32_t mode)
{
  int osflags = O_CREAT | O_RDWR | O_TRUNC;
#if HAVE_O_NOATIME
  osflags |= O_NOATIME;
#endif

  ham_fd_t fd = ::open(filename, osflags, mode ? mode : 0644);
  if (fd < 0) {
    ham_log(("creating file %s failed with status %u (%s)", filename,
        errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }

  /* lock the file - this is default behaviour since 1.1.0 */
  lock_exclusive(fd, true);

  /* enable O_LARGEFILE support */
  enable_largefile(fd);

  m_fd = fd;
}

void
File::flush()
{
  os_log(("File::flush: fd=%d", m_fd));
  /* unlike fsync(), fdatasync() does not flush the metadata unless
   * it's really required. it's therefore a lot faster. */
#if HAVE_FDATASYNC && !__APPLE__
  if (fdatasync(m_fd) == -1) {
#else
  if (fsync(m_fd) == -1) {
#endif
    ham_log(("fdatasync failed with status %u (%s)",
        errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
File::open(const char *filename, bool read_only)
{
  int osflags = 0;

  if (read_only)
    osflags |= O_RDONLY;
  else
    osflags |= O_RDWR;
#if HAVE_O_NOATIME
  osflags |= O_NOATIME;
#endif

  ham_fd_t fd = ::open(filename, osflags);
  if (fd < 0) {
    ham_log(("opening file %s failed with status %u (%s)", filename,
        errno, strerror(errno)));
    throw Exception(errno == ENOENT ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
  }

  /* lock the file - this is default behaviour since 1.1.0 */
  lock_exclusive(fd, true);

  /* enable O_LARGEFILE support */
  enable_largefile(fd);

  m_fd = fd;
}

void
File::close()
{
  if (m_fd != HAM_INVALID_FD) {
    // on posix, we most likely don't want to close descriptors 0 and 1
    ham_assert(m_fd != 0 && m_fd != 1);

    // unlock the file - this is default behaviour since 1.1.0
    lock_exclusive(m_fd, false);

    // now close the descriptor
    if (::close(m_fd) == -1)
      throw Exception(HAM_IO_ERROR);

    m_fd = HAM_INVALID_FD;
  }
}

void
Socket::connect(const char *hostname, uint16_t port, uint32_t timeout_sec)
{
  ham_socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) {
    ham_log(("failed creating socket: %s", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }

  struct hostent *server = ::gethostbyname(hostname);
  if (!server) {
    ham_log(("unable to resolve hostname %s: %s", hostname,
                hstrerror(h_errno)));
    ::close(s);
    throw Exception(HAM_NETWORK_ERROR);
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  memcpy(&addr.sin_addr.s_addr, server->h_addr, server->h_length);
  addr.sin_port = htons(port);
  if (::connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    ham_log(("unable to connect to %s:%d: %s", hostname, (int)port,
                strerror(errno)));
    ::close(s);
    throw Exception(HAM_NETWORK_ERROR);
  }

  if (timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    if (::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv)) < 0) {
      ham_log(("unable to set socket timeout to %d sec: %s", timeout_sec,
                  strerror(errno)));
      // fall through, this is not critical
    }
  }

  m_socket = s;
}

void
Socket::send(const uint8_t *data, size_t len)
{
  os_write(m_socket, data, len);
}

void
Socket::recv(uint8_t *data, size_t len)
{
  os_read(m_socket, data, len);
}

void
Socket::close()
{
  if (m_socket != HAM_INVALID_FD) {
    if (::close(m_socket) == -1)
      throw Exception(HAM_IO_ERROR);
    m_socket = HAM_INVALID_FD;
  }
}

} // namespace hamsterdb
