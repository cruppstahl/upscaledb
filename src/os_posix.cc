/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#include "config.h"

#define _GNU_SOURCE   1 // for O_LARGEFILE
#define __USE_XOPEN2K 1 // for ftruncate()
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

#include "error.h"
#include "os.h"

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

ham_u32_t
os_get_granularity()
{
  return ((ham_u32_t)sysconf(_SC_PAGE_SIZE));
}

void
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_u64_t position,
            ham_u64_t size, bool readonly, ham_u8_t **buffer)
{
  os_log(("os_mmap: fd=%d, position=%lld, size=%lld", fd, position, size));

  int prot = PROT_READ;
  if (!readonly)
    prot |= PROT_WRITE;

  (void)mmaph;  /* only used on win32-platforms */

#if HAVE_MMAP
  *buffer = (ham_u8_t *)mmap(0, size, prot, MAP_PRIVATE, fd, position);
  if (*buffer == (void *)-1) {
    *buffer = 0;
    ham_log(("mmap failed with status %d (%s)", errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  throw Exception(HAM_NOT_IMPLEMENTED);
#endif
}

void
os_munmap(ham_fd_t *mmaph, void *buffer, ham_u64_t size)
{
  os_log(("os_munmap: size=%lld", size));

  int r;
  (void)mmaph; /* only used on win32-platforms */

#if HAVE_MUNMAP
  r = munmap(buffer, size);
  if (r) {
    ham_log(("munmap failed with status %d (%s)", errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  throw Exception(HAM_NOT_IMPLEMENTED);
#endif
}

static void
os_read(ham_fd_t fd, ham_u8_t *buffer, ham_u64_t bufferlen)
{
  os_log(("_os_read: fd=%d, size=%lld", fd, bufferlen));

  int r;
  ham_u32_t total = 0;

  while (total < bufferlen) {
    r = read(fd, &buffer[total], bufferlen - total);
    if (r < 0) {
      ham_log(("os_read failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (r == 0)
      break;
    total += r;
  }

  if (total != bufferlen) {
    ham_log(("os_read() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
os_pread(ham_fd_t fd, ham_u64_t addr, void *buffer,
            ham_u64_t bufferlen)
{
#if HAVE_PREAD
  os_log(("os_pread: fd=%d, address=%lld, size=%lld", fd, addr, bufferlen));

  int r;
  ham_u64_t total = 0;

  while (total < bufferlen) {
    r = pread(fd, (ham_u8_t *)buffer + total, bufferlen - total, addr + total);
    if (r < 0) {
      ham_log(("os_pread failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (r == 0)
      break;
    total += r;
  }

  if (total != bufferlen) {
    ham_log(("os_pread() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  os_seek(fd, addr, HAM_OS_SEEK_SET);
  os_read(fd, (ham_u8_t *)buffer, bufferlen);
#endif
}

void
os_write(ham_fd_t fd, const void *buffer, ham_u64_t bufferlen)
{
  os_log(("os_write: fd=%d, size=%lld", fd, bufferlen));

  int w;
  ham_u64_t total = 0;
  const char *p = (const char *)buffer;

  while (total < bufferlen) {
    w = write(fd, p + total, bufferlen - total);
    if (w < 0) {
      ham_log(("os_write failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (w == 0)
      break;
    total += w;
  }

  if (total != bufferlen) {
    ham_log(("os_write() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
os_pwrite(ham_fd_t fd, ham_u64_t addr, const void *buffer,
            ham_u64_t bufferlen)
{
  os_log(("os_pwrite: fd=%d, address=%lld, size=%lld", fd, addr, bufferlen));

#if HAVE_PWRITE
  ssize_t s;
  ham_u64_t total = 0;

  while (total < bufferlen) {
    s = pwrite(fd, buffer, bufferlen, addr + total);
    if (s < 0) {
      ham_log(("pwrite() failed with status %u (%s)", errno, strerror(errno)));
      throw Exception(HAM_IO_ERROR);
    }
    if (s == 0)
      break;
    total += s;
  }

  if (total != bufferlen) {
    ham_log(("pwrite() failed with short read (%s)", strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
#else
  os_seek(fd, addr, HAM_OS_SEEK_SET);
  os_write(fd, buffer, bufferlen);
#endif
}

void
os_seek(ham_fd_t fd, ham_u64_t offset, int whence)
{
  os_log(("os_seek: fd=%d, offset=%lld, whence=%d", fd, offset, whence));
  if (lseek(fd, offset, whence) < 0)
    throw Exception(HAM_IO_ERROR);
}

ham_u64_t
os_tell(ham_fd_t fd)
{
  ham_u64_t offset = lseek(fd, 0, SEEK_CUR);
  os_log(("os_tell: fd=%d, offset=%lld", fd, offset));
  if (offset == (ham_u64_t) - 1)
    throw Exception(HAM_IO_ERROR);
  return (offset);
}

ham_u64_t
os_get_file_size(ham_fd_t fd)
{
  os_seek(fd, 0, HAM_OS_SEEK_END);
  ham_u64_t size = os_tell(fd);
  os_log(("os_get_file_size: fd=%d, size=%lld", fd, size));
  return (size);
}

void
os_truncate(ham_fd_t fd, ham_u64_t newsize)
{
  os_log(("os_truncate: fd=%d, size=%lld", fd, newsize));
  if (ftruncate(fd, newsize))
    throw Exception(HAM_IO_ERROR);
}

ham_fd_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode)
{
  int osflags = O_CREAT | O_RDWR | O_TRUNC;
#if HAVE_O_NOATIME
  flags |= O_NOATIME;
#endif
  (void)flags;

  ham_fd_t fd = open(filename, osflags, mode ? mode : 0644);
  if (fd < 0) {
    ham_log(("creating file %s failed with status %u (%s)", filename,
        errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }

  /* lock the file - this is default behaviour since 1.1.0 */
  lock_exclusive(fd, true);

  /* enable O_LARGEFILE support */
  enable_largefile(fd);

  return (fd);
}

void
os_flush(ham_fd_t fd)
{
  os_log(("os_flush: fd=%d", fd));
  /* unlike fsync(), fdatasync() does not flush the metadata unless
   * it's really required. it's therefore a lot faster. */
#if HAVE_FDATASYNC && !__APPLE__
  if (fdatasync(fd) == -1) {
#else
  if (fsync(fd) == -1) {
#endif
    ham_log(("fdatasync failed with status %u (%s)",
        errno, strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

ham_fd_t
os_open(const char *filename, ham_u32_t flags)
{
  int osflags = 0;

  if (flags & HAM_READ_ONLY)
    osflags |= O_RDONLY;
  else
    osflags |= O_RDWR;
#if HAVE_O_NOATIME
  osflags |= O_NOATIME;
#endif

  ham_fd_t fd = open(filename, osflags);
  if (fd < 0) {
    ham_log(("opening file %s failed with status %u (%s)", filename,
        errno, strerror(errno)));
    throw Exception(errno == ENOENT ? HAM_FILE_NOT_FOUND : HAM_IO_ERROR);
  }

  /* lock the file - this is default behaviour since 1.1.0 */
  lock_exclusive(fd, true);

  /* enable O_LARGEFILE support */
  enable_largefile(fd);

  return (fd);
}

void
os_close(ham_fd_t fd)
{
  // on posix, we most likely don't want to close descriptors 0 and 1
  ham_assert(fd != 0 && fd != 1);

  // unlock the file - this is default behaviour since 1.1.0
  lock_exclusive(fd, false);

  // now close the descriptor
  if (::close(fd) == -1)
    throw Exception(HAM_IO_ERROR);
}

ham_socket_t
os_socket_connect(const char *hostname, ham_u16_t port, ham_u32_t timeout_sec)
{
  ham_fd_t s = ::socket(AF_INET, SOCK_STREAM, 0);
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

  return (s);
}

void
os_socket_send(ham_fd_t socket, const ham_u8_t *data, ham_u32_t data_size)
{
  os_write(socket, data, data_size);
}

void
os_socket_recv(ham_fd_t socket, ham_u8_t *data, ham_u32_t data_size)
{
  os_read(socket, data, data_size);
}

void
os_socket_close(ham_fd_t *socket)
{
  if (*socket != HAM_INVALID_FD) {
    if (::close(*socket) == -1)
      throw Exception(HAM_IO_ERROR);
    *socket = HAM_INVALID_FD;
  }
}

} // namespace hamsterdb
