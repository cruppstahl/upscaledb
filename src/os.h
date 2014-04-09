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

#ifndef HAM_OS_H__
#define HAM_OS_H__

#include <stdio.h>
#include <limits.h>

#include <ham/types.h>

namespace hamsterdb {

// maps a file in memory
//
// mmap is called with MAP_PRIVATE - the allocated buffer
// is just a copy of the file; writing to the buffer will not alter
// the file itself.
//
// win32 needs a second handle for CreateFileMapping
extern void
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_u64_t position,
            ham_u64_t size, bool readonly, ham_u8_t **buffer);

// unmaps a buffer
extern void
os_munmap(ham_fd_t *mmaph, void *buffer, ham_u64_t size);

// positional read from a file
extern void
os_pread(ham_fd_t fd, ham_u64_t addr, void *buffer,
            ham_u64_t bufferlen);

// positional write to a file
extern void
os_pwrite(ham_fd_t fd, ham_u64_t addr, const void *buffer,
           ham_u64_t bufferlen);

// write data to a file; uses the current file position
extern void
os_write(ham_fd_t fd, const void *buffer, ham_u64_t bufferlen);

#ifdef HAM_OS_POSIX
#  define HAM_OS_SEEK_SET   SEEK_SET
#  define HAM_OS_SEEK_END   SEEK_END
#  define HAM_OS_SEEK_CUR   SEEK_CUR
#  define HAM_OS_MAX_PATH   PATH_MAX
#else
#  define HAM_OS_SEEK_SET   FILE_BEGIN
#  define HAM_OS_SEEK_END   FILE_END
#  define HAM_OS_SEEK_CUR   FILE_CURRENT
#  define HAM_OS_MAX_PATH   MAX_PATH
#endif

// get the page allocation granularity of the operating system
extern ham_u32_t
os_get_granularity();

// seek position in a file
extern void
os_seek(ham_fd_t fd, ham_u64_t offset, int whence);

// tell the position in a file
extern ham_u64_t
os_tell(ham_fd_t fd);

// returns the size of a database file
extern ham_u64_t
os_get_file_size(ham_fd_t fd);

// truncate/resize the file
extern void
os_truncate(ham_fd_t fd, ham_u64_t newsize);

// create a new file
extern ham_fd_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode);

// open an existing file
extern ham_fd_t
os_open(const char *filename, ham_u32_t flags);

// flush a file
extern void
os_flush(ham_fd_t fd);

// close a file descriptor
extern void
os_close(ham_fd_t fd);

// creates a socket, connects to a remote server
extern ham_socket_t
os_socket_connect(const char *hostname, ham_u16_t port, ham_u32_t timeout_sec);

// (blocking) writes |data_size| bytes in |data| to the socket
extern void
os_socket_send(ham_socket_t socket, const ham_u8_t *data, ham_u32_t data_size);

// (blocking) reads |data_size| bytes from |socket|, stores the data
// in |data|
extern void
os_socket_recv(ham_socket_t socket, ham_u8_t *data, ham_u32_t data_size);

// closes the socket, then sets |*socket| to HAM_INVALID_FD
extern void
os_socket_close(ham_socket_t *socket);

// Returns the number of 32bit integers that the CPU can process in
// parallel (the SIMD lane width) 
inline int
os_get_simd_lane_width()
{
#ifdef HAM_ENABLE_SIMD
#  ifdef __SSE2__
  return (4);
#  endif
#endif // HAM_ENABLE_SIMD
  return (0);
}

} // namespace hamsterdb

#endif /* HAM_OS_H__ */
