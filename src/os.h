/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_OS_H__
#define HAM_OS_H__

#include <stdio.h>
#include <limits.h>
#include <vector>

#include <ham/types.h>

namespace hamsterdb {

// maps a file in memory
//
// mmap is called with MAP_PRIVATE - the allocated buffer
// is just a copy of the file; writing to the buffer will not alter
// the file itself.
//
// win32 needs a second handle for CreateFileMapping
extern ham_status_t
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_u64_t position,
            ham_u64_t size, bool readonly, ham_u8_t **buffer);

// unmaps a buffer
extern ham_status_t
os_munmap(ham_fd_t *mmaph, void *buffer, ham_u64_t size);

// positional read from a file
extern ham_status_t
os_pread(ham_fd_t fd, ham_u64_t addr, void *buffer,
            ham_u64_t bufferlen);

// positional write to a file
extern ham_status_t
os_pwrite(ham_fd_t fd, ham_u64_t addr, const void *buffer,
           ham_u64_t bufferlen);

// write data to a file; uses the current file position
extern ham_status_t
os_write(ham_fd_t fd, const void *buffer, ham_u64_t bufferlen);

// write data from multiple buffers to a file; uses the current file position
extern ham_status_t
os_writev(ham_fd_t fd, void *buffer1, ham_u64_t buffer1_len,
            void *buffer2 = 0, ham_u64_t buffer2_len = 0,
            void *buffer3 = 0, ham_u64_t buffer3_len = 0,
            void *buffer4 = 0, ham_u64_t buffer4_len = 0,
            void *buffer5 = 0, ham_u64_t buffer5_len = 0);

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
extern ham_size_t
os_get_granularity(void);

// seek position in a file
extern ham_status_t
os_seek(ham_fd_t fd, ham_u64_t offset, int whence);

// tell the position in a file
extern ham_status_t
os_tell(ham_fd_t fd, ham_u64_t *offset);

// get the size of a database file
extern ham_status_t
os_get_filesize(ham_fd_t fd, ham_u64_t *size);

// truncate/resize the file
extern ham_status_t
os_truncate(ham_fd_t fd, ham_u64_t newsize);

// create a new file
extern ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd);

// open an existing file
extern ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd);

// flush a file
extern ham_status_t
os_flush(ham_fd_t fd);

// close a file descriptor
extern ham_status_t
os_close(ham_fd_t fd);

// creates a socket, connects to a remote server
extern ham_status_t
os_socket_connect(const char *hostname, ham_u16_t port, ham_u32_t timeout_sec,
            ham_socket_t *socket);

// (blocking) writes |data_size| bytes in |data| to the socket
extern ham_status_t
os_socket_send(ham_socket_t socket, const ham_u8_t *data, ham_size_t data_size);

// (blocking) reads |data_size| bytes from |socket|, stores the data
// in |data|
extern ham_status_t
os_socket_recv(ham_socket_t socket, ham_u8_t *data, ham_size_t data_size);

// closes the socket, then sets |*socket| to HAM_INVALID_FD
extern ham_status_t
os_socket_close(ham_socket_t *socket);

} // namespace hamsterdb

#endif /* HAM_OS_H__ */
