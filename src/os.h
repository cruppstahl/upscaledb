/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief operating-system specific functions (mostly I/O stuff)
 *
 */

#ifndef HAM_OS_H__
#define HAM_OS_H__

#include <ham/types.h>

#include <stdio.h>
#include <limits.h>


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * read data from a file with mmap
 *
 * @remark mmap is called with MAP_PRIVATE - the allocated buffer
 * is just a copy of the file; writing to the buffer will not alter
 * the file itself.
 *
 * @remark win32 needs a second handle for CreateFileMapping
 */
extern ham_status_t
os_mmap(ham_fd_t fd, ham_fd_t *mmaph, ham_offset_t position, 
		ham_offset_t size, ham_bool_t readonly, ham_u8_t **buffer);

/**
 * unmap a buffer 
 *
 * @remark win32 needs a second handle for CreateFileMapping
 */
extern ham_status_t
os_munmap(ham_fd_t *mmaph, void *buffer, ham_offset_t size);

/**
 * read data from a file
 */
extern ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer, 
        ham_offset_t bufferlen);

/**
 * write data to a file
 */
extern ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer, 
        ham_offset_t bufferlen);

/**
 * append data to a file
 */
extern ham_status_t
os_write(ham_fd_t fd, const void *buffer, ham_offset_t bufferlen);

/**
 * append data from two buffers to a file
 */
extern ham_status_t
os_writev(ham_fd_t fd, const void *buffer1, ham_offset_t buffer1_len,
                const void *buffer2=0, ham_offset_t buffer2_len=0,
                const void *buffer3=0, ham_offset_t buffer3_len=0,
                const void *buffer4=0, ham_offset_t buffer4_len=0);

#ifdef HAM_OS_POSIX
#    define HAM_OS_SEEK_SET     SEEK_SET
#    define HAM_OS_SEEK_END     SEEK_END
#    define HAM_OS_SEEK_CUR     SEEK_CUR
#    define HAM_OS_MAX_PATH     PATH_MAX
#else
#    define HAM_OS_SEEK_SET     FILE_BEGIN
#    define HAM_OS_SEEK_END     FILE_END
#    define HAM_OS_SEEK_CUR     FILE_CURRENT
#    define HAM_OS_MAX_PATH     MAX_PATH
#endif

/**
 * get the preferred pagesize of the operating system
 */
extern ham_size_t
os_get_pagesize(void);

/**
 * get the page allocation granularity of the operating system
 */
extern ham_size_t
os_get_granularity(void);

/**
 * seek position in a file
 */
extern ham_status_t
os_seek(ham_fd_t fd, ham_offset_t offset, int whence);

/**
 * tell the position in a file
 */
extern ham_status_t
os_tell(ham_fd_t fd, ham_offset_t *offset);

/**
 * get the size of the database file
 */
extern ham_status_t
os_get_filesize(ham_fd_t fd, ham_offset_t *size);

/**
 * truncate/resize the file
 */
extern ham_status_t
os_truncate(ham_fd_t fd, ham_offset_t newsize);

/**
 * create a new file
 */
extern ham_status_t
os_create(const char *filename, ham_u32_t flags, ham_u32_t mode, ham_fd_t *fd);

/**
 * open an existing file
 */
extern ham_status_t
os_open(const char *filename, ham_u32_t flags, ham_fd_t *fd);

/**
 * flush a file
 */
extern ham_status_t
os_flush(ham_fd_t fd);

/**
 * close a filedescriptor
 */
extern ham_status_t
os_close(ham_fd_t fd, ham_u32_t flags);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_OS_H__ */
