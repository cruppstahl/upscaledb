/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * operating-system specific functions (mostly I/O stuff)
 *
 */

#ifndef HAM_OS_H__
#define HAM_OS_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <stdio.h>
#include <ham/types.h>

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
		ham_size_t size, ham_u8_t **buffer);

/**
 * unmap a buffer 
 *
 * @remark win32 needs a second handle for CreateFileMapping
 */
extern ham_status_t
os_munmap(ham_fd_t *mmaph, void *buffer, ham_size_t size);

/**
 * read data from a file
 */
extern ham_status_t
os_pread(ham_fd_t fd, ham_offset_t addr, void *buffer, 
        ham_size_t bufferlen);

/**
 * write data to a file
 */
extern ham_status_t
os_pwrite(ham_fd_t fd, ham_offset_t addr, const void *buffer, 
        ham_size_t bufferlen);

#ifdef HAM_OS_POSIX
#    define HAM_OS_SEEK_SET     SEEK_SET
#    define HAM_OS_SEEK_END     SEEK_END
#    define HAM_OS_SEEK_CUR     SEEK_CUR
#else
#    define HAM_OS_SEEK_SET     FILE_BEGIN
#    define HAM_OS_SEEK_END     FILE_END
#    define HAM_OS_SEEK_CUR     FILE_CURRENT
#endif

/**
 * get the pagesize of the operating system
 */
extern ham_size_t
os_get_pagesize(void);

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
os_close(ham_fd_t fd);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_OS_H__ */
