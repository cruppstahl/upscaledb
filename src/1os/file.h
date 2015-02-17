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

/*
 * A simple wrapper around a file handle. Throws exceptions in
 * case of errors. Moves the file handle when copied.
 *
 * @exception_safe: strong
 * @thread_safe: unknown
 */

#ifndef HAM_FILE_H
#define HAM_FILE_H

#include "0root/root.h"

#include <stdio.h>
#include <limits.h>

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class File
{
  public:
    enum {
#ifdef HAM_OS_POSIX
      kSeekSet = SEEK_SET,
      kSeekEnd = SEEK_END,
      kSeekCur = SEEK_CUR,
      kMaxPath = PATH_MAX
#else
      kSeekSet = FILE_BEGIN,
      kSeekEnd = FILE_END,
      kSeekCur = FILE_CURRENT,
      kMaxPath = MAX_PATH
#endif
    };

    // Constructor: creates an empty File handle
    File()
      : m_fd(HAM_INVALID_FD), m_mmaph(HAM_INVALID_FD), m_posix_advice(0) {
    }

    // Copy constructor: moves ownership of the file handle
    File(File &other)
      : m_fd(other.m_fd), m_mmaph(other.m_mmaph),
        m_posix_advice(other.m_posix_advice) {
      other.m_fd = HAM_INVALID_FD;
	  other.m_mmaph = HAM_INVALID_FD;
    }

    // Destructor: closes the file
    ~File() {
      close();
    }

    // Assignment operator: moves ownership of the file handle
    File &operator=(File &other) {
      m_fd = other.m_fd;
      other.m_fd = HAM_INVALID_FD;
      return (*this);
    }

    // Creates a new file
    void create(const char *filename, uint32_t mode);

    // Opens an existing file
    void open(const char *filename, bool read_only);

    // Returns true if the file is open
    bool is_open() const {
      return (m_fd != HAM_INVALID_FD);
    }

    // Flushes a file
    void flush();

    // Sets the parameter for posix_fadvise()
    void set_posix_advice(int parameter);

    // Maps a file in memory
    //
    // mmap is called with MAP_PRIVATE - the allocated buffer
    // is just a copy of the file; writing to the buffer will not alter
    // the file itself.
    void mmap(uint64_t position, size_t size, bool readonly,
                    uint8_t **buffer);

    // Unmaps a buffer
    void munmap(void *buffer, size_t size);

    // Positional read from a file
    void pread(uint64_t addr, void *buffer, size_t len);

    // Positional write to a file
    void pwrite(uint64_t addr, const void *buffer, size_t len);

    // Write data to a file; uses the current file position
    void write(const void *buffer, size_t len);

    // Get the page allocation granularity of the operating system
    static size_t get_granularity();

    // Seek position in a file
    void seek(uint64_t offset, int whence);

    // Tell the position in a file
    uint64_t tell();

    // Returns the size of the file
    uint64_t get_file_size();

    // Truncate/resize the file
    void truncate(uint64_t newsize);

    // Closes the file descriptor
    void close();

  private:
    // The file handle
    ham_fd_t m_fd;

    // The mmap handle - required for Win32
    ham_fd_t m_mmaph;

    // Parameter for posix_fadvise()
    int m_posix_advice;
};

} // namespace hamsterdb

#endif /* HAM_FILE_H */
