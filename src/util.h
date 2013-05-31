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

/**
 * @brief utility functions
 *
 */

#ifndef HAM_UTIL_H__
#define HAM_UTIL_H__


#include <stdarg.h>
#include <stdio.h>

#include "ham/hamsterdb.h"

#include "mem.h"

namespace hamsterdb {

class ByteArray
{
  public:
    ByteArray(Allocator *alloc = 0, ham_size_t size = 0)
      : m_alloc(alloc), m_ptr(0), m_size(0), m_own(true) {
      resize(size);
    }

    ByteArray(Allocator *alloc, ham_size_t size, ham_u8_t fill_byte)
      : m_alloc(alloc), m_ptr(0), m_size(0), m_own(true) {
      resize(size);
      if (m_ptr)
        ::memset(m_ptr, fill_byte, m_size);
    }

    ~ByteArray() {
      if (m_own)
        clear();
    }

    void *resize(ham_size_t size) {
      if (size > m_size) {
        m_ptr = m_alloc->realloc(m_ptr, size);
        m_size = size;
      }
      return (m_ptr);
    }

    void *resize(ham_size_t size, ham_u8_t fill_byte) {
      resize(size);
      if (m_ptr)
        memset(m_ptr, fill_byte, size);
      return (m_ptr);
    }

    void set_allocator(Allocator *alloc) {
      m_alloc = alloc;
    }

    ham_size_t get_size() {
      return (m_size);
    }

    void *get_ptr() {
      return (m_ptr);
    }

    void assign(void *ptr, ham_size_t size) {
      clear();
      m_ptr = ptr;
      m_size = size;
    }

    void clear() {
      if (m_ptr)
        m_alloc->free(m_ptr);
      m_ptr = 0;
      m_size = 0;
    }

    void disown() {
      m_own = false;
    }

  private:
    Allocator *m_alloc;
    void *m_ptr;
    ham_size_t m_size;
    bool m_own;
};

/**
 * vsnprintf replacement/wrapper
 *
 * uses sprintf on platforms which do not define snprintf
 */
extern int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap);

/**
 * snprintf replacement/wrapper
 *
 * uses sprintf on platforms which do not define snprintf
 */
#ifndef HAM_OS_POSIX
#define util_snprintf _snprintf
#else
#define util_snprintf snprintf
#endif

} // namespace hamsterdb

#endif /* HAM_UTIL_H__ */
