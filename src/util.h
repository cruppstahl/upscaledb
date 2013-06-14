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
#include <string.h>

#include "ham/hamsterdb.h"

#include "mem.h"

namespace hamsterdb {

/*
 * The ByteArray class is a dynamic, resizable array. The internal memory
 * is released when the ByteArray instance is destructed.
 */
class ByteArray
{
  public:
    ByteArray(ham_size_t size = 0)
      : m_ptr(0), m_size(0), m_own(true) {
      resize(size);
    }

    ByteArray(ham_size_t size, ham_u8_t fill_byte)
      : m_ptr(0), m_size(0), m_own(true) {
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
        m_ptr = Memory::reallocate<void>(m_ptr, size);
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
      Memory::release(m_ptr);
      m_ptr = 0;
      m_size = 0;
    }

    void disown() {
      m_own = false;
    }

  private:
    void *m_ptr;
    ham_size_t m_size;
    bool m_own;
};

//
// vsnprintf replacement/wrapper
//
// uses vsprintf on platforms which do not define vsnprintf
//
extern int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap);

//
// snprintf replacement/wrapper
//
// uses sprintf on platforms which do not define snprintf
//
#ifndef HAM_OS_POSIX
#  define util_snprintf _snprintf
#else
#  define util_snprintf snprintf
#endif

} // namespace hamsterdb

#endif /* HAM_UTIL_H__ */
