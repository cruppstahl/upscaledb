/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "mem.h"

class ByteArray
{
  public:
  ByteArray(Allocator *alloc=0, ham_size_t size=0)
  : m_alloc(alloc), m_ptr(0), m_size(0) {
    resize(size);
  }
  
  ~ByteArray() {
    clear();
  }

  void resize(ham_size_t size) {
    if (size>m_size) {
      m_ptr=m_alloc->realloc(m_ptr, size);
      m_size=size;
    }
  }

  void set_allocator(Allocator *alloc) {
    m_alloc=alloc;
  }

  ham_size_t get_size() {
    return (m_size);
  }

  void *get_ptr() {
    return (m_ptr);
  }

  void assign(void *ptr, ham_size_t size) {
    clear();
    m_ptr=ptr;
    m_size=size;
  }

  void clear() {
    if (m_ptr)
      m_alloc->free(m_ptr);
    m_ptr=0;
    m_size=0;
  }

  private:
  Allocator *m_alloc;
  void *m_ptr;
  ham_size_t m_size;
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


#endif /* HAM_UTIL_H__ */
