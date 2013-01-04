/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "device.h"
#include "error.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "env.h"

using namespace hamsterdb;


ham_status_t
DiskDevice::alloc_page(Page *page)
{
  ham_u64_t pos;
  ham_size_t size = m_pagesize;

  ham_status_t st = os_get_filesize(m_fd, &pos);
  if (st)
    return (st);

  st = os_truncate(m_fd, pos + size);
  if (st)
    return (st);

  page->set_self(pos);
  return (read_page(page));
}

ham_status_t
DiskDevice::read_page(Page *page)
{
  /*
   * if this page is in the mapped area: return a pointer into that area.
   * otherwise fall back to read/write.
   */
  if (page->get_self() < m_open_filesize && m_mmapptr != 0) {
    /* ok, this page is mapped. If the Page object has a memory buffer:
     * free it */
    if (page->get_pers() != 0)
      get_env()->get_allocator()->free(page->get_pers());

    page->set_flags(page->get_flags() & ~Page::NPERS_MALLOC);
    page->set_pers((PageData *)&m_mmapptr[page->get_self()]);
    return (0);
  }

  /* this page is not in the mapped area; allocate a buffer */
  if (page->get_pers() == 0) {
    ham_u8_t *p = (ham_u8_t *)get_env()->get_allocator()->alloc(m_pagesize);
    if (!p)
      return (HAM_OUT_OF_MEMORY);
    page->set_pers((PageData *)p);
    page->set_flags(page->get_flags() | Page::NPERS_MALLOC);
  }

  return (os_pread(m_fd, page->get_self(), page->get_pers(), m_pagesize));
}

