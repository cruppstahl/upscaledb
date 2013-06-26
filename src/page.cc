/*
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

#include "cache.h"
#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "btree.h"

namespace hamsterdb {

int Page::sizeof_persistent_header = (OFFSETOF(PageData, _s._payload));

Page::Page(Environment *env, LocalDatabase *db)
  : m_self(0), m_db(db), m_device(0), m_flags(0), m_dirty(false),
    m_cursors(0), m_pers(0)
{
  if (env)
    m_device = env->get_device();
  memset(&m_prev[0], 0, sizeof(m_prev));
  memset(&m_next[0], 0, sizeof(m_next));
}

Page::~Page()
{
  ham_assert(get_pers() == 0);
  ham_assert(get_cursors() == 0);
}

ham_status_t
Page::allocate()
{
  return (get_device()->alloc_page(this));
}

ham_status_t
Page::fetch(ham_u64_t address)
{
  set_self(address);
  return (get_device()->read_page(this));
}

ham_status_t
Page::flush()
{
  if (!is_dirty())
    return (HAM_SUCCESS);

  ham_status_t st = get_device()->write_page(this);
  if (st)
    return (st);

  set_dirty(false);
  return (HAM_SUCCESS);
}

void
Page::free()
{
  ham_assert(get_cursors() == 0);
  get_device()->free_page(this);
}

} // namespace hamsterdb
