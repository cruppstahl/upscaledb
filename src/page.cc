/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "btree_index.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

int Page::sizeof_persistent_header = (OFFSETOF(PPageData, _s._payload));

Page::Page(LocalEnvironment *env, LocalDatabase *db)
  : m_env(env), m_db(db), m_address(0), m_flags(0), m_dirty(false),
    m_cursor_list(0), m_node_proxy(0), m_data(0)
{
  memset(&m_prev[0], 0, sizeof(m_prev));
  memset(&m_next[0], 0, sizeof(m_next));
}

Page::~Page()
{
  if (m_env && m_env->get_device() && m_data != 0)
    m_env->get_device()->free_page(this);

  if (m_node_proxy) {
    delete m_node_proxy;
    m_node_proxy = 0;
  }

  ham_assert(m_data == 0);
  ham_assert(m_cursor_list == 0);
}

void
Page::allocate()
{
  m_env->get_device()->alloc_page(this);
}

void
Page::fetch(ham_u64_t address)
{
  set_address(address);
  m_env->get_device()->read_page(this);
}

void
Page::flush()
{
  if (is_dirty()) {
    m_env->get_device()->write_page(this);
    set_dirty(false);
  }
}

} // namespace hamsterdb
