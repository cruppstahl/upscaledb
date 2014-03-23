/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "config.h"

#include <string.h>

#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "btree_index.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

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
Page::allocate(ham_u32_t type, ham_u32_t flags)
{
  m_env->get_device()->alloc_page(this, m_env->get_page_size());
  if (flags & kInitializeWithZeroes)
    memset(get_raw_payload(), 0, m_env->get_page_size());
  if (type)
    set_type(type);
}

void
Page::fetch(ham_u64_t address)
{
  set_address(address);
  m_env->get_device()->read_page(this, m_env->get_page_size());
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
