/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
