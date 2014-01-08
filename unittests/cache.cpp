/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/page.h"
#include "../src/error.h"
#include "../src/env.h"
#include "../src/os.h"
#include "../src/page_manager.h"

namespace hamsterdb {

struct CacheFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  CacheFixture() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATE_KEYS, 0));
  }

  ~CacheFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  Page *alloc_page() {
    return ((LocalEnvironment *)m_env)->get_page_manager()->alloc_page(
                (LocalDatabase *)m_db, 0, 0);
  }
};


} // namespace hamsterdb
