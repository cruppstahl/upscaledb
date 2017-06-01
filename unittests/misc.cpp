/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "3btree/btree_node_proxy.h"

#include "fixture.hpp"

namespace upscaledb {

struct MiscFixture : BaseFixture {

  ScopedPtr<Context> context;

  MiscFixture() {
    ups_parameter_t p[] = {
        { UPS_PARAM_PAGESIZE, 4096 },
        { 0, 0 }
    };

    require_create(UPS_IN_MEMORY, p);
    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~MiscFixture() {
    context->changeset.clear();
  }

  void copyKeyInt2PubEmptyTest() {
    PageProxy pp(lenv());
    pp.require_alloc(ldb(), 0, Page::kInitializeWithZeroes);

    ups_key_t key = {0};
    BtreeNodeProxyProxy npp(ldb()->btree_index.get(), pp.page);
    npp.require_insert(context.get(), &key, PBtreeNode::kInsertPrepend)
       .require_key(context.get(), 0, &key);
  }

  void copyKeyInt2PubTinyTest() {
    PageProxy pp(lenv());
    pp.require_alloc(ldb(), 0, Page::kInitializeWithZeroes);

    ups_key_t key = ups_make_key((void *)"a", 1);
    BtreeNodeProxyProxy npp(ldb()->btree_index.get(), pp.page);
    npp.require_insert(context.get(), &key, PBtreeNode::kInsertPrepend)
       .require_key(context.get(), 0, &key);
  }

  void copyKeyInt2PubSmallTest() {
    PageProxy pp(lenv());
    pp.require_alloc(ldb(), 0, Page::kInitializeWithZeroes);

    ups_key_t key = ups_make_key((void *)"01234567", 8);
    BtreeNodeProxyProxy npp(ldb()->btree_index.get(), pp.page);
    npp.require_insert(context.get(), &key, PBtreeNode::kInsertPrepend)
       .require_key(context.get(), 0, &key);
  }

  void copyKeyInt2PubFullTest() {
    PageProxy pp(lenv());
    pp.require_alloc(ldb(), 0, Page::kInitializeWithZeroes);

    ups_key_t key = ups_make_key((void *)"0123456701234567", 16);
    BtreeNodeProxyProxy npp(ldb()->btree_index.get(), pp.page);
    npp.require_insert(context.get(), &key, PBtreeNode::kInsertPrepend)
       .require_key(context.get(), 0, &key);
  }
};

TEST_CASE("MiscFixture/copyKeyInt2PubEmptyTest", "")
{
  MiscFixture mt;
  mt.copyKeyInt2PubEmptyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubTinyTest", "")
{
  MiscFixture mt;
  mt.copyKeyInt2PubTinyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubSmallTest", "")
{
  MiscFixture mt;
  mt.copyKeyInt2PubSmallTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubFullTest", "")
{
  MiscFixture mt;
  mt.copyKeyInt2PubFullTest();
}

} // namespace upscaledb
