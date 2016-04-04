/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "3btree/btree_node_proxy.h"

#include "fixture.hpp"

namespace upscaledb {

struct BtreeNodeProxyProxy {
  BtreeNodeProxyProxy(BtreeIndex *btree, Page *page)
    : node(btree->get_node_from_page(page)) {
  }

  BtreeNodeProxyProxy &require_insert(Context *context, ups_key_t *key,
                  uint32_t flags = 0) {
    node->insert(context, key, flags);
    return *this;
  }

  BtreeNodeProxyProxy &require_key(Context *context, int slot, ups_key_t *key) {
    ByteArray arena;
    ups_key_t k = {0};
    node->key(context, slot, &arena, &k);
    REQUIRE(k.size == key->size);
    REQUIRE(0 == ::memcmp(k.data, key->data, k.size));
    return *this;
  }

  BtreeNodeProxy *node;
};

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
