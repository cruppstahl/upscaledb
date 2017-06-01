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

#ifndef FIXTURE_HPP
#define FIXTURE_HPP

#include "3rdparty/catch/catch.hpp"

#include "3btree/btree_node_proxy.h"
#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

using namespace upscaledb;

#define REQUIRE_CATCH(x, y) \
        try { x; } catch (Exception &ex) { REQUIRE(ex.code == y); }

struct BaseFixture {
  ~BaseFixture() {
    close();
  }

  BaseFixture &close(uint32_t flags = UPS_AUTO_CLEANUP) {
    if (env) {
      REQUIRE(0 == ups_env_close(env, flags));
      env = 0;
    }
    return *this;
  }

  ups_status_t create_env(uint32_t env_flags, ups_parameter_t *params = 0) {
    return ups_env_create(&env, "test.db", env_flags, 0644, params);
  }

  ups_status_t open_env(uint32_t env_flags, ups_parameter_t *params = 0) {
    return ups_env_open(&env, "test.db", env_flags, params);
  }

  BaseFixture &require_create(uint32_t env_flags, ups_status_t status = 0) {
    return require_create(env_flags, 0, status);
  }

  BaseFixture &require_create(uint32_t env_flags, ups_parameter_t *params,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE(status == ups_env_create(&env, "test.db",
                              env_flags, 0644, params));
    }
    else {
      REQUIRE(0 == ups_env_create(&env, "test.db", env_flags, 0644, params));
      REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    }
    return *this;
  }

  BaseFixture &require_create(uint32_t env_flags, ups_parameter_t *env_params,
                  uint32_t db_flags, ups_parameter_t *db_params,
                  ups_status_t status = 0) {
    REQUIRE(0 == ups_env_create(&env, "test.db", env_flags, 0644, env_params));
    if (status) {
      REQUIRE(status == ups_env_create_db(env, &db, 1, db_flags, db_params));
      close();
    }
    else {
      REQUIRE(0 == ups_env_create_db(env, &db, 1, db_flags, db_params));
    }
    return *this;
  }

  BaseFixture &require_open(uint32_t env_flags = 0) {
    return require_open(env_flags, 0, 0);
  }

  BaseFixture &require_open(uint32_t env_flags, ups_parameter_t *params,
                  ups_status_t status = 0) {
    if (status) {
      REQUIRE(status == ups_env_open(&env, "test.db", env_flags, params));
    }
    else {
      REQUIRE(0 == ups_env_open(&env, "test.db", env_flags, params));
      REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    }
    return *this;
  }

  BaseFixture &require_parameter(uint32_t name, uint64_t value) {
    ups_parameter_t params[] = {
        { name, 0 },
        { 0, 0 }
    };
    REQUIRE(0 == ups_env_get_parameters(env, params));
    REQUIRE(value == params[0].value);
    return *this;
  }

  BaseFixture &require_filename(const char *value) {
    ups_parameter_t params[] = {
        { UPS_PARAM_FILENAME, 0 },
        { 0, 0 }
    };
    REQUIRE(0 == ups_env_get_parameters(env, params));
    REQUIRE(0 == ::strcmp(value, (const char *)params[0].value));
    return *this;
  }

  BaseFixture &require_flags(uint32_t flags, bool enabled = true) {
    if (enabled) {
      REQUIRE((lenv()->config.flags & flags) != 0);
    }
    else {
      REQUIRE((lenv()->config.flags & flags) == 0);
    }
    return *this;
  }

  LocalEnv *lenv() const {
    return (LocalEnv *)env;
  }

  LocalDb *ldb() const {
    return (LocalDb *)db;
  }

  LocalDb *ldb(ups_db_t *db_) const {
    return (LocalDb *)db_;
  }

  BtreeIndex *btree_index() {
    return ldb()->btree_index.get();
  }

  Device *device() const {
    return lenv()->device.get();
  }

  PageManager *page_manager() {
    return lenv()->page_manager.get();
  }

  bool is_in_memory() const {
    return ISSET(lenv()->config.flags, UPS_IN_MEMORY);
  }

  bool uses_transactions() const {
    return ISSET(lenv()->config.flags, UPS_ENABLE_TRANSACTIONS);
  }

  ups_db_t *db;
  ups_env_t *env;
};

struct PageProxy {
  PageProxy()
    : page(nullptr) {
  }

  PageProxy(LocalEnv *env)
    : page(new Page(env->device.get())) {
  }

  PageProxy(LocalEnv *env, LocalDb *db)
    : page(new Page(env->device.get(), db)) {
  }

  PageProxy(Device *device)
    : page(new Page(device)) {
  }

  ~PageProxy() {
    close();
  }

  PageProxy &allocate(LocalEnv *env) {
    page = new Page(env->device.get());
    return *this;
  }

  PageProxy &require_alloc(LocalDb *db, uint32_t type, uint32_t flags) {
    page->set_db(db);
    page->alloc(type, flags);
    return *this;
  }

  PageProxy &require_alloc(uint32_t type, uint32_t flags) {
    page->alloc(type, flags);
    return *this;
  }

  PageProxy &require_address(uint64_t address) {
    REQUIRE(page->address() == address);
    return *this;
  }

  PageProxy &require_flush() {
    page->flush();
    return *this;
  }

  PageProxy &require_fetch(uint64_t address) {
    page->fetch(address);
    return *this;
  }

  PageProxy &require_data(void *data, size_t size) {
    REQUIRE(0 == ::memcmp(data, page->data(), size));
    return *this;
  }

  PageProxy &require_payload(void *data, size_t size) {
    REQUIRE(0 == ::memcmp(data, page->payload(), size));
    return *this;
  }

  PageProxy &set_address(uint64_t address) {
    page->set_address(address);
    return *this;
  }

  PageProxy &set_dirty(bool dirty = true) {
    page->set_dirty(dirty);
    return *this;
  }

  PageProxy &require_dirty(bool dirty = true) {
    REQUIRE(page->is_dirty() == dirty);
    return *this;
  }

  PageProxy &require_allocated(bool allocated = true) {
    REQUIRE(page->is_allocated() == allocated);
    return *this;
  }

  PageProxy &close() {
    delete page;
    page = 0;
    return *this;
  }

  Page *page;
};

struct DeviceProxy {
  DeviceProxy(LocalEnv *env)
    : device(env->device.get()) {
  }

  DeviceProxy &create() {
    device->create();
    return *this;
  }

  DeviceProxy &open() {
    device->open();
    return *this;
  }

  DeviceProxy &require_open(bool open = true) {
    REQUIRE(device->is_open() == open);
    return *this;
  }

  DeviceProxy &alloc_page(PageProxy &pp) {
    device->alloc_page(pp.page);
    return *this;
  }

  DeviceProxy &free_page(PageProxy &pp) {
    device->free_page(pp.page);
    return *this;
  }

  DeviceProxy &require_flush() {
    device->flush();
    return *this;
  }

  DeviceProxy &require_truncate(uint64_t size) {
    device->truncate(size);
    return *this;
  }

  DeviceProxy &require_read_page(PageProxy &pp, uint64_t address) {
    device->read_page(pp.page, address);
    return *this;
  }

  DeviceProxy &require_read(uint64_t address, void *buffer, size_t len) {
    device->read(address, buffer, len);
    return *this;
  }

  DeviceProxy &require_write(uint64_t address, void *buffer, size_t len) {
    device->write(address, buffer, len);
    return *this;
  }

  DeviceProxy &close() {
    device->close();
    return *this;
  }

  Device *device;
};

struct DbProxy {
  DbProxy(ups_db_t *db_)
    : db(db_) {
  }

  DbProxy &require_insert(ups_txn_t *txn, uint32_t key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(txn, &key, sizeof(key),
                    record.data(), record.size(), 0, status);
  }

  DbProxy &require_insert(uint32_t key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, &key, sizeof(key),
                    record.data(), record.size(), 0, status);
  }

  DbProxy &require_insert(uint32_t key, const char *record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, &key, sizeof(key), (void *)record,
                    record ? ::strlen(record) + 1 : 0, 0, status);
  }

  DbProxy &require_insert(ups_key_t *key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, key->data, key->size,
                    record.data(), record.size(), 0, status);
  }

  DbProxy &require_insert(ups_txn_t *txn, ups_key_t *key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(txn, key->data, key->size,
                    record.data(), record.size(), 0, status);
  }


  DbProxy &require_insert(std::vector<uint8_t> &key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(nullptr, key.data(), (uint16_t)key.size(),
                    record.data(), record.size(), 0, status);
  }

  DbProxy &require_insert(const char *key, const char *record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, (void *)key, (uint16_t)::strlen(key) + 1,
                    (void *)record, record ? (uint32_t)::strlen(record) + 1 : 0,
                    0, status);
  }

  DbProxy &require_insert(const char *key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, (void *)key, (uint16_t)::strlen(key) + 1,
                    record.data(), record.size(), 0, status);
  }

  DbProxy &require_insert_duplicate(ups_txn_t *txn, uint32_t key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(txn, &key, sizeof(key),
                    record.data(), record.size(), UPS_DUPLICATE, status);
  }

  DbProxy &require_insert_duplicate(ups_txn_t *txn, std::vector<uint8_t> &key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(txn, key.data(), (uint16_t)key.size(),
                    record.data(), record.size(), UPS_DUPLICATE, status);
  }

  DbProxy &require_insert_duplicate(ups_txn_t *txn, std::vector<uint8_t> &key,
                  uint32_t record, ups_status_t status = 0) {
    return require_insert_impl(txn, key.data(), (uint16_t)key.size(),
                    &record, sizeof(record), UPS_DUPLICATE, status);
  }

  DbProxy &require_overwrite(ups_key_t *key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(nullptr, key->data, key->size,
                    record.data(), record.size(), UPS_OVERWRITE, status);
  }

  DbProxy &require_overwrite(std::vector<uint8_t> &key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    return require_insert_impl(nullptr, key.data(), (uint16_t)key.size(),
                    record.data(), record.size(), UPS_OVERWRITE, status);
  }

  DbProxy &require_overwrite(const char *key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_insert_impl(nullptr, (void *)key, (uint16_t)::strlen(key) + 1,
                    record.data(), record.size(), UPS_OVERWRITE, status);
  }

  DbProxy &require_insert_impl(ups_txn_t *txn, void *key, uint16_t key_size,
                  void *record, uint32_t record_size,
                  uint32_t flags, ups_status_t status = 0) {
    ups_key_t k = ups_make_key(key, key_size);
    ups_record_t r = ups_make_record(record, record_size);
    if (status) {
      REQUIRE(status == ups_db_insert(db, txn, &k, &r, flags));
    }
    else {
      REQUIRE(0 == ups_db_insert(db, txn, &k, &r, flags));
    }
    return *this;
  }

  DbProxy &require_find_useralloc(std::vector<uint8_t> &key,
                  std::vector<uint8_t> &record, ups_status_t status = 0) {
    std::vector<uint8_t> tmp(record.size());
    ups_key_t k = ups_make_key(key.data(), (uint16_t)key.size());
    ups_record_t r = ups_make_record(tmp.data(), (uint32_t)tmp.size());
    r.flags = UPS_RECORD_USER_ALLOC;

    if (status) {
      REQUIRE(status == ups_db_find(db, 0, &k, &r, 0));
    }
    else {
      REQUIRE(0 == ups_db_find(db, 0, &k, &r, 0));
      if (record.empty()) {
        REQUIRE(r.size == 0);
        REQUIRE(r.data == 0);
      }
      else {
        REQUIRE(r.size == record.size());
        REQUIRE(r.data == tmp.data());
        REQUIRE(0 == ::memcmp(r.data, record.data(), record.size()));
      }
    }
    return *this;
  }

  DbProxy &require_find_approx(std::vector<uint8_t> &key,
                  std::vector<uint8_t> &expected_key,
                  std::vector<uint8_t> &record, uint32_t flags,
                  ups_status_t status = 0) {
    ups_key_t k = ups_make_key(key.data(), (uint16_t)key.size());
    ups_record_t r = {0};
    if (status) {
      REQUIRE(status == ups_db_find(db, 0, &k, &r, flags));
    }
    else {
      REQUIRE(0 == ups_db_find(db, 0, &k, &r, flags));
      if (record.empty()) {
        REQUIRE(r.size == 0);
        REQUIRE(r.data == 0);
      }
      else {
        REQUIRE(r.size == record.size());
        REQUIRE(0 == ::memcmp(r.data, record.data(), record.size()));
      }
      REQUIRE(k.size == expected_key.size());
      REQUIRE(0 == ::memcmp(expected_key.data(), k.data, k.size));
    }
    return *this;
  }

  DbProxy &require_find(std::vector<uint8_t> &key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_find_impl(key.data(), (uint16_t)key.size(), record.data(),
                    record.size(), 0, status);
  }

  DbProxy &require_find(uint32_t key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_find_impl(&key, sizeof(key), record.data(),
                    record.size(), 0, status);
  }

  DbProxy &require_find(const char *key, const char *record,
                  ups_status_t status = 0) {
    return require_find_impl((void *)key,
                    (uint16_t)(key ? ::strlen(key) + 1 : 0), (void *)record,
                    record ? ::strlen(record) + 1 : 0, 0, status);
  }

  DbProxy &require_find(ups_key_t *key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_find_impl(key->data, key->size, record.data(),
                    record.size(), 0, status);
  }

  DbProxy &require_find(const char *key, std::vector<uint8_t> &record,
                  ups_status_t status = 0) {
    return require_find_impl((void *)key,
                    (uint16_t)(key ? ::strlen(key) + 1 : 0), record.data(),
                    record.size(), 0, status);
  }

  DbProxy &require_find_impl(void *key, uint16_t key_size, void *record,
                  uint32_t record_size, uint32_t flags,
                  ups_status_t status = 0) {
    ups_key_t k = ups_make_key(key, key_size);
    ups_record_t r = {0};
    if (status) {
      REQUIRE(status == ups_db_find(db, 0, &k, &r, flags));
    }
    else {
      REQUIRE(0 == ups_db_find(db, 0, &k, &r, flags));
      if (record == nullptr) {
        REQUIRE(r.size == 0);
        REQUIRE(r.data == 0);
      }
      else {
        REQUIRE(r.size == record_size);
        REQUIRE(0 == ::memcmp(r.data, record, record_size));
      }
    }
    return *this;
  }

  DbProxy &require_erase(uint32_t key, ups_status_t status = 0) {
    ups_key_t k = ups_make_key(&key, (uint16_t)sizeof(key));
    REQUIRE(status == ups_db_erase(db, 0, &k, 0));
    return *this;
  }

  DbProxy &require_erase(ups_txn_t *txn, uint32_t key,
                  ups_status_t status = 0) {
    ups_key_t k = ups_make_key(&key, (uint16_t)sizeof(key));
    REQUIRE(status == ups_db_erase(db, txn, &k, 0));
    return *this;
  }

  DbProxy &require_check_integrity() {
    REQUIRE(0 == ups_db_check_integrity(db, 0));
    return *this;
  }

  DbProxy &require_parameter(uint32_t name, uint64_t value) {
    ups_parameter_t params[] = {
        { name, 0 },
        { 0, 0 }
    };
    REQUIRE(0 == ups_db_get_parameters(db, params));
    REQUIRE(value == params[0].value);
    return *this;
  }

  DbProxy &require_parameters(ups_parameter_t *params) {
    REQUIRE(0 == ups_db_get_parameters(db, params));
    return *this;
  }

  DbProxy &require_key_count(uint64_t count) {
    uint64_t keycount;
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(keycount == count);
    return *this;
  }

  LocalDb *ldb() const {
    return (LocalDb *)db;
  }

  BtreeIndex *btree_index() {
    return ldb()->btree_index.get();
  }

  ups_db_t *db;
};

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

struct TxnProxy {
  TxnProxy(ups_env_t *env, const char *name = nullptr,
                  bool commit_on_exit = false)
    : _commit_on_exit(commit_on_exit) {
    REQUIRE(0 == ups_txn_begin(&txn, env, name, 0, 0));
    REQUIRE(txn != nullptr);
  }

  ~TxnProxy() {
    if (_commit_on_exit)
      commit();
    else
      abort();
  }

  uint64_t id() {
    return ((Txn *)txn)->id;
  }

  TxnProxy &abort() {
    if (txn) {
      REQUIRE(0 == ups_txn_abort(txn, 0));
      txn = nullptr;
    }
    return *this;
  }

  TxnProxy &commit() {
    if (txn) {
      REQUIRE(0 == ups_txn_commit(txn, 0));
      txn = nullptr;
    }
    return *this;
  }

  TxnProxy &require_next(ups_txn_t *next) {
    REQUIRE(((Txn *)txn)->next() == (Txn *)next);
    return *this;
  }

  LocalTxn *ltxn() const {
    return (LocalTxn *)txn;
  }

  bool _commit_on_exit;
  ups_txn_t *txn;
};

#endif // FIXTURE_HPP
