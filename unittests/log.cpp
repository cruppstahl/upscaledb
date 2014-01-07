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

#include <vector>
#include <sstream>

#include "../src/txn.h"
#include "../src/log.h"
#include "../src/os.h"
#include "../src/db.h"
#include "../src/page_manager.h"
#include "../src/device.h"
#include "../src/env.h"
#include "../src/btree_index.h"

using namespace hamsterdb;

/* this function pointer is defined in changeset.cc */
typedef void (*hook_func_t)(void);
namespace hamsterdb {
  extern hook_func_t g_CHANGESET_POST_LOG_HOOK;
}

namespace hamsterdb {

struct LogFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalEnvironment *m_lenv;

  LogFixture() {
    (void)os::unlink(Globals::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
  }

  ~LogFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  Log *disconnect_log_and_create_new_log() {
    Log *log = new Log(m_lenv);
    REQUIRE_CATCH(log->create(), HAM_WOULD_BLOCK);
    delete log;

    log = m_lenv->get_log();
    log->close();
    log->create();
    REQUIRE(log);
    return (log);
  }

  void createCloseTest() {
    Log *log = disconnect_log_and_create_new_log();

    /* TODO make sure that the file exists and
     * contains only the header */

    REQUIRE(true == log->is_empty());

    log->close();
  }

  void createCloseOpenCloseTest() {
    Log *log = disconnect_log_and_create_new_log();
    REQUIRE(true == log->is_empty());
    log->close();

    log->open();
    REQUIRE(true == log->is_empty());
    log->close();
  }

  void negativeCreateTest() {
    Log *log = new Log(m_lenv);
    std::string oldfilename = m_lenv->get_filename();
    m_lenv->test_set_filename("/::asdf");
    REQUIRE_CATCH(log->create(), HAM_IO_ERROR);
    m_lenv->test_set_filename(oldfilename);
    delete log;
  }

  void negativeOpenTest() {
    Log *log = new Log(m_lenv);
    ham_fd_t fd;
    std::string oldfilename = m_lenv->get_filename();
    m_lenv->test_set_filename("xxx$$test");
    REQUIRE_CATCH(log->create(), HAM_FILE_NOT_FOUND);

    /* if log->open() fails, it will call log->close() internally and
     * log->close() overwrites the header structure. therefore we have
     * to patch the file before we start the test. */
    fd = os_open("data/log-broken-magic.log0", 0);
    os_pwrite(fd, 0, (void *)"x", 1);
    os_close(fd);

    m_lenv->test_set_filename("data/log-broken-magic");
    REQUIRE_CATCH(log->open(), HAM_LOG_INV_FILE_HEADER);

    m_lenv->test_set_filename(oldfilename);
    delete log;
  }

  void appendWriteTest() {
    Log *log = disconnect_log_and_create_new_log();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u8_t data[100];
    for (int i = 0; i < 100; i++)
      data[i] = (ham_u8_t)i;

    log->append_write(1, 0, 0, data, sizeof(data));

    REQUIRE(0 == ham_txn_abort(txn, 0));
    log->close();
  }

  void clearTest() {
    ham_u8_t data[1024] = {0};
    Log *log = disconnect_log_and_create_new_log();
    REQUIRE(true == log->is_empty());

    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_lenv, 0, 0, 0));
    log->append_write(1, 0, 0, data, sizeof(data));
    REQUIRE(false == log->is_empty());

    log->clear();
    REQUIRE(true == log->is_empty());

    REQUIRE(0 == ham_txn_abort(txn, 0));
    log->close();
  }

  void iterateOverEmptyLogTest() {
    Log *log = disconnect_log_and_create_new_log();

    Log::Iterator iter = 0;

    Log::PEntry entry;
    ByteArray buffer;
    log->get_entry(&iter, &entry, &buffer);
    REQUIRE((ham_u64_t)0 == entry.lsn);
    REQUIRE(0 == buffer.get_size());

    log->close();
  }

  void iterateOverLogOneEntryTest() {
    ham_txn_t *txn;
    Log *log = disconnect_log_and_create_new_log();
    REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_lenv, 0, 0, 0));
    ham_u8_t buffer[1024] = {0};
    log->append_write(1, 0, 0, buffer, sizeof(buffer));
    log->close(true);
    log->open();

    Log::Iterator iter = 0;

    Log::PEntry entry;
    ByteArray data;
    log->get_entry(&iter, &entry, &data);
    REQUIRE((ham_u64_t)1 == entry.lsn);
    REQUIRE((ham_u64_t)1 == ((Transaction *)txn)->get_id());
    REQUIRE((ham_u32_t)1024 == entry.data_size);
    REQUIRE(data.get_size() > 0);
    REQUIRE((ham_u32_t)0 == entry.flags);

    REQUIRE((ham_u64_t)1 == log->get_lsn());

    REQUIRE(0 == ham_txn_abort(txn, 0));
    log->close();
  }

  void checkLogEntry(Log *log, Log::PEntry *entry, ham_u64_t lsn, void *data) {
    REQUIRE(lsn == entry->lsn);
    if (entry->data_size == 0) {
      REQUIRE(!data);
    }
    else {
      REQUIRE(data);
    }
  }

  void iterateOverLogMultipleEntryTest() {
    Log *log = m_lenv->get_log();

    for (int i = 0; i < 5; i++) {
      Page *page;
      page = new Page(m_lenv);
      page->allocate();
      log->append_page(page, 1 + i, 5 - i);
      delete page;
    }

    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    REQUIRE((Log *)0 == m_lenv->get_log());
    log = new Log(m_lenv);
    log->open();
    m_lenv->test_set_log(log);
    REQUIRE(log != 0);

    Log::Iterator iter = 0;

    Log::PEntry entry;
    ByteArray buffer;

    log->get_entry(&iter, &entry, &buffer);
    checkLogEntry(log, &entry, 5, buffer.get_ptr());
    REQUIRE(m_lenv->get_page_size() == (ham_u32_t)entry.data_size);
    log->get_entry(&iter, &entry, &buffer);
    checkLogEntry(log, &entry, 4, buffer.get_ptr());
    REQUIRE(m_lenv->get_page_size() == (ham_u32_t)entry.data_size);
    log->get_entry(&iter, &entry, &buffer);
    checkLogEntry(log, &entry, 3, buffer.get_ptr());
    REQUIRE(m_lenv->get_page_size() == (ham_u32_t)entry.data_size);
    log->get_entry(&iter, &entry, &buffer);
    checkLogEntry(log, &entry, 2, buffer.get_ptr());
    REQUIRE(m_lenv->get_page_size() == (ham_u32_t)entry.data_size);
    log->get_entry(&iter, &entry, &buffer);
    checkLogEntry(log, &entry, 1, buffer.get_ptr());
    REQUIRE(m_lenv->get_page_size() == (ham_u32_t)entry.data_size);
  }
};

TEST_CASE("Log/createCloseTest", "")
{
  LogFixture f;
  f.createCloseTest();
}

TEST_CASE("Log/createCloseOpenCloseTest", "")
{
  LogFixture f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("Log/negativeCreateTest", "")
{
  LogFixture f;
  f.negativeCreateTest();
}

TEST_CASE("Log/negativeOpenTest", "")
{
  LogFixture f;
  f.negativeOpenTest();
}

TEST_CASE("Log/appendWriteTest", "")
{
  LogFixture f;
  f.appendWriteTest();
}

TEST_CASE("Log/clearTest", "")
{
  LogFixture f;
  f.clearTest();
}

TEST_CASE("Log/iterateOverEmptyLogTest", "")
{
  LogFixture f;
  f.iterateOverEmptyLogTest();
}

TEST_CASE("Log/iterateOverLogOneEntryTest", "")
{
  LogFixture f;
  f.iterateOverLogOneEntryTest();
}

TEST_CASE("Log/iterateOverLogMultipleEntryTest", "")
{
  LogFixture f;
  f.iterateOverLogMultipleEntryTest();
}


struct LogEntry {
  LogEntry(ham_u64_t _lsn, ham_u64_t _offset, ham_u64_t _data_size)
    : lsn(_lsn), offset(_offset), data_size(_data_size) {
  }

  ham_u64_t lsn;
  ham_u64_t offset;
  ham_u64_t data_size;
};

struct LogHighLevelFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalEnvironment *m_lenv;

  LogHighLevelFixture() {
    (void)os::unlink(Globals::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS
            | HAM_ENABLE_RECOVERY, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATE_KEYS, 0));

    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
  }

  ~LogHighLevelFixture() {
    teardown();
  }

  void open() {
    // open without recovery and transactions (they imply recovery)!
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  Page *fetch_page(LocalDatabase *db, ham_u64_t address) {
    PageManager *pm = m_lenv->get_page_manager();
    return (pm->fetch_page(db, address));
  }

  void createCloseTest() {
    REQUIRE(m_lenv->get_log());
  }

  void createCloseEnvTest() {
    teardown();

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0664, 0));
    m_lenv = (LocalEnvironment *)m_env;
    REQUIRE(m_lenv->get_log() != 0);
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 333, 0, 0));
    REQUIRE(m_lenv->get_log() != 0);
    REQUIRE(0 == ham_db_close(m_db, 0));
  }

  void createCloseOpenCloseTest() {
    teardown();
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_lenv = (LocalEnvironment *)m_env;
    REQUIRE(m_lenv->get_log() != 0);
  }

  void createCloseOpenFullLogRecoverTest() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    ham_u8_t *buffer = (ham_u8_t *)malloc(m_lenv->get_page_size());
    memset(buffer, 0, m_lenv->get_page_size());
    ham_u32_t ps = m_lenv->get_page_size();

    m_lenv->get_log()->append_write(2, 0, ps, buffer, ps);
    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 ==
        ham_env_close(m_env,
            HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(HAM_NEED_RECOVERY ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);

    /* make sure that the log file was deleted and that the lsn is 1 */
    Log *log = m_lenv->get_log();
    REQUIRE(log != 0);
    ham_u64_t filesize = os_get_filesize(log->test_get_fd());
    REQUIRE((ham_u64_t)sizeof(Log::PEnvironmentHeader) == filesize);

    free(buffer);
  }

  void createCloseOpenFullLogTest() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_lenv, 0, 0, 0));
    ham_u8_t *buffer = (ham_u8_t *)malloc(m_lenv->get_page_size());
    memset(buffer, 0, m_lenv->get_page_size());

    m_lenv->get_log()->append_write(1, 0, 0, buffer, m_lenv->get_page_size());
    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 == ham_env_close(m_env,
            HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(HAM_NEED_RECOVERY ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0));

    free(buffer);
  }

  void createCloseOpenCloseEnvTest() {
    teardown();

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(((LocalEnvironment *)m_env)->get_log() != 0);
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 333, 0, 0));
    REQUIRE(((LocalEnvironment *)m_env)->get_log() != 0);
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(((LocalEnvironment *)m_env)->get_log() != 0);

    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0));
    REQUIRE(((LocalEnvironment *)m_env)->get_log() != 0);
  }

  void createCloseOpenFullLogEnvTest() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_lenv, 0, 0, 0));
    ham_u8_t *buffer = (ham_u8_t *)malloc(m_lenv->get_page_size());
    memset(buffer, 0, m_lenv->get_page_size());

    m_lenv->get_log()->append_write(1, 0, 0, buffer, m_lenv->get_page_size());
    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 == ham_env_close(m_env,
            HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(HAM_NEED_RECOVERY ==
        ham_env_open(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY, 0));

    free(buffer);
  }

  void createCloseOpenFullLogEnvRecoverTest() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, (ham_env_t *)m_lenv, 0, 0, 0));
    ham_u8_t *buffer = (ham_u8_t *)malloc(m_lenv->get_page_size());
    memset(buffer, 0, m_lenv->get_page_size());

    m_lenv->get_log()->append_write(1, 0, 0, buffer, m_lenv->get_page_size());
    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 == ham_env_close(m_env,
            HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));

    /* make sure that the log files are deleted and that the lsn is 1 */
    Log *log = ((LocalEnvironment *)m_env)->get_log();
    ham_u64_t filesize = os_get_filesize(log->test_get_fd());
    REQUIRE((ham_u64_t)sizeof(Log::PEnvironmentHeader) == filesize);

    free(buffer);
  }

  static void copyLog() {
    assert(true == os::copy(Globals::opath(".test.log0"),
          Globals::opath(".test2.log0")));
  }

  static void restoreLog() {
    assert(true == os::copy(Globals::opath(".test2.log0"),
          Globals::opath(".test.log0")));
  }

  void compareLog(const char *filename, LogEntry e) {
    std::vector<LogEntry> v;
    v.push_back(e);
    compareLog(filename, v);
  }

  void compareLog(const char *filename, std::vector<LogEntry> &vec) {
    Log::PEntry entry;
    Log::Iterator iter = 0;
    ByteArray data;
    size_t size = 0;
    Log *log;
    std::vector<LogEntry>::iterator vit = vec.begin();

    ham_env_t *env;
    /* for traversing the logfile we need a temp. Env handle */
    REQUIRE(0 == ham_env_create(&env, filename, 0, 0664, 0));
    log = ((LocalEnvironment *)env)->get_log();
    REQUIRE((Log *)0 == log);
    log = new Log((LocalEnvironment *)env);
    log->open();

    while (1) {
      log->get_entry(&iter, &entry, &data);
      if (entry.lsn == 0)
        break;

      if (vit == vec.end()) {
        REQUIRE(0ull == entry.lsn);
        break;
      }
      size++;

      REQUIRE((*vit).lsn == entry.lsn);
      REQUIRE((*vit).offset == entry.offset);
      REQUIRE((*vit).data_size == entry.data_size);

      vit++;
    }

    REQUIRE(vec.size() == size);

    delete log;
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void recoverAllocatePageTest() {
#ifndef WIN32
    LocalDatabase *db = (LocalDatabase *)m_db;
    g_CHANGESET_POST_LOG_HOOK = (hook_func_t)copyLog;
    ham_u32_t ps = m_lenv->get_page_size();
    Page *page;

    REQUIRE((page = m_lenv->get_page_manager()->alloc_page(db,
                        0, PageManager::kIgnoreFreelist)));
    page->set_dirty(true);
    REQUIRE((ham_u64_t)(ps * 2) == page->get_address());
    for (int i = 0; i < 200; i++)
      page->get_payload()[i] = (ham_u8_t)i;
    m_lenv->get_changeset().flush(1);
    m_lenv->get_changeset().clear();
    teardown();

    /* restore the backupped logfiles */
    restoreLog();

    /* now truncate the file - after all we want to make sure that
     * the log appends the new page */
    ham_fd_t fd;
    fd = os_open(Globals::opath(".test"), 0);
    os_truncate(fd, ps * 2);
    os_close(fd);

    /* make sure that the log has one alloc-page entry */
    compareLog(Globals::opath(".test2"), LogEntry(1, ps * 2, ps));

    /* recover and make sure that the page exists */
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    db = (LocalDatabase *)m_db;
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
    REQUIRE((page = fetch_page(db, ps * 2)));
    /* verify that the page contains the marker */
    for (int i = 0; i < 200; i++)
      REQUIRE((ham_u8_t)i == page->get_payload()[i]);

    /* verify the lsn */
    REQUIRE(1ull == m_lenv->get_log()->get_lsn());

    m_lenv->get_changeset().clear();
#endif
  }

  void recoverAllocateMultiplePageTest() {
#ifndef WIN32
    g_CHANGESET_POST_LOG_HOOK = (hook_func_t)copyLog;
    ham_u32_t ps = m_lenv->get_page_size();
    Page *page[10];
    LocalDatabase *db = (LocalDatabase *)m_db;

    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = m_lenv->get_page_manager()->alloc_page(db,
                            0, PageManager::kIgnoreFreelist)));
      page[i]->set_dirty(true);
      REQUIRE(page[i]->get_address() == ps * (2 + i));
      for (int j = 0; j < 200; j++)
        page[i]->get_payload()[j] = (ham_u8_t)(i+j);
    }
    m_lenv->get_changeset().flush(33);
    m_lenv->get_changeset().clear();
    teardown();

    /* restore the backupped logfiles */
    restoreLog();

    /* now truncate the file - after all we want to make sure that
     * the log appends the new page */
    ham_fd_t fd = os_open(Globals::opath(".test"), 0);
    os_truncate(fd, ps * 2);
    os_close(fd);

    /* make sure that the log has one alloc-page entry */
    std::vector<LogEntry> vec;
    for (int i = 0; i < 10; i++)
      vec.push_back(LogEntry(33, ps * (2 + i), ps));
    compareLog(Globals::opath(".test2"), vec);

    /* recover and make sure that the pages exists */
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    db = (LocalDatabase *)m_db;
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = fetch_page(db, ps * (2 + i))));
      /* verify that the pages contain the markers */
      for (int j = 0; j < 200; j++)
        REQUIRE((ham_u8_t)(i + j) == page[i]->get_payload()[j]);
    }

    /* verify the lsn */
    REQUIRE(33ull == m_lenv->get_log()->get_lsn());

    m_lenv->get_changeset().clear();
#endif
  }

  void recoverModifiedPageTest() {
#ifndef WIN32
    g_CHANGESET_POST_LOG_HOOK = (hook_func_t)copyLog;
    ham_u32_t ps = m_lenv->get_page_size();
    Page *page;
    LocalDatabase *db = (LocalDatabase *)m_db;

    REQUIRE((page = m_lenv->get_page_manager()->alloc_page(db,
                        0, PageManager::kIgnoreFreelist)));
    page->set_dirty(true);
    REQUIRE(page->get_address() == ps * 2);
    for (int i = 0; i < 200; i++)
      page->get_payload()[i] = (ham_u8_t)i;
    m_lenv->get_changeset().flush(2);
    m_lenv->get_changeset().clear();
    teardown();

    /* restore the backupped logfiles */
    restoreLog();

    /* now modify the file - after all we want to make sure that
     * the recovery overwrites the modification */
    ham_fd_t fd = os_open(Globals::opath(".test"), 0);
    os_pwrite(fd, ps * 2, "XXXXXXXXXXXXXXXXXXXX", 20);
    os_close(fd);

    /* make sure that the log has one alloc-page entry */
    compareLog(Globals::opath(".test2"), LogEntry(2, ps * 2, ps));

    /* recover and make sure that the page is ok */
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    db = (LocalDatabase *)m_db;
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
    REQUIRE((page = fetch_page(db, ps * 2)));
    /* verify that the page does not contain the "XXX..." */
    for (int i = 0; i < 20; i++)
      REQUIRE('X' != page->get_raw_payload()[i]);

    /* verify the lsn */
    REQUIRE(2ull == m_lenv->get_log()->get_lsn());

    m_lenv->get_changeset().clear();
#endif
  }

  void recoverModifiedMultiplePageTest() {
#ifndef WIN32
    g_CHANGESET_POST_LOG_HOOK = (hook_func_t)copyLog;
    ham_u32_t ps = m_lenv->get_page_size();
    Page *page[10];
    LocalDatabase *db = (LocalDatabase *)m_db;

    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = m_lenv->get_page_manager()->alloc_page(db,
                            0, PageManager::kIgnoreFreelist)));
      page[i]->set_dirty(true);
      REQUIRE(page[i]->get_address() == ps * (2 + i));
      for (int j = 0; j < 200; j++)
        page[i]->get_payload()[j] = (ham_u8_t)(i + j);
    }
    m_lenv->get_changeset().flush(5);
    m_lenv->get_changeset().clear();
    teardown();

    /* restore the backupped logfiles */
    restoreLog();

    /* now modify the file - after all we want to make sure that
     * the recovery overwrites the modification */
    ham_fd_t fd = os_open(Globals::opath(".test"), 0);
    for (int i = 0; i < 10; i++) {
      os_pwrite(fd, ps * (2 + i), "XXXXXXXXXXXXXXXXXXXX", 20);
    }
    os_close(fd);

    /* make sure that the log has one alloc-page entry */
    std::vector<LogEntry> vec;
    for (int i = 0; i < 10; i++)
      vec.push_back(LogEntry(5, ps * (2 + i), ps));
    compareLog(Globals::opath(".test2"), vec);

    /* recover and make sure that the page is ok */
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    db = (LocalDatabase *)m_db;
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
    /* verify that the pages does not contain the "XXX..." */
    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = fetch_page(db, ps * (2 + i))));
      for (int j = 0; j < 20; j++)
        REQUIRE('X' != page[i]->get_raw_payload()[i]);
    }

    /* verify the lsn */
    REQUIRE(5ull == m_lenv->get_log()->get_lsn());

    m_lenv->get_changeset().clear();
#endif
  }

  void recoverMixedAllocatedModifiedPageTest() {
#ifndef WIN32
    g_CHANGESET_POST_LOG_HOOK=(hook_func_t)copyLog;
    ham_u32_t ps = m_lenv->get_page_size();
    Page *page[10];
    LocalDatabase *db = (LocalDatabase *)m_db;

    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = m_lenv->get_page_manager()->alloc_page(db,
                            0, PageManager::kIgnoreFreelist)));
      page[i]->set_dirty(true);
      REQUIRE(page[i]->get_address() == ps * (2 + i));
      for (int j = 0; j < 200; j++)
        page[i]->get_payload()[j] = (ham_u8_t)(i + j);
    }
    m_lenv->get_changeset().flush(6);
    m_lenv->get_changeset().clear();
    teardown();

    /* restore the backupped logfiles */
    restoreLog();

    /* now modify the file - after all we want to make sure that
     * the recovery overwrites the modification, and then truncate
     * the file */
    ham_fd_t fd = os_open(Globals::opath(".test"), 0);
    for (int i = 0; i < 10; i++) {
      os_pwrite(fd, ps * (2 + i), "XXXXXXXXXXXXXXXXXXXX", 20);
    }
    os_truncate(fd, ps * 7);
    os_close(fd);

    /* make sure that the log has one alloc-page entry */
    std::vector<LogEntry> vec;
    for (int i = 0; i < 10; i++)
      vec.push_back(LogEntry(6, ps * (2 + i), ps));
    compareLog(Globals::opath(".test2"), vec);

    /* recover and make sure that the pages are ok */
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_AUTO_RECOVERY, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    db = (LocalDatabase *)m_db;
    m_lenv = (LocalEnvironment *)ham_db_get_env(m_db);
    /* verify that the pages do not contain the "XXX..." */
    for (int i = 0; i < 10; i++) {
      REQUIRE((page[i] = fetch_page(db, ps * (2 + i))));
      for (int j = 0; j < 20; j++)
        REQUIRE('X' != page[i]->get_raw_payload()[i]);
    }

    /* verify the lsn */
    REQUIRE(6ull == m_lenv->get_log()->get_lsn());

    m_lenv->get_changeset().clear();
#endif
  }

  void createAndEraseDbTest() {
    /* close m_db, otherwise ham_env_create fails on win32 */
    teardown();

    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"),
          HAM_ENABLE_RECOVERY, 0664, 0));

    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 333, 0, 0));
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == ham_env_rename_db(m_env, 333, 444, 0));

    REQUIRE(0 == ham_env_erase_db(m_env, 444, 0));
  }
};

TEST_CASE("Log-high/createCloseTest", "")
{
  LogHighLevelFixture f;
  f.createCloseTest();
}

TEST_CASE("Log-high/createCloseEnvTest", "")
{
  LogHighLevelFixture f;
  f.createCloseEnvTest();
}

TEST_CASE("Log-high/createCloseOpenCloseTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("Log-high/createCloseOpenFullLogTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenFullLogTest();
}

TEST_CASE("Log-high/createCloseOpenFullLogRecoverTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenFullLogRecoverTest();
}

TEST_CASE("Log-high/createCloseOpenCloseEnvTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenCloseEnvTest();
}

TEST_CASE("Log-high/createCloseOpenFullLogEnvTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenFullLogEnvTest();
}

TEST_CASE("Log-high/createCloseOpenFullLogEnvRecoverTest", "")
{
  LogHighLevelFixture f;
  f.createCloseOpenFullLogEnvRecoverTest();
}

TEST_CASE("Log-high/recoverAllocatePageTest", "")
{
  LogHighLevelFixture f;
  f.recoverAllocatePageTest();
}

TEST_CASE("Log-high/recoverAllocateMultiplePageTest", "")
{
  LogHighLevelFixture f;
  f.recoverAllocateMultiplePageTest();
}

TEST_CASE("Log-high/recoverModifiedPageTest", "")
{
  LogHighLevelFixture f;
  f.recoverModifiedPageTest();
}

TEST_CASE("Log-high/recoverModifiedMultiplePageTest", "")
{
  LogHighLevelFixture f;
  f.recoverModifiedMultiplePageTest();
}

TEST_CASE("Log-high/recoverMixedAllocatedModifiedPageTest", "")
{
  LogHighLevelFixture f;
  f.recoverMixedAllocatedModifiedPageTest();
}

TEST_CASE("Log-high/createAndEraseDbTest", "")
{
  LogHighLevelFixture f;
  f.createAndEraseDbTest();
}

} // namespace hamsterdb
