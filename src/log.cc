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
#ifndef HAM_OS_WIN32
#  include <libgen.h>
#endif

#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "page_manager.h"

namespace hamsterdb {

void
Log::create()
{
  Log::PEnvironmentHeader header;
  std::string path = get_path();

  // create the files
  m_fd = os_create(path.c_str(), 0, 0644);

  /* write the file header with the magic */
  header.magic = HEADER_MAGIC;

  os_write(m_fd, &header, sizeof(header));
}

void
Log::open()
{
  Log::PEnvironmentHeader header;
  std::string path = get_path();

  // open the file
  m_fd = os_open(path.c_str(), 0);

  // check the file header with the magic
  os_pread(m_fd, 0, &header, sizeof(header));
  if (header.magic != HEADER_MAGIC) {
    ham_trace(("logfile has unknown magic or is corrupt"));
    close();
    throw Exception(HAM_LOG_INV_FILE_HEADER);
  }

  // store the lsn
  m_lsn = header.lsn;
}

void
Log::get_entry(Log::Iterator *iter, Log::PEntry *entry, ByteArray *buffer)
{
  buffer->clear();

  // if the iterator is initialized and was never used before: read
  // the file size
  if (!*iter)
    *iter = os_get_filesize(m_fd);

  // if the current file is empty: no need to continue
  if (*iter <= sizeof(Log::PEnvironmentHeader)) {
    entry->lsn = 0;
    return;
  }

  // otherwise read the Log::PEntry header (without extended data)
  // from the file
  *iter -= sizeof(Log::PEntry);

  os_pread(m_fd, *iter, entry, sizeof(*entry));

  // now read the extended data, if it's available
  if (entry->data_size) {
    ham_u64_t pos = (*iter) - entry->data_size;
    pos -= (pos % 8);

    buffer->resize((ham_u32_t)entry->data_size);

    os_pread(m_fd, pos, buffer->get_ptr(), (ham_u32_t)entry->data_size);
    *iter = pos;
  }
}

void
Log::close(bool noclear)
{
  Log::PEnvironmentHeader header;

  if (m_fd == HAM_INVALID_FD)
    return;

  // write the file header with the magic and the last used lsn
  header.magic = HEADER_MAGIC;
  header.lsn = m_lsn;

  os_pwrite(m_fd, 0, &header, sizeof(header));

  if (!noclear)
    clear();

  os_close(m_fd);
  m_fd = HAM_INVALID_FD;
}

void
Log::append_page(Page *page, ham_u64_t lsn, ham_u32_t page_count)
{
  ham_u8_t *p = (ham_u8_t *)page->get_raw_payload();
  ham_u32_t size = m_env->get_page_size();

  append_write(lsn, page_count == 0
                        ? kChangesetIsComplete
                        : 0,
                page->get_address(), p, size);

  if (p != page->get_raw_payload())
    Memory::release(p);
}

void
Log::recover()
{
  Page *page;
  Device *device = m_env->get_device();
  Log::PEntry entry;
  Iterator it = 0;
  ByteArray buffer;
  bool first_loop = true;

  // get the file size of the database; otherwise we do not know if we
  // modify an existing page or if one of the pages has to be allocated
  ham_u64_t filesize = device->get_filesize();

  // temporarily disable logging
  m_env->set_flags(m_env->get_flags() & ~HAM_ENABLE_RECOVERY);

  // now start the loop once more and apply the log
  while (1) {
    // clean up memory of the previous loop
    buffer.clear();

    // get the next entry in the logfile
    get_entry(&it, &entry, &buffer);

    // first make sure that the log is complete; if not then it will not
    // be applied
    if (first_loop) {
      if (entry.flags != kChangesetIsComplete) {
        ham_log(("log is incomplete and will be ignored"));
        goto clear;
      }
      first_loop = false;
    }

    // reached end of the log file?
    if (entry.lsn == 0)
      break;

    /*
     * Was the page appended or overwritten?
     *
     * Either way we have to bypass the cache and all upper layers. We
     * cannot call PageManager::alloc_page() or PageManager::fetch_page()
     * since they are not yet properly set up.
     */
    if (entry.offset == filesize) {
      // appended...
      filesize += entry.data_size;

      page = new Page(m_env);
      page->allocate();
    }
    else {
      // overwritten...
      page = new Page(m_env);
      page->fetch(entry.offset);
    }

    ham_assert(page->get_address() == entry.offset);
    ham_assert(m_env->get_page_size() == entry.data_size);

    // overwrite the page data
    memcpy(page->get_data(), buffer.get_ptr(), entry.data_size);

    // flush the modified page to disk
    page->set_dirty(true);
    m_env->get_page_manager()->flush_page(page);
    delete page;

    // store the lsn in the log - will be needed later when recovering
    // the journal
    m_lsn = entry.lsn;
  }

clear:
  // and finally clear the log
  clear();

  // re-enable the logging
  m_env->set_flags(m_env->get_flags() | HAM_ENABLE_RECOVERY);
}

void
Log::append_write(ham_u64_t lsn, ham_u32_t flags, ham_u64_t offset,
                ham_u8_t *data, ham_u32_t size)
{
  Log::PEntry entry;

  // store the lsn - it will be needed later when the log file is closed
  if (lsn)
    m_lsn = lsn;

  entry.lsn = lsn;
  entry.flags = flags;
  entry.offset = offset;
  entry.data_size = size;

  os_writev(m_fd, data, size, &entry, sizeof(entry));
}

std::string
Log::get_path()
{
  std::string path;

  if (m_env->get_log_directory().empty()) {
    path = m_env->get_filename();
  }
  else {
    path = m_env->get_log_directory();
#ifdef HAM_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(m_env->get_filename().c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)m_env->get_filename().c_str());
#endif
  }
  path += ".log0";
  return (path);
}

} // namespace hamsterdb
