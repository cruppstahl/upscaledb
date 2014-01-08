/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "page.h"
#include "changeset.h"
#include "env.h"
#include "log.h"
#include "device.h"
#include "db.h"
#include "errorinducer.h"
#include "page_manager.h"

#define INDUCE(id)                                                  \
  while (m_inducer) {                                               \
    ham_status_t st = m_inducer->induce(id);                        \
    if (st)                                                         \
      throw Exception(st);                                          \
    break;                                                          \
  }

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
Changeset::add_page(Page *page)
{
  if (page->is_in_list(m_head, Page::kListChangeset))
    return;

  ham_assert(0 == page->get_next(Page::kListChangeset));
  ham_assert(0 == page->get_previous(Page::kListChangeset));
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  m_head = page->list_insert(m_head, Page::kListChangeset);
}

Page *
Changeset::get_page(ham_u64_t pageid)
{
  Page *page = m_head;

  while (page) {
    ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

    if (page->get_address() == pageid)
      return (page);
    page = page->get_next(Page::kListChangeset);
  }

  return (0);
}

void
Changeset::clear()
{
  while (m_head)
    m_head = m_head->list_remove(m_head, Page::kListChangeset);
}

void
Changeset::log_bucket(Page **bucket, ham_u32_t bucket_size,
            ham_u64_t lsn, ham_u32_t &page_count)
{
  for (ham_u32_t i = 0; i < bucket_size; i++) {
    ham_assert(bucket[i]->is_dirty());

    Log *log = m_env->get_log();
    INDUCE(ErrorInducer::kChangesetFlush);
    ham_assert(page_count > 0);
    log->append_page(bucket[i], lsn, --page_count);
  }
}

#define append(b, bs, bc, p)                                          \
  if (bs + 1 >= bc) {                                                 \
    bc = bc ? bc * 2 : 8;                                             \
    b = (Page **)::realloc(b, sizeof(void *) * bc);                   \
  }                                                                   \
  b[bs++] = p;

void
Changeset::flush(ham_u64_t lsn)
{
  ham_u32_t page_count = 0;
  Page *n, *p = m_head;
  if (!p)
    return;

  INDUCE(ErrorInducer::kChangesetFlush);

  m_blobs_size = 0;
  m_page_manager_size = 0;
  m_indices_size = 0;
  m_others_size = 0;

  // first step: remove all pages that are not dirty and sort all others
  // into the buckets
  while (p) {
    n = p->get_next(Page::kListChangeset);
    if (!p->is_dirty()) {
      p = n;
      continue;
    }

    if (p->is_header()) {
      append(m_indices, m_indices_size, m_indices_capacity, p);
    }
    else if (p->get_flags() & Page::kNpersNoHeader) {
      append(m_blobs, m_blobs_size, m_blobs_capacity, p);
    }
    else {
      switch (p->get_type()) {
        case Page::kTypeBlob:
          append(m_blobs, m_blobs_size, m_blobs_capacity, p);
          break;
        case Page::kTypeBroot:
        case Page::kTypeBindex:
        case Page::kTypeHeader:
          append(m_indices, m_indices_size, m_indices_capacity, p);
          break;
        case Page::kTypePageManager:
          append(m_page_manager, m_page_manager_size,
                          m_page_manager_capacity, p);
          break;
        default:
          append(m_others, m_others_size, m_others_capacity, p);
          break;
      }
    }
    page_count++;
    p = n;

    INDUCE(ErrorInducer::kChangesetFlush);
  }

  if (page_count == 0) {
    INDUCE(ErrorInducer::kChangesetFlush);
    clear();
    return;
  }

  INDUCE(ErrorInducer::kChangesetFlush);

  bool log_written = false;

  // if "others" is not empty then log everything because we don't really
  // know what's going on in this operation. otherwise we only need to log
  // if there's more than one page in a bucket:
  //
  // - if there's more than one index operation then the operation must
  //   be atomic
  if (m_others_size || m_page_manager_size || m_indices_size > 1) {
    log_bucket(m_blobs, m_blobs_size, lsn, page_count);
    log_bucket(m_page_manager, m_page_manager_size, lsn, page_count);
    log_bucket(m_indices, m_indices_size, lsn, page_count);
    log_bucket(m_others, m_others_size, lsn, page_count);
    log_written = true;
  }

  p = m_head;

  Log *log = m_env->get_log();

  /* flush the file handles (if required) */
  if (m_env->get_flags() & HAM_ENABLE_FSYNC && log_written)
    m_env->get_log()->flush();

  INDUCE(ErrorInducer::kChangesetFlush);

  // now flush all modified pages to disk
  ham_assert(log != 0);
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* now write all the pages to the file; if any of these writes fail,
   * we can still recover from the log */
  while (p) {
    m_env->get_page_manager()->flush_page(p);
    p = p->get_next(Page::kListChangeset);

    INDUCE(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (m_env->get_flags() & HAM_ENABLE_FSYNC)
    m_env->get_device()->flush();

  /* done - we can now clear the changeset and the log */
  clear();
  log->clear();
}

} // namespace hamsterdb
