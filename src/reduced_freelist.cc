/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include "error.h"
#include "reduced_freelist.h"

using namespace hamsterdb;

static bool
compare(const ReducedFreelist::Entry &lhs, const ReducedFreelist::Entry &rhs)
{
  return (lhs.first < rhs.first);
}

ham_status_t
ReducedFreelist::check_integrity()
{
  ham_assert(m_entries.size() <= kMaxEntries);

  // sort by address
  std::sort(m_entries.begin(), m_entries.end(), compare);

  ham_u64_t address = 0;

  for (EntryVec::iterator it = m_entries.begin(); it != m_entries.end(); it++) {
    if (it->first == 0 || it->second == 0) {
      ham_log(("Invalid freelist entry %llu/%u", it->first, it->second));
      return (HAM_INTEGRITY_VIOLATED);
    }
    if (it->first < address) {
      ham_log(("Invalid freelist address %llu/%u < %llu",
                  it->first, it->second, address));
      return (HAM_INTEGRITY_VIOLATED);
    }
    address = it->first + it->second;
  }

  return (0);
}
