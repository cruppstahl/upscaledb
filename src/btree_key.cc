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

#include "blob_manager.h"
#include "btree_index.h"
#include "db_local.h"
#include "env_local.h"
#include "error.h"
#include "extkeys.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"

namespace hamsterdb {

ham_size_t PBtreeKey::kSizeofOverhead = OFFSETOF(PBtreeKey, m_key);

ham_u64_t
PBtreeKey::get_extended_rid(LocalDatabase *db) const
{
  ham_u64_t rid;
  memcpy(&rid, get_key() + (db->get_keysize() - sizeof(ham_u64_t)),
          sizeof(rid));
  return (ham_db2h_offset(rid));
}

void
PBtreeKey::set_extended_rid(LocalDatabase *db, ham_u64_t rid)
{
  rid = ham_h2db_offset(rid);
  memcpy(get_key() + (db->get_keysize() - sizeof(ham_u64_t)),
          &rid, sizeof(rid));
}

} // namespace hamsterdb
