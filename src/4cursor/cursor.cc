/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

ham_status_t
Cursor::overwrite(ham_record_t *record, uint32_t flags)
{
  try {
    return (do_overwrite(record, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
Cursor::get_duplicate_position(uint32_t *pposition)
{
  try {
    return (do_get_duplicate_position(pposition));
  }
  catch (Exception &ex) {
    *pposition = 0;
    return (ex.code);
  }
}

ham_status_t
Cursor::get_duplicate_count(uint32_t flags, uint32_t *pcount)
{
  try {
    *pcount = 0;
    return (do_get_duplicate_count(flags, pcount));
  }
  catch (Exception &ex) {
    *pcount = 0;
    return (ex.code);
  }
}

ham_status_t
Cursor::get_record_size(uint64_t *psize)
{
  try {
    return (do_get_record_size(psize));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}


} // namespace hamsterdb
