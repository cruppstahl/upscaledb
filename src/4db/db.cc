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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4db/db.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

void
Db::add_cursor(Cursor *cursor)
{
  cursor->next = cursor_list;
  if (cursor_list)
    cursor_list->previous = cursor;
  cursor_list = cursor;
}

void
Db::remove_cursor(Cursor *cursor)
{
  // fix the linked list of cursors
  Cursor *p = cursor->previous;
  Cursor *n = cursor->next;

  if (p)
    p->next = n;
  else
    cursor_list = n;

  if (n)
    n->previous = p;

  cursor->next = 0;
  cursor->previous = 0;
}

} // namespace upscaledb
