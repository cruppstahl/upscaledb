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
