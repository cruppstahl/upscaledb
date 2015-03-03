/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
