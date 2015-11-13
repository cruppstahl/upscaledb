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

/*
 * UQI query parser
 *
 * @thread_safe: yes
 * @exception_safe: nothrow
 */

#ifndef UPS_UPSCALEDB_PARSER_H
#define UPS_UPSCALEDB_PARSER_H

#include "0root/root.h"

#include "ups/types.h"

#include "5upscaledb/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The struct Parser provides a common namespace for all
 * parser-related activities
 */
struct Parser
{
  /* Parses a SELECT statement into a SelectStatement object */
  static ups_status_t parse_select(const char *query, SelectStatement &stmt);
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_PARSER_H */
