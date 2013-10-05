/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */


#ifndef MISC_HPP__
#define MISC_HPP__

#include <stdio.h>

#define VERBOSE(x)  while (m_config->verbose) { printf x; break; }
#define TRACE(x)    do { printf("[info] "); printf x; } while (0)
#define ERROR(x)    do { printf("[error] "); printf x; } while (0)

#endif /* MISC_HPP__ */
