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

#ifndef UPS_BENCH_MISC_HPP
#define UPS_BENCH_MISC_HPP

#include <stdio.h>

#define LOG_VERBOSE(x)  while (m_config->verbose) { printf x; break; }
#define LOG_TRACE(x)    do { printf("[info] "); printf x; } while (0)
#define LOG_ERROR(x)    do { printf("[error] "); printf x; } while (0)

#endif /* UPS_BENCH_MISC_HPP */
