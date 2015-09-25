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
 * Abstraction layer for operating system functions
 *
 * @exception_safe: basic // for socket
 * @exception_safe: strong // for file
 * @thread_safe: unknown
 */

#ifndef HAM_OS_H
#define HAM_OS_H

#include "0root/root.h"

#include <stdio.h>
#include <limits.h>

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/*
 * typedefs for posix
 */
#ifdef HAM_OS_POSIX
typedef int                ham_fd_t;
typedef int	               ham_socket_t;
#  define HAM_INVALID_FD  (-1)
#endif

/*
 * typedefs for Windows 32- and 64-bit
 */
#ifdef WIN32
#  ifdef CYGWIN
typedef int                ham_fd_t;
typedef int	               ham_socket_t;
#  else
typedef HANDLE             ham_fd_t;
typedef UINT_PTR           SOCKET; // from WinSock2.h
typedef SOCKET             ham_socket_t;
#  endif
#  define HAM_INVALID_FD   (0)
#endif

// Returns true if the CPU supports AVX
extern bool
os_has_avx();

// Returns the number of 32bit integers that the CPU can process in
// parallel (the SIMD lane width) 
extern int
os_get_simd_lane_width();

} // namespace hamsterdb

#endif /* HAM_OS_H */
