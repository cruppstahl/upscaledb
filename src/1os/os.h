/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#ifdef HAM_OS_WIN32
#  ifdef CYGWIN
typedef int                ham_fd_t;
typedef int	               ham_socket_t;
#  else
typedef HANDLE             ham_fd_t;
typedef SOCKET             ham_socket_t;
#  endif
#  define HAM_INVALID_FD   (0)
#endif

// Returns the number of 32bit integers that the CPU can process in
// parallel (the SIMD lane width) 
extern int
os_get_simd_lane_width();

} // namespace hamsterdb

#endif /* HAM_OS_H */
