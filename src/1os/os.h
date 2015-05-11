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

// Returns true if the CPU supports AVX
extern bool
os_has_avx();

// Returns the number of 32bit integers that the CPU can process in
// parallel (the SIMD lane width) 
extern int
os_get_simd_lane_width();

} // namespace hamsterdb

#endif /* HAM_OS_H */
