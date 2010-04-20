/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief This file encapsulates os-specific functions
 *
 */

#ifndef HAM_SRV_OS_H__
#define HAM_SRV_OS_H__


#ifdef __cplusplus
extern "C" {
#endif 

/*
 * a critical section
 */
#ifdef WIN32
#   include <windows.h>
typedef CRITICAL_SECTION os_critsec_t;
#else
#   include <pthread.h>
typedef pthread_mutex_t os_critsec_t;
#endif

/*
 * Initialize a critical section
 */
extern void 
os_critsec_init(os_critsec_t *cs);

/*
 * enter a critical section
 */
extern void 
os_critsec_enter(os_critsec_t *cs);

/*
 * leave a critical section
 */
extern void 
os_critsec_leave(os_critsec_t *cs);

/*
 * release memory and clean up
 */
extern void
os_critsec_close(os_critsec_t *cs);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_SRV_OS_H__ */
