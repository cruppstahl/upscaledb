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

#include "os.h"

void 
os_critsec_init(os_critsec_t *cs)
{
#ifdef WIN32
    InitializeCriticalSection(cs);
#else
    pthread_mutex_init(cs, 0);
#endif
}

void 
os_critsec_enter(os_critsec_t *cs)
{
#ifdef WIN32
    EnterCriticalSection(cs);
#else
    pthread_mutex_lock(cs);
#endif
}

void 
os_critsec_leave(os_critsec_t *cs)
{
#ifdef WIN32
    LeaveCriticalSection(cs);
#else
    pthread_mutex_unlock(cs);
#endif
}

void
os_critsec_close(os_critsec_t *cs)
{
#ifdef WIN32
    DeleteCriticalSection(cs);
#else
    pthread_mutex_destroy(cs);
#endif
}
