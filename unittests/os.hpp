/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#ifndef OS_HPP__
#define OS_HPP__

#ifdef WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#endif

#include <ham/types.h>

class os
{
public:
    /*
     * delete a file
     */
    static bool unlink(const char *path)
    {
#ifdef WIN32
        return ((bool)DeleteFileA((LPCSTR)path));
#else
        return (0==::unlink(path));
#endif
    }

    /*
     * copy a file
     */
    static bool copy(const char *src, const char *dest)
    {
#ifdef WIN32
        return ((bool)CopyFile(src, dest, FALSE));
#else
        char buffer[1024*4];

        snprintf(buffer, sizeof(buffer), "\\cp %s %s", src, dest);
        return (0==system(buffer));
#endif
    }
};

#endif /* OS_HPP__ */
