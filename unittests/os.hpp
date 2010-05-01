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
#   include <stdlib.h>
#endif
#include <string.h>

#include <ham/types.h>
#include <../src/error.h>

class os
{
protected:
#ifdef WIN32
	static const char *DisplayError(char* buf, ham_size_t buflen, DWORD errorcode)
	{
#ifdef UNDER_CE
        strcpy(buf, "((WinCE: DisplayError() not implemented))");
#else
		buf[0] = 0;
		FormatMessageA(/* FORMAT_MESSAGE_ALLOCATE_BUFFER | */
					  FORMAT_MESSAGE_FROM_SYSTEM |
					  FORMAT_MESSAGE_IGNORE_INSERTS,
					  NULL, errorcode,
					  MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
					  (LPSTR)buf, buflen, NULL);
		buf[buflen-1]=0;
#endif
		return buf;
	}
#endif

public:
    /*
     * delete a file
     */
    static bool unlink(const char *path, bool fail_silently = true)
    {
#ifdef WIN32
#   ifdef UNDER_CE
    	wchar_t wpath[1024];
	    MultiByteToWideChar(CP_ACP, 0, path, -1, wpath, 
                sizeof(wpath)/sizeof(wchar_t));
        BOOL rv = DeleteFileW(wpath);
#   else
        BOOL rv = DeleteFileA((LPCSTR)path);
#   endif
	    if (!rv) {
			if (!fail_silently) {
				char buf[1024];
				char buf2[1024];
				DWORD st;
				st = GetLastError();
				_snprintf(buf2, sizeof(buf2), 
					"DeleteFileA('%s') failed with OS status %u (%s)", 
					path, st, DisplayError(buf, sizeof(buf), st));
				buf2[sizeof(buf2)-1] = 0;
				ham_log(("%s", buf2));
			}
			return false;
		}
		return true;
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
#   ifdef UNDER_CE
    	wchar_t wsrc[1024];
        wchar_t wdest[1024];
	    MultiByteToWideChar(CP_ACP, 0, src, -1, wsrc, 
                sizeof(wsrc)/sizeof(wchar_t));
	    MultiByteToWideChar(CP_ACP, 0, dest, -1, wdest, 
                sizeof(wdest)/sizeof(wchar_t));
        BOOL rv = CopyFileW(wsrc, wdest, FALSE);
#   else
        BOOL rv = CopyFileA((LPCSTR)src, dest, FALSE);
#   endif
	    if (!rv) {
			char buf[2048];
			char buf2[1024];
			DWORD st;
			st = GetLastError();
			_snprintf(buf2, sizeof(buf2), 
				"CopyFileA('%s', '%s') failed with OS status %u (%s)", 
				src, dest, st, DisplayError(buf, sizeof(buf), st));
			buf2[sizeof(buf2)-1] = 0;
			ham_log(("%s", buf2));
			return false;
		}
		return true;
#else
        char buffer[1024*4];

        snprintf(buffer, sizeof(buffer), "\\cp %s %s && chmod 644 %s", 
                        src, dest, dest);
        return (0==system(buffer));
#endif
    }
};

#endif /* OS_HPP__ */
