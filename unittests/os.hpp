/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef OS_HPP__
#define OS_HPP__

#ifdef WIN32
#   include <windows.h>
#else
#   include <unistd.h>
#   include <stdlib.h>
#   include <boost/filesystem.hpp>
using namespace boost::filesystem;
#endif
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <ham/hamsterdb.h>
#include <ham/types.h>
#include <../src/error.h>
#include <../src/util.h>

class os
{
protected:
#ifdef WIN32
  static const char *DisplayError(char* buf, ham_u32_t buflen, DWORD errorcode)
  {
    buf[0] = 0;
    FormatMessageA(/* FORMAT_MESSAGE_ALLOCATE_BUFFER | */
            FORMAT_MESSAGE_FROM_SYSTEM |
            FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL, errorcode,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPSTR)buf, buflen, NULL);
    buf[buflen - 1] = 0;
    return (buf);
  }
#endif

public:
  /*
   * delete a file
   */
  static bool unlink(const char *path, bool fail_silently = true)
  {
#ifdef WIN32
    BOOL rv = DeleteFileA((LPCSTR)path);
    if (!rv) {
      if (!fail_silently) {
        char buf[1024];
        char buf2[1024];
        DWORD st = GetLastError();
        _snprintf(buf2, sizeof(buf2),
                "DeleteFileA('%s') failed with OS status %u (%s)",
                path, st, DisplayError(buf, sizeof(buf), st));
        buf2[sizeof(buf2)-1] = 0;
        ham_log(("%s", buf2));
      }
      return (false);
    }
    return (true);
#else
    return (0 == ::unlink(path));
#endif
  }

  /*
   * copy a file
   */
  static bool copy(const char *src, const char *dest) {
#ifdef WIN32
    BOOL rv = CopyFileA((LPCSTR)src, dest, FALSE);
    if (!rv) {
      char buf[2048];
      char buf2[1024];
      DWORD st = GetLastError();
      _snprintf(buf2, sizeof(buf2),
            "CopyFileA('%s', '%s') failed with OS status %u (%s)",
            src, dest, st, DisplayError(buf, sizeof(buf), st));
      buf2[sizeof(buf2) - 1] = 0;
      ham_log(("%s", buf2));
      return (false);
    }
#else
    copy_file(path(src), path(dest), copy_option::overwrite_if_exists);
    //char buffer[1024 * 4];

    //snprintf(buffer, sizeof(buffer), "cp %s %s && chmod 644 %s",
            //src, dest, dest);
    //return (0 == system(buffer));
#endif
    return (true);
  }

  /*
   * check if a file exists
   */
  static bool file_exists(const char *path) {
#ifdef WIN32
    struct _stat buf = {0};
    if (::_stat(path, &buf) < 0)
      return (false);
#else
    struct stat buf = {0};
    if (::stat(path, &buf)<0)
      return (false);
#endif
    return (true);
  }
};

#endif /* OS_HPP__ */
