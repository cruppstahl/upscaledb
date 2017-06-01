/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifndef OS_HPP
#define OS_HPP

#ifdef WIN32
#   define WIN32_LEAN_AND_MEAN
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

#include <ups/upscaledb.h>
#include "1base/error.h"
#include "1base/util.h"

#ifdef WIN32
static inline const char *
DisplayError(char* buf, uint32_t buflen, DWORD errorcode)
{
  buf[0] = 0;
  ::FormatMessageA(/* FORMAT_MESSAGE_ALLOCATE_BUFFER | */
          FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
          NULL, errorcode,
          MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
          (LPSTR)buf, buflen, NULL);
  buf[buflen - 1] = 0;
  return buf;
}
#endif

struct os {
  // deletes a file
  static bool unlink(const char *path) {
#ifdef WIN32
    return 0 != ::DeleteFileA((LPCSTR)path);
#else
    return 0 == ::unlink(path);
#endif
  }

  // copies a file
  static bool copy(const char *src, const char *dest) {
#ifdef WIN32
    return 0 != ::CopyFileA((LPCSTR)src, dest, FALSE);
#else
    // TODO FIX THIS!
    //copy_file(path(src), path(dest), copy_option::overwrite_if_exists);
    char buffer[1024 * 4];
    snprintf(buffer, sizeof(buffer), "cp %s %s && chmod 644 %s",
            src, dest, dest);
    return 0 == ::system(buffer);
#endif
  }

  // checks if a file exists
  static bool file_exists(const char *path) {
#ifdef WIN32
    struct _stat buf = {0};
    return ::_stat(path, &buf) >= 0;
#else
    struct stat buf = {0};
    return ::stat(path, &buf) >= 0;
#endif
  }
};

#endif /* OS_HPP */
