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
 * Macros for packing structures; should work with most compilers.
 *
 * Example usage:
 *
 *  #include "packstart.h"
 *
 *  typedef HAM_PACK_0 struct HAM_PACK_1 foo {
 *    int bar;
 *  } HAM_PACK_2 foo_t;
 *
 *  #include "packstop.h"
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

/* This class does NOT include root.h! */

#ifdef __GNUC__
#  if (((__GNUC__==2) && (__GNUC_MINOR__>=7)) || (__GNUC__>2))
#  define HAM_PACK_2 __attribute__ ((packed))
#  define _NEWGNUC_
#  endif
#endif

#ifdef __WATCOMC__
#  define HAM_PACK_0 _Packed
#endif

#if (defined(_MSC_VER) && (_MSC_VER >= 900)) || defined(__BORLANDC__)
#  define _NEWMSC_
#endif
#if !defined(_NEWGNUC_) && !defined(__WATCOMC__) && !defined(_NEWMSC_)
#  pragma pack(1)
#endif
#ifdef _NEWMSC_
#  pragma pack(push, 1)
#  define HAM_PACK_2 __declspec(align(1))
#endif

#if defined(_NEWMSC_) && !defined(_WIN32_WCE)
#  pragma pack(push, 1)
#  define HAM_PACK_2 __declspec(align(1))
#endif

#ifndef HAM_PACK_0
#  define HAM_PACK_0
#endif

#ifndef HAM_PACK_1
#  define HAM_PACK_1
#endif

#ifndef HAM_PACK_2
#  define HAM_PACK_2
#endif

