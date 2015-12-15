/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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
 * Macros for packing structures; should work with most compilers.
 *
 * Example usage:
 *
 *  #include "packstart.h"
 *
 *  typedef UPS_PACK_0 struct UPS_PACK_1 foo {
 *    int bar;
 *  } UPS_PACK_2 foo_t;
 *
 *  #include "packstop.h"
 */

/* This class does NOT include root.h! */

#ifdef __GNUC__
#  if (((__GNUC__==2) && (__GNUC_MINOR__>=7)) || (__GNUC__>2))
#  define UPS_PACK_2 __attribute__ ((packed))
#  define _NEWGNUC_
#  endif
#endif

#ifdef __WATCOMC__
#  define UPS_PACK_0 _Packed
#endif

#if (defined(_MSC_VER) && (_MSC_VER >= 900)) || defined(__BORLANDC__)
#  define _NEWMSC_
#endif
#if !defined(_NEWGNUC_) && !defined(__WATCOMC__) && !defined(_NEWMSC_)
#  pragma pack(1)
#endif
#ifdef _NEWMSC_
#  pragma pack(push, 1)
#  define UPS_PACK_2 __declspec(align(1))
#endif

#if defined(_NEWMSC_) && !defined(_WIN32_WCE)
#  pragma pack(push, 1)
#  define UPS_PACK_2 __declspec(align(1))
#endif

#ifndef UPS_PACK_0
#  define UPS_PACK_0
#endif

#ifndef UPS_PACK_1
#  define UPS_PACK_1
#endif

#ifndef UPS_PACK_2
#  define UPS_PACK_2
#endif

