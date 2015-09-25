/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * See packstart.h for a usage example.
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

/* This class does NOT include root.h! */

#if !defined(_NEWGNUC_) && !defined(__WATCOMC__) && !defined(_NEWMSC_)
#  pragma pack()
#endif
#ifdef _NEWMSC_
#  pragma pack(pop)
#endif
#if defined(_NEWMSC_) && !defined(_WIN32_WCE)
#  pragma pack(pop)
#endif

