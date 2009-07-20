/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * macros for packing structures; should work with most compilers.
 *
 */

#if !defined(_NEWGNUC_) && !defined(__WATCOMC__) && !defined(_NEWMSC_)
#  pragma pack()
#endif
#ifdef _NEWMSC_
#  pragma pack(pop)
#endif
#if defined(_NEWMSC_) && !defined(_WIN32_WCE)
#  pragma pack(pop)
#endif
