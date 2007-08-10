/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * macros for packing structures; should work with most compilers.
 *
 */

#ifndef HAM_PACKSTOP_H__
#define HAM_PACKSTOP_H__

#if !defined(_NEWGNUC_) && !defined(__WATCOMC__) && !defined(_NEWMSC_)
#  pragma pack()
#endif
#ifdef _NEWMSC_
#  pragma pack(pop)
#endif

#endif /* HAM_PACKSTOP_H__ */
