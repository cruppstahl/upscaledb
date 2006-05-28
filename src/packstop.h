/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
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
#  pragma pack(pop, HAM_PACKING)
#endif

#endif /* HAM_PACKSTOP_H__ */
