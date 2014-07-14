/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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

