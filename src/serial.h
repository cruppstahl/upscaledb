/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 * 
 *
 * this file describes the configuration of hamster - serial number, 
 * licensing information etc.
 *
 */

#ifndef SERIAL_H__
#define SERIAL_H__


/* 
 * the serial number; for GPL versions, this is always
 * 0x0; only non-GPL versions get a serial number.
 */
#define HAM_SERIALNO                  0x0

/*
 * the name of the licensee; for GPL, this string is empty ("")
 */
#define HAM_LICENSEE                  ""

/*
 * product list; describes the products which were licensed.
 *
 * the basic storage functionality is always enabled.
 */
#define HAM_PRODUCT_STORAGE           1

/*
 * same as above, but as a readable string
 */
#define HAM_PRODUCT_NAME              "hamsterdb storage"


#endif /* SERIAL_H__ */

