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
 * this file describes the configuration of hamster - serial number, 
 * enabled features etc. 
 *
 */

#ifndef SERIAL_H__
#define SERIAL_H__

/* 
 * the serial number; for non-commercial versions, this is always
 * 0x0; commercial versions get a serial number
 */
#define HAM_SERIALNO               0x0

/*
 * feature list; describes the features that are enabled or 
 * disabled
 *
 * the basic functionality is always enabled.
 */
#define HAM_ENABLE_BASIC           1

#endif /* SERIAL_H__ */
