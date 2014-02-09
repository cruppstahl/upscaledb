/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_VERSION_H__
#define HAM_VERSION_H__

/*
 * The version numbers
 *
 * @remark A change of the major revision means a significant update
 * with a lot of new features and API changes.
 * 
 * The minor version means a significant update without API changes, and the 
 * revision is incremented for each release with minor improvements only.
 *
 * The file version describes the version of the binary database format.
 * hamsterdb is neither backwards- nor forwards-compatible regarding file
 * format changes. 
 *
 * History of file versions:
 *   2.1.0: introduced the file version; version is 0
 *   2.1.3: new btree format, file format cleanups; version is 1
 *   2.1.4: new btree format for duplicate keys/var. length keys; version is 2
 *   2.1.5: new freelist; version is 3
 */
#define HAM_VERSION_MAJ     2
#define HAM_VERSION_MIN     1
#define HAM_VERSION_REV     5
#define HAM_FILE_VERSION    3
#define HAM_VERSION_STR     "2.1.5"

#endif /* HAM_VERSION_H__ */
