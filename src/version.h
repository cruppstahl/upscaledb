/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
 */
#define HAM_VERSION_MAJ     2
#define HAM_VERSION_MIN     1
#define HAM_VERSION_REV     1
#define HAM_FILE_VERSION    0
#define HAM_VERSION_STR     "2.1.1"

#endif /* HAM_VERSION_H__ */
