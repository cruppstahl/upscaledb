/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * this file contains the version of hamster
 *
 */

#ifndef HAM_VERSION_H__
#define HAM_VERSION_H__

#ifdef __cplusplus
extern "C" {
#endif 

/**
 * the version numbers
 *
 * @remark a change of the major revision means a significant update with
 * a lot of new features, API changes and an incompatible database format. 
 * the minor revision means that updates were not so significant, but 
 * the database format is no longer compatible. a change of the revision 
 * means the release is a bugfix with a compatible database format.
 */
#define HAM_VERSION_MAJ 0
#define HAM_VERSION_MIN 4
#define HAM_VERSION_REV 2
#define HAM_VERSION_STR "0.4.2"


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_VERSION_H__ */
