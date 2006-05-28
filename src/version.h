/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
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
 * a lot of new features. the minor revision means that updates were not
 * so significant, but the database format is no longer compatible.
 * a change of the revision means the release is a bugfix with a 
 * compatible database format.
 */
#define HAM_VERSION_MAJ 0
#define HAM_VERSION_MIN 0
#define HAM_VERSION_REV 1
#define HAM_VERSION_STR "0.0.1"

/** 
 * same as above, but packed to 32 bits
 */
#define HAM_VERSION_INT ((HAM_VERSION_MAJ<<16)| \
                         (HAM_VERSION_MIN<< 8)| \
                          HAM_VERSION_REV)


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_VERSION_H__ */
