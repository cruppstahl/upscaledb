/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief this is the configuration file parser/manager
 *
 */

#ifndef HAM_SRV_CONFIG_H__
#define HAM_SRV_CONFIG_H__


#ifdef __cplusplus
extern "C" {
#endif 

#include <stdio.h>

/*
 * initialize the config module
 *
 * This will also read the default configuration.
 *
 * @return true on success, false on failure
 */
extern ham_bool_t 
cfg_init(void);

/*
 * merge the configuration strings with those of a file handle
 *
 * note that keys are merged - only *existing* keys are overwritten.
 * if a key does not exist, the key will be ignored and a warning will
 * be printed.
 *
 * lower/upper case of the key is ignored
 *
 * @return true on success, false on failure
 */
extern ham_bool_t 
cfg_merge(FILE *f);

/*
 * extract a configuration value - will return NULL if the value is not
 * available
 *
 * lower/upper case of the key is ignored
 */
extern const char *
cfg_get(const char *key);

/*
 * overwrite an existing configuration value
 *
 * only existing values can be overwritten! lower/upper case of the key 
 * is ignored.
 *
 * @return true on success, false on failure
 */
extern const char *
cfg_put(const char *key, const char *value);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_SRV_CONFIG_H__ */
