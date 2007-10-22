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
 * aes encryption routines
 *
 */

#ifndef HAM_AES_H__
#define HAM_AES_H__

#ifdef __cplusplus
extern "C" {
#endif 

/**
 * encrypt a 16byte block
 */
extern void 
aes_encrypt(unsigned char *in, unsigned char *key, unsigned char *out);

/**
 * decrypt a 16byte block
 */
extern void 
aes_decrypt(unsigned char *in, unsigned char *key, unsigned char *out);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_AES_H__ */
