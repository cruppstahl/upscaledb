/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_FRACTION_H__
#define HAM_FRACTION_H__

#include <ham/types.h>

#ifdef __cplusplus
extern "C" {
#endif 


typedef struct 
{
    ham_u32_t num;
    ham_u32_t denom;
} ham_fraction_t;


extern double fract2dbl(const ham_fraction_t *src);

extern void to_fract_w_prec(ham_fraction_t *dst, double val, double precision);

extern void to_fract(ham_fraction_t *dst, double val);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif 
