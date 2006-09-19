/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>

#include "util.h"
#include "mem.h"

ham_key_t *
util_copy_key(const ham_key_t *source, ham_key_t *dest)
{
    dest->data=(ham_u8_t *)ham_mem_alloc(source->size);
    if (!dest->data)
        return (0);

    memcpy(dest->data, source->data, source->size);
    dest->size=source->size;
    dest->_flags=source->_flags;
    return (dest);
}
