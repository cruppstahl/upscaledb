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
#include "txn.h"
#include "db.h"
#include "keys.h"
#include "error.h"

ham_key_t *
util_copy_key(ham_db_t *db, ham_txn_t *txn, 
            const ham_key_t *source, ham_key_t *dest)
{
    /*
     * extended key: copy the whole key
     */
    if (source->_flags&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, txn, source->data, 
                    source->size, source->_flags, (ham_u8_t **)&dest->data);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        dest->size=source->size;
        /* the extended flag is set later, when this key is inserted */
        dest->_flags=source->_flags&(~KEY_IS_EXTENDED);
    }
    else {
        dest->data=(ham_u8_t *)ham_mem_alloc(source->size);
        if (!dest->data) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }

        memcpy(dest->data, source->data, source->size);
        dest->size=source->size;
        dest->_flags=source->_flags;
    }

    return (dest);
}
