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
 * @brief extended key cache
 *
 */

#ifndef HAM_EXTKEYS_H__
#define HAM_EXTKEYS_H__

#include <vector>

#include "internal_fwd_decl.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * a cache for extended keys
 */
class extkey_cache_t
{
  public:
    /** the default constructor */
    extkey_cache_t(ham_db_t *db);

    /** the destructor */
    ~extkey_cache_t();

    /**
     * insert a new extended key in the cache
     * will assert that there's no duplicate key! 
     */
    ham_status_t insert(ham_offset_t blobid, ham_size_t size, 
                const ham_u8_t *data);

    /**
     * remove an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    void remove(ham_offset_t blobid);

    /**
     * fetches an extended key from the cache
     * returns HAM_KEY_NOT_FOUND if the extkey was not found
     */
    ham_status_t fetch(ham_offset_t blobid, ham_size_t *size, ham_u8_t **data);
    
    /**
     * removes all OLD keys from the cache
     */
    void purge(void);
    
    /**
     * removes ALL keys from the cache
     */
    void purge_all(void);


  private:
    /** the owner of the cache */
    ham_db_t *m_db;

    /** the used size, in byte */
    ham_size_t m_usedsize;

    /** the buckets - a list of extkey_t pointers */
    std::vector<extkey_t *> m_buckets;

};

/**
 * a combination of extkey_cache_remove and blob_free
 */
extern ham_status_t 
extkey_remove(ham_db_t *db, ham_offset_t blobid);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_EXTKEYS_H__ */
