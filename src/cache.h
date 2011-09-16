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
 * @brief the cache manager
 *
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#include <vector>

#include "internal_fwd_decl.h"
#include "env.h"

	
#ifdef __cplusplus
extern "C" {
#endif 

/** CACHE_BUCKET_SIZE should be a prime number or similar, as it is used in 
 * a MODULO hash scheme */
#define CACHE_BUCKET_SIZE    10317


/**
 * the cache manager
 */
class ham_cache_t
{
  public:
    /** don't remove the page from the cache */
    static const int NOREMOVE=1;

    /** the default constructor
     * @remark max_size is in bytes!
     */
    ham_cache_t(ham_env_t *env, 
                ham_size_t capacity_bytes=HAM_DEFAULT_CACHESIZE);

    /**
     * the destructor closes and destroys the cache manager object
     * @remark this will NOT flush the cache!
     */
    ~ham_cache_t() {
    }
    
    /**
     * get an unused page (or an unreferenced page, if no unused page
     * was available
     *
     * @remark if the page is dirty, it's the caller's responsibility to 
     * write it to disk!
     *
     * @remark the page is removed from the cache
     */
    ham_page_t *get_unused_page(void);

    /**
     * get a page from the cache
     *
     * @remark the page is removed from the cache
     *
     * @return 0 if the page was not cached
     */
    ham_page_t *get_page(ham_offset_t address, ham_u32_t flags=0);

    /**
     * store a page in the cache
     */
    void put_page(ham_page_t *page);
    
    /**
     * remove a page from the cache
     */
    void remove_page(ham_page_t *page);

    /**
     * returns true if the caller should purge the cache
     */
    bool is_too_big(void) {
        return (m_cur_elements*env_get_pagesize(m_env)>m_capacity);
    }

    /**
     * get number of currently stored pages
     */
    ham_size_t get_cur_elements(void) {
        return (m_cur_elements);
    }

    /**
     * set the HEAD of the global page list
     */
    void set_totallist(ham_page_t *l) { 
        m_totallist=l; 
    }

    /**
     * retrieve the HEAD of the global page list
     */
    ham_page_t * get_totallist(void) { 
        return (m_totallist); 
    }

    /**
     * decrease number of current elements
     * TODO this should be private, but db.c needs it
     */
    void dec_cur_elements() { 
        m_cur_elements--; 
    }

    /**
     * get the capacity (in bytes)
     */
    ham_size_t get_capacity(void) { 
        return (m_capacity); 
    }

    /**
     * check the cache integrity
     */
    ham_status_t check_integrity(void);

  private:
    /** the current Environment */
    ham_env_t *m_env;

    /** the capacity (in bytes) */
    ham_size_t m_capacity;

    /** the current number of cached elements */
    ham_size_t m_cur_elements;

    /** linked list of ALL cached pages */
    ham_page_t *m_totallist;

    /** the tail of the linked "totallist" - this is the oldest element,
     * and therefore the highest candidate for a flush */
    ham_page_t *m_totallist_tail;

    /** the buckets - a linked list of ham_page_t pointers */
    std::vector<ham_page_t *> m_buckets;

};


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CACHE_H__ */
