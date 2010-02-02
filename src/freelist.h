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
 * @brief freelist structures, functions and macros
 *
 */

#ifndef HAM_FREELIST_H__
#define HAM_FREELIST_H__

#include "internal_fwd_decl.h"
#include "freelist_statistics.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * an entry in the freelist cache
 */
struct freelist_entry_t
{
    /** the start address of this freelist page */
    ham_offset_t _start_address;

    /**
     * maximum bits in this page
     */
    ham_size_t _max_bits;

    /**
     * allocated bits in this page
     */
    ham_size_t _allocated_bits;

    /**
     * the page ID
     */
    ham_offset_t _page_id;

	/**
	 * some freelist algorithm specific run-time data
	 *
	 * This is done as a union as it will reduce code complexity
	 * significantly in the common freelist processing areas.
	 */
	runtime_statistics_pagedata_t _perf_data;
};


/** get the start address of a freelist cache entry */
#define freel_entry_get_start_address(f)                (f)->_start_address

/** set the start address of a freelist cache entry */
#define freel_entry_set_start_address(f, s)             (f)->_start_address=(s)

/** get max number of bits in the cache entry */
#define freel_entry_get_max_bits(f)                     (f)->_max_bits

/** set max number of bits in the cache entry */
#define freel_entry_set_max_bits(f, m)                  (f)->_max_bits=(m)

/** get number of allocated bits in the cache entry */
#define freel_entry_get_allocated_bits(f)               (f)->_allocated_bits

/** set number of allocated bits in the cache entry */
#define freel_entry_set_allocated_bits(f, b)            (f)->_allocated_bits=(b)

/** get the page ID of this freelist entry */
#define freel_entry_get_page_id(f)                      (f)->_page_id

/** set the page ID of this freelist entry */
#define freel_entry_set_page_id(f, id)                  (f)->_page_id=(id)

/** get the access performance data of this freelist entry */
#define freel_entry_get_perf_data(f)                    (f)->_perf_data

/** set the access performance data of this freelist entry */
#define freel_entry_set_perf_data(f, id)                (f)->_perf_data=(id)

/** get the statistics of this freelist entry */
#define freel_entry_get_statistics(f)                   &(f)->_perf_data._persisted_stats

/** check if the statistics have changed since we last flushed them */
#define freel_entry_statistics_is_dirty(f)              (f)->_perf_data._dirty

/** signal the statistics of this freelist entry as changed */
#define freel_entry_statistics_set_dirty(f)             (f)->_perf_data._dirty = HAM_TRUE

/** signal the statistics of this freelist entry as changed */
#define freel_entry_statistics_reset_dirty(f)           (f)->_perf_data._dirty = HAM_FALSE

/**
 * the freelist class structure - these functions and members are "inherited"
 * by every freelist management class (i.e. oldskool_16, skiplist, etc). 
 */
#define FREELIST_DECLARATIONS(clss)                                     \
	/**                                                                 \
     * create and initialize a new class instance                       \
     */                                                                 \
    ham_status_t                                                        \
	(*_constructor)(clss *be, ham_device_t *dev, ham_env_t *env);       \
                                                                        \
	/**                                                                 \
	 * release all freelist pages (and their statistics)                \
	 */                                                                 \
	ham_status_t                                                        \
	(*_destructor)(ham_device_t *dev, ham_env_t *env);                  \
                                                                        \
	/**                                                                 \
	 * flush all freelist page statistics                               \
	 */                                                                 \
	ham_status_t                                                        \
	(*_flush_stats)(ham_device_t *dev, ham_env_t *env);                 \
                                                                        \
	/**                                                                 \
	 * mark an area in the file as "free"                               \
	 *                                                                  \
	 * if 'overwrite' is true, will not assert that the bits are all    \
     * set to zero                                                      \
	 *                                                                  \
	 * @note                                                            \
	 * will assert that address and size are DB_CHUNKSIZE-aligned!      \
	 */                                                                 \
	ham_status_t                                                        \
	(*_mark_free)(ham_device_t *dev, ham_env_t *env, ham_db_t *db,      \
            ham_offset_t address, ham_size_t size,                      \
			ham_bool_t overwrite);                                      \
                                                                        \
	/**                                                                 \
	 * try to allocate (possibly aligned) space from the freelist,      \
	 * where the allocated space should be positioned at or beyond		\
	 * the given address.												\
	 *																	\
	 * returns 0 on failure												\
	 *																	\
	 * @note															\
	 * will assert that size is DB_CHUNKSIZE-aligned!					\
	 *																	\
	 * @note															\
	 * The lower_bound_address is assumed to be on a DB_CHUNKSIZE		\
	 * boundary at least. @a aligned space will end up at a             \
     * @ref DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes boundary.              \
	 * Regardless, the lower address bound check will be performed		\
	 * on a DB_CHUNKSIZE boundary level anyhow.							\
	 */                                                                 \
	ham_status_t                                                        \
	(*_alloc_area)(ham_offset_t *addr_ref, ham_device_t *dev,			\
			   ham_env_t *env, ham_db_t *db, ham_size_t size,           \
               ham_bool_t aligned, ham_offset_t lower_bound_address);   \
                                                                        \
	/**																	\
	 check whether the given block is administrated in the freelist.    \
     If it isn't yet, make it so.                                 		\
																		\
     @return one of @ref ham_status_codes on error, @ref HAM_SUCCESS	\
	         when the given storage area is within the scope of the		\
			 freelist.													\
	*/																	\
	ham_status_t                                                        \
	(*_check_area_is_allocated)(ham_device_t *dev, ham_env_t *env,		\
								ham_offset_t address, ham_size_t size);	\
                                                                        \
	/**																	\
	 * setup / initialize the proper performance data for this			\
	 * freelist page.													\
	 *																	\
	 * Yes, this data will (very probably) be lost once the page is		\
	 * removed from the in-memory cache, unless the currently active	\
	 * freelist algorithm persists this data to disc.					\
	 */																	\
	ham_status_t														\
	(*_init_perf_data)(clss *be, ham_device_t *dev, ham_env_t *env,		\
						freelist_entry_t *entry,						\
						freelist_payload_t *payload)




/**
 * the freelist structure is a table to track the deleted pages and blobs
 */
struct freelist_cache_t
{
    /** the number of cached elements */
    ham_u32_t _count;

    /** the cached freelist entries */
    freelist_entry_t *_entries;

	/** class methods which handle all things freelist */
	FREELIST_DECLARATIONS(struct freelist_cache_t);
};


/** get the number of freelist cache elements */
#define freel_cache_get_count(f)                        (f)->_count

/** set the number of freelist cache elements */
#define freel_cache_set_count(f, c)                     (f)->_count=(c)

/** get the cached freelist entries */
#define freel_cache_get_entries(f)                      (f)->_entries

/** set the cached freelist entries */
#define freel_cache_set_entries(f, e)                   (f)->_entries=(e)


#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a ham_page_t
 */
HAM_PACK_0 struct HAM_PACK_1 freelist_payload_t
{
    /**
     * "real" address of the first bit
     */
    ham_u64_t _start_address;

    /**
     * address of the next freelist page
     */
    ham_offset_t _overflow;

	HAM_PACK_0 union HAM_PACK_1 
	{
		/**
		 * This structure represents the backwards compatible v1.0.9 freelist
		 * payload layout, which can cope with up to 65535 chunks per page.
		 */
		HAM_PACK_0 struct HAM_PACK_1 
		{
			/**
			 * maximum number of bits for this page
			 */
			ham_u16_t _max_bits;

			/**
			 * number of already allocated bits in the page 
			 */
			ham_u16_t _allocated_bits;

			/**
			 * the bitmap; the size of the bitmap is _max_bits/8
			 */
			ham_u8_t _bitmap[1];
		} HAM_PACK_2 _s16;

		HAM_PACK_0 struct HAM_PACK_1 
		{
			/**
			 * 'zero': must be 0; serves as a doublecheck we're not 
			 * processing an old-style 16-bit freelist page, where this 
			 * spot would have the ham_u16_t _max_bits, which would 
			 * always != 0 ...
			 */
			ham_u16_t _zero;

			ham_u16_t _reserved;

			/**
			 * maximum number of bits for this page
			 */
			ham_u32_t _max_bits;

			/**
			 * number of already allocated bits in the page 
			 */
			ham_u32_t _allocated_bits;

            /**
             * The persisted statistics.
             *
             * Note that a copy is held in the nonpermanent section of
             * each freelist entry; after all, it's ludicrous to keep
             * the cache clogged with freelist pages which our
             * statistics show are useless given our usage patterns
             * (determined at run-time; this is meant to help many-insert, 
             * few-delete usage patterns the most, while many-delete usage 
             * patterns will benefit most from a good cache page aging system 
             * (see elsewhere in the code) as that will ensure relevant
             * freelist pages stay in the cache for as long as we need
             * them. Meanwhile, we've complicated things a little here
             * as we need to flush statistics to the persistent page
             * memory when flushing a cached page.
             *
             * TODO: A callback will be provided for that.
             */
			freelist_page_statistics_t _statistics;

			/**
			 * the algorithm-specific payload starts here.
			 */
			ham_u8_t _bitmap[1];
		} HAM_PACK_2 _s32;
	} HAM_PACK_2 _s;
} HAM_PACK_2;

#include "packstop.h"

/**
 * get the size of the persistent freelist header (old style)
 */
#define db_get_freelist_header_size16()   (OFFSETOF(freelist_payload_t, _s._s16._bitmap) /*(sizeof(freelist_payload_t)-1)*/ )

/**
 * get the size of the persistent freelist header (new style)
 */
#define db_get_freelist_header_size32()   (OFFSETOF(freelist_payload_t, _s._s32._bitmap))

/**
 * get the address of the first bitmap-entry of this page
 */
#define freel_get_start_address(fl)       (ham_db2h64((fl)->_start_address))

/**
 * set the start-address
 */
#define freel_set_start_address(fl, s)    (fl)->_start_address=ham_h2db64(s)

/**
 * get the maximum number of bits which are handled by this bitmap
 */
#define freel_get_max_bits16(fl)          (ham_db2h16((fl)->_s._s16._max_bits))

/**
 * set the maximum number of bits which are handled by this bitmap
 */
#define freel_set_max_bits16(fl, m)       (fl)->_s._s16._max_bits=ham_h2db16(m)

/**
 * get the maximum number of bits which are handled by this bitmap
 */
#define freel_get_max_bits32(fl)          (ham_db2h32((fl)->_s._s32._max_bits))

/**
 * set the maximum number of bits which are handled by this bitmap
 */
#define freel_set_max_bits32(fl, m)       (fl)->_s._s32._max_bits=ham_h2db32(m)

/**
 * get the number of currently used bits which are handled by this bitmap
 */
#define freel_get_allocated_bits16(fl)    (ham_db2h16((fl)->_s._s16._allocated_bits))

/**
 * set the number of currently used bits which are handled by this bitmap
 */
#define freel_set_allocated_bits16(fl, u) (fl)->_s._s16._allocated_bits=ham_h2db16(u)

/**
 * get the number of currently used bits which are handled by this
 * bitmap
 */
#define freel_get_allocated_bits32(fl)    (ham_db2h32((fl)->_s._s32._allocated_bits))

/**
 * set the number of currently used bits which are handled by this
 * bitmap
 */
#define freel_set_allocated_bits32(fl, u) (fl)->_s._s32._allocated_bits=ham_h2db32(u)

/**
 * get the address of the next overflow page
 */
#define freel_get_overflow(fl)            (ham_db2h_offset((fl)->_overflow))

/**
 * set the address of the next overflow page
 */
#define freel_set_overflow(fl, o)         (fl)->_overflow=ham_h2db_offset(o)

/**
 * get a freelist_payload_t from a ham_page_t
 */
#define page_get_freelist(p)     ((freelist_payload_t *)p->_pers->_s._payload)

/**
 * get the bitmap of the freelist
 */
#define freel_get_bitmap16(fl)            (fl)->_s._s16._bitmap

/**
 * get the bitmap of the freelist
 */
#define freel_get_bitmap32(fl)            (fl)->_s._s32._bitmap

/**
 * get the v1.1.0+ persisted entry performance statistics
 */
#define freel_get_statistics32(fl)        &((fl)->_s._s32._statistics)

/**
 * Initialize a v1.1.0+ compatible freelist management object
 */
extern ham_status_t
freel_constructor_prepare32(freelist_cache_t **cache_ref, ham_device_t *dev, 
                ham_env_t *env);

/**
 * Initialize a v1.0.x compatible freelist management object
 */
extern ham_status_t
freel_constructor_prepare16(freelist_cache_t **cache_ref, ham_device_t *dev, 
                ham_env_t *env);

/**
 * flush and release all freelist pages
 */
extern ham_status_t
freel_shutdown(ham_env_t *env);

/**
 * mark an area in the file as "free"
 *
 * if 'overwrite' is true, will not assert that the bits are all set to
 * zero
 *
 * @note
 * will assert that address and size are DB_CHUNKSIZE-aligned!
 * @a db can be NULL
 */
extern ham_status_t
freel_mark_free(ham_env_t *env, ham_db_t *db, 
            ham_offset_t address, ham_size_t size, ham_bool_t overwrite);

/**
 * try to allocate an (unaligned) space from the freelist
 *
 * returns 0 on failure
 *
 * @note
 * will assert that size is DB_CHUNKSIZE-aligned!
 */
extern ham_status_t
freel_alloc_area(ham_offset_t *addr_ref, ham_env_t *env, ham_db_t *db, 
            ham_size_t size);

/**
 * Try to allocate (possibly aligned) space from the freelist,
 * where the allocated space should be positioned at or beyond
 * the given address.
 *
 * returns 0 on failure
 *
 * @note
 * will assert that size is DB_CHUNKSIZE-aligned!
 *
 * @note															
 * The lower_bound_address is assumed to be on a DB_CHUNKSIZE		
 * boundary at least. @a aligned space will end up at a             
 * @ref DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes boundary.              
 * Regardless, the lower address bound check will be performed		
 * on a DB_CHUNKSIZE boundary level anyhow.							
 */
extern ham_status_t
freel_alloc_area_ex(ham_offset_t *addr_ref, ham_env_t *env, ham_db_t *db,
                ham_size_t size, ham_bool_t aligned, 
                ham_offset_t lower_bound_address);

/**
 * try to allocate an (aligned) page from the freelist
 *
 * @a db can be NULL!
 *
 * returns 0 on failure
 */
extern ham_status_t
freel_alloc_page(ham_offset_t *addr_ref, ham_env_t *env, ham_db_t *db);

/**																	
 * check whether the given block is administrated in the freelist.
 * If it isn't yet, make it so.                                 		
 * 
 * @return one of @ref ham_status_codes on error, @ref HAM_SUCCESS	
 *   when the given storage area is within the scope of the		
 *   freelist.													
 */
extern ham_status_t
freel_check_area_is_allocated(ham_env_t *env, ham_db_t *db, 
                ham_offset_t address, ham_size_t size);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_FREELIST_H__ */
