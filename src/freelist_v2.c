/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#define IMPLEMENT_MODERN_FREELIST32

#include "freelist.c"

ham_status_t
__freel_flush_stats32(ham_device_t *dev, ham_env_t *env)
{
	ham_status_t st;

    ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB), (0));
    ham_assert(device_get_freelist_cache(dev), (0));

	/*
	 * do not update the statistics in a READ ONLY database!
	 */
    if (!(env_get_rt_flags(env) & HAM_READ_ONLY)) {
		freelist_cache_t *cache;
		freelist_entry_t *entries;

		cache=device_get_freelist_cache(dev);
		ham_assert(cache, (0));

		entries = freel_cache_get_entries(cache);

		if (entries && freel_cache_get_count(cache) > 0) {
			/*
			 * only persist the statistics when we're using a v1.1.0+ format DB 
             *
             * if freelist_v2 is used, the file is always 1.1.+ format.
			 */
			ham_size_t i;
			
			for (i = freel_cache_get_count(cache); i-- > 0; ) {
				freelist_entry_t *entry = entries + i;

				if (freel_entry_statistics_is_dirty(entry)) {
					freelist_payload_t *fp;
					freelist_page_statistics_t *pers_stats;

					if (!freel_entry_get_page_id(entry)) {
						/* header page */
						fp = env_get_freelist(env);
						env_set_dirty(env);
					}
					else {
						/*
						 * otherwise just fetch the page from the cache or the 
                         * disk
						 */
						ham_page_t *page;
						
						st = env_fetch_page(&page, env,
                                freel_entry_get_page_id(entry), 0);
						if (!page)
							return st ? st : HAM_INTERNAL_ERROR;
						fp = page_get_freelist(page);
						ham_assert(freel_get_start_address(fp) != 0, (0));
						page_set_dirty(page);
					}

					ham_assert(fp->_s._s32._zero == 0, (0));

					pers_stats = freel_get_statistics32(fp);

					ham_assert(sizeof(*pers_stats) == 
                            sizeof(*freel_entry_get_statistics(entry)), (0));
					memcpy(pers_stats, freel_entry_get_statistics(entry), 
                            sizeof(*pers_stats));

					/* and we're done persisting/flushing this entry */
					freel_entry_statistics_reset_dirty(entry);
				}
			}
		}
	}

	return (0);
}


