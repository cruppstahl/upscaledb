/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief btree find/insert/erase statistical structures, functions and macros
 */

#ifndef HAM_STATISTICS_H__
#define HAM_STATISTICS_H__

#include "internal_fwd_decl.h"


/**
 * get a reference to the statistics data of the given operation 
 */
#define db_get_op_perf_data(db, o)        ((db->get_perf_data())->op + (o))

#define btree_stats_memmove_cost(size)          (((size) + 512 - 1) / 512)

struct find_hints_t
{
    /* [in] insert flags */
    ham_u32_t original_flags;

    /* [in/out] find flags */
    ham_u32_t flags; 
 
    /* [out] page/btree leaf to check first */
    ham_offset_t leaf_page_addr;

    /* [out] - signals whether the key is out of bounds in the database */
    ham_bool_t key_is_out_of_bounds; 

    /* [out] check specified btree leaf node page first */
    ham_bool_t try_fast_track;

    /* [feedback] cost tracking for our statistics */
    ham_size_t cost; 
};

struct insert_hints_t
{
    /* [in] insert flags */
    ham_u32_t original_flags;
    
    /* [in/out] insert flags; may be modified while performing the insert */
    ham_u32_t flags;

    /* [in] cursor, if any */
    ham_cursor_t *cursor; 

    /* [out] page/btree leaf to check first */
    ham_offset_t leaf_page_addr;

    /* [out] check specified btree leaf node page first */
    ham_bool_t try_fast_track;
    
    /* [out] not (yet) part of the hints, but a result from it: informs 
     * insert_nosplit() that the insertion position (slot) is already known */
    ham_bool_t force_append;
    
    /* [out] not (yet) part of the hints, but a result from it: informs 
     * insert_nosplit() that the insertion position (slot) is already known */
    ham_bool_t force_prepend;
 
    /* [feedback] cost tracking for our statistics */
    ham_size_t cost;
 
    /* [feedback] the btree leaf page which received the inserted key */
    ham_page_t *processed_leaf_page;

    /* [feedback] >=0: entry slot index of the key within the btree leaf node;
     * -1: failure condition */
    ham_s32_t processed_slot;
};

struct erase_hints_t
{
 
    /* [in] insert flags */
    ham_u32_t original_flags;
 
    /* [in/out] insert flags; may be modified while performing the insert */
    ham_u32_t flags;
 
    /* [in] */
    ham_cursor_t *cursor;
 
    /* [out] page/btree leaf to check first */
    ham_offset_t leaf_page_addr;
 
    /* [out] */
    ham_bool_t key_is_out_of_bounds;
    
    /* [out] check specified btree leaf node page first */
    ham_bool_t try_fast_track;
 
    /* [feedback] cost tracking for our statistics */
    ham_size_t cost;
 
    /* [feedback] the btree leaf page which received the inserted key */
    ham_page_t *processed_leaf_page;

    /* [feedback] >=0: entry slot index of the key within the btree leaf node;
     * -1: failure condition */
    ham_s32_t processed_slot;
};

/**
 * statistics gathering call for btree_find
 */
extern void
db_update_global_stats_find_query(Database *db, ham_size_t key_size);

/**
 * statistics gathering call for btree_insert
 */
extern void
db_update_global_stats_insert_query(Database *db, ham_size_t key_size, 
                    ham_size_t record_size);

/**
 * statistics gathering call for btree_erase
 */
extern void
db_update_global_stats_erase_query(Database *db, ham_size_t key_size);

/**
 * statistics gathering call for failing lookups (key is out of bounds)
 */
extern void 
stats_update_fail_oob(int op, Database *db, ham_size_t cost, 
                    ham_bool_t try_fast_track);

#define btree_stats_update_find_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)

#define btree_stats_update_erase_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)

/**
 * statistics gathering call for other failures
 */
extern void 
stats_update_fail(int op, Database *db, ham_size_t cost, 
                    ham_bool_t try_fast_track);

#define btree_stats_update_find_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)

#define btree_stats_update_insert_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_INSERT, db, (hints)->cost, (hints)->try_fast_track)

#define btree_stats_update_erase_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)

extern void 
stats_update(int op, Database *db, struct ham_page_t *page, 
                    ham_size_t cost, ham_bool_t try_fast_track);

#define btree_stats_update_find(db, page, hints)    stats_update(HAM_OPERATION_STATS_FIND, db, page, (hints)->cost, (hints)->try_fast_track)

#define btree_stats_update_insert(db, page, hints)    stats_update(HAM_OPERATION_STATS_INSERT, db, page, (hints)->cost, (hints)->try_fast_track)

#define btree_stats_update_erase(db, page, hints)    stats_update(HAM_OPERATION_STATS_ERASE, db, page, (hints)->cost, (hints)->try_fast_track)

extern void 
btree_stats_page_is_nuked(Database *db, struct ham_page_t *page, 
                    ham_bool_t split);

extern void 
btree_stats_update_any_bound(int op, Database *db, struct ham_page_t *page, 
                    ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot);

extern void 
btree_find_get_hints(find_hints_t *hints, Database *db, ham_key_t *key);

extern void 
btree_insert_get_hints(insert_hints_t *hints, Database *db, ham_key_t *key);

extern void 
btree_erase_get_hints(erase_hints_t *hints, Database *db, ham_key_t *key);

extern void
btree_stats_init_globdata(ham_env_t *env, 
                    ham_runtime_statistics_globdata_t *globdata);

extern void
btree_stats_trash_globdata(ham_env_t *env, 
                    ham_runtime_statistics_globdata_t *globdata);

extern void
btree_stats_init_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata);

extern void
btree_stats_flush_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata,
                    ham_bool_t last_in_env);

extern void
btree_stats_trash_dbdata(Database *db, ham_runtime_statistics_dbdata_t *dbdata);

extern ham_status_t
btree_stats_fill_ham_statistics_t(ham_env_t *env, Database *db, 
                    ham_statistics_t *dst);


#endif /* HAM_FREELIST_H__ */
