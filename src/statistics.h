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
 * @brief btree find/insert/erase statistical structures, functions and macros
 */

#ifndef HAM_STATISTICS_H__
#define HAM_STATISTICS_H__

#include "internal_fwd_decl.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * get a reference to the statistics data of the given operation 
 */
#define db_get_op_perf_data(db, o)        ((db_get_db_perf_data(db))->op + (o))

#define stats_memmove_cost(size)          (((size) + 512 - 1) / 512)

struct find_hints_t
{
    ham_u32_t original_flags; /* [in] insert flags */
    ham_u32_t flags; /* [in/out] find flags */

    ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
    ham_bool_t key_is_out_of_bounds; /* [out] */
    ham_bool_t try_fast_track;    /* [out] check specified btree leaf node page first */

    ham_size_t cost; /* [feedback] cost tracking for our statistics */
};

struct insert_hints_t
{
    ham_u32_t original_flags; /* [in] insert flags */
    ham_u32_t flags; /* [in/out] insert flags; may be modified while performing the insert */
    ham_cursor_t *cursor; /* [in] */

    ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
    ham_bool_t try_fast_track;    /* [out] check specified btree leaf node page first */
    ham_bool_t force_append;    /* [out] not (yet) part of the hints, but a result from it: informs insert_nosplit() that the insertion position (slot) is already known */
    ham_bool_t force_prepend;    /* [out] not (yet) part of the hints, but a result from it: informs insert_nosplit() that the insertion position (slot) is already known */
    ham_size_t cost; /* [feedback] cost tracking for our statistics */
    ham_page_t *processed_leaf_page; /* [feedback] the btree leaf page which received the inserted key */
    ham_s32_t processed_slot;    /* [feedback] >=0: entry slot index of the key within the btree leaf node; -1: failure condition */

};

struct erase_hints_t
{
    ham_u32_t original_flags; /* [in] insert flags */
    ham_u32_t flags; /* [in/out] insert flags; may be modified while performing the insert */
    ham_cursor_t *cursor; /* [in] */

    ham_offset_t leaf_page_addr; /* [out] page/btree leaf to check first */
    ham_bool_t key_is_out_of_bounds; /* [out] */
    ham_bool_t try_fast_track;    /* [out] check specified btree leaf node page first */
    ham_size_t cost; /* [feedback] cost tracking for our statistics */
    ham_page_t *processed_leaf_page; /* [feedback] the btree leaf page which received the inserted key */
    ham_s32_t processed_slot;    /* [feedback] >=0: entry slot index of the key within the btree leaf node; -1: failure condition */
};

/**
 * statistics gathering calls for insert / erase (delete) / find
 */
extern void
db_update_global_stats_find_query(ham_db_t *db, ham_size_t key_size);

extern void
db_update_global_stats_insert_query(ham_db_t *db, ham_size_t key_size, ham_size_t record_size);

extern void
db_update_global_stats_erase_query(ham_db_t *db, ham_size_t key_size);

extern void 
stats_update_fail_oob(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)

#define stats_update_erase_fail_oob(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)

extern void 
stats_update_fail(int op, ham_db_t *db, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_FIND, db, (hints)->cost, (hints)->try_fast_track)

#define stats_update_insert_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_INSERT, db, (hints)->cost, (hints)->try_fast_track)

#define stats_update_erase_fail(db, hints)  stats_update_fail(HAM_OPERATION_STATS_ERASE, db, (hints)->cost, (hints)->try_fast_track)

extern void 
stats_update(int op, ham_db_t *db, struct ham_page_t *page, ham_size_t cost, ham_bool_t try_fast_track);

#define stats_update_find(db, page, hints)    stats_update(HAM_OPERATION_STATS_FIND, db, page, (hints)->cost, (hints)->try_fast_track)

#define stats_update_insert(db, page, hints)    stats_update(HAM_OPERATION_STATS_INSERT, db, page, (hints)->cost, (hints)->try_fast_track)

#define stats_update_erase(db, page, hints)    stats_update(HAM_OPERATION_STATS_ERASE, db, page, (hints)->cost, (hints)->try_fast_track)

extern void 
stats_page_is_nuked(ham_db_t *db, struct ham_page_t *page, ham_bool_t split);

extern void 
stats_update_any_bound(int op, ham_db_t *db, struct ham_page_t *page, ham_key_t *key, ham_u32_t find_flags, ham_s32_t slot);

extern void 
btree_find_get_hints(find_hints_t *hints, ham_db_t *db, ham_key_t *key);

extern void 
btree_insert_get_hints(insert_hints_t *hints, ham_db_t *db, ham_key_t *key);

extern void 
btree_erase_get_hints(erase_hints_t *hints, ham_db_t *db, ham_key_t *key);

extern void
stats_init_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);

extern void
stats_flush_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);

extern void
stats_trash_globdata(ham_env_t *env, ham_runtime_statistics_globdata_t *globdata);

extern void
stats_init_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata);

extern void
stats_flush_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata, ham_bool_t last_in_env);

extern void
stats_trash_dbdata(ham_db_t *db, ham_runtime_statistics_dbdata_t *dbdata);

extern ham_status_t
stats_fill_ham_statistics_t(ham_env_t *env, ham_db_t *db, ham_statistics_t *dst);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_FREELIST_H__ */
