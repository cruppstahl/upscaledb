/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BTREE_STATS_H__
#define HAM_BTREE_STATS_H__

#include "internal_fwd_decl.h"

namespace ham {

class BtreeStatistics {
  public:
    struct FindHints {
      /* the original flags of ham_find */
      ham_u32_t original_flags;

      /* the modified flags */
      ham_u32_t flags;

      /* page/btree leaf to check first */
      ham_offset_t leaf_page_addr;

      /* signals whether the key is out of bounds in the database */
      bool key_is_out_of_bounds;

      /* check specified btree leaf node page first */
      bool try_fast_track;
    };

    struct InsertHints {
      /* the original flags of ham_insert */
      ham_u32_t original_flags;

      /* the modified flags */
      ham_u32_t flags;

      /* page/btree leaf to check first */
      ham_offset_t leaf_page_addr;

      /* check specified btree leaf node page first */
      bool try_fast_track;

      /* not (yet) part of the hints, but a result from it: informs
       * insert_nosplit() that the insertion position (slot) is already
       * known */
      bool force_append;

      /* not (yet) part of the hints, but a result from it: informs
       * insert_nosplit() that the insertion position (slot) is already
       * known */
      bool force_prepend;

      /* the btree leaf page which received the inserted key */
      Page *processed_leaf_page;

      /* >=0: entry slot index of the key within the btree leaf
       * node; -1: failure condition */
      ham_s32_t processed_slot;
    };

    struct EraseHints {
      /* the original flags of ham_erase */
      ham_u32_t original_flags;

      /* the modified flags */
      ham_u32_t flags;

      /* page/btree leaf to check first */
      ham_offset_t leaf_page_addr;

      /* true if the key is out of bounds (does not exist) */
      bool key_is_out_of_bounds;

      /* check specified btree leaf node page first */
      bool try_fast_track;

      /* the btree leaf page which received the inserted key */
      Page *processed_leaf_page;

      /* >=0: entry slot index of the key within the btree leaf
       * node; -1: failure condition */
      ham_s32_t processed_slot;
    };

    /** constructor */
    BtreeStatistics(Database *db);

    /** retrieve database hints for ham_erase */
    EraseHints get_erase_hints(ham_u32_t flags, ham_key_t *key);

    /** retrieve database hints for ham_find */
    FindHints get_find_hints(ham_key_t *key, ham_u32_t flags);

    /** retrieve database hints for insert */
    InsertHints get_insert_hints(ham_u32_t flags, Cursor *cursor,
                ham_key_t *key);

    /** An update succeeded */
    void update_succeeded(int op, Page *page, bool try_fast_track);

    /** An update failed */
    void update_failed(int op, bool try_fast_track);

    /** update statistics following an out-of-bound hint */
    void update_failed_oob(int op, bool try_fast_track);

    /** update database boundaries */
    void update_any_bound(int op, Page *page, ham_key_t *key,
                    ham_u32_t find_flags, ham_s32_t slot);

    /** reset page statistics */
    void reset_page(Page *page, bool split);

  private:
    /** get a reference to the per-database statistics */
    DatabaseStatistics *get_perf_data() {
      return (&m_perf_data);
    }

    /** get a reference to the statistics data of the given operation */
    OperationStatistics *get_op_data(int op) {
      return (get_perf_data()->op + op);
    }

    /** the Database */
    Database *m_db;

    /** some database specific run-time data */
    DatabaseStatistics m_perf_data;

    /** dynamic arrays which back the cached lower bound key */
    ByteArray m_lower_arena;

    /** dynamic arrays which back the cached upper bound key */
    ByteArray m_upper_arena;
};


} // namespace ham

#endif /* HAM_BTREE_STATS_H__ */
