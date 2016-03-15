/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * @exception_safe: unknown
 * @thread_safe: no
 */

#ifndef UPS_DB_LOCAL_H
#define UPS_DB_LOCAL_H

#include "0root/root.h"

#include <limits>

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
// need to include the header file, a forward declaration of class Compressor
// is not sufficient because std::auto_ptr then fails to call the
// destructor
#include "2compressor/compressor.h"
#include "3btree/btree_index.h"
#include "4txn/txn_local.h"
#include "4db/db.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct TxnNode;
struct TxnIndex;
struct TxnCursor;
struct TxnOperation;
class LocalEnvironment;
struct LocalTxn;
struct SelectStatement;
struct Result;

template<typename T>
class RecordNumberFixture;

//
// The database implementation for local file access
//
class LocalDb : public Db {
  public:
    // Constructor
    LocalDb(Environment *env, DbConfig &config)
      : Db(env, config), compare_function(0), _current_record_number(0) {
    }

    // Creates a new database
    ups_status_t create(Context *context, PBtreeHeader *btree_header);

    // Opens an existing database
    ups_status_t open(Context *context, PBtreeHeader *btree_header);

    // Erases this database
    ups_status_t drop(Context *context);

    // Fills in the current metrics
    virtual void fill_metrics(ups_env_metrics_t *metrics);

    // Returns database parameters (ups_db_get_parameters)
    virtual ups_status_t get_parameters(ups_parameter_t *param);

    // Checks database integrity (ups_db_check_integrity)
    virtual ups_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual ups_status_t count(Txn *txn, bool distinct, uint64_t *pcount);

    // Scans the whole database, applies a processor function
    virtual ups_status_t scan(Txn *txn, ScanVisitor *visitor, bool distinct);

    // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
    virtual ups_status_t insert(Cursor *cursor, Txn *txn, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
    virtual ups_status_t erase(Cursor *cursor, Txn *txn, ups_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
    virtual ups_status_t find(Cursor *cursor, Txn *txn, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ups_cursor_move)
    virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Creates a cursor (ups_cursor_create)
    virtual Cursor *cursor_create(Txn *txn, uint32_t flags);

    // Clones a cursor (ups_cursor_clone)
    virtual Cursor *cursor_clone(Cursor *src);

    // Closes the database (ups_db_close)
    virtual ups_status_t close(uint32_t flags);

    // (Non-virtual) Performs a range select over the database
    ups_status_t select_range(SelectStatement *stmt, LocalCursor *begin,
                    LocalCursor *end, Result **result);

    // Flushes a TxnOperation to the btree
    ups_status_t flush_txn_operation(Context *context, LocalTxn *txn,
                    TxnOperation *op);

    // Inserts a key/record pair in a txn node; if cursor is not NULL it will
    // be attached to the new txn_op structure
    ups_status_t insert_txn(Context *context, ups_key_t *key,
                    ups_record_t *record, uint32_t flags,
                    TxnCursor *cursor);

    // the btree index
    ScopedPtr<BtreeIndex> btree_index;

    // the transaction index
    ScopedPtr<TxnIndex> txn_index;

    // the comparison function
    ups_compare_func_t compare_function;

    // The record compressor; can be null
    std::auto_ptr<Compressor> record_compressor;

  protected:
    friend class LocalCursor;

    // Move Cursor::overwrite to the database, then make begin_temp_txn
    // and finalize static (in the .cc file) (and maybe insert_txn as well?)

    // Begins a new temporary Txn
    LocalTxn *begin_temp_txn();

    // Finalizes an operation by committing or aborting the |local_txn|
    // and clearing or flushing the Changeset.
    // Returns |status|.
    ups_status_t finalize(Context *context, ups_status_t status,
                    Txn *local_txn);

  private:
    friend struct DbFixture;
    friend struct UpscaledbFixture;
    friend struct ExtendedKeyFixture;
    friend class RecordNumberFixture<uint32_t>;
    friend class RecordNumberFixture<uint64_t>;

    // Returns true if this database is modified by an active transaction
    bool is_modified_by_active_transaction();

    // Returns true if a (btree) key was erased in a Txn
    bool is_key_erased(Context *context, ups_key_t *key);

    // Erases a key/record pair from a txn; on success, cursor will be set to
    // nil
    ups_status_t erase_txn(Context *context, ups_key_t *key, uint32_t flags,
                    TxnCursor *cursor);

    // Lookup of a key/record pair in the Txn index and in the btree,
    // if transactions are disabled/not successful; copies the
    // record into |record|. Also performs approx. matching.
    ups_status_t find_txn(Context *context, LocalCursor *cursor,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ups_cursor_move)
    ups_status_t cursor_move_impl(Context *context, LocalCursor *cursor,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // The actual implementation of insert()
    ups_status_t insert_impl(Context *context, LocalCursor *cursor,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // The actual implementation of find()
    ups_status_t find_impl(Context *context, LocalCursor *cursor,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // The actual implementation of erase()
    ups_status_t erase_impl(Context *context, LocalCursor *cursor,
                    ups_key_t *key, uint32_t flags);

    // returns the next record number
    uint64_t next_record_number() {
      _current_record_number++;
      if (config.flags & UPS_RECORD_NUMBER32
            && _current_record_number > std::numeric_limits<uint32_t>::max())
        throw Exception(UPS_LIMITS_REACHED);
      else if (_current_record_number == 0)
        throw Exception(UPS_LIMITS_REACHED);
      return (_current_record_number);
    }

    // Checks if an insert operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ups_status_t check_insert_conflicts(Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags);

    // Checks if an erase operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ups_status_t check_erase_conflicts(Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags);

    // Increments dupe index of all cursors with a dupe index > |start|;
    // only cursor |skip| is ignored
    void increment_dupe_index(Context *context, TxnNode *node,
                    LocalCursor *skip, uint32_t start);

    // Sets all cursors attached to a TxnNode to nil
    void nil_all_cursors_in_node(LocalTxn *txn, LocalCursor *current,
                    TxnNode *node);

    // Sets all cursors to nil if they point to |key| in the btree index
    void nil_all_cursors_in_btree(Context *context, LocalCursor *current,
                    ups_key_t *key);

    // the current record number
    uint64_t _current_record_number;
};

} // namespace upscaledb

#endif /* UPS_DB_LOCAL_H */
