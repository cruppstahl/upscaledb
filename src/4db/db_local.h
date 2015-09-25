/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

namespace hamsterdb {

class TransactionNode;
class TransactionIndex;
class TransactionCursor;
class TransactionOperation;
class LocalEnvironment;
class LocalTransaction;

template<typename T>
class RecordNumberFixture;

//
// The database implementation for local file access
//
class LocalDatabase : public Database {
  public:
    enum {
      // The default threshold for inline records
      kInlineRecordThreshold = 32
    };

    // Constructor
    LocalDatabase(Environment *env, DatabaseConfiguration &config)
      : Database(env, config), m_recno(0), m_cmp_func(0) {
    }

    // Returns the btree index
    BtreeIndex *btree_index() {
      return (m_btree_index.get());
    }

    // Returns the transactional index
    TransactionIndex *txn_index() {
      return (m_txn_index.get());
    }

    // Returns the LocalEnvironment instance
    LocalEnvironment *lenv() {
      return ((LocalEnvironment *)m_env);
    }

    // Creates a new Database
    ups_status_t create(Context *context, PBtreeHeader *btree_header);

    // Opens an existing Database
    ups_status_t open(Context *context, PBtreeHeader *btree_header);

    // Erases this Database
    ups_status_t drop(Context *context);

    // Fills in the current metrics
    virtual void fill_metrics(ups_env_metrics_t *metrics);

    // Returns Database parameters (ups_db_get_parameters)
    virtual ups_status_t get_parameters(ups_parameter_t *param);

    // Checks Database integrity (ups_db_check_integrity)
    virtual ups_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual ups_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount);

    // Scans the whole database, applies a processor function
    virtual ups_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct);

    // Inserts a key/value pair (ups_db_insert, ups_cursor_insert)
    virtual ups_status_t insert(Cursor *cursor, Transaction *txn,
                    ups_key_t *key, ups_record_t *record, uint32_t flags);

    // Erase a key/value pair (ups_db_erase, ups_cursor_erase)
    virtual ups_status_t erase(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ups_db_find, ups_cursor_find)
    virtual ups_status_t find(Cursor *cursor, Transaction *txn, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ups_cursor_move)
    virtual ups_status_t cursor_move(Cursor *cursor, ups_key_t *key,
                    ups_record_t *record, uint32_t flags);

    // Inserts a key/record pair in a txn node; if cursor is not NULL it will
    // be attached to the new txn_op structure
    // TODO this should be private
    ups_status_t insert_txn(Context *context, ups_key_t *key,
                    ups_record_t *record, uint32_t flags,
                    TransactionCursor *cursor);

    // Returns the default comparison function
    ups_compare_func_t compare_func() {
      return (m_cmp_func);
    }

    // Sets the default comparison function (ups_db_set_compare_func)
    ups_status_t set_compare_func(ups_compare_func_t f) {
      if (m_config.key_type != UPS_TYPE_CUSTOM) {
        ups_trace(("ups_set_compare_func only allowed for UPS_TYPE_CUSTOM "
                        "databases!"));
        return (UPS_INV_PARAMETER);
      }
      m_cmp_func = f;
      return (0);
    }

    // Flushes a TransactionOperation to the btree
    // TODO should be private
    ups_status_t flush_txn_operation(Context *context, LocalTransaction *txn,
                    TransactionOperation *op);

    // Returns the compressor for compressing/uncompressing the records
    Compressor *get_record_compressor() {
      return (m_record_compressor.get());
    }

    // Returns the key compression algorithm
    int get_key_compression_algorithm() {
      return (m_key_compression_algo);
    }
  protected:
    friend class LocalCursor;

    // Copies the ups_record_t structure from |op| into |record|
    static ups_status_t copy_record(LocalDatabase *db, Transaction *txn,
                    TransactionOperation *op, ups_record_t *record);

    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a database; this is the actual implementation
    virtual ups_status_t close_impl(uint32_t flags);

    // Begins a new temporary Transaction
    LocalTransaction *begin_temp_txn();

    // Finalizes an operation by committing or aborting the |local_txn|
    // and clearing or flushing the Changeset.
    // Returns |status|.
    ups_status_t finalize(Context *context, ups_status_t status,
                    Transaction *local_txn);

  private:
    friend struct DbFixture;
    friend struct HamsterdbFixture;
    friend struct ExtendedKeyFixture;
    friend class RecordNumberFixture<uint32_t>;
    friend class RecordNumberFixture<uint64_t>;

    // Returns true if a (btree) key was erased in a Transaction
    bool is_key_erased(Context *context, ups_key_t *key);

    // Erases a key/record pair from a txn; on success, cursor will be set to
    // nil
    ups_status_t erase_txn(Context *context, ups_key_t *key, uint32_t flags,
                    TransactionCursor *cursor);

    // Lookup of a key/record pair in the Transaction index and in the btree,
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
    // Enables record compression for this database
    void enable_record_compression(Context *context, int algo);

    // Enables key compression for this database
    void enable_key_compression(Context *context, int algo);

    // returns the next record number
    uint64_t next_record_number() {
      m_recno++;
      if (m_config.flags & UPS_RECORD_NUMBER32
            && m_recno > std::numeric_limits<uint32_t>::max())
        throw Exception(UPS_LIMITS_REACHED);
      else if (m_recno == 0)
        throw Exception(UPS_LIMITS_REACHED);
      return (m_recno);
    }

    // Checks if an insert operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ups_status_t check_insert_conflicts(Context *context, TransactionNode *node,
                    ups_key_t *key, uint32_t flags);

    // Checks if an erase operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ups_status_t check_erase_conflicts(Context *context, TransactionNode *node,
                    ups_key_t *key, uint32_t flags);

    // Increments dupe index of all cursors with a dupe index > |start|;
    // only cursor |skip| is ignored
    void increment_dupe_index(Context *context, TransactionNode *node,
                    LocalCursor *skip, uint32_t start);

    // Sets all cursors attached to a TransactionNode to nil
    void nil_all_cursors_in_node(LocalTransaction *txn, LocalCursor *current,
                    TransactionNode *node);

    // Sets all cursors to nil if they point to |key| in the btree index
    void nil_all_cursors_in_btree(Context *context, LocalCursor *current,
                    ups_key_t *key);

    // the current record number
    uint64_t m_recno;

    // the btree index
    ScopedPtr<BtreeIndex> m_btree_index;

    // the transaction index
    ScopedPtr<TransactionIndex> m_txn_index;

    // the comparison function
    ups_compare_func_t m_cmp_func;

    // The record compressor; can be null
    std::auto_ptr<Compressor> m_record_compressor;

    // The key compression algorithm
    int m_key_compression_algo;
};

} // namespace hamsterdb

#endif /* UPS_DB_LOCAL_H */
