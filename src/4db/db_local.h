/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @exception_safe: unknown
 * @thread_safe: no
 */

#ifndef HAM_DB_LOCAL_H
#define HAM_DB_LOCAL_H

#include "0root/root.h"

#include <limits>

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
#include "3btree/btree_index.h"
#include "4txn/txn_local.h"
#include "4db/db.h"

#ifndef HAM_ROOT_H
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
    ham_status_t create(Context *context, PBtreeHeader *btree_header);

    // Opens an existing Database
    ham_status_t open(Context *context, PBtreeHeader *btree_header);

    // Erases this Database
    ham_status_t drop(Context *context);

    // Fills in the current metrics
    virtual void fill_metrics(ham_env_metrics_t *metrics);

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(uint32_t flags);

    // Returns the number of keys
    virtual ham_status_t count(Transaction *txn, bool distinct,
                    uint64_t *pcount);

    // Scans the whole database, applies a processor function
    virtual ham_status_t scan(Transaction *txn, ScanVisitor *visitor,
                    bool distinct);

    // Inserts a key/value pair (ham_db_insert, ham_cursor_insert)
    virtual ham_status_t insert(Cursor *cursor, Transaction *txn,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // Erase a key/value pair (ham_db_erase, ham_cursor_erase)
    virtual ham_status_t erase(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    uint32_t flags);

    // Lookup of a key/value pair (ham_db_find, ham_cursor_find)
    virtual ham_status_t find(Cursor *cursor, Transaction *txn, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor, uint32_t flags,
                    uint32_t *pcount);

    // Returns position in duplicate list (ham_cursor_get_duplicate_position)
    virtual ham_status_t cursor_get_duplicate_position(Cursor *cursor,
                    uint32_t *pposition);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    uint64_t *psize);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, uint32_t flags);

    // Inserts a key/record pair in a txn node; if cursor is not NULL it will
    // be attached to the new txn_op structure
    // TODO this should be private
    ham_status_t insert_txn(Context *context, ham_key_t *key,
                    ham_record_t *record, uint32_t flags,
                    TransactionCursor *cursor);

    // Returns the default comparison function
    ham_compare_func_t compare_func() {
      return (m_cmp_func);
    }

    // Sets the default comparison function (ham_db_set_compare_func)
    ham_status_t set_compare_func(ham_compare_func_t f) {
      if (m_config.key_type != HAM_TYPE_CUSTOM) {
        ham_trace(("ham_set_compare_func only allowed for HAM_TYPE_CUSTOM "
                        "databases!"));
        return (HAM_INV_PARAMETER);
      }
      m_cmp_func = f;
      return (0);
    }

    // Flushes a TransactionOperation to the btree
    // TODO should be private
    ham_status_t flush_txn_operation(Context *context, LocalTransaction *txn,
                    TransactionOperation *op);

  protected:
    friend class Cursor;

    // Copies the ham_record_t structure from |op| into |record|
    static ham_status_t copy_record(LocalDatabase *db, Transaction *txn,
                    TransactionOperation *op, ham_record_t *record);

    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, uint32_t flags);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c);

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(uint32_t flags);

  private:
    friend struct DbFixture;
    friend struct HamsterdbFixture;
    friend struct ExtendedKeyFixture;
    friend class RecordNumberFixture<uint32_t>;
    friend class RecordNumberFixture<uint64_t>;

    // Erases a key/record pair from a txn; on success, cursor will be set to
    // nil
    ham_status_t erase_txn(Context *context, ham_key_t *key, uint32_t flags,
                    TransactionCursor *cursor);

    // Lookup of a key/record pair in the Transaction index and in the btree,
    // if transactions are disabled/not successful; copies the
    // record into |record|. Also performs approx. matching.
    ham_status_t find_txn(Context *context, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    ham_status_t cursor_move_impl(Context *context, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // The actual implementation of insert()
    ham_status_t insert_impl(Context *context, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // The actual implementation of find()
    ham_status_t find_impl(Context *context, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, uint32_t flags);

    // The actual implementation of erase()
    ham_status_t erase_impl(Context *context, Cursor *cursor,
                    ham_key_t *key, uint32_t flags);

    // Finalizes an operation by committing or aborting the |local_txn|
    // and clearing or flushing the Changeset.
    // Returns |status|.
    ham_status_t finalize(Context *context, ham_status_t status,
                    Transaction *local_txn);

    // Begins a new temporary Transaction
    LocalTransaction *begin_temp_txn();

    // returns the next record number
    uint64_t next_record_number() {
      m_recno++;
      if (m_config.flags & HAM_RECORD_NUMBER32
            && m_recno > std::numeric_limits<uint32_t>::max())
        throw Exception(HAM_LIMITS_REACHED);
      else if (m_recno == 0)
        throw Exception(HAM_LIMITS_REACHED);
      return (m_recno);
    }

    // Checks if an insert operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_insert_conflicts(Context *context, TransactionNode *node,
                    ham_key_t *key, uint32_t flags);

    // Checks if an erase operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_erase_conflicts(Context *context, TransactionNode *node,
                    ham_key_t *key, uint32_t flags);

    // Increments dupe index of all cursors with a dupe index > |start|;
    // only cursor |skip| is ignored
    void increment_dupe_index(Context *context, TransactionNode *node,
                    Cursor *skip, uint32_t start);

    // Sets all cursors attached to a TransactionNode to nil
    void nil_all_cursors_in_node(LocalTransaction *txn, Cursor *current,
                    TransactionNode *node);

    // Sets all cursors to nil if they point to |key| in the btree index
    void nil_all_cursors_in_btree(Context *context, Cursor *current,
                    ham_key_t *key);

    // the current record number
    uint64_t m_recno;

    // the btree index
    ScopedPtr<BtreeIndex> m_btree_index;

    // the transaction index
    ScopedPtr<TransactionIndex> m_txn_index;

    // the comparison function
    ham_compare_func_t m_cmp_func;
};

} // namespace hamsterdb

#endif /* HAM_DB_LOCAL_H */
