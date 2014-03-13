/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#ifndef HAM_DB_LOCAL_H__
#define HAM_DB_LOCAL_H__

#include "db.h"

namespace hamsterdb {

class BtreeIndex;
class TransactionNode;
class TransactionIndex;
class TransactionCursor;
class TransactionOperation;
class LocalEnvironment;
class LocalTransaction;

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
    LocalDatabase(Environment *env, ham_u16_t name, ham_u32_t flags)
      : Database(env, name, flags), m_recno(0), m_btree_index(0),
        m_txn_index(0), m_cmp_func(0) {
    }

    // Returns the btree index
    BtreeIndex *get_btree_index() {
      return (m_btree_index);
    }

    // Returns the transactional index
    TransactionIndex *get_txn_index() {
      return (m_txn_index);
    }

    // Returns the LocalEnvironment instance
    LocalEnvironment *get_local_env() {
      return ((LocalEnvironment *)m_env);
    }

    // Opens an existing Database
    virtual ham_status_t open(ham_u16_t descriptor);

    // Creates a new Database
    virtual ham_status_t create(ham_u16_t descriptor, ham_u16_t key_type,
                        ham_u16_t key_size, ham_u32_t rec_size);

    // Erases this Database
    void erase_me();

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(ham_u32_t flags);

    // Returns the number of keys (ham_db_get_key_count)
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount);

    // Inserts a key/value pair (ham_db_insert)
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erase a key/value pair (ham_db_erase)
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags);

    // Lookup of a key/value pair (ham_db_find)
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Inserts a key with a cursor (ham_cursor_insert)
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Erases the key of a cursor (ham_cursor_erase)
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags);

    // Positions the cursor on a key and returns the record (ham_cursor_find)
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor,
                    ham_u32_t *count, ham_u32_t flags);

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size);

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags);

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    // Inserts a key/record pair in a txn node; if cursor is not NULL it will
    // be attached to the new txn_op structure
    // TODO this should be private
    ham_status_t insert_txn(LocalTransaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags,
                TransactionCursor *cursor);

    // Erases a key/record pair from a txn; on success, cursor will be set to
    // nil
    // TODO should be private
    ham_status_t erase_txn(LocalTransaction *txn, ham_key_t *key,
                ham_u32_t flags, TransactionCursor *cursor);

    // Returns the default comparison function
    ham_compare_func_t get_compare_func() {
      return (m_cmp_func);
    }

    // Sets the default comparison function (ham_db_set_compare_func)
    ham_status_t set_compare_func(ham_compare_func_t f) {
      if (get_key_type() != HAM_TYPE_CUSTOM) {
        ham_trace(("ham_set_compare_func only allowed for HAM_TYPE_CUSTOM "
                        "databases!"));
        return (HAM_INV_PARAMETER);
      }
      m_cmp_func = f;
      return (0);
    }

    // Returns the key type (set with HAM_PARAM_KEY_TYPE)
    ham_u16_t get_key_type();

    // Returns the key size of the btree
    ham_u16_t get_key_size();

    // Returns the record size specified by the user (or
    // HAM_RECORD_SIZE_UNLIMITED if none was specified)
    ham_u32_t get_record_size();

    // Flushes a TransactionOperation to the btree
    ham_status_t flush_txn_operation(LocalTransaction *txn,
                    TransactionOperation *op);

  protected:
    // Copies the ham_record_t structure from |op| into |record|
    static ham_status_t copy_record(LocalDatabase *db, Transaction *txn,
                    TransactionOperation *op, ham_record_t *record);

    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, ham_u32_t flags);

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src);

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c);

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(ham_u32_t flags);

  private:
    friend struct DbFixture;
    friend struct HamsterdbFixture;
    friend struct ExtendedKeyFixture;

    // returns the next record number
    ham_u64_t get_incremented_recno() {
      return (++m_recno);
    }

    // Checks if an insert operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_insert_conflicts(LocalTransaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    // Checks if an erase operation conflicts with another txn; this is the
    // case if the same key is modified by another active txn.
    ham_status_t check_erase_conflicts(LocalTransaction *txn,
                TransactionNode *node, ham_key_t *key, ham_u32_t flags);

    // Increments dupe index of all cursors with a dupe index > |start|;
    // only cursor |skip| is ignored
    void increment_dupe_index(TransactionNode *node, Cursor *skip,
                    ham_u32_t start);

    // Sets all cursors attached to a TransactionNode to nil
    void nil_all_cursors_in_node(LocalTransaction *txn, Cursor *current,
                    TransactionNode *node);

    // Sets all cursors to nil if they point to |key| in the btree index
    void nil_all_cursors_in_btree(Cursor *current, ham_key_t *key);

    // Lookup of a key/record pair in the Transaction index and in the btree,
    // if transactions are disabled/not successful; copies the
    // record into |record|. Also performs approx. matching.
    ham_status_t find_txn(LocalTransaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags);

    // the current record number
    ham_u64_t m_recno;

    // the btree index
    BtreeIndex *m_btree_index;

    // the transaction index
    TransactionIndex *m_txn_index;

    // the comparison function
    ham_compare_func_t m_cmp_func;
};

} // namespace hamsterdb

#endif /* HAM_DB_LOCAL_H__ */
