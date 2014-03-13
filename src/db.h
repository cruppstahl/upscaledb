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

#ifndef HAM_DB_H__
#define HAM_DB_H__

#include "config.h"
#include "util.h"
#include "env.h"

// A helper structure; ham_db_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_db_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ham_db_t {
  int dummy;
};

namespace hamsterdb {

class Cursor;

// a macro to cast pointers to u64 and vice versa to avoid compiler
// warnings if the sizes of ptr and u64 are not equal
#if defined(HAM_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (ham_u8_t *)(int)p
#   define PTR_TO_U64(p)  (ham_u64_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

/*
 * An abstract base class for a Database; is overwritten for local and
 * remote implementations
 */
class Database
{
  public:
    enum {
      // The default number of indices in an Environment
      kMaxIndices1k = 32
    };

    // Constructor
    Database(Environment *env, ham_u16_t name, ham_u32_t flags);

    // Virtual destructor; can be overwritten by base-classes
    virtual ~Database() {
    }

    // Returns the Environment pointer
    Environment *get_env() {
      return (m_env);
    }

    // Returns the runtime-flags - the flags are "mixed" with the flags from
    // the Environment
    ham_u32_t get_rt_flags(bool raw = false) {
      if (raw)
        return (m_rt_flags);
      else
        return (m_env->get_flags() | m_rt_flags);
    }

    // Returns the database name
    ham_u16_t get_name() const {
      return (m_name);
    }

    // Sets the database name
    void set_name(ham_u16_t name) {
      m_name = name;
    }

    // Returns Database parameters (ham_db_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    // Checks Database integrity (ham_db_check_integrity)
    virtual ham_status_t check_integrity(ham_u32_t flags) = 0;

    // Returns the number of keys (ham_db_get_key_count)
    virtual ham_status_t get_key_count(Transaction *txn, ham_u32_t flags,
                    ham_u64_t *keycount) = 0;

    // Inserts a key/value pair (ham_db_insert)
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Erase a key/value pair (ham_db_erase)
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags) = 0;

    // Lookup of a key/value pair (ham_db_find)
    virtual ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Creates a cursor (ham_cursor_create)
    virtual Cursor *cursor_create(Transaction *txn, ham_u32_t flags);

    // Clones a cursor (ham_cursor_clone)
    virtual Cursor *cursor_clone(Cursor *src);

    // Inserts a key with a cursor (ham_cursor_insert)
    virtual ham_status_t cursor_insert(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Erases the key of a cursor (ham_cursor_erase)
    virtual ham_status_t cursor_erase(Cursor *cursor, ham_u32_t flags) = 0;

    // Positions the cursor on a key and returns the record (ham_cursor_find)
    virtual ham_status_t cursor_find(Cursor *cursor, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Returns number of duplicates (ham_cursor_get_record_count)
    virtual ham_status_t cursor_get_record_count(Cursor *cursor,
                    ham_u32_t *count, ham_u32_t flags) = 0;

    // Get current record size (ham_cursor_get_record_size)
    virtual ham_status_t cursor_get_record_size(Cursor *cursor,
                    ham_u64_t *size) = 0;

    // Overwrites the record of a cursor (ham_cursor_overwrite)
    virtual ham_status_t cursor_overwrite(Cursor *cursor,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Moves a cursor, returns key and/or record (ham_cursor_move)
    virtual ham_status_t cursor_move(Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags) = 0;

    // Closes a cursor (ham_cursor_close)
    void cursor_close(Cursor *cursor);

    // Closes the Database (ham_db_close)
    ham_status_t close(ham_u32_t flags);

    // Returns the last error code
    ham_status_t get_error() const {
      return (m_error);
    }

    // Sets the last error code
    ham_status_t set_error(ham_status_t e) {
      return ((m_error = e));
    }

    // Returns the user-provided context pointer (ham_get_context_data)
    void *get_context_data() {
      return (m_context);
    }

    // Sets the user-provided context pointer (ham_set_context_data)
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    // Returns the head of the linked list with all cursors
    Cursor *get_cursor_list() {
      return (m_cursor_list);
    }

    // Returns the memory buffer for the key data
    ByteArray &get_key_arena() {
      return (m_key_arena);
    }

    // Returns the memory buffer for the record data
    ByteArray &get_record_arena() {
      return (m_record_arena);
    }

  protected:
    // Creates a cursor; this is the actual implementation
    virtual Cursor *cursor_create_impl(Transaction *txn, ham_u32_t flags) = 0;

    // Clones a cursor; this is the actual implementation
    virtual Cursor *cursor_clone_impl(Cursor *src) = 0;

    // Closes a cursor; this is the actual implementation
    virtual void cursor_close_impl(Cursor *c) = 0;

    // Closes a database; this is the actual implementation
    virtual ham_status_t close_impl(ham_u32_t flags) = 0;

    // the current Environment
    Environment *m_env;

    // the Database name
    ham_u16_t m_name;

    // the last error code
    ham_status_t m_error;

    // the user-provided context data
    void *m_context;

    // linked list of all cursors
    Cursor *m_cursor_list;

    // The database flags - a combination of the persistent flags
    // and runtime flags
    ham_u32_t m_rt_flags;

    // This is where key->data points to when returning a
    // key to the user; used if Transactions are disabled
    ByteArray m_key_arena;

    // This is where record->data points to when returning a
    // record to the user; used if Transactions are disabled
    ByteArray m_record_arena;
};

} // namespace hamsterdb

#endif /* HAM_DB_H__ */
