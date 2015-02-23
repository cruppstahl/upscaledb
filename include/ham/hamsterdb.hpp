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

/**
 * @file hamsterdb.hpp
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.1.10
 *
 * This C++ wrapper class is a very tight wrapper around the C API. It does
 * not attempt to be STL compatible.
 *
 * All functions throw exceptions of class @sa ham::error in case of an error.
 * Please refer to the C API documentation for more information. You can find
 * it here: http://hamsterdb.com/?page=doxygen&module=globals.html
 *
 */

#ifndef HAM_HAMSTERDB_HPP
#define HAM_HAMSTERDB_HPP

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>
#include <cstring>
#include <vector>

#if defined(_MSC_VER) && defined(_DEBUG) && !defined(_CRTDBG_MAP_ALLOC)
#  define _CRTDBG_MAP_ALLOC
#  include <crtdbg.h>
#endif

/**
 * @defgroup ham_cpp hamsterdb C++ API wrapper
 * @{
 */

/**
 * The global hamsterdb namespace.
 */
namespace hamsterdb {

class txn;
class db;
class env;

/**
 * An error class.
 *
 * The hamsterdb C++ API throws this class as Exceptions.
 */
class error {
  public:
    /** Constructor */
    error(ham_status_t st)
      : m_errno(st) {
    };

    /** Returns the error code. */
    ham_status_t get_errno() const {
        return (m_errno);
    }

    /** Returns an English error description. */
    const char *get_string() const {
        return (ham_strerror(m_errno));
    }

private:
    ham_status_t m_errno;
};

/**
 * A key class.
 *
 * This class wraps structures of type ham_key_t.
 */
class key {
  public:
    /** Constructor */
    key(void *data = 0, uint16_t size = 0, uint32_t flags = 0) {
      memset(&m_key, 0, sizeof(m_key));
      m_key.data = data;
      m_key.size = size;
      m_key.flags = flags;
      if (m_key.size != size) // check for overflow
        throw error(HAM_INV_KEYSIZE);
    }

    /** Copy constructor. */
    key(const key &other)
      : m_key(other.m_key) {
    }

    /** Assignment operator. */
    key &operator=(const key &other) {
      if (&other != this)
        m_key = other.m_key;
      return (*this);
    }

    /** Returns the key data. */
    void *get_data() const {
      return (m_key.data);
    }

    /** Sets the key data. */
    void set_data(void *data) {
      m_key.data = data;
    }

    /** Returns the size of the key. */
    uint16_t get_size() const {
      return (m_key.size);
    }

    /** Sets the size of the key. */
    void set_size(uint16_t size) {
      m_key.size = size;
    }

    /** Template assignment */
    template <class T>
    void set(T &t) {
      set_data(&t);
      set_size(sizeof(t));
    }

    /** Returns the flags of the key. */
    uint32_t get_flags() const {
      return (m_key.flags);
    }

    /** Sets the flags of the key. */
    void set_flags(uint32_t flags) {
      m_key.flags = flags;
    }

    /** Returns a pointer to the internal ham_key_t structure. */
    ham_key_t *get_handle() {
      return (&m_key);
    }

    /** Returns 'sign' of Approximate Match */
    int get_approximate_match_type() {
      return (ham_key_get_approximate_match_type(&m_key));
    }

private:
    ham_key_t m_key;
};

/**
 * A record class.
 *
 * This class wraps structures of type ham_record_t.
 */
class record {
  public:
    /** Constructor */
    record(void *data = 0, uint32_t size = 0, uint32_t flags = 0) {
      memset(&m_rec, 0, sizeof(m_rec));
      m_rec.data = data;
      m_rec.size = size;
      m_rec.flags = flags;
    }

    /** Copy constructor. */
    record(const record &other)
      : m_rec(other.m_rec) {
    }

    /** Assignment operator. */
    record &operator=(const record &other) {
      m_rec = other.m_rec;
      return (*this);
    }

    /** Returns the record data. */
    void *get_data() const {
      return (m_rec.data);
    }

    /** Sets the record data. */
    void set_data(void *data) {
      m_rec.data = data;
    }

    /** Returns the size of the record. */
    uint32_t get_size() const {
      return (m_rec.size);
    }

    /** Sets the size of the record. */
    void set_size(uint32_t size) {
      m_rec.size = size;
    }

    /** Returns the flags of the record. */
    uint32_t get_flags() const {
      return (m_rec.flags);
    }

    /** Sets the flags of the record. */
    void set_flags(uint32_t flags) {
      m_rec.flags = flags;
    }

    /** Returns a pointer to the internal ham_record_t structure. */
    ham_record_t *get_handle() {
      return (&m_rec);
    }

  protected:
    ham_record_t m_rec;
};


/**
 * A Transaction class
 *
 * This class wraps structures of type ham_txn_t.
 */
class txn {
  public:
    /** Constructor */
    txn(ham_txn_t *t = 0)
      : m_txn(t) {
    }

    /** Abort the Transaction */
    void abort() {
      ham_status_t st = ham_txn_abort(m_txn, 0);
      if (st)
        throw error(st);
    }

    /** Commit the Transaction */
    void commit() {
      ham_status_t st = ham_txn_commit(m_txn, 0);
      if (st)
        throw error(st);
    }

    std::string get_name() {
      const char *p = ham_txn_get_name(m_txn);
      return (p ? p : "");
    }

    /** Returns a pointer to the internal ham_txn_t structure. */
    ham_txn_t *get_handle() {
      return (m_txn);
    }

  protected:
    ham_txn_t *m_txn;
};


/**
 * A Database class.
 *
 * This class wraps the ham_db_t Database handles.
 */
class db {
  public:
    /** Set error handler function. */
    static void set_errhandler(ham_errhandler_fun f) {
      ham_set_errhandler(f);
    }

    /** Retrieves the hamsterdb library version. */
    static void get_version(uint32_t *major, uint32_t *minor,
                  uint32_t *revision) {
      ham_get_version(major, minor, revision);
    }

    /** Constructor */
    db()
      : m_db(0) {
    }

    /** Destructor - automatically closes the Database, if necessary. */
    ~db() {
      close();
    }

    /**
     * Assignment operator.
     *
     * <b>Important!</b> This operator transfers the ownership of the
     * Database handle.
     */
    db &operator=(const db &other) {
      db &rhs = (db &)other;
      if (this == &other)
        return (*this);
      close();
      m_db = rhs.m_db;
      rhs.m_db = 0;
      return (*this);
    }

    /** Returns the last Database error. */
    ham_status_t get_error() {
      return (ham_db_get_error(m_db));
    }

    /** Sets the comparison function. */
    void set_compare_func(ham_compare_func_t foo) {
      ham_status_t st = ham_db_set_compare_func(m_db, foo);
      if (st)
        throw error(st);
    }

    /** Finds a record by looking up the key. */
    record find(txn *t, key *k, uint32_t flags = 0) {
      record r;
      ham_status_t st = ham_db_find(m_db,
                t ? t->get_handle() : 0,
                k ? k->get_handle() : 0,
                r.get_handle(), flags);
      if (st)
        throw error(st);
      return (r);
    }

    /** Finds a record by looking up the key. */
    record &find(txn *t, key *k, record *r, uint32_t flags = 0) {
      ham_status_t st = ham_db_find(m_db,
                t ? t->get_handle() : 0,
                k ? k->get_handle() : 0,
                r->get_handle(), flags);
      if (st)
        throw error(st);
      return (*r);
    }

    /** Finds a record by looking up the key. */
    record find(key *k, uint32_t flags = 0) {
      return (find(0, k, flags));
    }

    /** Inserts a key/record pair. */
    void insert(txn *t, key *k, record *r, uint32_t flags = 0) {
      ham_status_t st = ham_db_insert(m_db,
                t ? t->get_handle() : 0,
                k ? k->get_handle() : 0,
                r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Inserts a key/record pair. */
    void insert(key *k, record *r, uint32_t flags=0) {
      insert(0, k, r, flags);
    }

    /** Erases a key/record pair. */
    void erase(key *k, uint32_t flags = 0) {
      erase(0, k, flags);
    }

    /** Erases a key/record pair. */
    void erase(txn *t, key *k, uint32_t flags = 0) {
      ham_status_t st = ham_db_erase(m_db,
                t ? t->get_handle() : 0,
                k ? k->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Returns number of items in the Database. */
    uint64_t get_key_count(ham_txn_t *txn = 0, uint32_t flags = 0) {
      uint64_t count = 0;
      ham_status_t st = ham_db_get_key_count(m_db, txn, flags, &count);
      if (st)
        throw error(st);
      return (count);
    }

    /** Retrieves Database parameters. */
    void get_parameters(ham_parameter_t *param) {
      ham_status_t st = ham_db_get_parameters(m_db, param);
      if (st)
        throw error(st);
    }

    /** Closes the Database. */
    void close(uint32_t flags = 0) {
      if (!m_db)
        return;
      // disable auto-cleanup; all objects will be destroyed when 
      // going out of scope
      flags &= ~HAM_AUTO_CLEANUP;
      ham_status_t st = ham_db_close(m_db, flags);
      if (st)
        throw error(st);
      m_db = 0;
    }

    /** Returns a pointer to the internal ham_db_t structure. */
    ham_db_t *get_handle() {
      return (m_db);
    }

protected:
    friend class env;

    /* Copy Constructor. Is protected and should not be used. */
    db(ham_db_t *db)
      : m_db(db) {
    }

  private:
    ham_db_t *m_db;
};


/**
 * A Database Cursor.
 *
 * This class wraps the ham_cursor_t Cursor handles.
 */
class cursor {
  public:
    /** Constructor */
    cursor(db *db = 0, txn *t = 0, uint32_t flags = 0)
      : m_cursor(0) {
      create(db, t, flags);
    }

    /** Constructor */
    cursor(txn *t, db *db = 0, uint32_t flags = 0)
      : m_cursor(0) {
      create(db, t, flags);
    }

    /** Destructor - automatically closes the Cursor, if necessary. */
    ~cursor() {
      close();
    }

    /** Creates a new Cursor. */
    void create(db *db, txn *t = 0, uint32_t flags = 0) {
      if (m_cursor)
        close();
      if (db) {
        ham_status_t st = ham_cursor_create(&m_cursor, db->get_handle(),
                    t ? t->get_handle() : 0, flags);
        if (st)
          throw error(st);
      }
    }

    /** Clones the Cursor. */
    cursor clone() {
      ham_cursor_t *dest;
      ham_status_t st = ham_cursor_clone(m_cursor, &dest);
      if (st)
        throw error(st);
      return (cursor(dest));
    }

    /** Moves the Cursor, and retrieves the key/record of the new position. */
    void move(key *k, record *r, uint32_t flags = 0) {
      ham_status_t st = ham_cursor_move(m_cursor, k ? k->get_handle() : 0,
                        r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Moves the Cursor to the first Database element. */
    void move_first(key *k = 0, record *r = 0) {
      move(k, r, HAM_CURSOR_FIRST);
    }

    /** Moves the Cursor to the last Database element. */
    void move_last(key *k = 0, record *r = 0) {
      move(k, r, HAM_CURSOR_LAST);
    }

    /** Moves the Cursor to the next Database element. */
    void move_next(key *k = 0, record *r = 0) {
      move(k, r, HAM_CURSOR_NEXT);
    }

    /** Moves the Cursor to the previous Database element. */
    void move_previous(key *k = 0, record *r = 0) {
      move(k, r, HAM_CURSOR_PREVIOUS);
    }

    /** Overwrites the current record. */
    void overwrite(record *r, uint32_t flags = 0) {
      ham_status_t st = ham_cursor_overwrite(m_cursor,
                            r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Finds a key. */
    void find(key *k, record *r = 0, uint32_t flags = 0) {
      ham_status_t st = ham_cursor_find(m_cursor, k->get_handle(),
                        (r ? r->get_handle() : 0), flags);
      if (st)
        throw error(st);
    }

    /** Inserts a key/record pair. */
    void insert(key *k, record *r, uint32_t flags = 0) {
      ham_status_t st = ham_cursor_insert(m_cursor, k ? k->get_handle() : 0,
                          r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Erases the current key/record pair. */
    void erase(uint32_t flags = 0) {
      ham_status_t st = ham_cursor_erase(m_cursor, flags);
      if (st)
        throw error(st);
    }

    /** Returns the number of duplicate keys. */
    uint32_t get_duplicate_count(uint32_t flags = 0) {
      uint32_t c;
      ham_status_t st = ham_cursor_get_duplicate_count(m_cursor, &c, flags);
      if (st)
        throw error(st);
      return (c);
    }

    /** Returns the size of the current record. */
    uint64_t get_record_size() {
      uint64_t s;
      ham_status_t st = ham_cursor_get_record_size(m_cursor, &s);
      if (st)
        throw error(st);
      return (s);
    }

    /** Closes the Cursor. */
    void close() {
      if (!m_cursor)
        return;
      ham_status_t st = ham_cursor_close(m_cursor);
      if (st)
        throw error(st);
      m_cursor = 0;
    }

  protected:
    /* Copy Constructor. Is protected and should not be used. */
    cursor(ham_cursor_t *c) {
      m_cursor = c;
    }

  private:
    ham_cursor_t *m_cursor;
};

/**
 * An Environment class.
 *
 * This class wraps the ham_env_t structure.
 */
class env {
  public:
    /** Constructor */
    env()
      : m_env(0) {
    }

    /** Destructor - automatically closes the Cursor, if necessary. */
    ~env() {
      close();
    }

    /** Creates a new Environment. */
    void create(const char *filename, uint32_t flags = 0,
                uint32_t mode = 0644, const ham_parameter_t *param = 0) {
      ham_status_t st = ham_env_create(&m_env, filename, flags, mode, param);
      if (st)
        throw error(st);
    }

    /** Opens an existing Environment. */
    void open(const char *filename, uint32_t flags = 0,
                const ham_parameter_t *param = 0) {
      ham_status_t st = ham_env_open(&m_env, filename, flags, param);
      if (st)
        throw error(st);
    }

    /** Flushes the Environment to disk. */
    void flush(uint32_t flags = 0) {
      ham_status_t st = ham_env_flush(m_env, flags);
      if (st)
        throw error(st);
    }

    /** Creates a new Database in the Environment. */
    db create_db(uint16_t name, uint32_t flags = 0,
                const ham_parameter_t *param = 0) {
      ham_db_t *dbh;

      ham_status_t st = ham_env_create_db(m_env, &dbh, name, flags, param);
      if (st)
        throw error(st);

      return (hamsterdb::db(dbh));
    }

    /** Opens an existing Database in the Environment. */
    db open_db(uint16_t name, uint32_t flags = 0,
                const ham_parameter_t *param = 0) {
      ham_db_t *dbh;

      ham_status_t st = ham_env_open_db(m_env, &dbh, name, flags, param);
      if (st)
        throw error(st);

      return (hamsterdb::db(dbh));
    }

    /** Renames an existing Database in the Environment. */
    void rename_db(uint16_t oldname, uint16_t newname, uint32_t flags = 0) {
      ham_status_t st = ham_env_rename_db(m_env, oldname, newname, flags);
      if (st)
        throw error(st);
    }

    /** Deletes a Database from the Environment. */
    void erase_db(uint16_t name, uint32_t flags = 0) {
      ham_status_t st = ham_env_erase_db(m_env, name, flags);
      if (st)
        throw error(st);
    }

    /** Begin a new Transaction */
    txn begin(const char *name = 0) {
      ham_txn_t *h;
      ham_status_t st = ham_txn_begin(&h, m_env, name, 0, 0);
      if (st)
        throw error(st);
      return (txn(h));
    }


    /** Closes the Environment. */
    void close(uint32_t flags = 0) {
      if (!m_env)
        return;
      // disable auto-cleanup; all objects will be destroyed when 
      // going out of scope
      flags &= ~HAM_AUTO_CLEANUP;
      ham_status_t st = ham_env_close(m_env, flags);
      if (st)
        throw error(st);
      m_env = 0;
    }

    /** Retrieves Environment parameters. */
    void get_parameters(ham_parameter_t *param) {
      ham_status_t st = ham_env_get_parameters(m_env, param);
      if (st)
        throw error(st);
    }

    /** Get all Database names. */
    std::vector<uint16_t> get_database_names() {
      uint32_t count = 32;
      ham_status_t st;
      std::vector<uint16_t> v(count);

      for (;;) {
        st = ham_env_get_database_names(m_env, &v[0], &count);
        if (!st)
          break;
        if (st && st!=HAM_LIMITS_REACHED)
          throw error(st);
        count += 16;
        v.resize(count);
      }

      v.resize(count);
      return (v);
    }

  private:
    ham_env_t *m_env;
};

} // namespace hamsterdb

/**
 * @}
 */

#endif // HAMSTERDB_HPP
