/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/**
 * @file upscaledb.hpp
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.2.1
 *
 * This C++ wrapper class is a very tight wrapper around the C API. It does
 * not attempt to be STL compatible.
 *
 * All functions throw exceptions of class @sa ups::error in case of an error.
 * Please refer to the C API documentation for more information. You can find
 * it here: http://upscaledb.com/?page=doxygen&module=globals.html
 *
 */

#ifndef UPS_UPSCALEDB_HPP
#define UPS_UPSCALEDB_HPP

#include <ups/upscaledb.h>
#include <ups/upscaledb_int.h>
#include <cstring>
#include <cassert>
#include <vector>

#if defined(_MSC_VER) && defined(_DEBUG) && !defined(_CRTDBG_MAP_ALLOC)
#  define _CRTDBG_MAP_ALLOC
#  include <crtdbg.h>
#endif

/**
 * @defgroup ups_cpp upscaledb C++ API wrapper
 * @{
 */

/**
 * The global upscaledb namespace.
 */
namespace upscaledb {

class txn;
class db;
class env;

/**
 * An error class.
 *
 * The upscaledb C++ API throws this class as Exceptions.
 */
class error {
  public:
    /** Constructor */
    error(ups_status_t st)
      : _status(st) {
    };

    /** Returns the error code. */
    ups_status_t get_errno() const {
      return _status;
    }

    /** Returns an English error description. */
    const char *get_string() const {
      return ups_strerror(_status);
    }

  private:
    ups_status_t _status;
};

/**
 * A key class.
 *
 * This class wraps structures of type ups_key_t.
 */
class key {
  public:
    /** Constructor */
    key(void *data = 0, uint16_t size = 0, uint32_t flags = 0) {
      ::memset(&_key, 0, sizeof(_key));
      _key.data = data;
      _key.size = size;
      _key.flags = flags;
    }

    /** Copy constructor. */
    key(const key &other)
      : _key(other._key) {
    }

    /** Assignment operator. */
    key &operator=(const key &other) {
      if (&other != this)
        _key = other._key;
      return *this;
    }

    /** Returns the key data. */
    void *get_data() const {
      return _key.data;
    }

    /** Sets the key data. */
    void set_data(void *data) {
      _key.data = data;
    }

    /** Returns the size of the key. */
    uint16_t get_size() const {
      return _key.size;
    }

    /** Sets the size of the key. */
    void set_size(uint16_t size) {
      _key.size = size;
    }

    /** Template assignment */
    template <class T>
    void set(T &t) {
      set_data(&t);
      set_size(sizeof(t));
    }

    /** Returns the flags of the key. */
    uint32_t get_flags() const {
      return _key.flags;
    }

    /** Sets the flags of the key. */
    void set_flags(uint32_t flags) {
      _key.flags = flags;
    }

    /** Returns a pointer to the internal ups_key_t structure. */
    ups_key_t *get_handle() {
      return &_key;
    }

  private:
    ups_key_t _key;
};

/**
 * A record class.
 *
 * This class wraps structures of type ups_record_t.
 */
class record {
  public:
    /** Constructor */
    record(void *data = 0, uint32_t size = 0, uint32_t flags = 0) {
      memset(&_record, 0, sizeof(_record));
      _record.data = data;
      _record.size = size;
      _record.flags = flags;
    }

    /** Copy constructor. */
    record(const record &other)
      : _record(other._record) {
    }

    /** Assignment operator. */
    record &operator=(const record &other) {
      _record = other._record;
      return (*this);
    }

    /** Returns the record data. */
    void *get_data() const {
      return _record.data;
    }

    /** Sets the record data. */
    void set_data(void *data) {
      _record.data = data;
    }

    /** Returns the size of the record. */
    uint32_t get_size() const {
      return _record.size;
    }

    /** Sets the size of the record. */
    void set_size(uint32_t size) {
      _record.size = size;
    }

    /** Returns the flags of the record. */
    uint32_t get_flags() const {
      return _record.flags;
    }

    /** Sets the flags of the record. */
    void set_flags(uint32_t flags) {
      _record.flags = flags;
    }

    /** Returns a pointer to the internal ups_record_t structure. */
    ups_record_t *get_handle() {
      return &_record;
    }

  private:
    ups_record_t _record;
};


/**
 * A Transaction class
 *
 * This class wraps structures of type ups_txn_t.
 */
class txn {
  public:
    /** Constructor */
    txn(ups_txn_t *t = 0)
      : _txn(t) {
    }

    /** Abort the Txn */
    void abort() {
      ups_status_t st = ups_txn_abort(_txn, 0);
      if (st)
        throw error(st);
    }

    /** Commit the Txn */
    void commit() {
      ups_status_t st = ups_txn_commit(_txn, 0);
      if (st)
        throw error(st);
    }

    std::string get_name() {
      const char *p = ups_txn_get_name(_txn);
      return p ? p : "";
    }

    /** Returns a pointer to the internal ups_txn_t structure. */
    ups_txn_t *get_handle() {
      return _txn;
    }

  private:
    ups_txn_t *_txn;
};


/**
 * A Database class.
 *
 * This class wraps the ups_db_t Database handles.
 */
class db {
  public:
    /** Set error handler function. */
    static void set_errhandler(ups_error_handler_fun f) {
      ups_set_error_handler(f);
    }

    /** Retrieves the upscaledb library version. */
    static void get_version(uint32_t *major, uint32_t *minor,
                  uint32_t *revision) {
      ups_get_version(major, minor, revision);
    }

    /** Constructor */
    db()
      : _db(0) {
    }

    /**
     * Destructor - automatically closes the Database, if necessary.
     *
     * !!
     * Any exception is silently discarded. Use of the destructor to clean up
     * open databases is therefore not recommended, because there are valid
     * reasons why an Exception can be thrown (i.e. not all Cursors of this
     * database were closed).
     *
     * An assert() was added to catch this condition in debug builds.
     */
    ~db() {
      try {
        close();
      }
      catch (error &ex) {
        assert(ex.get_errno() == 0); // this will fail!
      }
    }

    /**
     * Assignment operator.
     *
     * <b>Important!</b> This operator transfers the ownership of the
     * Database handle.B
     */
    db &operator=(const db &other) {
      db &rhs = (db &)other;
      if (this == &other)
        return *this;
      close();
      _db = rhs._db;
      rhs._db = 0;
      return *this;
    }

    /** Sets the comparison function. */
    void set_compare_func(ups_compare_func_t foo) {
      ups_status_t st = ups_db_set_compare_func(_db, foo);
      if (st)
        throw error(st);
    }

    /** Finds a record by looking up the key. */
    record find(txn *t, key *k, uint32_t flags = 0) {
      record r;
      ups_status_t st = ups_db_find(_db, t ? t->get_handle() : 0,
                      k ? k->get_handle() : 0, r.get_handle(), flags);
      if (st)
        throw error(st);
      return r;
    }

    /** Finds a record by looking up the key. */
    record &find(txn *t, key *k, record *r, uint32_t flags = 0) {
      ups_status_t st = ups_db_find(_db, t ? t->get_handle() : 0,
                      k ? k->get_handle() : 0, r->get_handle(), flags);
      if (st)
        throw error(st);
      return *r;
    }

    /** Finds a record by looking up the key. */
    record find(key *k, uint32_t flags = 0) {
      return find(0, k, flags);
    }

    /** Inserts a key/record pair. */
    void insert(txn *t, key *k, record *r, uint32_t flags = 0) {
      ups_status_t st = ups_db_insert(_db, t ? t->get_handle() : 0,
                      k ? k->get_handle() : 0, r ? r->get_handle() : 0, flags);
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
      ups_status_t st = ups_db_erase(_db, t ? t->get_handle() : 0,
                      k ? k->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Returns number of items in the Database. */
    uint64_t count(ups_txn_t *txn = 0, uint32_t flags = 0) {
      uint64_t count = 0;
      ups_status_t st = ups_db_count(_db, txn, flags, &count);
      if (st)
        throw error(st);
      return count;
    }

    /** Retrieves Database parameters. */
    void get_parameters(ups_parameter_t *param) {
      ups_status_t st = ups_db_get_parameters(_db, param);
      if (st)
        throw error(st);
    }

    /** Closes the Database.
     * Ignores the flag |UPS_AUTO_CLEANUP|. All objects will be destroyed
     * automatically when they are destructed.
     */
    void close(uint32_t flags = 0) {
      if (!_db)
        return;
      flags &= ~UPS_AUTO_CLEANUP;
      ups_status_t st = ups_db_close(_db, flags);
      _db = 0;
      if (st)
        throw error(st);
    }

    /** Returns a pointer to the internal ups_db_t structure. */
    ups_db_t *get_handle() {
      return _db;
    }

  protected:
    friend class env;

    /* Copy Constructor. Is protected and should not be used. */
    db(ups_db_t *db)
      : _db(db) {
    }

  private:
    ups_db_t *_db;
};


/**
 * A Database Cursor.
 *
 * This class wraps the ups_cursor_t Cursor handles.
 */
class cursor {
  public:
    /** Constructor */
    cursor(db *db = 0, txn *t = 0, uint32_t flags = 0)
      : _cursor(0) {
      create(db, t, flags);
    }

    /** Constructor */
    cursor(txn *t, db *db = 0, uint32_t flags = 0)
      : _cursor(0) {
      create(db, t, flags);
    }

    /**
     * Destructor - automatically closes the Cursor, if necessary.
     *
     * !!
     * Any exception is silently discarded. Use of the destructor to close
     * cursors is therefore not recommended, because there are valid
     * reasons why an Exception can be thrown.
     *
     * An assert() was added to catch this condition in debug builds.
     */
    ~cursor() {
      try {
        close();
      }
      catch (error &ex) {
        assert(ex.get_errno() == 0); // this will fail!
      }
    }

    /** Creates a new Cursor. */
    void create(db *db, txn *t = 0, uint32_t flags = 0) {
      if (_cursor)
        close();
      if (db) {
        ups_status_t st = ups_cursor_create(&_cursor, db->get_handle(),
                    t ? t->get_handle() : 0, flags);
        if (st)
          throw error(st);
      }
    }

    /** Clones the Cursor. */
    cursor clone() {
      ups_cursor_t *dest;
      ups_status_t st = ups_cursor_clone(_cursor, &dest);
      if (st)
        throw error(st);
      return cursor(dest);
    }

    /** Moves the Cursor, and retrieves the key/record of the new position. */
    void move(key *k, record *r, uint32_t flags = 0) {
      ups_status_t st = ups_cursor_move(_cursor, k ? k->get_handle() : 0,
                        r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Moves the Cursor to the first Database element. */
    void move_first(key *k = 0, record *r = 0) {
      move(k, r, UPS_CURSOR_FIRST);
    }

    /** Moves the Cursor to the last Database element. */
    void move_last(key *k = 0, record *r = 0) {
      move(k, r, UPS_CURSOR_LAST);
    }

    /** Moves the Cursor to the next Database element. */
    void move_next(key *k = 0, record *r = 0) {
      move(k, r, UPS_CURSOR_NEXT);
    }

    /** Moves the Cursor to the previous Database element. */
    void move_previous(key *k = 0, record *r = 0) {
      move(k, r, UPS_CURSOR_PREVIOUS);
    }

    /** Overwrites the current record. */
    void overwrite(record *r, uint32_t flags = 0) {
      ups_status_t st = ups_cursor_overwrite(_cursor,
                            r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Finds a key. */
    void find(key *k, record *r = 0, uint32_t flags = 0) {
      ups_status_t st = ups_cursor_find(_cursor, k->get_handle(),
                        (r ? r->get_handle() : 0), flags);
      if (st)
        throw error(st);
    }

    /** Inserts a key/record pair. */
    void insert(key *k, record *r, uint32_t flags = 0) {
      ups_status_t st = ups_cursor_insert(_cursor, k ? k->get_handle() : 0,
                          r ? r->get_handle() : 0, flags);
      if (st)
        throw error(st);
    }

    /** Erases the current key/record pair. */
    void erase(uint32_t flags = 0) {
      ups_status_t st = ups_cursor_erase(_cursor, flags);
      if (st)
        throw error(st);
    }

    /** Returns the number of duplicate keys. */
    uint32_t get_duplicate_count(uint32_t flags = 0) {
      uint32_t c;
      ups_status_t st = ups_cursor_get_duplicate_count(_cursor, &c, flags);
      if (st)
        throw error(st);
      return c;
    }

    /** Returns the size of the current record. */
    uint32_t get_record_size() {
      uint32_t s;
      ups_status_t st = ups_cursor_get_record_size(_cursor, &s);
      if (st)
        throw error(st);
      return s;
    }

    /** Closes the Cursor. */
    void close() {
      if (!_cursor)
        return;
      ups_status_t st = ups_cursor_close(_cursor);
      if (st)
        throw error(st);
      _cursor = 0;
    }

  protected:
    /* Copy Constructor. Is protected and should not be used. */
    cursor(ups_cursor_t *c) {
      _cursor = c;
    }

  private:
    ups_cursor_t *_cursor;
};

/**
 * An Environment class.
 *
 * This class wraps the ups_env_t structure.
 */
class env {
  public:
    /** Constructor */
    env()
      : _env(0) {
    }

    /**
     * Destructor - automatically closes the Environment, if necessary.
     *
     * !!
     * Any exception is silently discarded. Use of the destructor to close
     * environments is therefore not recommended, because there are valid
     * reasons why an Exception can be thrown (i.e. not all Databases were
     * closed or not all Txns were committed/aborted).
     *
     * An assert() was added to catch this condition in debug builds.
     */
    ~env() {
      try {
        close();
      }
      catch (error &ex) {
        assert(ex.get_errno() == 0); // this will fail!
      }
    }

    /** Creates a new Environment. */
    void create(const char *filename, uint32_t flags = 0,
                uint32_t mode = 0644, const ups_parameter_t *param = 0) {
      ups_status_t st = ups_env_create(&_env, filename, flags, mode, param);
      if (st)
        throw error(st);
    }

    /** Opens an existing Environment. */
    void open(const char *filename, uint32_t flags = 0,
                const ups_parameter_t *param = 0) {
      ups_status_t st = ups_env_open(&_env, filename, flags, param);
      if (st)
        throw error(st);
    }

    /** Flushes the Environment to disk. */
    void flush(uint32_t flags = 0) {
      ups_status_t st = ups_env_flush(_env, flags);
      if (st)
        throw error(st);
    }

    /** Creates a new Database in the Environment. */
    db create_db(uint16_t name, uint32_t flags = 0,
                const ups_parameter_t *param = 0) {
      ups_db_t *dbh;

      ups_status_t st = ups_env_create_db(_env, &dbh, name, flags, param);
      if (st)
        throw error(st);

      return upscaledb::db(dbh);
    }

    /** Opens an existing Database in the Environment. */
    db open_db(uint16_t name, uint32_t flags = 0,
                const ups_parameter_t *param = 0) {
      ups_db_t *dbh;

      ups_status_t st = ups_env_open_db(_env, &dbh, name, flags, param);
      if (st)
        throw error(st);

      return upscaledb::db(dbh);
    }

    /** Renames an existing Database in the Environment. */
    void rename_db(uint16_t oldname, uint16_t newname, uint32_t flags = 0) {
      ups_status_t st = ups_env_rename_db(_env, oldname, newname, flags);
      if (st)
        throw error(st);
    }

    /** Deletes a Database from the Environment. */
    void erase_db(uint16_t name, uint32_t flags = 0) {
      ups_status_t st = ups_env_erase_db(_env, name, flags);
      if (st)
        throw error(st);
    }

    /** Begin a new Txn */
    txn begin(const char *name = 0, uint32_t flags = 0) {
      ups_txn_t *h;
      ups_status_t st = ups_txn_begin(&h, _env, name, 0, flags);
      if (st)
        throw error(st);
      return txn(h);
    }


    /** Closes the Environment. */
    void close(uint32_t flags = 0) {
      if (!_env)
        return;
      // disable auto-cleanup; all objects will be destroyed when 
      // going out of scope
      flags &= ~UPS_AUTO_CLEANUP;
      ups_status_t st = ups_env_close(_env, flags);
      if (st)
        throw error(st);
      _env = 0;
    }

    /** Retrieves Environment parameters. */
    void get_parameters(ups_parameter_t *param) {
      ups_status_t st = ups_env_get_parameters(_env, param);
      if (st)
        throw error(st);
    }

    /** Get all Database names. */
    std::vector<uint16_t> get_database_names() {
      uint32_t count = 32;
      ups_status_t st;
      std::vector<uint16_t> v(count);

      for (;;) {
        st = ups_env_get_database_names(_env, &v[0], &count);
        if (!st)
          break;
        if (st && st!=UPS_LIMITS_REACHED)
          throw error(st);
        count += 16;
        v.resize(count);
      }

      v.resize(count);
      return v;
    }

  private:
    ups_env_t *_env;
};

} // namespace upscaledb

/**
 * @}
 */

#endif // UPS_UPSCALEDB_HPP
