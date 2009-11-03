/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * \file hamsterdb.hpp
 * \author Christoph Rupp, chris@crupp.de
 * \version 1.0.6
 *
 * This C++ wrapper class is a very tight wrapper around the C API. It does
 * not attempt to be STL compatible. 
 *
 * All functions throw exceptions of class @sa ham::error in case of an error. 
 * Please refer to the C API documentation for more information. You can find
 * it here: http://hamsterdb.com/?page=doxygen&module=globals.html
 *
 */

#ifndef HAM_HAMSTERDB_HPP__
#define HAM_HAMSTERDB_HPP__

#include <ham/hamsterdb_int.h>
#include <cstring>
#include <vector>

#if defined(_MSC_VER)
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

/**
 * @defgroup ham_cpp hamsterdb C++ API wrapper
 * @{
 */

/**
 * The global hamsterdb namespace.
 */
namespace ham {

/*
 * forward declarations
 */
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
    error(ham_status_t st) : m_errno(st) {
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
    key(void *data=0, ham_size_t size=0, ham_u32_t flags=0) {
        memset(&m_key, 0, sizeof(m_key));
        m_key.data=data;
        m_key.size=size;
        m_key.flags=flags;
    }

    /** Copy constructor. */
    key(const key &other) : m_key(other.m_key) {
    }

    /** Assignment operator. */
    key &operator=(const key &other) {
        m_key=other.m_key;
        return (*this);
    }

    /** Returns the key data. */
    void *get_data() const {
        return (m_key.data);
    }

    /** Sets the key data. */
    void set_data(void *data) {
        m_key.data=data;
    }

    /** Returns the size of the key. */
    ham_size_t get_size() const {
        return (m_key.size);
    }

    /** Sets the size of the key. */
    void set_size(ham_size_t size) {
        m_key.size=size;
    }

    /** Template assignment */
    template <class T>
    void set(T &t) {
        set_data(&t);
        set_size(sizeof(t));
    }

    /** Returns the flags of the key. */
    ham_u32_t get_flags() const {
        return (m_key.flags);
    }

    /** Sets the flags of the key. */
    void set_flags(ham_u32_t flags) {
        m_key.flags=flags;
    }

    /** Returns a pointer to the internal ham_key_t structure. */
    ham_key_t *get_handle() {
        return (&m_key);
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
    record(void *data=0, ham_size_t size=0, ham_u32_t flags=0) {
        memset(&m_rec, 0, sizeof(m_rec));
        m_rec.data=data;
        m_rec.size=size;
        m_rec.flags=flags;
    }

    /** Copy constructor. */
    record(const record &other) : m_rec(other.m_rec) {
    }

    /** Assignment operator. */
    record &operator=(const record &other) {
        m_rec=other.m_rec;
        return (*this);
    }

    /** Returns the record data. */
    void *get_data() const {
        return (m_rec.data);
    }

    /** Sets the record data. */
    void set_data(void *data) {
        m_rec.data=data;
    }

    /** Returns the size of the record. */
    ham_size_t get_size() const {
        return (m_rec.size);
    }

    /** Sets the size of the record. */
    void set_size(ham_size_t size) {
        m_rec.size=size;
    }

    /** Returns the flags of the record. */
    ham_u32_t get_flags() const {
        return (m_rec.flags);
    }

    /** Sets the flags of the record. */
    void set_flags(ham_u32_t flags) {
        m_rec.flags=flags;
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
    txn(ham_txn_t *t=0) : m_txn(t) {
    }

    /** Abort the Transaction */
    void abort() {
        ham_status_t st;
        st=ham_txn_abort(m_txn, 0);
        if (st)
            throw error(st);
    }

    /** Commit the Transaction */
    void commit() {
        ham_status_t st;
        st=ham_txn_commit(m_txn, 0);
        if (st)
            throw error(st);
    }

    /** Returns a pointer to the internal ham_txn_t structure. */
    ham_txn_t *get_handle() {
        return (m_txn);
    }

protected:
    ham_txn_t *m_txn;
    db *m_db;
};


/**
 * A Database class.
 *
 * This class wraps the ham_db_t Database handles.
 */
class db
{
public:
    /** Set error handler function. */
    static void set_errhandler(ham_errhandler_fun f) {
        ham_set_errhandler(f);
    }

    /** Retrieves the hamsterdb library version. */ 
    static void get_version(ham_u32_t *major, ham_u32_t *minor,
                    ham_u32_t *revision) {
        ham_get_version(major, minor, revision);
    }

    /** Retrieves hamsterdb library license information. */ 
    static void get_license(const char **licensee, const char **product) {
        ham_get_license(licensee, product);
    }

    /** Constructor */
    db() : m_db(0) {
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
		db &rhs=(db &)other;
        if (this==&other)
            return (*this);
        close();
        m_db=rhs.m_db;
        rhs.m_db=0; 
        return (*this);
    }

    /** Creates a Database. */
    void create(const char *filename, ham_u32_t flags=0,
            ham_u32_t mode=0644, const ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_create_ex(m_db, filename, flags, mode, param)))
            throw error(st);
    }

    /** Opens an existing Database. */
    void open(const char *filename, ham_u32_t flags=0,
            const ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_open_ex(m_db, filename, flags, param)))
            throw error(st);
    }

    /** Returns the last Database error. */
    ham_status_t get_error() {
        if (!m_db)
			return (HAM_NOT_INITIALIZED);
        return (ham_get_error(m_db));
    }

    /** Begin a new Transaction */
    txn begin() {
        ham_txn_t *h;
        ham_status_t st=ham_txn_begin(&h, get_handle(), 0);
        if (st)
            throw error(st);
        return (txn(h));
    }

    /** Sets the prefix comparison function. */
    void set_prefix_compare_func(ham_prefix_compare_func_t foo) {
        ham_status_t st=ham_set_prefix_compare_func(m_db, foo);
        if (st)
            throw error(st);
    }

    /** Sets the comparison function. */
    void set_compare_func(ham_compare_func_t foo) {
        ham_status_t st=ham_set_compare_func(m_db, foo);
        if (st)
            throw error(st);
    }

    /** Enable zlib compression. */
    void enable_compression(ham_u32_t level, ham_u32_t flags=0) {
        ham_status_t st=ham_enable_compression(m_db, level, flags);
        if (st)
            throw error(st);
    }

    /** Finds a record by looking up the key. */
    record find(txn *t, key *k, ham_u32_t flags=0) {
        record r;
        ham_status_t st=ham_find(m_db, 
                t ? t->get_handle() : 0, 
                k ? k->get_handle() : 0,
                r.get_handle(), flags);
        if (st)
            throw error(st);
        return (r);
    }

    /** Finds a record by looking up the key. */
    record find(key *k, ham_u32_t flags=0) {
        return (find(0, k, flags));
    }

    /** Inserts a key/record pair. */
    void insert(txn *t, key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_insert(m_db, 
                t ? t->get_handle() : 0, 
                k ? k->get_handle() : 0, 
                r ? r->get_handle() : 0, flags);
        if (st)
            throw error(st);
    }

    /** Inserts a key/record pair. */
    void insert(key *k, record *r, ham_u32_t flags=0) {
        insert(0, k, r, flags);
    }

    /** Erases a key/record pair. */
    void erase(key *k, ham_u32_t flags=0) {
        erase(0, k, flags);
    }

    /** Erases a key/record pair. */
    void erase(txn *t, key *k, ham_u32_t flags=0) {
        ham_status_t st=ham_erase(m_db, 
                t ? t->get_handle() : 0, 
                k ? k->get_handle() : 0, flags);
        if (st)
            throw error(st);
    }

    /** Flushes the Database to disk. */
    void flush(ham_u32_t flags=0) {
        ham_status_t st=ham_flush(m_db, flags);
        if (st)
            throw error(st);
    }

    /** Closes the Database. */
    void close(ham_u32_t flags=0) {
        if (!m_db)
            return;
        ham_status_t st=ham_close(m_db, flags);
        if (st)
            throw error(st);
        st=ham_delete(m_db);
        if (st)
            throw error(st);
        m_db=0;
    }

    /** Returns a pointer to the internal ham_db_t structure. */
    ham_db_t *get_handle() {
        return (m_db);
    }

protected:
    friend class env;

    /* Copy Constructor. Is protected and should not be used. */
    db(ham_db_t *db) : m_db(db) {
    }

private:
    ham_db_t *m_db;
};


/**
 * A Database Cursor.
 *
 * This class wraps the ham_cursor_t Cursor handles.
 */
class cursor
{
public:
    /** Constructor */
    cursor(db *db=0, txn *t=0, ham_u32_t flags=0)
    :   m_cursor(0) {
        create(db, t, flags);
    }

    /** Constructor */
    cursor(txn *t, db *db=0, ham_u32_t flags=0)
    :   m_cursor(0) {
        create(db, t, flags);
    }

    /** Destructor - automatically closes the Cursor, if necessary. */
    ~cursor() {
        close();
    }

    /** Creates a new Cursor. */
    void create(db *db, txn *t=0, ham_u32_t flags=0) {
        if (m_cursor)
            close();
        if (db) {
            ham_status_t st=ham_cursor_create(db->get_handle(), 
                    t ? t->get_handle() : 0, 
                    flags, &m_cursor);
            if (st)
                throw error(st);
        }
    }

    /** Clones the Cursor. */
    cursor clone() {
        ham_cursor_t *dest;
        ham_status_t st=ham_cursor_clone(m_cursor, &dest);
        if (st)
            throw error(st);
        return (cursor(dest));
    }

    /** Moves the Cursor, and retrieves the key/record of the new position. */
    void move(key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_move(m_cursor, k ? k->get_handle() : 0,
                        r ? r->get_handle() : 0, flags);
        if (st)
            throw error(st);
    }

    /** Moves the Cursor to the first Database element. */
    void move_first(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_FIRST);
    }

    /** Moves the Cursor to the last Database element. */
    void move_last(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_LAST);
    }

    /** Moves the Cursor to the next Database element. */
    void move_next(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_NEXT);
    }

    /** Moves the Cursor to the previous Database element. */
    void move_previous(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_PREVIOUS);
    }

    /** Overwrites the current record. */
    void overwrite(record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_overwrite(m_cursor, 
                        r ? r->get_handle() : 0, flags);
        if (st)
            throw error(st);
    }

    /** Finds a key. */
    void find(key *k, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_find(m_cursor, k->get_handle(), flags);
        if (st)
            throw error(st);
    }

    /** Finds a key. */
    void find_ex(key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_find_ex(m_cursor, k->get_handle(), 
                        (r ? r->get_handle() : 0), flags);
        if (st)
            throw error(st);
    }

    /** Inserts a key/record pair. */
    void insert(key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_insert(m_cursor, k ? k->get_handle() : 0,
                        r ? r->get_handle() : 0, flags);
        if (st)
            throw error(st);
    }

    /** Erases the current key/record pair. */
    void erase(ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_erase(m_cursor, flags);
        if (st)
            throw error(st);
    }

    /** Returns the number of duplicate keys. */
    ham_u32_t get_duplicate_count(ham_u32_t flags=0) {
        ham_u32_t c;
        ham_status_t st=ham_cursor_get_duplicate_count(m_cursor, &c, flags);
        if (st)
            throw error(st);
        return (c);
    }

    /** Closes the Cursor. */
    void close() {
        if (!m_cursor)
            return;
        ham_status_t st=ham_cursor_close(m_cursor);
        if (st)
            throw error(st);
        m_cursor=0;
    }

protected:
    /* Copy Constructor. Is protected and should not be used. */
    cursor(ham_cursor_t *c) {
        m_cursor=c;
    }

private:
    ham_cursor_t *m_cursor;
};

/**
 * An Environment class.
 *
 * This class wraps the ham_env_t structure.
 */
class env
{
public:
    /** Constructor */
    env() : m_env(0) {
    }

    /** Destructor - automatically closes the Cursor, if necessary. */
    ~env() {
        close();
    }

    /** Creates a new Environment. */
    void create(const char *filename, ham_u32_t flags=0,
            ham_u32_t mode=0644, const ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_env) {
            st=ham_env_new(&m_env);
            if (st)
                throw error(st);
        }
        if ((st=ham_env_create_ex(m_env, filename, flags, mode, param)))
            throw error(st);
    }

    /** Opens an existing Environment. */
    void open(const char *filename, ham_u32_t flags=0,
            const ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_env) {
            st=ham_env_new(&m_env);
            if (st)
                throw error(st);
        }
        if ((st=ham_env_open_ex(m_env, filename, flags, param)))
            throw error(st);
    }

    /** Creates a new Database in the Environment. */
    db create_db(ham_u16_t name, ham_u32_t flags=0, 
            const ham_parameter_t *param=0) {
        ham_status_t st;
        ham_db_t *dbh;

        st=ham_new(&dbh);
        if (st)
            throw error(st);
        st=ham_env_create_db(m_env, dbh, name, flags, param);
        if (st) {
            ham_delete(dbh);
            throw error(st);
        }

		return (ham::db(dbh));
    }

    /** Opens an existing Database in the Environment. */
    db open_db(ham_u16_t name, ham_u32_t flags=0, 
            const ham_parameter_t *param=0) {
        ham_status_t st;
        ham_db_t *dbh;

        st=ham_new(&dbh);
        if (st)
            throw error(st);
        st=ham_env_open_db(m_env, dbh, name, flags, param);
        if (st) {
            ham_delete(dbh);
            throw error(st);
        }

		return (ham::db(dbh));
    }

    /** Renames an existing Database in the Environment. */
    void rename_db(ham_u16_t oldname, ham_u16_t newname, ham_u32_t flags=0) {
        ham_status_t st=ham_env_rename_db(m_env, oldname, newname, flags);
        if (st)
            throw error(st);
    }

    /** Deletes a Database from the Environment. */
    void erase_db(ham_u16_t name, ham_u32_t flags=0) {
        ham_status_t st=ham_env_erase_db(m_env, name, flags);
        if (st)
            throw error(st);
    }

    /** 
     * Closes the Environment. 
     */
    void close(void) {
        if (!m_env)
            return;
        ham_status_t st=ham_env_close(m_env, 0);
        if (st)
            throw error(st);
        st=ham_env_delete(m_env);
        if (st)
            throw error(st);
        m_env=0;
    }

    /** Enable AES encryption. */
    void enable_encryption(ham_u8_t key[16], ham_u32_t flags=0) {
        ham_status_t st=ham_env_enable_encryption(m_env, key, flags);
        if (st)
            throw error(st);
    }

    /** Get all Database names. */
    std::vector<ham_u16_t> get_database_names(void) {
        ham_size_t count=32;
        ham_status_t st;
        std::vector<ham_u16_t> v(count);

        do {
            st=ham_env_get_database_names(m_env, &v[0], &count);
            if (!st)
                break;
            if (st && st!=HAM_LIMITS_REACHED)
                throw error(st);
            count+=16;
            v.resize(count);
        } while (1);

        v.resize(count);
        return (v);
    }

private:
    ham_env_t *m_env;
};

}; // namespace ham

/**
 * @}
 */

#endif // HAMSTERDB_HPP__
