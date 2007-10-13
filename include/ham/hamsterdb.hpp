/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * \mainpage hamsterdb C++ API
 * \file hamsterdb.hpp
 * \author Christoph Rupp, chris@crupp.de
 * \version 0.4.7
 */

#ifndef HAM_HAMSTERDB_HPP__
#define HAM_HAMSTERDB_HPP__

/*
 * TODO TODO TODO
 *
 * Questions:
 *  o parameters as references or pointers?
 *  o cursors are no iterators - can we change that? -> don't think so
 *  o make key/record functions virtual? -> can derive classes from 
 *      keys/records --> would be cool if we could use both interfaces in
 *      one class! 
 *  o use exceptions or error codes? -> we actually *could* use return codes,
 *      since the constructor cannot fail, and we don't have operators
 *      -> currently i favor exceptions for one reason: they HAVE to be
 *         handled; the user is forced to check for errors.
 *  o the assignment operator of class db is ugly; how else can i make
 *      this work: 
 *      ham::db db=env.create_db(1);
 *
 * o documentation!!
 * TODO TODO TODO
 */

#include <ham/hamsterdb_int.h>
#include <string.h>

namespace ham {

class error {
public:
    /* constructor */
    error(ham_status_t st) : m_errno(st) {
    };

    /* get the error code */
    ham_status_t get_errno() const {
        return (m_errno);
    }

    /* get an error string */
    const char *get_string() const {
        return (ham_strerror(m_errno));
    }

private:
    ham_status_t m_errno;
};

class key {
    friend class db;
    friend class cursor;

public:
    /* constructor */
    key(void *data=0, ham_size_t size=0, ham_u32_t flags=0) {
        memset(&m_key, 0, sizeof(m_key));
        m_key.data=data;
        m_key.size=size;
        m_key.flags=flags;
    }

    /* copy constructor */
    key(const key &other) : m_key(other.m_key) {
    }

    /* assignment operator */
    key &operator=(const key &other) {
        m_key=other.m_key;
        return (*this);
    }

    /* get the key data */
    void *get_data() const {
        return (m_key.data);
    }

    /* set the key data */
    void set_data(void *data) {
        m_key.data=data;
    }

    /* get the key size */
    ham_size_t get_size() const {
        return (m_key.size);
    }

    /* set the key size */
    void set_size(ham_size_t size) {
        m_key.size=size;
    }

    /* get the key flags */
    ham_u32_t get_flags() const {
        return (m_key.flags);
    }

    /* set the key flags */
    void set_flags(ham_u32_t flags) {
        m_key.flags=flags;
    }

private:
    ham_key_t m_key;
};

class record {
    friend class db;
    friend class cursor;

public:
    /* constructor */
    record(void *data=0, ham_size_t size=0, ham_u32_t flags=0) {
        memset(&m_rec, 0, sizeof(m_rec));
        m_rec.data=data;
        m_rec.size=size;
        m_rec.flags=flags;
    }

    /* copy constructor */
    record(const record &other) : m_rec(other.m_rec) {
    }

    /* assignment operator */
    record &operator=(const record &other) {
        m_rec=other.m_rec;
        return (*this);
    }

    /* get the record data */
    void *get_data() const {
        return (m_rec.data);
    }

    /* set the record data */
    void set_data(void *data) {
        m_rec.data=data;
    }

    /* get the record size */
    ham_size_t get_size() const {
        return (m_rec.size);
    }

    /* set the record size */
    void set_size(ham_size_t size) {
        m_rec.size=size;
    }

    /* get the record flags */
    ham_u32_t get_flags() const {
        return (m_rec.flags);
    }

    /* set the record flags */
    void set_flags(ham_u32_t flags) {
        m_rec.flags=flags;
    }

protected:
    ham_record_t m_rec;
};

class db
{
    friend class env;
    friend class cursor;

public:
    class cursor;
    typedef cursor iterator;

    /* set error handler function */
    static void set_errhandler(ham_errhandler_fun f) {
        ham_set_errhandler(f);
    }

    /* get hamsterdb library version */
    static void get_version(ham_u32_t *major, ham_u32_t *minor,
                    ham_u32_t *revision) {
        ham_get_version(major, minor, revision);
    }

    /* constructor */
    db() : m_db(0), m_opened(false) {
    }

    /* destructor - automatically closes the database, if necessary */
    ~db() {
        close();
        if (m_db)
            ham_delete(m_db);
        m_db=0;
    }

    /* the assignment operator transfers the ownership of the database
     * handle */
    db &operator=(db other) {
        if (this==&other)
            return (*this);
        close();
        m_db=other.m_db;
        m_opened=true;
        other.m_db=0; 
        other.m_opened=false;
        return (*this);
    }

    /* create a database */
    void create(const char *filename, ham_u32_t flags=0,
            ham_u32_t mode=0644, ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_create_ex(m_db, filename, flags, mode, param)))
            throw error(st);
        m_opened=true;
    }

    /* open a database */
    void open(const char *filename, ham_u32_t flags=0,
            ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_open_ex(m_db, filename, flags, param)))
            throw error(st);
        m_opened=true;
    }

    /* get the last database error */
    ham_status_t get_error() {
        return (ham_get_error(m_db));
    }

    /* set the prefix comparison function */
    void set_prefix_compare_func(ham_prefix_compare_func_t foo) {
        ham_status_t st=ham_set_prefix_compare_func(m_db, foo);
        if (st)
            throw error(st);
    }

    /* set the comparison function */
    void set_compare_func(ham_compare_func_t foo) {
        ham_status_t st=ham_set_compare_func(m_db, foo);
        if (st)
            throw error(st);
    }

    /* find a value */
    record find(key *k, ham_u32_t flags=0) {
        record r;
        ham_status_t st=ham_find(m_db, 0, &k->m_key, &r.m_rec, flags);
        if (st)
            throw error(st);
        return (r);
    }

    /* insert a value */
    void insert(key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_insert(m_db, 0, &k->m_key, &r->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* erase a value */
    void erase(key *k, ham_u32_t flags=0) {
        ham_status_t st=ham_erase(m_db, 0, &k->m_key, flags);
        if (st)
            throw error(st);
    }

    /* flush to disk */
    void flush(ham_u32_t flags=0) {
        ham_status_t st=ham_flush(m_db, flags);
        if (st)
            throw error(st);
    }

    /* close the database */
    void close(ham_u32_t flags=0) {
        if (!m_opened)
            return;
        ham_status_t st=ham_close(m_db, flags);
        if (st)
            throw error(st);
        m_opened=false;
        st=ham_delete(m_db);
        if (st)
            throw error(st);
        m_db=0;
    }

protected:
    /* get the database handle */
    ham_db_t *get_handle() {
        return (m_db);
    }

    /* constructor */
    db(ham_db_t *db) : m_db(db), m_opened(true) {
    }

private:
    ham_db_t *m_db;
    bool m_opened;
};

class cursor
{
public:
    /* constructor */
    cursor(db *db=0, ham_u32_t flags=0)
    :   m_cursor(0) {
        create(db, flags);
    }

    /* destructor */
    ~cursor() {
        close();
    }

    /* create a cursor */
    void create(db *db, ham_u32_t flags=0) {
        if (db) {
            ham_status_t st=ham_cursor_create(db->get_handle(), 0,
                            flags, &m_cursor);
            if (st)
                throw error(st);
        }
    }

    /* clone the cursor */
    cursor clone() {
        ham_cursor_t *dest;
        ham_status_t st=ham_cursor_clone(m_cursor, &dest);
        if (st)
            throw error(st);
        return (cursor(dest));
    }

    /* move the cursor */
    void move(key *k, record *rec, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_move(m_cursor, &k->m_key,
                        &rec->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* move to the very first element */
    void begin(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_FIRST);
    }

    /* move to the very last element */
    void end(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_LAST);
    }

    /* move to the next element */
    void next(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_NEXT);
    }

    /* move to the previous element */
    void previous(key *k=0, record *r=0) {
        move(k, r, HAM_CURSOR_PREVIOUS);
    }

    /* overwrite the current record */
    void overwrite(record *rec, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_overwrite(m_cursor, &rec->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* find a key */
    void find(key *k, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_find(m_cursor, &k->m_key, flags);
        if (st)
            throw error(st);
    }

    /* insert a key */
    void insert(key *k, record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_insert(m_cursor, &k->m_key,
                        &r->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* erase the current key */
    void erase(ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_erase(m_cursor, flags);
        if (st)
            throw error(st);
    }

    /* get number of duplicate keys */
    ham_u32_t get_duplicate_count(ham_u32_t flags=0) {
        ham_u32_t c;
        ham_status_t st=ham_cursor_get_duplicate_count(m_cursor, &c, flags);
        if (st)
            throw error(st);
        return (c);
    }

    /* close the cursor */
    void close() {
        if (!m_cursor)
            return;
        ham_status_t st=ham_cursor_close(m_cursor);
        if (st)
            throw error(st);
        m_cursor=0;
    }

protected:
    /* constructor */
    cursor(ham_cursor_t *c) {
        m_cursor=c;
    }

private:
    ham_cursor_t *m_cursor;
};

class env
{
public:
    /* constructor */
    env() : m_env(0), m_opened(false) {
    }

    /* destructor - automatically closes the environment, if necessary */
    ~env() {
        close();
        if (m_env)
            ham_env_delete(m_env);
        m_env=0;
    }

    /* create an environment */
    void create(const char *filename, ham_u32_t flags=0,
            ham_u32_t mode=0644, ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_env) {
            st=ham_env_new(&m_env);
            if (st)
                throw error(st);
        }
        if ((st=ham_env_create_ex(m_env, filename, flags, mode, param)))
            throw error(st);
        m_opened=true;
    }

    /* open an environment */
    void open(const char *filename, ham_u32_t flags=0,
            ham_parameter_t *param=0) {
        ham_status_t st;
        if (!m_env) {
            st=ham_env_new(&m_env);
            if (st)
                throw error(st);
        }
        if ((st=ham_env_open_ex(m_env, filename, flags, param)))
            throw error(st);
        m_opened=true;
    }

    /* create a new database in the environment */
    db create_db(ham_u16_t name, ham_u32_t flags=0, ham_parameter_t *param=0) {
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

        ham::db d(dbh);
        return (d);
    }

    /* open an existing database in the environment */
    db open_db(ham_u16_t name, ham_u32_t flags=0, ham_parameter_t *param=0) {
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

        ham::db d(dbh);
        return (d);
    }

    /* rename a database */
    void rename_db(ham_u16_t oldname, ham_u16_t newname, ham_u32_t flags=0) {
        ham_status_t st=ham_env_rename_db(m_env, oldname, newname, flags);
        if (st)
            throw error(st);
    }

    /* delete a database from the environment */
    void erase_db(ham_u16_t name, ham_u32_t flags=0) {
        ham_status_t st=ham_env_erase_db(m_env, name, flags);
        if (st)
            throw error(st);
    }

    /* closes the environment */
    void close(ham_u32_t flags=0) {
        if (!m_opened)
            return;
        ham_status_t st=ham_env_close(m_env, flags);
        if (st)
            throw error(st);
        m_opened=false;
        st=ham_env_delete(m_env);
        if (st)
            throw error(st);
        m_env=0;
    }

private:
    ham_env_t *m_env;
    bool m_opened;
};

}; // namespace ham

#endif // HAMSTERDB_HPP__
