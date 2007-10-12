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
 * \mainpage hamsterdb C++ header file
 * \file hamsterdb.hpp
 * \author Christoph Rupp, chris@crupp.de
 * \version 0.4.7
 */

#ifndef HAM_HAMSTERDB_HPP__
#define HAM_HAMSTERDB_HPP__

/*
 * TODO TODO TODO
 *
 * x syntaxfehler raus
 * x header-kommentar einfuegen!
 *
 * x cursor: begin/end/++/--: sind das die iterator-deklarationen??
 *
 * o sicherstellen dass die std::algorithm-funktionen mit den iteratoren
 *      funktionieren
 *
 * o environments fehlen komplett
 *
 * o 100%ige abdeckung durch unittests!
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
    ham_status_t get_errno() {
        return (m_errno);
    }

    /* get an error string */
    const char *get_string() {
        return (ham_strerror(m_errno));
    }

private:
    ham_status_t m_errno;
};

class key {
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
        return *this;
    }

    /* get the key data */
    void *get_data() { 
        return m_key.data; 
    }

    /* set the key data */
    void set_data(void *data) { 
        m_key.data=data; 
    }

    /* get the key size */
    ham_size_t get_size() { 
        return m_key.size; 
    }

    /* set the key size */
    void set_size(ham_size_t size) { 
        m_key.size=size;
    }

    /* get the key flags */
    ham_u32_t get_flags() { 
        return m_key.flags; 
    }

    /* set the key flags */
    void set_flags(ham_u32_t flags) { 
        m_key.flags=flags;
    }

private:
    friend class db;
    friend class cursor;
    ham_key_t m_key;
};

class record {
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
        return *this;
    }

    /* get the record data */
    void *get_data() { 
        return m_rec.data; 
    }

    /* set the record data */
    void set_data(void *data) { 
        m_rec.data=data; 
    }

    /* get the record size */
    ham_size_t get_size() { 
        return m_rec.size; 
    }

    /* set the record size */
    void set_size(ham_size_t size) { 
        m_rec.size=size;
    }

    /* get the record flags */
    ham_u32_t get_flags() { 
        return m_rec.flags; 
    }

    /* set the record flags */
    void set_flags(ham_u32_t flags) { 
        m_rec.flags=flags;
    }

protected:
    friend class db;
    friend class cursor;
    ham_record_t m_rec;
};

class db
{
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
        if (m_opened)
            close();
        if (m_db)
            ham_delete(m_db);
        m_db=0;
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
    record find(key *k, record *r, ham_u32_t flags=0) {
        record r;
        ham_status_t st=ham_find(m_db, 0, &k->m_key, &r->m_rec, flags);
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
        ham_status_t st=ham_close(m_db, flags);
        if (st)
            throw error(st);
        m_opened=false;
    }

    /* get the database handle */
    ham_db_t *get_handle() {
        return (m_db);
    }

private:
    ham_db_t *m_db;
    bool m_opened;

private:
    /* forbid the use of the copy constructor */
    db(const db &other) { (void)other; }
};

class cursor
{
public:
    /* constructor */
    cursor(db *db, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_create(db->get_handle(), 0, 
                        flags, &m_cursor);
        if (st)
            throw error(st);
    }

    /* destructor */
    ~cursor() {
        close();
    }

    /* clone the cursor */
    cursor *clone() {
        ham_cursor_t *dest;
        ham_status_t st=ham_cursor_clone(m_cursor, &dest);
        if (st)
            throw error(st);
        return (new cursor(dest));
    }

    /* move the cursor */
    void move(key *k, record *rec, ham_u32_t flags=0) {
        ham_status_t st=ham_cursor_move(m_cursor, &k->m_key, 
                        &rec->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* move to the very first element */
    void begin() {
        move(0, 0, HAM_CURSOR_FIRST);
    }

    /* move to the very last element */
    void end() {
        move(0, 0, HAM_CURSOR_LAST);
    }

    /* move to the next element */
    cursor &operator++() {
        move(0, 0, HAM_CURSOR_NEXT);
        return (*this);
    }

    /* move to the previous element */
    cursor &operator--() {
        move(0, 0, HAM_CURSOR_PREVIOUS);
        return (*this);
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

}; // namespace ham

#endif // HAMSTERDB_HPP__
