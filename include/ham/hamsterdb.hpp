
/*
 * TODO TODO TODO
 *
 * x syntaxfehler raus
 *
 * o header-kommentar einfügen!
 *
 * o 100% inline, keine c++-datei!
 *
 * o cursor fehlen komplett
 * o operator[] kommt nicht! rein
 * o cursor::begin(), cursor::end()
 * o cursor::next(), cursor::previous(), ++, -- (wie bei anderen iteratoren)
 *
 * o environments fehlen komplett
 *
 * o typedef cursor iterator;
 * o cursor::operator++, cursor::operator--
 *
 * o sicherstellen dass die std::algorithm-funktionen mit den iteratoren
 *      funktionieren
 *
 * o funktionen haben keinen ham_status_t als rückgabewert, sondern void
 *      - fehler werden durch exceptions zurückgegeben
 *
 * o 100%ige abdeckung durch unittests!
 *
 * o documentation!!
 * TODO TODO TODO
 */

#ifndef HAMSTERDB_HPP__
#define HAMSTERDB_HPP__

#include <ham/hamsterdb.h>
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
    const char *get_desc() {
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
        if (this!=&*other) /* TODO correct? */
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
        if (this!=&*other) /* TODO correct? */
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
    ham_record_t m_rec;
};

class db
{
public:
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
            ham_u32_t mode=0644, ham_u16_t pagesize=0, 
            ham_u16_t keysize=0, ham_size_t cachesize=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_create_ex(m_db, filename, flags, mode, pagesize, keysize, 
                cachesize)))
            throw error(st);
        m_opened=true;
    }

    /* open a database */
    void open(const char *filename, ham_u32_t flags=0,
            ham_size_t cachesize=0) {
        ham_status_t st;
        if (!m_db) {
            st=ham_new(&m_db);
            if (st)
                throw error(st);
        }
        if ((st=ham_open_ex(m_db, filename, flags, cachesize)))
            throw error(st);
        m_opened=true;
    }

    /* get the last database error */
    ham_status_t get_last_error() { 
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
    record find(const key &k, ham_u32_t flags=0) {
        record r;
        ham_status_t st=ham_find(m_db, 0, (ham_key_t *)&k.m_key, 
                &r.m_rec, flags);
        if (st)
            throw error(st);
        return (r);
    }

    /* insert a value */
    void insert(const key *k, const record *r, ham_u32_t flags=0) {
        ham_status_t st=ham_insert(m_db, 0, (ham_key_t *)&k->m_key, 
                (ham_record_t *)&r->m_rec, flags);
        if (st)
            throw error(st);
    }

    /* erase a value */
    void erase(const key *k, ham_u32_t flags=0) {
        ham_status_t st=ham_erase(m_db, 0, (ham_key_t *)&k->m_key, flags);
        if (st)
            throw error(st);
    }

    /* flush to disk */
    void flush(ham_u32_t flags=0) { 
        ham_status_t st=ham_flush(m_db);
        if (st)
            throw error(st);
    }

    /* close the database */
    void close() {
        ham_status_t st=ham_close(m_db);
        if (st)
            throw error(st);
        m_opened=false;
    }

protected:
    ham_db_t *m_db;
    bool m_opened;

private:
    /* forbid the use of the copy constructor */
    db(const db &other) { (void)other; }
};

}; // namespace ham

#endif // HAMSTERDB_HPP__
