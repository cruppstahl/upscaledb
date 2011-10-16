/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief a simple hash_table implementation using linked lists as 
 * overflow buckets.
 *
 * this hash table does not require any allocations.
 *
 * class T: the cached object
 * class Key: the key which identifies objects of class T
 * class Helper: a helper object which knows how to retrieve the "next"
 *   pointer of the linked list, how to map T to key etc
 */

#ifndef HAM_HASHTABLE_H__
#define HAM_HASHTABLE_H__

#include <vector>

template<class T, class Key, class Helper>
class hash_table
{
  typedef std::vector<T *> bucket_list;
  typedef typename std::vector<T *>::iterator bucket_iterator;

  public:
    /** constructor */
    hash_table(Helper &h, int bucket_size=10317)
    :   m_helper(h), m_buckets(bucket_size) {
    }

    /** 
     * inserts a key; does not verify if the key already exists. 
     * complexity: O(1)
     */
    void put(T *p) {
        unsigned h=hash(p);
        m_helper.set_next(p, m_buckets[h]);
        m_buckets[h]=p;
    }

    /** 
     * fetches a key
     */
    T *get(const Key &key) const {
        unsigned h=hash(key);
        T *p=m_buckets[h];
        while (p) {
            if  (m_helper.matches(p, key))
                return (p);
            p=m_helper.next(p);
        }
        return (p);
    }

    /** 
     * removes an object
     */
    T *remove(const T *p) {
        return (remove(m_helper.key(p)));
    }

    /** 
     * removes an object
     */
    T *remove(const Key &key) {
        unsigned h=hash(key);
        T *last=0, *p=m_buckets[h];
        while (p) {
            if  (m_helper.matches(p, key)) {
                if (last)
                    m_helper.set_next(last, m_helper.next(p));
                else
                    m_buckets[h]=m_helper.next(p);
                return p;
            }
            last=p;
            p=m_helper.next(p);
        }
        return 0;
    }

    /** 
     * traverses the hash; calls helper.visit() for each stored object
     */
    void foreach() {
        bucket_iterator it;
        for (it=m_buckets.begin(); it!=m_buckets.end(); ++it) {
            T *p=*it;
            while (p) {
                T *n=m_helper.next(p);
                m_helper.visit(p);
                p=n;
            }
        }
    }

    /** 
     * same as foreach(), but whenever helper.matches() returns true it
     * will remove the element from the hash
     */
    void remove_if() {
        bucket_iterator it;
        for (size_t i=0; i<m_buckets.size(); i++) {
            T *previous=0, *next=0, *p=m_buckets[i];
            while (p) {
                next=m_helper.next(p);
                if  (m_helper.remove_if(p)) {
                    if (previous)
                        m_helper.set_next(previous, next);
                    else
                        m_buckets[i]=next;
                }
                previous=p;
                p=next;
            }
        }
    }

  private:
    unsigned hash(const Key &key) const {
        return (m_helper.hash(key)%m_buckets.size()); 
    }

    unsigned hash(const T *p) const {
        return (m_helper.hash(p)%m_buckets.size()); 
    }

    Helper &m_helper;
	bucket_list m_buckets;
};

#endif /* HAM_HASHTABLE_H__ */
