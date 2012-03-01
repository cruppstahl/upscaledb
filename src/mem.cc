/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#include <stack>

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "db.h"
#include "error.h"
#include "mem.h"
#include "txn.h"

class DefaultAllocator : public Allocator
{
    struct Lookasides
    {
        typedef std::stack<void *> LookasideList;

        Lookasides() 
        : max_sizes(2) {
            sizes[0]=sizeof(txn_op_t);
            sizes[1]=sizeof(txn_opnode_t);
        }

        LookasideList lists[2];
        ham_u32_t sizes[2];
        int max_sizes;
    };

  public:
    /** a constructor */
    DefaultAllocator() {
    }

    /** a virtual destructor */
    virtual ~DefaultAllocator() {
        for (int i=0; i<m_ls.max_sizes; i++) {
            while (!m_ls.lists[i].empty()) {
                void *p=(char *)m_ls.lists[i].top();
                m_ls.lists[i].pop();
#if defined(_CRTDBG_MAP_ALLOC)
                ::_free_dbg((void *)p, _NORMAL_BLOCK);
#else
                ::free((void *)p);
#endif
            }
        }
    }

    /** allocate a chunk of memory */
    virtual void *alloc(ham_size_t size) {
        void *p=0;

        for (int i=0; i<m_ls.max_sizes; i++) {
            if (size==m_ls.sizes[i] && !m_ls.lists[i].empty()) {
                p=m_ls.lists[i].top();
                m_ls.lists[i].pop();
                break;
            }
        }

        if (p)
            return ((char *)p+sizeof(ham_u32_t));

#if defined(_CRTDBG_MAP_ALLOC)
        p=::_malloc_dbg(size+sizeof(ham_u32_t), _NORMAL_BLOCK, 
			          __FILE__, __LINE__);
#else
        p=::malloc(size+sizeof(ham_u32_t));
#endif
        if (p) {
            *(ham_u32_t *)p=size;
            return ((char *)p+sizeof(ham_u32_t));
        }
        return (0);
    }

    /** release a chunk of memory */
    void free(const void *ptr) {
        ham_u32_t size;
        void *p=0;

        ham_assert(ptr, ("freeing NULL pointer"));

        p=(char *)ptr-sizeof(ham_u32_t);
        size=*(ham_u32_t *)p;

        for (int i=0; i<m_ls.max_sizes; i++) {
            if (size==m_ls.sizes[i] && m_ls.lists[i].size()<10) {
                m_ls.lists[i].push(p);
                return;
            }
        }

#if defined(_CRTDBG_MAP_ALLOC)
        ::_free_dbg((void *)p, _NORMAL_BLOCK);
#else
        ::free((void *)p);
#endif
    }

    /** re-allocate a chunk of memory */
    virtual void *realloc(const void *ptr, ham_size_t size) {
        void *p=ptr ? (char *)ptr-sizeof(ham_u32_t) : 0;

#if defined(_CRTDBG_MAP_ALLOC)
        ptr=::_realloc_dbg((void *)p, size+sizeof(ham_u32_t), 
                    _NORMAL_BLOCK, __FILE__, __LINE__);
#else
        ptr=::realloc((void *)p, size+sizeof(ham_u32_t));
#endif
        if (ptr) {
            *(ham_u32_t *)ptr=size;
            return ((char *)ptr+sizeof(ham_u32_t));
        }
        return (0);
    }

  private:
    Lookasides m_ls;
};

Allocator *
ham_default_allocator_new()
{
    return (new DefaultAllocator());
}

