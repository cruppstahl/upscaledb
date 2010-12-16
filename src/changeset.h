/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief A changeset collects all pages that are modified during a single
 * operation.
 *
 */

#ifndef HAM_CHANGESET_H__
#define HAM_CHANGESET_H__

#include "internal_fwd_decl.h"

#ifdef __cplusplus
extern "C" {
#endif


/**
 * The changeset structure
 */
typedef struct changeset_t 
{
    /* the head of our linked list */
    ham_page_t *_head;

} changeset_t;

/** is the changeset empty? */
#define changeset_is_empty(cs)          ((cs)->_head==0)

/** get the head of the linked list */
#define changeset_get_head(cs)          (cs)->_head

/** set the head of the linked list */
#define changeset_set_head(cs, p)       (cs)->_head=p

/** a static changeset initializer */
#define CHANGESET_STATIC_INITIALIZER    {0}

/**
 * append a new page to the changeset
 *
 * this function will assert that the page is not yet part of the changeset!
 */
extern void
changeset_add_page(changeset_t *cs, ham_page_t *page);

/**
 * get a page from the changeset
 * returns NULL if the page is not part of the changeset
 */
extern ham_page_t *
changeset_get_page(changeset_t *cs, ham_offset_t pageid);

/**
 * removes all pages from the changeset
 */
extern void
changeset_clear(changeset_t *cs);

/**
 * flush all pages in the changeset - first write them to the log, then 
 * write them to the disk
 *
 * on success: will clear the changeset and the log 
 */
extern ham_status_t
changeset_flush(changeset_t *cs, ham_u64_t lsn);


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_CHANGESET_H__ */
