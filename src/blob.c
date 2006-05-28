/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 */

#include <string.h>
#include "os.h"
#include "db.h"
#include "blob.h"
#include "error.h"
#include "freelist.h"
#include "mem.h"
#include "page.h"

/*
 * initial header of a blob
 */
#define BLOB_INITIAL_HEADER 1

/*
 * mark this blob as deleted
 */
#define BLOB_FLAG_DELETED   2


blob_t *
ham_page_get_blob(ham_page_t *page, ham_offset_t blobid)
{
    ham_db_t *db=page_get_owner(page);
    ham_offset_t pageid;

    /*
     * get the page-ID of this blob
     */
    blobid-=SIZEOF_PERS_HEADER;
    pageid=blobid;
    pageid=(blobid/db_get_pagesize(db))*db_get_pagesize(db);

    /*
     * is this the right page?
     */
    if (page_get_self(page)!=pageid+SIZEOF_PERS_HEADER) /* TODO log */
        return (0);

    /*
     * return the blob
     */
    return ((blob_t *)(&page->_pers->_s._payload[blobid%db_get_pagesize(db)]));
}

static blob_t *
my_blob_load(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, 
        ham_page_t **page)
{
    ham_offset_t pageid;

    /*
     * get page id from the blob id
     */
    pageid=blobid-SIZEOF_PERS_HEADER;
    pageid=(pageid/db_get_pagesize(db))*db_get_pagesize(db);

    /*
     * load the page
     */
    *page=db_fetch_page(db, txn, pageid+SIZEOF_PERS_HEADER, 0);
    if (!*page)
        return (0);

    return (ham_page_get_blob(*page, blobid));
}

static ham_page_t *
my_initialize_header(ham_db_t *db, ham_txn_t *txn, blob_t **blob, 
        ham_size_t datasize, ham_u32_t flags, ham_size_t *writestart, 
        ham_size_t *writesize)
{
    ham_size_t pagecount;
    ham_size_t headersize, pagesize=db_get_pagesize(db), size;
    blob_t *hdrblob;
    ham_page_t *hdrpage;
    ham_offset_t address;

    /*
     * try to estimate the number of pages we need
     */
    pagecount=datasize/pagesize;
    if (datasize%pagesize)
        pagecount++;
    pagecount+=2;

    /*
     * get a blob_t header for 'pagecount' pages (but make sure
     * that the blob_t header still fits in one page)
     */
    size=sizeof(blob_t)+sizeof(hdrblob->_parts[0])*(pagecount-1);
    if (size>pagesize) {
        pagecount=(pagesize-sizeof(blob_t))/sizeof(hdrblob->_parts[0]);
        size=sizeof(blob_t)+sizeof(hdrblob->_parts[0])*(pagecount-1);
    }

    /*
     * now allocate a page with enough room for the blob_t header
     * and the data itself (or, if this is too much, just a single page)
     */
    hdrpage=db_alloc_page(db, txn, DB_READ_WRITE);
            /* size+datasize>pagesize ? pagesize : size+datasize); */
    if (!hdrpage) 
        return (0);
    address=page_get_self(hdrpage);

    /*
     * initialize the blob
     */
    hdrblob=ham_page_get_blob(hdrpage, address);
    headersize=sizeof(blob_t)+sizeof(hdrblob->_parts[0])*(pagecount-1);
    memset(hdrblob, 0, headersize);
    blob_set_self(hdrblob, address);
    if (flags & BLOB_INITIAL_HEADER) {
        blob_set_flags(hdrblob, flags);
        blob_set_total_size(hdrblob, datasize);
    }
    blob_set_parts_size(hdrblob, pagecount);
    blob_set_part_size(hdrblob, 0, pagesize-(address%pagesize)-headersize);
    blob_set_part_offset(hdrblob, 0, address+headersize);

    *blob=hdrblob;
    *writesize =blob_get_part_size(hdrblob, 0);
    *writestart=blob_get_part_offset(hdrblob, 0);
    return (hdrpage);
}

ham_status_t
blob_allocate(ham_db_t *db, ham_txn_t *txn, ham_u8_t *data, 
        ham_size_t datasize, ham_u32_t flags, ham_offset_t *blobid)
{
    ham_status_t st;
    blob_t *hdrblob;
    ham_page_t *hdrpage=0, *page=0;
    ham_u8_t *dataptr=data;
    ham_size_t remaining=datasize, pagesize=db_get_pagesize(db);
    ham_size_t parts_left, writestart, writesize;

    /*
     * initialize a page with a blob_t header
     */
    hdrpage=my_initialize_header(db, txn, &hdrblob, remaining, 
            BLOB_INITIAL_HEADER, &writestart, &writesize);
    if (!hdrpage) {
        ham_trace("header initialization failed", 0, 0);
        return (db_get_error(db));
    }
    *blobid=blob_get_self(hdrblob);
    parts_left=blob_get_parts_size(hdrblob)-1;

    /*
     * write data to the header page
     */
    if (writesize) {
        writestart-=SIZEOF_PERS_HEADER;
        writestart%=db_get_pagesize(db);
        if (remaining<=writesize) {
            memcpy(&hdrpage->_pers->_s._payload[writestart], dataptr, 
                    remaining);
            blob_set_part_size(hdrblob, 0, remaining);
            blob_set_parts_size(hdrblob, 1);
            page_set_dirty(hdrpage, 1);
            remaining=0;
        }
        else {
            memcpy(&hdrpage->_pers->_s._payload[writestart], dataptr, 
                    writesize);
            dataptr+=writesize;
            remaining-=writesize;
            blob_set_parts_size(hdrblob, 1);
            page_set_dirty(hdrpage, 1);
        }
    }

    /*
     * while we have data left...
     */
    while (remaining) {
        /*
         * can we append another page to the current blob_t header?
         */
        if (parts_left==0) {
            blob_t *b=hdrblob;
            /*
             * no, we need a new page for additional parts
             *
             * 1. allocate a page
             * 2. set the overflow pointer of the old blob_t structure
             * 3. set parts_left 
             */
            hdrpage=my_initialize_header(db, txn, &hdrblob, remaining, 0, 
                    &writestart, &writesize);
            if (!hdrpage) {
                ham_trace("header initialization failed", 0, 0);
                st=db_get_error(db);
                (void)blob_free(db, txn, *blobid, 0);
                return (st);
            }
            blob_set_parts_overflow(b, page_get_self(hdrpage));
            parts_left=blob_get_parts_size(hdrblob);
            blob_set_parts_size(hdrblob, 0);
            if (writesize>remaining)
                writesize=remaining;
            writestart-=SIZEOF_PERS_HEADER;
            writestart%=db_get_pagesize(db);
        }
        else {
            /*
             * make sure that we don't write more than pagesize bytes
             */
            writestart=0;
            writesize=remaining<pagesize ? remaining : pagesize;
        }

        /*
         * get a new page - first, check the freelist for a page.
         * if that fails, then allocate a new one
         */

        page=db_alloc_page(db, txn, DB_READ_WRITE);
        if (!page) {
            ham_trace("page allocation failed", 0, 0);
            st=db_get_error(db);
            (void)blob_free(db, txn, *blobid, 0);
            return (st);
        }

        /*
         * write the data to the page
         */
        memcpy(&page->_pers->_s._payload[writestart], dataptr, writesize);
        dataptr+=writesize;
        parts_left--;
        remaining-=writesize;
        page_set_dirty(page, 1);

        /*
         * if writesize<pagesize, and we allocated a new page: 
         * add the remaining padding to the freelist
         */
        if (writesize<pagesize) {
            /* TODO */
        }

        /*
         * store the page ID and length
         */
        blob_set_part_offset(hdrblob, blob_get_parts_size(hdrblob), 
                page_get_self(page));
        blob_set_part_size(hdrblob, blob_get_parts_size(hdrblob), 
                writesize);
        blob_set_parts_size(hdrblob, blob_get_parts_size(hdrblob)+1);
        page_set_dirty(hdrpage, 1);
    }

    return (0);
}

ham_status_t
blob_read(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_size_t i;
    blob_t *blobhdr;
    ham_page_t *hdrpage, *page;
    ham_offset_t overflow;
    ham_u8_t *recdata;

    record->size=0;

    /*
     * load the blob header
     */
    blobhdr=my_blob_load(db, txn, blobid, &hdrpage);
    if (!blobhdr)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * check if this blob was deleted
     */
    if (blob_get_flags(blobhdr)&BLOB_FLAG_DELETED)
        return (HAM_BLOB_NOT_FOUND);

    record->size=blob_get_total_size(blobhdr);

    /*
     * if the memory was not allocated by the user, we might have
     * to resize it
     */
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        if (blob_get_total_size(blobhdr)>db_get_record_allocsize(db)) {
            void *newdata=ham_mem_alloc(blob_get_total_size(blobhdr));
            if (!newdata) 
                return (HAM_OUT_OF_MEMORY);
            if (record->data)
                ham_mem_free(record->data);
            record->data=newdata;
            db_set_record_allocdata(db, newdata);
            db_set_record_allocsize(db, blob_get_total_size(blobhdr));
        }
        else
            record->data=db_get_record_allocdata(db);
    }

    recdata=(ham_u8_t *)record->data;

    while (1) {
        /*
         * foreach page in blobhdr->_parts...
         */
        for (i=0; i<blob_get_parts_size(blobhdr); i++) {
            ham_offset_t pageid=blob_get_part_offset(blobhdr, i);
            pageid =(pageid/db_get_pagesize(db))*db_get_pagesize(db);
            /*pageid+=SIZEOF_PERS_HEADER; only needed if header is not 
             * page-aligned??? TODO test it! */

            /*
             * get the page
             */
            if (i==0 && pageid==page_get_self(hdrpage)) {
                page=hdrpage;
            }
            else {
                page=db_fetch_page(db, txn, pageid, 0);
                if (!page) 
                    return (db_get_error(db));
            }

            /*
             * append the page data to the record
             */
            pageid= blob_get_part_offset(blobhdr, i)-SIZEOF_PERS_HEADER;
            pageid%=db_get_pagesize(db);
            memcpy(recdata, &page->_pers->_s._payload[pageid],
                    blob_get_part_size(blobhdr, i));
            recdata+=blob_get_part_size(blobhdr, i);
        }

        /* 
         * get overflow pointer 
         */
        overflow=blob_get_parts_overflow(blobhdr);

        /*
         * get the next blob header, if available
         */
        if (!overflow)
            break;
        blobhdr=my_blob_load(db, txn, blobid, &hdrpage);
        if (!blobhdr)
            return (db_get_error(db));
    }

    return (0);
}

ham_status_t
blob_replace(ham_db_t *db, ham_txn_t *txn, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t datasize, ham_u32_t flags, 
        ham_offset_t *new_blobid)
{
    ham_status_t st;

    st=blob_free(db, txn, old_blobid, flags);
    if (st)
        ; /* TODO wat nu? */
    st=blob_allocate(db, txn, data, datasize, flags, new_blobid);
    if (st) /* TODO log */
        return (st);

    return (0);
}

ham_status_t
blob_free(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, ham_u32_t flags)
{
    ham_status_t st;
    blob_t *blobhdr;
    ham_page_t *page;
    ham_size_t i, headersize;
    ham_offset_t overflow;

    (void)flags;

    /*
     * load the blob header
     */
    blobhdr=my_blob_load(db, txn, blobid, &page);
    if (!blobhdr) /* TODO log */
        return (HAM_BLOB_NOT_FOUND);

    /*
     * mark this blob as deleted
     */
    blob_set_flags(blobhdr, 
            blob_get_flags(blobhdr)|BLOB_FLAG_DELETED);
    page_set_dirty(page, 1);

    while (1) {
        /*
         * foreach page in blobhdr->_parts...
         *
         * we skip part #0, since it contains the blob_t header.
         * to avoid concurrency problems (i.e. the blob parts
         * are freed, while the blob header page is already re-used
         * by another thread), we free part #0 later.
         */
        for (i=1; i<blob_get_parts_size(blobhdr); i++) {
            /*
             * move the page to the freelist
             */
            st=freel_add_area(db, txn, blob_get_part_offset(blobhdr, i), 
                    blob_get_part_size(blobhdr, i));
            if (st) /* TODO log */
                return (st);

            if (i>0)
                ham_assert(blob_get_part_offset(blobhdr, i-1)
                        !=blob_get_part_offset(blobhdr, i), 
                        "invalid offset in blob [%d,%d]", i-1, i);
        }

        /*
         * calculate size of part #0 + blob_t header
         */
        headersize=sizeof(blob_t)+sizeof(blobhdr->_parts[0])*
            (blob_get_parts_size(blobhdr)-1);

        /* 
         * get overflow pointer
         */
        overflow=blob_get_parts_overflow(blobhdr);
        if (!overflow)
            break;

        /*
         * now get rid of part #0
         */
        st=freel_add_area(db, txn, blobid, headersize);
        if (st) /* TODO log */
            return (st);

        /*
         * get the next blob header, if available
         */
        blobhdr=my_blob_load(db, txn, blobid, &page);
        if (!blobhdr) /* TODO log */
            return (HAM_BLOB_NOT_FOUND);
    }

    return (0);
}
