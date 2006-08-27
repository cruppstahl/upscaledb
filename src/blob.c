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

static ham_page_t *
my_allocate_header(ham_db_t *db, ham_txn_t *txn, 
        ham_size_t size, blob_t **hdr)
{
    ham_page_t *page;
    ham_size_t hdrsize, tocsize, writesize;
    ham_offset_t writestart;

    *hdr=0;

    /*
     * calculate the (estimated) toc size and header size
     */
    tocsize=(size/db_get_usable_pagesize(db))+3;
    hdrsize=sizeof(blob_t)+(tocsize-1)*sizeof(struct blob_chunk_t);

    /*
     * allocate a page or an area which is large enough for the header 
     * and maybe even the size
     */
    page=db_alloc_area(db, PAGE_TYPE_BLOBHDR, txn, 0, 
            hdrsize+size, &writestart, &writesize);
    if (!page)
        return (0);

    /*
     * how many toc-elements can we really have?
     */
    if (writesize!=hdrsize+size) {
        if (writesize<hdrsize)
            tocsize=(writesize-sizeof(blob_t))/sizeof(struct blob_chunk_t);
    }

    /*
     * get a pointer to the header structure and initialize the header
     */
    *hdr=(blob_t *)&page_get_payload(page)[writestart]; 
    memset(*hdr, 0, writesize);
    blob_set_self(*hdr, page_get_self(page)+writestart);
    blob_set_total_size(*hdr, size);
    blob_set_toc_maxsize(*hdr, tocsize);

    /*
     * if the allocated area is large enough: store the first chunk 
     * in this page
     *
     * the start offset and size of this chunk is a bit tricky - it's the
     * id of the blob + sizeof(blob_t) + the toc entries
     */
    if (writesize>hdrsize) {
        blob_set_toc_usedsize(*hdr, 1);
        blob_set_chunk_offset(*hdr, 0, page_get_self(page)+writestart+hdrsize);
        blob_set_chunk_size  (*hdr, 0, writesize-hdrsize);
        blob_set_chunk_flags (*hdr, 0, BLOB_CHUNK_NEXT_TO_HEADER);

        if (page_get_self(page)==blob_get_self(*hdr) &&
            writesize==db_get_usable_pagesize(db))
            blob_set_chunk_flags(*hdr, 0, 
                    blob_get_chunk_flags(*hdr, 0)|BLOB_CHUNK_SPANS_PAGE);
    }

    page_set_dirty(page, 1);
    return (page);
}

static ham_page_t *
my_load_header(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, 
        blob_t **hdr)
{
    ham_page_t *page;
    ham_offset_t pageid=blobid;

    *hdr=0;

    pageid=(pageid/db_get_pagesize(db))*db_get_pagesize(db);

    page=db_fetch_page(db, txn, pageid, 0);
    if (!page)
        return (0);

    *hdr=(blob_t *)&page_get_payload(page)[blobid-pageid];
    return (page);
}

ham_status_t
blob_allocate(ham_db_t *db, ham_txn_t *txn, ham_u8_t *data, 
        ham_size_t size, ham_u32_t flags, ham_offset_t *blobid)
{
    ham_size_t remaining=size;
    ham_size_t writesize;
    ham_offset_t writestart;
    ham_page_t *page=0;
    blob_t *hdr=0;

    *blobid=0;

    /*
     * while we have to write remaining data
     */
    while (remaining) {
        writestart=0; 
        writesize=0;

        /*
         * if the toc is full (or no toc was yet allocated): 
         * allocate a header for this blob; the allocation 
         * also returns the address and size of the first chunk
         * in toc[0]
         */
        if (!hdr || blob_get_toc_usedsize(hdr)==blob_get_toc_maxsize(hdr)) {
            blob_t *oldhdr=hdr;
            page=my_allocate_header(db, txn, remaining, &hdr);
            if (!page)
                return (db_get_error(db));
            writestart=blob_get_chunk_offset(hdr, 0);
            writesize =blob_get_chunk_size(hdr, 0);
            /* fix the overflow links */
            if (oldhdr)
                blob_set_overflow(oldhdr, blob_get_self(hdr));
            else
                *blobid=blob_get_self(hdr);
            /* chunk offset is absolute! */
            writestart-=page_get_self(page);
        }

        /*
         * otherwise allocate a page or an area for the next chunk
         */
        if (!writesize) {
            page=db_alloc_area(db, PAGE_TYPE_BLOBDATA, txn, 0, 
                    remaining, &writestart, &writesize);
            if (!page)
                return (db_get_error(db));
            /* chunk offset is absolute! */
            blob_set_chunk_offset(hdr, blob_get_toc_usedsize(hdr), 
                    writestart+page_get_self(page));
            blob_set_chunk_size(hdr, blob_get_toc_usedsize(hdr), writesize);
            blob_set_toc_usedsize(hdr, blob_get_toc_usedsize(hdr)+1);
        }

        /*
         * write the data
         */
        memcpy(&page_get_payload(page)[writestart], data, writesize);

        /*
         * update remaining size and data pointer
         */
        data+=writesize;
        remaining-=writesize;
        page_set_dirty(page, 1);
    }

    return (0);
}

ham_status_t
blob_read(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_page_t *page, *cpage;
    blob_t *hdr=0;
    ham_u8_t *data=0;
    ham_size_t i, total_size;

    record->size=0;

    /*
     * load the blob header
     */
    page=my_load_header(db, txn, blobid, &hdr);
    if (!page)
        return (HAM_BLOB_NOT_FOUND);

    total_size=blob_get_total_size(hdr);

    /*
     * if the memory was not allocated by the user, we might have
     * to resize it
     */
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        if (blob_get_total_size(hdr)>db_get_record_allocsize(db)) {
            void *newdata=ham_mem_alloc(blob_get_total_size(hdr));
            if (!newdata) 
                return (HAM_OUT_OF_MEMORY);
            if (record->data)
                ham_mem_free(record->data);
            record->data=newdata;
            db_set_record_allocdata(db, newdata);
            db_set_record_allocsize(db, blob_get_total_size(hdr));
        }
        else
            record->data=db_get_record_allocdata(db);
    }
    data=(ham_u8_t *)record->data;

    while (1) {
        /*
         * foreach toc entry
         */
        for (i=0; i<blob_get_toc_usedsize(hdr); i++) {
            ham_offset_t chunkstart, pageid;
            ham_size_t chunksize;

            /*
             * read the chunk
             */
            pageid=chunkstart=blob_get_chunk_offset(hdr, i);
            chunksize=blob_get_chunk_size(hdr, i);

            /*
             * fetch the page
             *
             * !!!
             * call db_fetch_page without a txn pointer - the page_t
             * structure is no longer needed and can be reused
             *
             * !!!
             * this is not thread safe and therefore bad for concurrency
             */
            pageid=(pageid/db_get_pagesize(db))*db_get_pagesize(db);
            if (pageid==page_get_self(page))
                cpage=page;
            else
                cpage=db_fetch_page(db, 0, pageid, 0);
            if (!cpage)
                return (HAM_BLOB_NOT_FOUND);

            /*
             * append the data
             */
            memcpy(data, &page_get_payload(cpage)[chunkstart-pageid], 
                    chunksize);
            data+=chunksize;
        }

        /*
         * load the next blob header
         */
        if (!blob_get_overflow(hdr)) 
            break;
        page=my_load_header(db, txn, blob_get_overflow(hdr), &hdr);
        if (!page)
            return (HAM_BLOB_NOT_FOUND);
    }

    record->size=total_size;
    return (0);
}

ham_status_t
blob_replace(ham_db_t *db, ham_txn_t *txn, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t size, ham_u32_t flags, 
        ham_offset_t *new_blobid)
{
    ham_status_t st;

    st=blob_free(db, txn, old_blobid, flags);
    if (st)
        return (st);
    st=blob_allocate(db, txn, data, size, flags, new_blobid);
    if (st) 
        return (st);

    return (0);
}

ham_status_t
blob_free(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, ham_u32_t flags)
{
    ham_page_t *page;
    blob_t *hdr;
    ham_size_t i;

    /*
     * load the blob header
     */
    page=my_load_header(db, txn, blobid, &hdr);
    if (!page)
        return (HAM_BLOB_NOT_FOUND);

    while (1) {
        /*
         * foreach toc entry
         */
        for (i=0; i<blob_get_toc_usedsize(hdr); i++) {
            ham_offset_t chunkstart;
            ham_size_t chunksize;

            /*
             * get the chunk
             */
            chunkstart=blob_get_chunk_offset(hdr, 0);
            chunksize=blob_get_chunk_size(hdr, 0);

            /*
             * if the chunk spans the full page: load the page and delete
             * it
             */
            if (blob_get_chunk_flags(hdr, 0)&BLOB_CHUNK_SPANS_PAGE) {
                page=db_fetch_page(db, txn, chunkstart, 0);
                if (page)
                    (void)db_free_page(db, txn, page, 0);
            }
            /*
             * otherwise: if this chunk is adjacent to the blob_t header, 
             * we also delete the blob_t header
             */
            else if (blob_get_chunk_flags(hdr, 0)&BLOB_CHUNK_NEXT_TO_HEADER) {
                chunkstart-=sizeof(blob_t)+(blob_get_toc_maxsize(hdr)-1)*
                    sizeof(struct blob_chunk_t);
                (void)freel_add_area(db, chunkstart, chunksize);
            }
            /*
             * otherwise delete the chunk
             */
            else {
                (void)freel_add_area(db, chunkstart, chunksize);
            }
        }

        /*
         * load the next blob header
         */
        if (!blob_get_overflow(hdr)) 
            break;
        page=my_load_header(db, txn, blob_get_overflow(hdr), &hdr);
        if (!page)
            return (HAM_BLOB_NOT_FOUND);
    }

    return (0);
}
