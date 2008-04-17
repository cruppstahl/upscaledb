/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <string.h>
#include "os.h"
#include "db.h"
#include "txn.h"
#include "log.h"

#define LOG_DEFAULT_THRESHOLD   64

static ham_size_t 
my_get_alligned_entry_size(ham_size_t data_size)
{
    ham_size_t s=sizeof(log_entry_t)+data_size;
    if (s%8!=0)
        s=((s/8)*8)+8;
    return (s);
}

static ham_status_t
my_log_clear_file(ham_log_t *log, int idx)
{
    ham_status_t st;

    st=os_truncate(log_get_fd(log, idx), sizeof(log_header_t));
    if (st)
        return (st);

    /* after truncate, the file pointer is far beyond the new end of file;
     * reset the file pointer, or the next write will resize the file to
     * the original size */
    st=os_seek(log_get_fd(log, idx), sizeof(log_header_t), HAM_OS_SEEK_SET);
    if (st)
        return (st);

    /* clear the transaction counters */
    log_set_open_txn(log, idx, 0);
    log_set_closed_txn(log, idx, 0);

    return (0);
}

static ham_status_t
my_insert_checkpoint(ham_log_t *log, ham_db_t *db)
{
    ham_status_t st;
    
    /*
     * first, flush the file; then append the checkpoint
     *
     * for this flush, we don't need to insert LOG_ENTRY_TYPE_FLUSH_PAGE;
     * therefore, set the state of the log accordingly. the page_flush()
     * routine can then check the state and not write logfile-entries
     * for each flush
     */
    log_set_state(log, log_get_state(log)|LOG_STATE_CHECKPOINT);
    st=ham_flush(db, 0);
    log_set_state(log, log_get_state(log)&~LOG_STATE_CHECKPOINT);
    if (st)
        return (st);

    return (ham_log_append_checkpoint(log));
}

ham_status_t
ham_log_create(mem_allocator_t *alloc, const char *dbpath, 
        ham_u32_t mode, ham_u32_t flags, ham_log_t **plog)
{
    int i;
    log_header_t header;
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)allocator_alloc(alloc, sizeof(ham_log_t));
    if (!log)
        return (HAM_OUT_OF_MEMORY);
    memset(log, 0, sizeof(ham_log_t));

    *plog=0;

    log_set_allocator(log, alloc);
    log_set_lsn(log, 1);
    log_set_flags(log, flags);
    log_set_threshold(log, LOG_DEFAULT_THRESHOLD);

    /* create the two files */
    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_create(filename, 0, mode, &log_get_fd(log, 0));
    if (st) {
        allocator_free(alloc, log);
        return (st);
    }

    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 1);
    st=os_create(filename, 0, mode, &log_get_fd(log, 1));
    if (st) {
        os_close(log_get_fd(log, 0), 0);
        allocator_free(alloc, log);
        return (st);
    }

    /* write the magic to both files */
    memset(&header, 0, sizeof(header));
    log_header_set_magic(&header, HAM_LOG_HEADER_MAGIC);

    for (i=0; i<2; i++) {
        st=os_write(log_get_fd(log, i), &header, sizeof(header));
        if (st) {
            (void)ham_log_close(log, HAM_FALSE);
            return (st);
        }
    }

    *plog=log;
    return (st);
}

ham_status_t
ham_log_open(mem_allocator_t *alloc, const char *dbpath, ham_u32_t flags,
        ham_log_t **plog)
{
    int i;
    log_header_t header;
    log_entry_t entry;
    ham_u64_t lsn[2];
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)allocator_alloc(alloc, sizeof(ham_log_t));
    if (!log)
        return (HAM_OUT_OF_MEMORY);
    memset(log, 0, sizeof(ham_log_t));

    *plog=0;

    log_set_allocator(log, alloc);
    log_set_flags(log, flags);

    /* open the two files */
    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_open(filename, 0, &log_get_fd(log, 0));
    if (st) {
        allocator_free(alloc, log);
        return (st);
    }

    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 1);
    st=os_open(filename, 0, &log_get_fd(log, 1));
    if (st) {
        os_close(log_get_fd(log, 0), 0);
        allocator_free(alloc, log);
        return (st);
    }

    /* check the magic in both files */
    memset(&header, 0, sizeof(header));
    for (i=0; i<2; i++) {
        st=os_pread(log_get_fd(log, i), 0, &header, sizeof(header));
        if (st) {
            (void)ham_log_close(log, HAM_FALSE);
            return (st);
        }
        if (log_header_get_magic(&header)!=HAM_LOG_HEADER_MAGIC) {
            ham_trace(("logfile has unknown magic or is corrupt"));
            (void)ham_log_close(log, HAM_FALSE);
            return (HAM_LOG_INV_FILE_HEADER);
        }
    }

    /* now read the LSN's of both files; the file with the older
     * LSN becomes file[0] */
    for (i=0; i<2; i++) {
        /* but make sure that the file is large enough! */
        ham_offset_t size;
        st=os_get_filesize(log_get_fd(log, i), &size);
        if (st) {
            (void)ham_log_close(log, HAM_FALSE);
            return (st);
        }

        if (size>=sizeof(entry)) {
            st=os_pread(log_get_fd(log, i), size-sizeof(log_entry_t), 
                            &entry, sizeof(entry));
            if (st) {
                (void)ham_log_close(log, HAM_FALSE);
                return (st);
            }
            lsn[i]=log_entry_get_lsn(&entry);
        }
        else
            lsn[i]=0;
    }

    if (lsn[1]<lsn[0]) {
        ham_fd_t temp=log_get_fd(log, 0);
        log_set_fd(log, 0, log_get_fd(log, 1));
        log_set_fd(log, 1, temp);
        log_set_current_fd(log, 1);
    }

    *plog=log;
    return (0);
}

ham_status_t
ham_log_is_empty(ham_log_t *log, ham_bool_t *isempty)
{
    ham_status_t st; 
    ham_offset_t size;
    int i;

    for (i=0; i<2; i++) {
        st=os_get_filesize(log_get_fd(log, i), &size);
        if (st)
            return (st);
        if (size && size!=sizeof(log_header_t)) {
            *isempty=HAM_FALSE;
            return (0);
        }
    }

    *isempty=HAM_TRUE;
    return (0);
}

ham_status_t
ham_log_append_entry(ham_log_t *log, int fdidx, void *entry, ham_size_t size)
{
    ham_status_t st;
   
    st=os_write(log_get_fd(log, fdidx), entry, size);
    if (st)
        return (st);

    st=os_flush(log_get_fd(log, fdidx));
    return (st);
}

ham_status_t
ham_log_append_txn_begin(ham_log_t *log, struct ham_txn_t *txn)
{
    ham_status_t st;
    log_entry_t entry;
    int cur=log_get_current_file(log);
    int other=cur ? 0 : 1;

    memset(&entry, 0, sizeof(entry));
    log_entry_set_prev_lsn(&entry, txn_get_last_lsn(txn));
    log_entry_set_txn_id(&entry, txn_get_id(txn));
    log_entry_set_type(&entry, LOG_ENTRY_TYPE_TXN_BEGIN);

    /* 
     * determine the log file which is used for this transaction 
     *
     * if the "current" file is not yet full, continue to write to this file
     */
    if (log_get_open_txn(log, cur)+log_get_closed_txn(log, cur)<
            log_get_threshold(log)) {
        txn_set_log_desc(txn, cur);
    }
    /*
     * otherwise, if the other file does no longer have open transactions,
     * insert a checkpoint, delete the other file and use the other file
     * as the current file
     */
    else if (log_get_open_txn(log, other)==0) {
        /* checkpoint! */
        st=my_insert_checkpoint(log, txn_get_db(txn));
        if (st)
            return (st);
        /* now clear the other file */
        st=my_log_clear_file(log, other);
        if (st)
            return (st);
        /* continue writing to the other file */
        cur=other;
        log_set_current_file(log, cur);
        txn_set_log_desc(txn, cur);
    }
    /*
     * otherwise continue writing to the current file, till the other file
     * can be deleted safely
     */
    else {
        txn_set_log_desc(txn, cur);
    }

    /*
     * now set the lsn (it might have been modified in 
     * my_insert_checkpoint())
     */
    log_entry_set_lsn(&entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);

    st=ham_log_append_entry(log, cur, &entry, sizeof(entry));
    if (st)
        return (st);
    log_set_open_txn(log, cur, log_get_open_txn(log, cur)+1);
    txn_set_last_lsn(txn, log_entry_get_lsn(&entry));

    /* store the fp-index in the log structure; it's needed so
     * log_append_checkpoint() can quickly find out which file is 
     * the newest */
    log_set_current_fd(log, cur);

    return (0);
}

ham_status_t
ham_log_append_txn_abort(ham_log_t *log, struct ham_txn_t *txn)
{
    int idx;
    ham_status_t st;
    log_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    log_entry_set_lsn(&entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_prev_lsn(&entry, txn_get_last_lsn(txn));
    log_entry_set_txn_id(&entry, txn_get_id(txn));
    log_entry_set_type(&entry, LOG_ENTRY_TYPE_TXN_ABORT);

    /*
     * update the transaction counters of this logfile
     */
    idx=txn_get_log_desc(txn);
    log_set_open_txn(log, idx, log_get_open_txn(log, idx)-1);
    log_set_closed_txn(log, idx, log_get_closed_txn(log, idx)+1);

    st=ham_log_append_entry(log, idx, &entry, sizeof(entry));
    if (st)
        return (st);
    txn_set_last_lsn(txn, log_entry_get_lsn(&entry));

    return (0);
}

ham_status_t
ham_log_append_txn_commit(ham_log_t *log, struct ham_txn_t *txn)
{
    int idx;
    ham_status_t st;
    log_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    log_entry_set_lsn(&entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_prev_lsn(&entry, txn_get_last_lsn(txn));
    log_entry_set_txn_id(&entry, txn_get_id(txn));
    log_entry_set_type(&entry, LOG_ENTRY_TYPE_TXN_COMMIT);

    /*
     * update the transaction counters of this logfile
     */
    idx=txn_get_log_desc(txn);
    log_set_open_txn(log, idx, log_get_open_txn(log, idx)-1);
    log_set_closed_txn(log, idx, log_get_closed_txn(log, idx)+1);

    st=ham_log_append_entry(log, idx, &entry, sizeof(entry));
    if (st)
        return (st);
    txn_set_last_lsn(txn, log_entry_get_lsn(&entry));

    return (0);
}

ham_status_t
ham_log_append_checkpoint(ham_log_t *log)
{
    ham_status_t st;
    log_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    log_entry_set_lsn(&entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_type(&entry, LOG_ENTRY_TYPE_CHECKPOINT);

    /* always write the checkpoint to the newer file */
    st=ham_log_append_entry(log, log_get_current_fd(log), 
            &entry, sizeof(entry));
    if (st)
        return (st);

    return (0);
}

ham_status_t
ham_log_append_flush_page(ham_log_t *log, struct ham_page_t *page)
{
    ham_status_t st;
    ham_u8_t buffer[sizeof(ham_offset_t)+sizeof(log_entry_t)];
    log_entry_t *entry=(log_entry_t *)(buffer+sizeof(ham_offset_t));
    ham_offset_t o=page_get_self(page);

    /* make sure that this is never called during a checkpoint! */
    ham_assert(!(log_get_state(log)&LOG_STATE_CHECKPOINT), (0));
    
    /* write the page ID _before_ the header */
    memcpy(&buffer[0], &o, sizeof(ham_offset_t));

    /* write the header */
    memset(entry, 0, sizeof(entry));
    log_entry_set_lsn(entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_type(entry, LOG_ENTRY_TYPE_FLUSH_PAGE);
    log_entry_set_data_size(entry, sizeof(ham_offset_t));

    st=ham_log_append_entry(log, 
            txn_get_log_desc(db_get_txn(page_get_owner(page))), 
            &buffer[0], sizeof(buffer));
    if (st)
        return (st);

    return (0);
}

ham_status_t
ham_log_append_write(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                ham_u8_t *data, ham_size_t size)
{
    ham_status_t st;
    ham_size_t alloc_size=my_get_alligned_entry_size(size);
    log_entry_t *entry;
    ham_u8_t *alloc_buf;
    
    alloc_buf=allocator_alloc(log_get_allocator(log), alloc_size);
    if (!alloc_buf)
        return (HAM_OUT_OF_MEMORY);

    entry=(log_entry_t *)(alloc_buf+alloc_size-sizeof(log_entry_t));

    memset(entry, 0, sizeof(*entry));
    log_entry_set_lsn(entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_type(entry, LOG_ENTRY_TYPE_WRITE);
    log_entry_set_offset(entry, offset);
    log_entry_set_data_size(entry, size);
    memcpy(alloc_buf, data, size);

    st=ham_log_append_entry(log, 
                    txn ? txn_get_log_desc(txn) : log_get_current_fd(log), 
                    alloc_buf, alloc_size);
    allocator_free(log_get_allocator(log), alloc_buf);
    if (st)
        return (st);

    return (0);
}

ham_status_t
ham_log_append_overwrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
        const ham_u8_t *old_data, const ham_u8_t *new_data, ham_size_t size)
{
    ham_status_t st;
    ham_size_t alloc_size=my_get_alligned_entry_size(size*2);
    log_entry_t *entry;
    ham_u8_t *alloc_buf;
    
    alloc_buf=allocator_alloc(log_get_allocator(log), alloc_size);
    if (!alloc_buf)
        return (HAM_OUT_OF_MEMORY);

    entry=(log_entry_t *)(alloc_buf+alloc_size-sizeof(log_entry_t));

    memset(entry, 0, sizeof(*entry));
    log_entry_set_lsn(entry, log_get_lsn(log));
    log_set_lsn(log, log_get_lsn(log)+1);
    log_entry_set_type(entry, LOG_ENTRY_TYPE_OVERWRITE);
    log_entry_set_data_size(entry, size*2);
    log_entry_set_offset(entry, offset);
    memcpy(alloc_buf, old_data, size);
    memcpy(alloc_buf+size, new_data, size);

    st=ham_log_append_entry(log, txn_get_log_desc(txn), alloc_buf, alloc_size);
    allocator_free(log_get_allocator(log), alloc_buf);
    if (st)
        return (st);

    return (0);
}

ham_status_t
ham_log_clear(ham_log_t *log)
{
    ham_status_t st; 
    int i;

    for (i=0; i<2; i++) {
        if ((st=my_log_clear_file(log, i)))
            return (st);
    }

    return (0);
}

ham_status_t
ham_log_get_entry(ham_log_t *log, log_iterator_t *iter, log_entry_t *entry,
                ham_u8_t **data)
{
    ham_status_t st;

    *data=0;

    /*
     * if state is 0: start from the end of the second file
     */
    if (!iter->_offset) {
        iter->_fdidx=1;
        st=os_get_filesize(log_get_fd(log, iter->_fdidx), &iter->_offset);
        if (st)
            return (st);
    }

    /* 
     * if the current file is empty: try to continue with the older file
     */
    if (iter->_offset<=sizeof(log_header_t)) {
        if (iter->_fdidx==0) {
            log_entry_set_lsn(entry, 0);
            return (0);
        }
        iter->_fdidx=0;
        st=os_get_filesize(log_get_fd(log, iter->_fdidx), &iter->_offset);
        if (st)
            return (st);
    }

    if (iter->_offset<=sizeof(log_header_t)) {
        log_entry_set_lsn(entry, 0);
        return (0);
    }

    /*
     * now read the entry-header from the file
     */
    iter->_offset-=sizeof(log_entry_t);

    st=os_pread(log_get_fd(log, iter->_fdidx), iter->_offset, 
                    entry, sizeof(*entry));
    if (st)
        return (st);

    /*
     * now read the data
     */
    if (log_entry_get_data_size(entry)) {
        ham_offset_t pos=iter->_offset-log_entry_get_data_size(entry);
        if (pos%8!=0)
            pos=(pos/8)*8;

        *data=allocator_alloc(log_get_allocator(log), 
                        log_entry_get_data_size(entry));
        if (!*data)
            return (HAM_OUT_OF_MEMORY);

        st=os_pread(log_get_fd(log, iter->_fdidx), pos, *data, 
                    log_entry_get_data_size(entry));
        if (st) {
            allocator_free(log_get_allocator(log), *data);
            *data=0;
            return (st);
        }

        iter->_offset=pos;
    }
    else
        *data=0;

    return (0);
}

ham_status_t
ham_log_prepare_overwrite(ham_log_t *log, const ham_u8_t *old_data, 
                ham_size_t size)
{
    ham_u8_t *p;

    ham_assert(log_get_overwrite_data(log)==0, (""));
    ham_assert(log_get_overwrite_size(log)==0, (""));

    p=allocator_alloc(log_get_allocator(log), size);
    if (!p)
        return (HAM_OUT_OF_MEMORY);
    memcpy(p, old_data, size);
    log_set_overwrite_data(log, p);
    log_set_overwrite_size(log, size);

    return (0);
}

ham_status_t
ham_log_finalize_overwrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                const ham_u8_t *new_data, ham_size_t size)
{
    ham_status_t st;

    ham_assert(log_get_overwrite_data(log)!=0, (""));
    ham_assert(log_get_overwrite_size(log)==size, (""));

    st=ham_log_append_overwrite(log, txn, offset, 
                    log_get_overwrite_data(log), new_data, size);

    allocator_free(log_get_allocator(log), log_get_overwrite_data(log));
    log_set_overwrite_data(log, 0);
    log_set_overwrite_size(log, 0);

    return (st);
}

ham_status_t
ham_log_close(ham_log_t *log, ham_bool_t noclear)
{
    ham_status_t st; 
    int i;

    if (!noclear) {
        st=ham_log_clear(log);
        if (st)
            return (st);
    }

    for (i=0; i<2; i++) {
        if (log_get_fd(log, i)!=HAM_INVALID_FD) {
            if ((st=os_close(log_get_fd(log, i), 0)))
                return (st);
            log_set_fd(log, i, HAM_INVALID_FD);
        }
    }

    allocator_free(log_get_allocator(log), log);
    return (0);
}

