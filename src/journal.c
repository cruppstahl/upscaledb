/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "journal.h"

#define JOURNAL_DEFAULT_THRESHOLD   64

#if 0
static ham_size_t 
__get_aligned_entry_size(ham_size_t data_size)
{
    ham_size_t s=sizeof(journal_entry_t)+data_size;
    s += 8-1;
	s -= (s % 8);
    return (s);
}
#endif

static ham_status_t
__journal_clear_file(journal_t *journal, int idx)
{
    ham_status_t st;

    st=os_truncate(journal_get_fd(journal, idx), sizeof(journal_header_t));
    if (st)
        return (st);

    /* after truncate, the file pointer is far beyond the new end of file;
     * reset the file pointer, or the next write will resize the file to
     * the original size */
    st=os_seek(journal_get_fd(journal, idx), 
                    sizeof(journal_header_t), HAM_OS_SEEK_SET);
    if (st)
        return (st);

    /* clear the transaction counters */
    journal_set_open_txn(journal, idx, 0);
    journal_set_closed_txn(journal, idx, 0);

    return (0);
}

static ham_status_t
__insert_checkpoint(journal_t *journal)
{
    /* TODO is this function really needed? */
    return (0);
}

ham_status_t
journal_create(ham_env_t *env, ham_u32_t mode, ham_u32_t flags, 
                journal_t **pjournal)
{
    int i;
    journal_header_t header;
    mem_allocator_t *alloc=env_get_allocator(env);
    const char *dbpath=env_get_filename(env);
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    journal_t *journal=(journal_t *)allocator_alloc(alloc, sizeof(journal_t));
    if (!journal)
        return (HAM_OUT_OF_MEMORY);
    memset(journal, 0, sizeof(*journal));

    *pjournal=0;

	ham_assert(env, (0));

    journal_set_allocator(journal, alloc);
	journal_set_env(journal, env);
    journal_set_lsn(journal, 1);
    journal_set_threshold(journal, JOURNAL_DEFAULT_THRESHOLD);

    /* initialize the magic */
    memset(&header, 0, sizeof(header));
    journal_header_set_magic(&header, HAM_JOURNAL_HEADER_MAGIC);

    /* create the two files */
    for (i=0; i<2; i++) {
        util_snprintf(filename, sizeof(filename), "%s.jrn%d", dbpath, i);
        st=os_create(filename, 0, mode, &journal_get_fd(journal, i));
        if (st) {
            (void)journal_close(journal, HAM_FALSE);
            return (st);
        }

        /* and write the magic */
        st=os_write(journal_get_fd(journal, i), &header, sizeof(header));
        if (st) {
            (void)journal_close(journal, HAM_FALSE);
            return (st);
        }
    }

    *pjournal=journal;

    return (st);
}

ham_status_t
journal_open(ham_env_t *env, ham_u32_t flags, journal_t **pjournal)
{
    int i;
    journal_header_t header;
    journal_entry_t entry;
    mem_allocator_t *alloc=env_get_allocator(env);
    const char *dbpath=env_get_filename(env);
    ham_u64_t lsn[2];
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    journal_t *journal=(journal_t *)allocator_alloc(alloc, sizeof(journal_t));
    if (!journal)
        return (HAM_OUT_OF_MEMORY);
    memset(journal, 0, sizeof(journal_t));

    *pjournal=0;

	ham_assert(env, (0));

    journal_set_allocator(journal, alloc);
	journal_set_env(journal, env);

    memset(&header, 0, sizeof(header));

    /* open the two files */
    for (i=0; i<2; i++) {
        util_snprintf(filename, sizeof(filename), "%s.jrn%d", dbpath, i);
        st=os_open(filename, 0, &journal_get_fd(journal, i));
        if (st) {
            journal_close(journal, HAM_FALSE);
            return (st);
        }

        /* check the magic */
        st=os_pread(journal_get_fd(journal, i), 0, &header, sizeof(header));
        if (st) {
            (void)journal_close(journal, HAM_FALSE);
            return (st);
        }
        if (journal_header_get_magic(&header)!=HAM_JOURNAL_HEADER_MAGIC) {
            ham_trace(("journal has unknown magic or is corrupt"));
            (void)journal_close(journal, HAM_FALSE);
            return (HAM_LOG_INV_FILE_HEADER);
        }
    }

    /* now read the LSN's of both files; the file with the older
     * LSN becomes file[0] */
    for (i=0; i<2; i++) {
        /* but make sure that the file is large enough! */
        ham_offset_t size;
        st=os_get_filesize(journal_get_fd(journal, i), &size);
        if (st) {
            (void)journal_close(journal, HAM_FALSE);
            return (st);
        }

        if (size>=sizeof(entry)) {
            st=os_pread(journal_get_fd(journal, i), 
                            size-sizeof(journal_entry_t), 
                            &entry, sizeof(entry));
            if (st) {
                (void)journal_close(journal, HAM_FALSE);
                return (st);
            }
            lsn[i]=journal_entry_get_lsn(&entry);
        }
        else
            lsn[i]=0;
    }

    if (lsn[1]>lsn[0]) {
        ham_fd_t temp=journal_get_fd(journal, 0);
        journal_set_fd(journal, 0, journal_get_fd(journal, 1));
        journal_set_fd(journal, 1, temp);
    }

    *pjournal=journal;
    return (0);
}

ham_status_t
journal_is_empty(journal_t *journal, ham_bool_t *isempty)
{
    ham_status_t st; 
    ham_offset_t size;
    int i;

    for (i=0; i<2; i++) {
        st=os_get_filesize(journal_get_fd(journal, i), &size);
        if (st)
            return (st);
        if (size && size!=sizeof(journal_header_t)) {
            *isempty=HAM_FALSE;
            return (0);
        }
    }

    *isempty=HAM_TRUE;
    return (0);
}

ham_status_t
journal_append_entry(journal_t *journal, int fdidx, journal_entry_t *entry, 
            ham_size_t size)
{
    ham_status_t st;

    st=os_write(journal_get_fd(journal, fdidx), entry, size);
    if (st)
        return (st);

    return (os_flush(journal_get_fd(journal, fdidx)));
}

ham_status_t
journal_append_txn_begin(journal_t *journal, struct ham_txn_t *txn)
{
    ham_status_t st;
    journal_entry_t entry;
    int cur=journal_get_current_fd(journal);
    int other=cur ? 0 : 1;

    memset(&entry, 0, sizeof(entry));
    journal_entry_set_txn_id(&entry, txn_get_id(txn));
    journal_entry_set_type(&entry, JOURNAL_ENTRY_TYPE_TXN_BEGIN);

    /* 
     * determine the journal file which is used for this transaction 
     *
     * if the "current" file is not yet full, continue to write to this file
     */
    if (journal_get_open_txn(journal, cur)+journal_get_closed_txn(journal, cur)<
            journal_get_threshold(journal)) {
        txn_set_log_desc(txn, cur);
    }
    else if (journal_get_open_txn(journal, other)==0) {
		/*
		 * Otherwise, if the other file does no longer have open Transactions,
		 * insert a checkpoint, delete the other file and use the other file
		 * as the current file
		 */

		/* checkpoint! */
        st=__insert_checkpoint(journal);
        if (st)
            return (st);
        /* now clear the other file */
        st=__journal_clear_file(journal, other);
        if (st)
            return (st);
        /* continue writing to the other file */
        cur=other;
        journal_set_current_fd(journal, cur);
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
     * Now set the lsn (it might have been modified in __insert_checkpoint())
     */
    journal_entry_set_lsn(&entry, journal_get_lsn(journal));
    journal_increment_lsn(journal);

    st=journal_append_entry(journal, cur, &entry, sizeof(entry));
    if (st)
        return (st);
    journal_set_open_txn(journal, cur, journal_get_open_txn(journal, cur)+1);

    /* store the fp-index in the journal structure; it's needed for
     * journal_append_checkpoint() to quickly find out which file is 
     * the newest */
    journal_set_current_fd(journal, cur);

    return (0);
}

ham_status_t
journal_append_txn_abort(journal_t *journal, struct ham_txn_t *txn)
{
    int idx;
    journal_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    journal_entry_set_lsn(&entry, journal_get_lsn(journal));
    journal_increment_lsn(journal);
    journal_entry_set_txn_id(&entry, txn_get_id(txn));
    journal_entry_set_type(&entry, JOURNAL_ENTRY_TYPE_TXN_ABORT);

    /*
     * update the transaction counters of this logfile
     */
    idx=txn_get_log_desc(txn);
    journal_set_open_txn(journal, idx, 
                    journal_get_open_txn(journal, idx)-1);
    journal_set_closed_txn(journal, idx, 
                    journal_get_closed_txn(journal, idx)+1);

    return (journal_append_entry(journal, idx, &entry, sizeof(entry)));
}

ham_status_t
journal_append_txn_commit(journal_t *journal, struct ham_txn_t *txn)
{
    int idx;
    journal_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    journal_entry_set_lsn(&entry, journal_get_lsn(journal));
    journal_increment_lsn(journal);
    journal_entry_set_txn_id(&entry, txn_get_id(txn));
    journal_entry_set_type(&entry, JOURNAL_ENTRY_TYPE_TXN_COMMIT);

    /*
     * update the transaction counters of this logfile
     */
    idx=txn_get_log_desc(txn);
    journal_set_open_txn(journal, idx, 
                    journal_get_open_txn(journal, idx)-1);
    journal_set_closed_txn(journal, idx, 
                    journal_get_closed_txn(journal, idx)+1);

    return (journal_append_entry(journal, idx, &entry, sizeof(entry)));
}

ham_status_t
journal_append_insert(journal_t *journal, ham_txn_t *txn, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags)
{
    /* TODO */
    return 0;
}

ham_status_t
journal_append_erase(journal_t *journal, ham_txn_t *txn, ham_key_t *key)
{
    /* TODO */
    return 0;
}

ham_status_t
journal_clear(journal_t *journal)
{
    ham_status_t st; 
    int i;

    for (i=0; i<2; i++) {
        if ((st=__journal_clear_file(journal, i)))
            return (st);
    }

    return (0);
}

ham_status_t
journal_get_entry(journal_t *journal, journal_iterator_t *iter, 
                    journal_entry_t *entry, ham_u8_t **data)
{
    ham_status_t st;

    *data=0;

    /* continue reading from the current file */
    if (!iter->_offset) {
        iter->_fdstart=iter->_fdidx=journal_get_current_fd(journal);
        st=os_get_filesize(journal_get_fd(journal, 
                    iter->_fdidx), &iter->_offset);
        if (st)
            return (st);
    }

    /* if the current file is empty: try to continue with the other file */
    if (iter->_offset<=sizeof(journal_header_t)) {
        if (iter->_fdidx!=iter->_fdstart) {
            journal_entry_set_lsn(entry, 0);
            return (0);
        }
        iter->_fdidx=(iter->_fdidx==0 ? 1 : 0);
        st=os_get_filesize(journal_get_fd(journal, iter->_fdidx), 
                        &iter->_offset);
        if (st)
            return (st);
    }

    if (iter->_offset<=sizeof(journal_header_t)) {
        journal_entry_set_lsn(entry, 0);
        return (0);
    }

    /* now read the entry-header from the file */
    iter->_offset-=sizeof(journal_entry_t);

    st=os_pread(journal_get_fd(journal, iter->_fdidx), iter->_offset, 
                    entry, sizeof(*entry));
    if (st)
        return (st);

    /* and finally read the data of the entry */
    if (journal_entry_get_data_size(entry)) {
        ham_offset_t pos=iter->_offset-journal_entry_get_data_size(entry);
		pos -= (pos % 8);

        *data=allocator_alloc(journal_get_allocator(journal), 
                        (ham_size_t)journal_entry_get_data_size(entry));
        if (!*data)
            return (HAM_OUT_OF_MEMORY);

        st=os_pread(journal_get_fd(journal, iter->_fdidx), pos, *data, 
                    (ham_size_t)journal_entry_get_data_size(entry));
        if (st) {
            allocator_free(journal_get_allocator(journal), *data);
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
journal_close(journal_t *journal, ham_bool_t noclear)
{
    ham_status_t st = 0; 
    int i;

    if (!noclear) {
        st = journal_clear(journal);
        if (st)
            return (st);
    }

    for (i=0; i<2; i++) {
        if (journal_get_fd(journal, i)!=HAM_INVALID_FD) {
            (void)os_close(journal_get_fd(journal, i), 0);
            journal_set_fd(journal, i, HAM_INVALID_FD);
        }
    }

    allocator_free(journal_get_allocator(journal), journal);

	return (0);
}

ham_status_t
journal_recover(journal_t *journal, ham_device_t *device, ham_env_t *env)
{
/* TODO */
#if 0
    ham_status_t st=0;
    journal_entry_t entry;
    log_iterator_t iter;
    ham_u8_t *data=0;
    ham_u64_t *txn_list=0;
    log_flush_entry_t *flush_list=0;
    ham_size_t txn_list_size=0;
	ham_size_t flush_list_size=0;
	ham_size_t i;
    ham_bool_t committed;
	ham_bool_t flushed;

    memset(&iter, 0, sizeof(iter));

    /*
     * walk backwards through the journal; every action which was not
     * committed, but flushed, is undone; every action which was 
     * committed, but not flushed, is redone
     */
    while (1) {
        if ((st=journal_get_entry(journal, &iter, &entry, &data)))
            goto bail;
        
        if (log_entry_get_lsn(&entry)==0)
            break;

        switch (log_entry_get_type(&entry)) {
            /* checkpoint: no need to continue */
            case LOG_ENTRY_TYPE_CHECKPOINT:
                goto bail;

            /* commit: store the txn-id */
            case LOG_ENTRY_TYPE_TXN_COMMIT:
                txn_list_size++;
                txn_list=(ham_u64_t *)allocator_realloc(log_get_allocator(journal),
                        txn_list, txn_list_size*sizeof(ham_u64_t));
                if (!txn_list) {
                    st=HAM_OUT_OF_MEMORY;
                    goto bail;
                }
                txn_list[txn_list_size-1]=log_entry_get_txn_id(&entry);
                break;

            /* an after-image: undo if flushed but not committed, 
             * redo if committed and not flushed */
            case LOG_ENTRY_TYPE_WRITE:
                /* 
				check if this page was flushed at a later time within 
				the same journal section (up to the next checkpoint): we're 
				walking BACKWARDS in time here and we must only restore
				the LATEST state.
				*/
                flushed=0;
                for (i=0; i<flush_list_size; i++) {
                    if (flush_list[i].page_id==log_entry_get_offset(&entry)
                            && flush_list[i].lsn>log_entry_get_lsn(&entry)) {
                        flushed=1;
                        break;
                    }
                }
                /* check if this txn was committed */
                committed=0;
                for (i=0; i<txn_list_size; i++) {
                    if (txn_list[i]==log_entry_get_txn_id(&entry)) {
                        committed=1;
                        break;
                    }
                }

                /* flushed and not committed: undo */
                if (flushed && !committed) {
                    ham_u8_t *udata;
                    log_iterator_t uiter=iter;
                    st=__undo(journal, &uiter, 
                            log_entry_get_offset(&entry), &udata);
                    if (st)
                        goto bail;
                    st=device->write(device, log_entry_get_offset(&entry),
                            udata, env_get_pagesize(env));
                    allocator_free(log_get_allocator(journal), udata);
                    if (st)
                        goto bail;
                    break;
                }
                /* not flushed and committed: redo */
                else if (!flushed && committed) {
                    st=device->write(device, log_entry_get_offset(&entry),
                            data, env_get_pagesize(env));
                    if (st)
                        goto bail;
                    /* since we just flushed the page: add page_id and lsn
                     * to the flush_list 
                     *
                     * fall through...
                     */

                }
                else
                    break;
            /* flush: store the page-id and the lsn*/
            case LOG_ENTRY_TYPE_FLUSH_PAGE:
                flush_list_size++;
                flush_list=(log_flush_entry_t *)allocator_realloc(
                        log_get_allocator(journal), flush_list, 
                        flush_list_size*sizeof(log_flush_entry_t));
                if (!flush_list) {
                    st=HAM_OUT_OF_MEMORY;
                    goto bail;
                }
                flush_list[flush_list_size-1].page_id=
                    log_entry_get_offset(&entry);
                flush_list[flush_list_size-1].lsn=
                    log_entry_get_lsn(&entry);
                break;

            /* ignore everything else */
            default:
                break;
        }

        if (data) {
            allocator_free(log_get_allocator(journal), data);
            data=0;
        }
    }

bail:
    if (txn_list)
        allocator_free(log_get_allocator(journal), txn_list);
    if (flush_list)
        allocator_free(log_get_allocator(journal), flush_list);
    if (data)
        allocator_free(log_get_allocator(journal), data);

    /*
     * did we goto bail because of an earlier error? then do not
     * clear the logfile but return
     */
    if (st)
        return (st);

    /*
     * clear the journal files and set the lsn to 1
     */
    st=journal_clear(journal);
    if (st) {
        ham_log(("unable to clear logfiles; please manually delete the "
                "journal files before re-opening the Database"));
        return (st);
    }

    log_set_lsn(journal, 1);
    log_set_current_fd(journal, 0);
#endif
    return (0);
}

