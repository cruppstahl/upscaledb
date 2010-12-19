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
#include "log.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "journal.h"

#define JOURNAL_DEFAULT_THRESHOLD   16

static ham_size_t 
__get_aligned_entry_size(ham_size_t s)
{
    s += 8-1;
    s -= (s % 8);
    return (s);
}

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
    journal_set_fd(journal, 0, HAM_INVALID_FD);
    journal_set_fd(journal, 1, HAM_INVALID_FD);

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
    journal_entry_t entry={0};
    mem_allocator_t *alloc=env_get_allocator(env);
    const char *dbpath=env_get_filename(env);
    ham_u64_t lsn[2];
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    journal_t *journal=(journal_t *)allocator_alloc(alloc, sizeof(journal_t));
    if (!journal)
        return (HAM_OUT_OF_MEMORY);

    memset(journal, 0, sizeof(journal_t));
    journal_set_fd(journal, 0, HAM_INVALID_FD);
    journal_set_fd(journal, 1, HAM_INVALID_FD);

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

        /* read the lsn from the header structure */
        if (journal_get_lsn(journal)<journal_header_get_lsn(&header))
            journal_set_lsn(journal, journal_header_get_lsn(&header));
    }

    /* However, we now just read the lsn from the header structure. if there
     * are any additional logfile entries then the lsn of those entries is
     * more up-to-date than the one in the header structure. */
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

    /* The file with the higher lsn will become file[0] */
    if (lsn[1]>lsn[0]) {
        ham_fd_t temp=journal_get_fd(journal, 0);
        journal_set_fd(journal, 0, journal_get_fd(journal, 1));
        journal_set_fd(journal, 1, temp);
        if (journal_get_lsn(journal)<lsn[1])
            journal_set_lsn(journal, lsn[1]);
    }
    else {
        if (journal_get_lsn(journal)<lsn[0])
            journal_set_lsn(journal, lsn[0]);
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
journal_append_entry(journal_t *journal, int fdidx, 
            journal_entry_t *entry, void *aux, ham_size_t size)
{
    ham_status_t st;

    st=os_write(journal_get_fd(journal, fdidx), entry, sizeof(*entry));
    if (st)
        return (st);

    if (size) {
        st=os_write(journal_get_fd(journal, fdidx), aux, size);
        if (st)
            return (st);
    }

    return (os_flush(journal_get_fd(journal, fdidx)));
}

ham_status_t
journal_append_txn_begin(journal_t *journal, struct ham_txn_t *txn)
{
    ham_status_t st;
    journal_entry_t entry={0};
    int cur=journal_get_current_fd(journal);
    int other=cur ? 0 : 1;

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

    st=journal_append_entry(journal, cur, &entry, 0, 0);
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
    journal_entry_t entry={0};

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

    return (journal_append_entry(journal, idx, &entry, 0, 0));
}

ham_status_t
journal_append_txn_commit(journal_t *journal, struct ham_txn_t *txn)
{
    int idx;
    journal_entry_t entry={0};

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

    return (journal_append_entry(journal, idx, &entry, 0, 0));
}

ham_status_t
journal_append_insert(journal_t *journal, ham_txn_t *txn, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    journal_entry_t entry={0};
    journal_entry_insert_t *ins;
    ham_size_t size=sizeof(journal_entry_insert_t)+key->size+record->size-1;
    size=__get_aligned_entry_size(size);

    ins=(journal_entry_insert_t *)allocator_alloc(
                        journal_get_allocator(journal), size);
    if (!ins)
        return (HAM_OUT_OF_MEMORY);
    memset(ins, 0, size); /* TODO required? */
    journal_entry_set_lsn(&entry, journal_get_lsn(journal));
    journal_increment_lsn(journal);
    journal_entry_set_txn_id(&entry, txn_get_id(txn));
    journal_entry_set_type(&entry, JOURNAL_ENTRY_TYPE_INSERT);
    journal_entry_set_followup_size(&entry, size);
    journal_entry_insert_set_key_size(ins, key->size);
    journal_entry_insert_set_record_size(ins, record->size);
    journal_entry_insert_set_record_partial_size(ins, record->partial_size);
    journal_entry_insert_set_record_partial_offset(ins, record->partial_offset);
    journal_entry_insert_set_flags(ins, flags);
    memcpy(journal_entry_insert_get_key_data(ins), 
                    key->data, key->size);
    memcpy(journal_entry_insert_get_record_data(ins), 
                    record->data, record->size);

    /* append the entry to the logfile */
    st=journal_append_entry(journal, txn_get_log_desc(txn), 
                    &entry, (void *)ins, size);
    allocator_free(journal_get_allocator(journal), ins);
    
    return (st);
}

ham_status_t
journal_append_erase(journal_t *journal, ham_txn_t *txn, ham_key_t *key,
                ham_u32_t dupe, ham_u32_t flags)
{
    ham_status_t st;
    journal_entry_t entry={0};
    journal_entry_erase_t *aux;
    ham_size_t size=sizeof(journal_entry_erase_t)+key->size-1;
    size=__get_aligned_entry_size(size);

    aux=(journal_entry_erase_t *)allocator_alloc(
                        journal_get_allocator(journal), size);
    if (!aux)
        return (HAM_OUT_OF_MEMORY);
    memset(aux, 0, size); /* TODO required? */
    journal_entry_set_lsn(&entry, journal_get_lsn(journal));
    journal_increment_lsn(journal);
    journal_entry_set_txn_id(&entry, txn_get_id(txn));
    journal_entry_set_type(&entry, JOURNAL_ENTRY_TYPE_INSERT);
    journal_entry_set_followup_size(&entry, size);
    journal_entry_erase_set_key_size(aux, key->size);
    journal_entry_erase_set_flags(aux, flags);
    journal_entry_erase_set_dupe(aux, dupe);
    memcpy(journal_entry_erase_get_key_data(aux), key->data, key->size);

    /* append the entry to the logfile */
    st=journal_append_entry(journal, txn_get_log_desc(txn), 
                    &entry, (journal_entry_t *)aux, size);
    allocator_free(journal_get_allocator(journal), aux);
    
    return (st);
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
                    journal_entry_t *entry, void **aux)
{
    ham_status_t st;
    ham_offset_t filesize;

    *aux=0;

    /* if iter->_offset is 0, then the iterator was created from scratch
     * and we start reading from the first (oldest) entry.
     *
     * The oldest of the two logfiles is always the "other" one (the one
     * NOT in current_fd).
     */
    if (iter->_offset==0) {
        iter->_fdstart=iter->_fdidx=
                    journal_get_current_fd(journal)==0
                        ? 1
                        : 0;
        iter->_offset=sizeof(journal_header_t);
    }

    /* get the size of the journal file */
    st=os_get_filesize(journal_get_fd(journal, iter->_fdidx), &filesize);
    if (st)
        return (st);

    /* reached EOF? then either skip to the next file or we're done */
    if (filesize==iter->_offset) {
        if (iter->_fdstart==iter->_fdidx) {
            iter->_fdidx=iter->_fdidx==1 ? 0 : 1;
            iter->_offset=sizeof(journal_header_t);
            st=os_get_filesize(journal_get_fd(journal, iter->_fdidx), 
                        &filesize);
            if (st)
                return (st);
        }
        else {
            journal_entry_set_lsn(entry, 0);
            return (0);
        }
    }

    /* second file is also empty? then return */
    if (filesize==iter->_offset) {
        journal_entry_set_lsn(entry, 0);
        return (0);
    }

    /* now try to read the next entry */
    st=os_pread(journal_get_fd(journal, iter->_fdidx), iter->_offset, 
                    entry, sizeof(*entry));
    if (st)
        return (st);

    iter->_offset+=sizeof(*entry);

    /* read auxiliary data if it's available */
    if (journal_entry_get_followup_size(entry)) {
        ham_size_t size=journal_entry_get_followup_size(entry);
        *aux=allocator_alloc(journal_get_allocator(journal), size);
        if (!*aux)
            return (HAM_OUT_OF_MEMORY);

        st=os_pread(journal_get_fd(journal, iter->_fdidx), 
                        iter->_offset, *aux, size);
        if (st) {
            allocator_free(journal_get_allocator(journal), *aux);
            *aux=0;
            return (st);
        }

        iter->_offset+=size;
    }

    return (0);
}

ham_status_t
journal_close(journal_t *journal, ham_bool_t noclear)
{
    int i;
    ham_status_t st=0;

    if (!noclear) {
        journal_header_t header;

        (void)journal_clear(journal);

        /* update the header page of file 0 to store the lsn */
        memset(&header, 0, sizeof(header));
        journal_header_set_magic(&header, HAM_JOURNAL_HEADER_MAGIC);
        journal_header_set_lsn(&header, journal_get_lsn(journal));

        st=os_pwrite(journal_get_fd(journal, 0), 0, &header, sizeof(header));
    }

    for (i=0; i<2; i++) {
        if (journal_get_fd(journal, i)!=HAM_INVALID_FD) {
            (void)os_close(journal_get_fd(journal, i), 0);
            journal_set_fd(journal, i, HAM_INVALID_FD);
        }
    }

    allocator_free(journal_get_allocator(journal), journal);

    return (st);
}

static ham_status_t
__recover_get_db(ham_env_t *env, ham_u16_t dbname, ham_db_t **pdb)
{
    ham_status_t st;

    /* first check if the Database is already open */
    ham_db_t *db=env_get_list(env);
    while (db) {
        ham_u16_t name=index_get_dbname(env_get_indexdata_ptr(env,
                            db_get_indexdata_offset(db)));
        if (dbname==name) {
            *pdb=db;
            return (0);
        }
        db=db_get_next(db);
    }

    /* not found - open it */
    st=ham_new(&db);
    if (st)
        return (st);

    st=ham_env_open_db(env, db, dbname, 0, 0);
    if (st)
        return (st);

    *pdb=db;
    return (0);
}

static ham_status_t
__recover_get_txn(ham_env_t *env, ham_u32_t txn_id, ham_txn_t **ptxn)
{
    ham_txn_t *txn=env_get_oldest_txn(env);
    while (txn) {
        if (txn_get_id(txn)==txn_id) {
            *ptxn=txn;
            return (0);
        }
        txn=txn_get_older(txn);
    }

    *ptxn=0;
    return (HAM_INTERNAL_ERROR);
}

static ham_status_t
__close_all_databases(ham_env_t *env)
{
    ham_status_t st;
    ham_db_t *db;

    while ((db=env_get_list(env))) {
        st=ham_close(db, 0);
        if (st) {
            ham_log(("ham_close() failed w/ error %d (%s)", st, 
                    ham_strerror(st)));
            return (st);
        }

        ham_delete(db);
    }

    return (0);
}

static ham_status_t
__abort_uncommitted_txns(ham_env_t *env)
{
    ham_status_t st;
    ham_txn_t *older, *txn=env_get_oldest_txn(env);

    while (txn) {
        older=txn_get_older(txn);
        if (!(txn_get_flags(txn)&TXN_STATE_COMMITTED)) {
            st=ham_txn_abort(txn, 0);
            if (st)
                return (st);
        }
        txn=older;
    }

    return (0);
}

ham_status_t
journal_recover(journal_t *journal)
{
    ham_status_t st;
    ham_env_t *env=journal_get_env(journal);
    ham_u64_t start_lsn=log_get_lsn(env_get_log(env));
    journal_iterator_t it={0};

    /* recovering the journal is rather simple - we iterate over the 
     * files and re-issue every operation (incl. txn_begin and txn_abort).
     *
     * we start with the lsn described in the logfile - all previous
     * operations will be ignored because they were already flushed to disk.
     *
     * when we're done, we auto-abort all transactions that were not yet
     * committed.
     */

    /* make sure that there are no pending transactions - we start with 
     * a clean state! */
    ham_assert(env_get_oldest_txn(env)==0, (""));

    ham_assert(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS, (""));
    ham_assert(env_get_rt_flags(env)&HAM_ENABLE_RECOVERY, (""));

    /* officially disable recovery - otherwise while recovering we log
     * more stuff */
    env_set_rt_flags(env, env_get_rt_flags(env)&~HAM_ENABLE_RECOVERY);

    do {
        journal_entry_t entry={0};
        void *aux;

        /* get the next entry */
        st=journal_get_entry(journal, &it, &entry, (void **)&aux);
        if (st)
            goto bail;

        /* reached end of logfile? */
        if (!journal_entry_get_lsn(&entry))
            break;

        /* skip entries past start_lsn */
        if (journal_entry_get_lsn(&entry)<=start_lsn)
            continue;

        /* re-apply this operation */
        switch (journal_entry_get_type(&entry)) {
        case JOURNAL_ENTRY_TYPE_TXN_BEGIN: {
            ham_txn_t *txn;
            ham_db_t *db;
            st=__recover_get_db(env, journal_entry_get_dbname(&entry), &db);
            if (st)
                break;
            st=ham_txn_begin(&txn, db, 0);
            /* on success: patch the txn ID */
            if (st==0)
                txn_set_id(txn, journal_entry_get_txn_id(&entry));
            break;
        }
        case JOURNAL_ENTRY_TYPE_TXN_ABORT: {
            ham_txn_t *txn;
            st=__recover_get_txn(env, journal_entry_get_txn_id(&entry), &txn);
            if (st)
                break;
            st=ham_txn_abort(txn, 0);
            break;
        }
        case JOURNAL_ENTRY_TYPE_TXN_COMMIT: {
            ham_txn_t *txn;
            st=__recover_get_txn(env, journal_entry_get_txn_id(&entry), &txn);
            if (st)
                break;
            st=ham_txn_commit(txn, 0);
            break;
        }
        case JOURNAL_ENTRY_TYPE_INSERT: {
            journal_entry_insert_t *ins=(journal_entry_insert_t *)aux;
            ham_txn_t *txn;
            ham_db_t *db;
            ham_key_t key={0};
            ham_record_t record={0};
            if (!ins) {
                st=HAM_IO_ERROR;
                goto bail;
            }
            key.data=journal_entry_insert_get_key_data(ins);
            key.size=journal_entry_insert_get_key_size(ins);
            record.data=journal_entry_insert_get_record_data(ins);
            record.size=journal_entry_insert_get_record_size(ins);
            record.partial_size=
                        journal_entry_insert_get_record_partial_size(ins);
            record.partial_offset=
                        journal_entry_insert_get_record_partial_offset(ins);
            st=__recover_get_txn(env, journal_entry_get_txn_id(&entry), &txn);
            if (st)
                break;
            st=__recover_get_db(env, journal_entry_get_dbname(&entry), &db);
            if (st)
                break;
            st=ham_insert(db, txn, &key, &record, 
                        journal_entry_insert_get_flags(ins));
            break;
        }
        case JOURNAL_ENTRY_TYPE_ERASE: {
            journal_entry_erase_t *e=(journal_entry_erase_t *)aux;
            ham_txn_t *txn;
            ham_db_t *db;
            ham_key_t key={0};
            if (!e) {
                st=HAM_IO_ERROR;
                goto bail;
            }
            st=__recover_get_txn(env, journal_entry_get_txn_id(&entry), &txn);
            if (st)
                break;
            st=__recover_get_db(env, journal_entry_get_dbname(&entry), &db);
            if (st)
                break;
            key.data=journal_entry_erase_get_key_data(e);
            key.size=journal_entry_erase_get_key_size(e);
            st=ham_erase(db, txn, &key, journal_entry_erase_get_flags(e));
            break;
        }
        default:
            ham_log(("invalid journal entry type or journal is corrupt"));
            st=HAM_IO_ERROR;
        }

        if (st)
            goto bail;

        journal_set_lsn(journal, journal_entry_get_lsn(journal));
    } while (1);

bail:
    /* all transactions which are not yet committed will be aborted */
    (void)__abort_uncommitted_txns(env);

    /* also close and delete all open databases - they were created in
     * __recover_get_db() */
    (void)__close_all_databases(env);

    /* restore original flags */
    env_set_rt_flags(env, env_get_rt_flags(env)|HAM_ENABLE_RECOVERY);

    if (st)
        return (st);

    /* clear the journal files */
    st=journal_clear(journal);
    if (st) {
        ham_log(("unable to clear journal; please manually delete the "
                "journal files before re-opening the Database"));
        return (st);
    }

    return (0);
}

