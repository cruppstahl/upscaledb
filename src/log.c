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
#include "log.h"
#include "os.h"

ham_log_t *
ham_log_create(ham_db_t *db, const char *dbpath, 
        ham_u32_t mode, ham_u32_t flags)
{
    int i;
    log_header_t header;
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)ham_mem_calloc(db, sizeof(ham_log_t));
    if (!log) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }

    log_set_db(log, db);
    log_set_flags(log, flags);

    /* create the two files */
    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_create(filename, 0, mode, &log_get_fd(log, 0));
    if (st) {
        ham_mem_free(db, log);
        db_set_error(db, st);
        return (0);
    }

    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 1);
    st=os_create(filename, 0, mode, &log_get_fd(log, 1));
    if (st) {
        os_close(log_get_fd(log, 0), 0);
        ham_mem_free(db, log);
        db_set_error(db, st);
        return (0);
    }

    /* write the magic to both files */
    memset(&header, 0, sizeof(header));
    log_header_set_magic(&header, HAM_LOG_HEADER_MAGIC);

    for (i=0; i<2; i++) {
        st=os_write(log_get_fd(log, i), &header, sizeof(header));
        if (st) {
            (void)ham_log_close(log);
            db_set_error(db, st);
            return (0);
        }
    }

    return (log);
}

ham_log_t *
ham_log_open(ham_db_t *db, const char *dbpath, ham_u32_t flags)
{
    int i;
    log_header_t header;
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)ham_mem_calloc(db, sizeof(ham_log_t));
    if (!log) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }

    log_set_db(log, db);
    log_set_flags(log, flags);

    /* open the two files */
    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_open(filename, 0, &log_get_fd(log, 0));
    if (st) {
        ham_mem_free(db, log);
        db_set_error(db, st);
        return (0);
    }

    snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 1);
    st=os_open(filename, 0, &log_get_fd(log, 1));
    if (st) {
        os_close(log_get_fd(log, 0), 0);
        ham_mem_free(db, log);
        db_set_error(db, st);
        return (0);
    }

    /* check the magic in both files */
    memset(&header, 0, sizeof(header));
    log_header_set_magic(&header, HAM_LOG_HEADER_MAGIC);

    for (i=0; i<2; i++) {
        st=os_pread(log_get_fd(log, i), 0, &header, sizeof(header));
        if (st) {
            (void)ham_log_close(log);
            db_set_error(db, st);
            return (0);
        }
        if (log_header_get_magic(&header)!=HAM_LOG_HEADER_MAGIC) {
            ham_trace(("logfile has unknown magic or is corrupt"));
            (void)ham_log_close(log);
            db_set_error(db, HAM_LOG_INV_FILE_HEADER);
            return (0);
        }
    }

    return (log);
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
ham_log_close(ham_log_t *log)
{
    ham_status_t st; 
    int i;

    for (i=0; i<2; i++) {
        if (log_get_fd(log, i)!=HAM_INVALID_FD) {
            if ((st=os_close(log_get_fd(log, i), 0)))
                return (st);
            log_set_fd(log, i, HAM_INVALID_FD);
        }
    }

    ham_mem_free(log_get_db(log), log);
    return (0);
}

