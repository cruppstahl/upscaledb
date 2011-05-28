/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <vector>
#include <iostream>
#include <sstream>
#include <cstdlib>

#include "../src/internal_fwd_decl.h"
#include "../src/log.h"
#include "../src/env.h"

#include "getopts.h"

#define ARG_HELP            1

/*
 * command line parameters
 */
static option_t opts[]={
    {
        ARG_HELP,               // symbolic name of this option
        "h",                    // short option 
        "help",                 // long option 
        "this help screen",     // help string
        0 },                    // no flags
    { 0, 0, 0, 0, 0 } /* terminating element */
};


class LogEntry : public log_entry_t
{
public:
    LogEntry(log_entry_t *entry) { 
        memcpy(&m_entry, entry, sizeof(m_entry));
    }

    log_entry_t m_entry;

public:
    static std::string log_entry_type2str(int type) {
        switch (type) {
            case LOG_ENTRY_TYPE_TXN_BEGIN:
                return "LOG_ENTRY_TYPE_TXN_BEGIN";
            case LOG_ENTRY_TYPE_TXN_ABORT:
                return "LOG_ENTRY_TYPE_TXN_ABORT";
            case LOG_ENTRY_TYPE_TXN_COMMIT:
                return "LOG_ENTRY_TYPE_TXN_COMMIT";
            case LOG_ENTRY_TYPE_PREWRITE:
                return "LOG_ENTRY_TYPE_PREWRITE";
            case LOG_ENTRY_TYPE_WRITE:
                return "LOG_ENTRY_TYPE_WRITE";
            case LOG_ENTRY_TYPE_CHECKPOINT:
                return "LOG_ENTRY_TYPE_CHECKPOINT";
            case LOG_ENTRY_TYPE_FLUSH_PAGE:
                return "LOG_ENTRY_TYPE_FLUSH_PAGE";
            default:
                return "LOG_ENTRY_TYPE_???";
        }
    }

    std::string to_str() {
        std::ostringstream o(std::ostringstream::out);
        o << "txn:" << log_entry_get_txn_id(&m_entry);
        o << ", lsn:" << log_entry_get_lsn(&m_entry);
        o << ", type:" << log_entry_get_type(&m_entry) << "(" << log_entry_type2str(log_entry_get_type(&m_entry)) << ")";
        o << ", offset:" << log_entry_get_offset(&m_entry);
        o << ", datasize:" << log_entry_get_data_size(&m_entry);
        return o.str();
    }
};

typedef std::vector<LogEntry> log_vector_t;

static void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

static log_vector_t 
readLog(ham_env_t *env, const char *filename)
{
    ham_status_t st;
    log_vector_t vec;
    ham_log_t *log;
    mem_allocator_t *alloc=env_get_allocator(env);

    st=ham_log_open(alloc, env, filename, 0, &log);
    if (st)
        error("ham_log_open", st);

    log_iterator_t iter;
    memset(&iter, 0, sizeof(iter));

    log_entry_t entry={0};
    ham_u8_t *data;
    while (1) {
        st=ham_log_get_entry(log, &iter, &entry, &data);
        if (st)
            error("ham_log_get_entry", st);

        /*
        printf("lsn: %d, txn: %d, type: %d, offset: %d, size %d\n",
                    (int)log_entry_get_lsn(&entry),
                    (int)log_entry_get_txn_id(&entry),
                    (int)log_entry_get_type(&entry),
                    (int)log_entry_get_offset(&entry),
                    (int)log_entry_get_data_size(&entry));
                    */

        vec.push_back(LogEntry(&entry));

        if (log_entry_get_lsn(&entry)==0)
            break;
    }

    ham_log_close(log, HAM_TRUE);
    return (vec);
}

static void
printLog(log_vector_t &vec)
{
    std::cout << std::endl;

    log_vector_t::iterator it=vec.begin();
    for (int i=0; it!=vec.end(); it++, i++) {
        std::cout << "[" << i << "]\t" << (*it).to_str() << std::endl;
    }
}

int 
main(int argc, char **argv)
{
    unsigned opt;
    char *filename=0, *param=0;

    ham_u32_t maj, min, rev;
    const char *licensee, *product;
    ham_get_license(&licensee, &product);
    ham_get_version(&maj, &min, &rev);

    ham_status_t st;
    ham_db_t *db;

    getopts_init(argc, argv, "ham_log");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case GETOPTS_PARAMETER:
                if (filename) {
                    printf("Multiple files specified. Please specify "
                           "only one filename.\n");
                    return (-1);
                }
                filename=param;
                break;
            case ARG_HELP:
                printf("hamsterdb %d.%d.%d - Copyright (C) 2005-20011 "
                       "Christoph Rupp (chris@crupp.de).\n\n",
                       maj, min, rev);

                if (licensee[0]=='\0')
                    printf(
                       "This program is free software; you can redistribute "
                       "it and/or modify it\nunder the terms of the GNU "
                       "General Public License as published by the Free\n"
                       "Software Foundation; either version 2 of the License,\n"
                       "or (at your option) any later version.\n\n"
                       "See file COPYING.GPL2 and COPYING.GPL3 for License "
                       "information.\n\n");
                else
                    printf("Commercial version; licensed for %s (%s)\n\n",
                            licensee, product);

                printf("usage: ham_log file \n");
                printf("usage: ham_log -h\n");
                printf("       -h:         this help screen (alias: --help)\n");

                return (0);
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `ham_log --help' for usage.", param);
                return (-1);
        }
    }

    if (!filename) {
        printf("Filename is missing. Enter `ham_log --help' for usage.\n");
        return (-1);
    }

    /*
     * open the database
     */
    st=ham_new(&db);
    if (st!=HAM_SUCCESS)
        error("ham_new", st);
    // open without recovery and transactions (they imply recovery)!
    st=ham_open_ex(db, filename, 0, 0);
    if (st==HAM_FILE_NOT_FOUND) {
        printf("File `%s' not found or unable to open it\n", filename);
        return (-1);
    }
    else if (st!=HAM_SUCCESS)
        error("ham_open_ex", st);

    log_vector_t vec=readLog(db_get_env(db), filename);
    printLog(vec);

    (void)ham_close(db, HAM_DONT_CLEAR_LOG);
    ham_delete(db);

    return (0);
}

