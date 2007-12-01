/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/backend.h"
#include "../src/btree.h"
#include "../src/os.h"
#include "../src/keys.h"

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

static void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

static ham_page_t *
load_page(FILE *f, ham_offset_t address, ham_size_t pagesize)
{
    ham_status_t st;
    ham_page_t *p=(ham_page_t *)malloc(sizeof(*p));
    if (!p) {
        printf("out of memory\n");
        return (0);
    }
    memset(p, 0, sizeof(*p));

    p->_pers=malloc(pagesize);
    if (!p->_pers) {
        free(p);
        printf("out of memory\n");
        return (0);
    }

    st=os_pread(fileno(f), address, p->_pers, pagesize);
    if (st) {
        free(p->_pers);
        free(p);
        printf("failed to read page: %s\n", ham_strerror(st));
        return (0);
    }

    return (p);
}

static void
recover_database(FILE *f, ham_u16_t dbname, ham_u16_t maxkeys, 
        ham_u16_t keysize, ham_u32_t pagesize, ham_u64_t rootaddr, 
        ham_u32_t flags)
{
    ham_page_t *page;
    btree_node_t *node;
    ham_offset_t ptr_left=rootaddr, right;
    int_key_t *bte;
    ham_size_t i;

    /*
     * traverse down to the smallest element in the leaf
     */
    while (1) {
        page=load_page(f, ptr_left, pagesize);
        if (!page) {
            printf("failed to read root page at address %llu\n", 
                    (unsigned long long)rootaddr);
            return;
        }

        node=ham_page_get_btree_node(page);

        ptr_left=btree_node_get_ptr_left(node);
        if (!ptr_left)
            break;

        free(page->_pers);
        free(page);
    }

    /*
     * we're at the leaf level - traverse through each node, 
     * dump the keys and then move to the right sibling
     */
    while (1) {
        node=ham_page_get_btree_node(page);

        /*
         * need a few macros for btree_node_get_key()
         */
#undef  db_get_keysize
#define db_get_keysize(x)      keysize

        if (btree_node_get_count(node)>maxkeys) {
            printf("broken page; skipping page\n");
            goto skip_page;
        }

        for (i=0; i<btree_node_get_count(node); i++) {
            ham_key_t key;
            ham_record_t rec;
            memset(&rec, 0, sizeof(rec));
            memset(&key, 0, sizeof(key));

            bte=btree_node_get_key(db, node, i);

            if (key_get_flags(bte)&KEY_HAS_DUPLICATES) {
                /* TODO */
            }
            else if (key_get_flags(bte)&KEY_IS_EXTENDED) {
                ham_record_t keyrec;
                memset(&keyrec, 0, sizeof(keyrec));

                if (!read_blob(bte, &keyrec)) {
                    printf("failed to read extended key; skipping key\n");
                    continue;
                }
                key.size=keyrec.size;
                key.data=keyrec.data;
            }
            else {
                key.size=key_get_size(bte);
                key.data=malloc(key.size);
                memcpy(key.data, key_get_key(bte), key.size);

                /* recno: switch endianness! */
                if (flags&HAM_RECORD_NUMBER) {
                    ham_u64_t recno=*(ham_u64_t *)key.data;
                    if (key.size!=sizeof(ham_u64_t)) {
                        printf("invalid key size of recno database; skipping "
                               "key\n");
                        if (key.data)
                            free(key.data);
                        continue;
                    }
                    recno=ham_db2h64(recno);
                    memcpy(key.data, &recno, sizeof(ham_u64_t));
                }
            }

            if (key_get_flags(bte)&KEY_BLOB_SIZE_TINY) {
                ham_offset_t rid=key_get_ptr(bte);
                char *p=(char *)&rid;
                rec.size=p[sizeof(ham_offset_t)-1];
                rec.data=malloc(rec.size);
                memcpy(rec.data, &rid, rec.size);
            }
            else if (key_get_flags(bte)&KEY_BLOB_SIZE_SMALL) {
                ham_offset_t rid=key_get_ptr(bte);
                rec.size=sizeof(ham_offset_t);
                rec.data=malloc(rec.size);
                memcpy(rec.data, &rid, rec.size);
            }
            else if (key_get_flags(bte)&KEY_BLOB_SIZE_EMPTY) {
                rec.size=0;
                rec.data=0;
            }
            else if (key_get_flags(bte)) {
                printf("unknown key flag 0x%08x, skipping key\n", 
                        key_get_flags(bte));
                continue;
            }

            /* TODO insert key/record to the new database */

            if (rec.data)
                free(rec.data);
        }

skip_page:
        right=btree_node_get_right(node);
        if (!right)
            break;

        free(page->_pers);
        free(page);
    }

    /*
     * clean up and return
     */
    free(page->_pers);
    free(page);
}

static void
recover_environment(const char *source, const char *destination)
{
    ham_env_t *denv;
    ham_status_t st;
    FILE *f;
    ham_u8_t hdrbuf[512], *indexdata, *hdrpage;
    int i, r;
    db_header_t *hdr;
    ham_u32_t pagesize, max_dbs;
    ham_parameter_t params[3];

    /*
     * open the source file
     */
    f=fopen(source, "rb");
    if (!f) {
        printf("failed to open source file %s: %s\n", source, strerror(errno));
        return;
    }

    /*
     * read the header page
     */
    r=fread(hdrbuf, 1, sizeof(hdrbuf), f);
    if (r!=sizeof(hdrbuf)) {
        printf("failed to read source header: %s\n", strerror(errno));
        return;
    }

    /*
     * now get a pointer to a headerpage structure and extract pagesize
     * and max-databases
     */
    hdr=(db_header_t *)(hdrbuf+12);
    pagesize=ham_db2h32(hdr->_pagesize);
    max_dbs =ham_db2h32(hdr->_max_databases);

    /*
     * re-read the full first page
     */
    hdrpage=(ham_u8_t *)malloc(pagesize);
    if (!hdrpage) {
        printf("out of memory\n");
        return;
    }
    st=os_pread(fileno(f), 0, hdrpage, pagesize);
    if (st) {
        printf("failed to read header page: %s\n", ham_strerror(st));
        return;
    }

    params[0].name =HAM_PARAM_PAGESIZE;
    params[0].value=pagesize;
    params[1].name =HAM_PARAM_MAX_ENV_DATABASES;
    params[1].value=max_dbs;
    params[2].name =0;
    params[2].value=0;

    /*
     * create a new environment with the same pagesize as the original
     */
    st=ham_env_new(&denv);
    if (st)
        error("ham_env_new", st);
    st=ham_env_create_ex(denv, destination, 0, 644, params);
    if (st)
        error("ham_env_create", st);

    indexdata=&hdrpage[12+sizeof(db_header_t)];
    for (i=0; i<max_dbs; i++, indexdata+=32) {
        ham_u16_t    dbname =ham_db2h16     (*(ham_u16_t    *)&indexdata[ 0]);
        ham_u16_t    maxkeys=ham_db2h16     (*(ham_u16_t    *)&indexdata[ 2]);
        ham_u16_t    keysize=ham_db2h16     (*(ham_u16_t    *)&indexdata[ 4]);
        ham_offset_t rootadd=ham_db2h_offset(*(ham_offset_t *)&indexdata[ 8]);
        ham_u32_t    flags  =ham_db2h32     (*(ham_u32_t    *)&indexdata[16]);
        ham_offset_t recno  =ham_db2h_offset(*(ham_offset_t *)&indexdata[20]);

        if (!dbname)
            continue;

        printf("database %d (0x%x)\n", (int)dbname, (int)dbname);
        printf("\tmax keys:            %d\n", (int)maxkeys);
        printf("\tkey size:            %d\n", (int)keysize);
        printf("\troot address:        %llu\n", (unsigned long long)rootadd);
        printf("\tflags:               0x%08x\n", flags);
        printf("\tmaximum key (recno): %llu\n", (unsigned long long)recno);

        recover_database(f, dbname, maxkeys, keysize, pagesize, rootadd, flags);
    }

    /*
     * clean up
     */
    fclose(f);
    free(hdrpage);
    st=ham_env_close(denv, 0);
    if (st)
        error("ham_env_close", st);
    ham_env_delete(denv);
}

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *source=0, *destination=0;

    getopts_init(argc, argv, "ham_recover");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case GETOPTS_PARAMETER:
                if (!source)
                    source=param;
                else if (!destination)
                    destination=param;
                else {
                    printf("Multiple files specified. Please specify "
                           "only two filenames.\n");
                    return (-1);
                }
                break;
            case ARG_HELP:
                printf("Copyright (C) 2005-2007 Christoph Rupp "
                       "(chris@crupp.de).\n\n"
                       "This program is free software; you can redistribute "
                       "it and/or modify it\nunder the terms of the GNU "
                       "General Public License as published by the Free\n"
                       "Software Foundation; either version 2 of the License,\n"
                       "or (at your option) any later version.\n\n"
                       "See file COPYING.GPL2 and COPYING.GPL3 for License "
                       "information.\n\n");
                getopts_usage(&opts[0]);
                return (0);
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `ham_recover --help' for usage.", param);
                return (-1);
        }
    }

    if (!source || !destination) {
        printf("Filename is missing. Enter `ham_recover --help' for usage.\n");
        return (-1);
    }

    /*
     * start the recovery process
     */
    recover_environment(source, destination);

    return (0);
}
