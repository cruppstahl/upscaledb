/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * A simple example which creates a server with one Environment and
 * several Databases
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/hamsterdb_srv.h>

#ifdef WIN32
#   define EXT ".exe"
#else
#   define EXT ""
#endif

int
main(void)
{
    ham_db_t *db;
    ham_env_t *env;
    ham_srv_t *srv;
    ham_srv_config_t cfg;
    ham_status_t st;
    char input[1024];

    st=ham_env_create(&env, "env1.db", HAM_ENABLE_TRANSACTIONS, 0644, 0);
    if (st) {
        printf("ham_env_create: %d\n", st);
        exit(-1);
    }

    st=ham_env_create_db(env, &db, 12, HAM_ENABLE_DUPLICATES, 0);
    if (st) {
        printf("ham_env_create_db: %d\n", st);
        exit(-1);
    }
    ham_close(db, 0);

    st=ham_env_create_db(env, &db, 13, HAM_ENABLE_DUPLICATES, 0);
    if (st) {
        printf("ham_env_create_db: %d\n", st);
        exit(-1);
    }
    ham_close(db, 0);

    st=ham_env_create_db(env, &db, 33,
                HAM_RECORD_NUMBER|HAM_ENABLE_DUPLICATES, 0);
    if (st) {
        printf("ham_env_create_db: %d\n", st);
        exit(-1);
    }
    ham_close(db, 0);

    memset(&cfg, 0, sizeof(cfg));
    cfg.port=8080;
    ham_srv_init(&cfg, &srv);
    ham_srv_add_env(srv, env, "/env1.db");

    printf("server1%s started - please run sample 'client1%s' for a test\n",
            EXT, EXT);
    printf("type 'exit' to end the server\n");

    while (1) {
        printf("> ");
        scanf("%s", &input[0]);
        if (!strcmp(input, "exit")) {
            printf("exiting...\n");
            break;
        }
        printf("unknown command\n");
    }

    ham_srv_close(srv);
    ham_env_close(env, HAM_AUTO_CLEANUP);

    return (0);
}
