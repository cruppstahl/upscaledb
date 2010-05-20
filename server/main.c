
#include <stdio.h>
#include <stdlib.h>
#include <ham/hamsterdb.h>

#include "hamserver.h"

int main(void)
{
    ham_db_t *db;
    ham_env_t *env;
    hamserver_t *srv;
    hamserver_config_t cfg;
    ham_status_t st;

    ham_env_new(&env);
    st=ham_env_create(env, "test.db", 0, 0644);
    if (st) {
        printf("ham_env_create: %d\n", st);
        exit(-1);
    }

    ham_new(&db);
    st=ham_env_create_db(env, db, 14, 0, 0);
    if (st) {
        printf("ham_env_create_db: %d\n", st);
        exit(-1);
    }
    ham_close(db, 0);

    st=ham_env_create_db(env, db, 13, 0, 0);
    if (st) {
        printf("ham_env_create_db: %d\n", st);
        exit(-1);
    }

    cfg.port=8080;
    hamserver_init(&cfg, &srv);
    hamserver_add_env(srv, env, "/test.db");
    
    while (1)
        getchar();

    hamserver_close(srv);
    ham_env_close(env, HAM_AUTO_CLEANUP);
    ham_env_delete(env);
    ham_delete(db);

    return (0);
}
