
#include <stdio.h>
#include <ham/hamsterdb.h>

#include "hamserver.h"

int main(void)
{
    ham_env_t *env;
    hamserver_t *srv;
    hamserver_config_t cfg;
    ham_status_t st;

    ham_env_new(&env);
    st=ham_env_create(env, "test.db", HAM_IN_MEMORY_DB, 0644);
    if (st) {
        printf("ham_env_create: %d\n", st);
        exit(-1);
    }

    cfg.port=8080;
    hamserver_init(&cfg, &srv);
    hamserver_add_environment(srv, env, "/test.db");
    
    while (1)
        getchar();

    hamserver_close(srv);
    ham_env_close(env, HAM_AUTO_CLEANUP);

    return (0);
}
