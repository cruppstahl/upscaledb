
#include <stdio.h>

#include "hamserver.h"

int main(void)
{
    hamserver_t *srv;
    hamserver_config_t cfg;
    cfg.port=8080;

    hamserver_init(&cfg, &srv);
    hamserver_add_environment(srv, 0, "/test");
    
    while (1)
        getchar();

    hamserver_close(srv);
    return (0);
}
