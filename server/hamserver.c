/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */


#include <stdio.h> /* needed for mongoose.h */
#include <malloc.h>
#include <string.h>

#include <ham/types.h>
#include <mongoose/mongoose.h>

#include "hamserver.h"
#include "os.h"

/* max. number of open hamsterdb Environments - if you change this, also change
 * MAX_CALLBACKS in 3rdparty/mongoose/mongoose.c! */
#define MAX_ENVIRONMENTS    128

static const char *standard_reply =	"HTTP/1.1 200 OK\r\n"
                                    "Content-Type: text/plain\r\n"
                                    "Connection: close\r\n\r\n";


struct hamserver_t
{
    /* the mongoose context structure */
    struct mg_context *mg_ctxt;

    /* handler for each Environment */
    struct env_t {
        ham_env_t *env;
        os_critsec_t cs;
        char *urlname;
    } environments[MAX_ENVIRONMENTS];
};

static void
request_handler(struct mg_connection *conn, const struct mg_request_info *ri,
                void *user_data)
{
    struct env_t *env=(struct env_t *)user_data;
    os_critsec_enter(&env->cs);
	int	i;

	mg_printf(conn, "%s", standard_reply);

	mg_printf(conn, "Method: [%s]\n", ri->request_method);
	mg_printf(conn, "URI: [%s]\n", ri->uri);
	mg_printf(conn, "HTTP version: [%d.%d]\n", ri->http_version_major, 
            ri->http_version_minor);

	for (i = 0; i < ri->num_headers; i++)
		mg_printf(conn, "HTTP header [%s]: [%s]\n",
			 ri->http_headers[i].name,
			 ri->http_headers[i].value);

	mg_printf(conn, "Query string: [%s]\n",
			ri->query_string ? ri->query_string: "");
	mg_printf(conn, "POST data: [%.*s]\n",
			ri->post_data_len, ri->post_data);
	mg_printf(conn, "Remote IP: [%lu]\n", ri->remote_ip);
	mg_printf(conn, "Remote port: [%d]\n", ri->remote_port);
	mg_printf(conn, "Remote user: [%s]\n",
			ri->remote_user ? ri->remote_user : "");
	mg_printf(conn, "Hamsterdb url: [%s]\n", env->urlname);

    os_critsec_leave(&env->cs);
}

ham_bool_t 
hamserver_init(hamserver_config_t *config, hamserver_t **psrv)
{
    hamserver_t *srv;
    char buf[32];
    sprintf(buf, "%d", (int)config->port);

    srv=(hamserver_t *)malloc(sizeof(hamserver_t));
    if (!srv)
        return (HAM_FALSE);
    memset(srv, 0, sizeof(*srv));

    srv->mg_ctxt=mg_start();
    mg_set_option(srv->mg_ctxt, "ports", buf);

    *psrv=srv;
    return (HAM_TRUE);
}

ham_bool_t 
hamserver_add_env(hamserver_t *srv, ham_env_t *env, const char *urlname)
{
    int i;

    /* search for a free handler */
    for (i=0; i<MAX_ENVIRONMENTS; i++) {
        if (!srv->environments[i].env) {
            srv->environments[i].env=env;
            srv->environments[i].urlname=strdup(urlname);
            os_critsec_init(&srv->environments[i].cs);
            break;
        }
    }

    if (i==MAX_ENVIRONMENTS)
        return (HAM_FALSE);
    
    mg_set_uri_callback(srv->mg_ctxt, urlname, 
                        request_handler, &srv->environments[i]);
    return (HAM_TRUE);
}

void
hamserver_close(hamserver_t *srv)
{
    int i;

    /* clean up Environment handlers */
    for (i=0; i<MAX_ENVIRONMENTS; i++) {
        if (srv->environments[i].env) {
            os_critsec_close(&srv->environments[i].cs);
            free(srv->environments[i].urlname);
            /* env will be closed by the caller */
            srv->environments[i].env=0;
        }
    }

    mg_stop(srv->mg_ctxt);
    free(srv);
}

