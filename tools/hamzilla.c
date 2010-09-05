/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

/**
 * This is the hamsterdb Database Server.
 *
 * On Unix it's implemented as a daemon, on Windows it's a Win32 Service. 
 * The configuration file has json format - see example.config.
 * The Win32 implementation is based on 
 * http://www.devx.com/cplus/Article/9857/1954
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#ifndef WIN32
#  include <unistd.h>
#  include <signal.h>
#  include <sys/signal.h>
#  include <sys/ioctl.h>
#  include <sys/stat.h>
#  include <sys/types.h>
#  include <fcntl.h>
#  define STRTOK_SAFE strtok_r
#  define EXENAME "hamsrv.exe"
#else
#  include <process.h> /* _getpid() */
#  include <tchar.h>
#  include <windows.h>
#  define STRTOK_SAFE strtok_s
#  define EXENAME "hamsrvd"
#endif
#include <errno.h>

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_srv.h>

#include "getopts.h"
#include "config.h"

#define ARG_HELP            1
#define ARG_FOREGROUND      2
#define ARG_CONFIG          3
#define ARG_PIDFILE         4
#define ARG_INSTALL         5
#define ARG_UNINSTALL       6
#define ARG_STOP            7
#define ARG_START			8
#define ARG_RUN 			9

FILE *f;

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
    {
        ARG_FOREGROUND,
        "f",
        "foreground",
        "run in foreground",
        0 },
    {
        ARG_CONFIG,
        "c",
        "config",
        "specify config file",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_PIDFILE,
        "p",
        "pid",
        "store pid in file",
        GETOPTS_NEED_ARGUMENT },
#ifdef WIN32
    {
        ARG_INSTALL,
        "i",
        "install",
        "(only Win32) installs the Service",
        0 },
    {
        ARG_UNINSTALL,
        "u",
        "uninstall",
        "(only Win32) uninstalls the Service",
        0 },
    {
        ARG_START,
        "s",
        "start",
        "(only Win32) starts the Service",
        0 },
    {
        ARG_STOP,
        "x",
        "stop",
        "(only Win32) stops the Service",
        0 },
#endif
    { 0, 0, 0, 0, 0 } /* terminating element */
};

static int running = 1;

#ifndef WIN32
static void 
signal_handler(int sig)
{
    (void)sig;
    running = 0;
}

static void
daemonize(void)
{
    int fd;

    switch(fork()) {
    case 0:  /* i'm the child */
        break;
    case -1:
        printf("fork failed: %s\n", strerror(errno));
        break;
    default: /* i'm the parent */
        exit(0);
    }

    /* go to root directory */
    chdir("/");

    /* reset umask */
    umask(0);

    /* disassociate from process group */
    setpgrp();

    /* disassociate from control terminal */
    if ((fd=open("dev/tty", O_RDWR)) >= 0) {
        ioctl(fd, TIOCNOTTY, NULL);
        close(fd);
    }
}
#endif

void
read_config(const char *configfile, config_table_t **params)
{
    ham_status_t st;
    char *buf;
    FILE *fp;
    long len;

    /* read the whole file into 'buf' */
    fp=fopen(configfile, "rt");
    if (!fp) {
fprintf(f, "%s:%d - failed to open config file %s: %s\n", __FILE__, __LINE__, configfile, strerror(errno));
        printf("failed to open config file: %s\n", strerror(errno));
        exit(-1);
    }
    fseek(fp, 0, SEEK_END);
    len=ftell(fp);
    fseek(fp, 0, SEEK_SET);
    buf=(char *)malloc(len+1); /* for zero-terminating byte */
    fread(buf, len, 1, fp);
    fclose(fp);
    buf[len]='\0';

    /* parse the file */
    st=config_parse_string(buf, params);
    if (st) {
        printf("failed to read configuration file: %s\n", ham_strerror(st));
        exit(-1);
    }

    /* clean up */
    free(buf);
}

void
write_pidfile(const char *pidfile)
{
    FILE *fp=fopen(pidfile, "wt");
    if (!fp) {
        printf("failed to write pidfile: %s\n", strerror(errno));
        exit(-1);
    }
#ifdef WIN32	
    fprintf(fp, "%u", (unsigned)_getpid());
#else
	fprintf(fp, "%u", (unsigned)getpid());
#endif
    fclose(fp);
}

#define COMPARE_FLAG(n)             if (!strcmp(#n, p)) f|=n

ham_u32_t
format_flags(char *flagstr)
{
    ham_u32_t f=0;
    char *saveptr=0;
    char *p;

    if (!flagstr || flagstr[0]=='\0')
        return (0);
    p=STRTOK_SAFE(flagstr, "|", &saveptr);
    while (p) {
        COMPARE_FLAG(HAM_WRITE_THROUGH);
        else COMPARE_FLAG(HAM_IN_MEMORY_DB);
        else COMPARE_FLAG(HAM_DISABLE_MMAP);
        else COMPARE_FLAG(HAM_CACHE_STRICT);
        else COMPARE_FLAG(HAM_CACHE_UNLIMITED);
        else COMPARE_FLAG(HAM_DISABLE_FREELIST_FLUSH);
        else COMPARE_FLAG(HAM_LOCK_EXCLUSIVE);
        else COMPARE_FLAG(HAM_ENABLE_RECOVERY);
        else COMPARE_FLAG(HAM_ENABLE_TRANSACTIONS);
        else COMPARE_FLAG(HAM_READ_ONLY);
        else COMPARE_FLAG(HAM_USE_BTREE);
        else COMPARE_FLAG(HAM_DISABLE_VAR_KEYLEN);
        else COMPARE_FLAG(HAM_ENABLE_DUPLICATES);
        else COMPARE_FLAG(HAM_SORT_DUPLICATES);
        else COMPARE_FLAG(HAM_RECORD_NUMBER);
        else {
            printf("ignoring unknown flag %s\n", p);
        }
        p=STRTOK_SAFE(0, "|", &saveptr);
    }

    return (f);
}

void
initialize_server(ham_srv_t *srv, config_table_t *params)
{
    unsigned e, d;
    ham_env_t *env;
    ham_status_t st;

    for (e=0; e<params->env_count; e++) {
        ham_u32_t flags=format_flags(params->envs[e].flags);
        ham_bool_t created_env=HAM_FALSE;

        ham_env_new(&env);

        /* First try to open the Environment */
        st=ham_env_open(env, params->envs[e].path, flags);
        if (st) {
            /* Not found? if open_exclusive is false then we create the
             * Environment */
            if (st==HAM_FILE_NOT_FOUND && !params->envs[e].open_exclusive) {
                st=ham_env_create(env, params->envs[e].path, flags, 0644);
                if (st) {
                    printf("ham_env_create failed: %s\n", ham_strerror(st));
                    exit(-1);
                }
                created_env=1;
            }
            else {
                printf("ham_env_open failed: %s\n", ham_strerror(st));
                exit(-1);
            }
        }

        /* Now create each of the Databases if the Environment was
         * created */
        if (created_env) {
            ham_db_t *db;
    
            for (d=0; d<params->envs[e].db_count; d++) {
                ham_u32_t flags=format_flags(params->envs[e].dbs[d].flags);

                ham_new(&db);

                st=ham_env_create_db(env, db, params->envs[e].dbs[d].name, 
                                    flags, 0);
                if (st) {
                    printf("ham_env_create_db: %d\n", st);
                    exit(-1);
                }

                ham_close(db, 0);
                ham_delete(db);
            }
        }

        /* Add the Environment to the server */
        st=ham_srv_add_env(srv, env, params->envs[e].url);
        if (st) {
            printf("ham_srv_add_env failed: %s\n", ham_strerror(st));
            exit(-1);
        }

        /* Store env in configuration object */
        params->envs[e].env=env;
    }
}

#ifdef WIN32
static TCHAR *serviceName = TEXT("hamsterdb Database Server");
static TCHAR *serviceDescription = TEXT("Provides network access to hamsterdb Databases.");

static void
win32_service_install(void)
{
	SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CREATE_SERVICE);

	if (scm) {
		TCHAR path[_MAX_PATH+1];
		if (GetModuleFileName(0, path, sizeof(path)/sizeof(path[0])) > 0) {
			SC_HANDLE service = CreateService(scm,
							serviceName, serviceName,
							SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
							SERVICE_AUTO_START, SERVICE_ERROR_IGNORE, path,
							0, 0, 0, 0, 0 );
			if (service) {
				SERVICE_DESCRIPTION sd;
				sd.lpDescription=serviceDescription;
				ChangeServiceConfig2(service, SERVICE_CONFIG_DESCRIPTION, &sd);
				CloseServiceHandle(service);
			}
			else switch(GetLastError()) {
			case ERROR_ACCESS_DENIED:
				printf("The handle to the SCM database does not have the "
					"SC_MANAGER_CREATE_SERVICE access right.\n");
				break;
			case ERROR_CIRCULAR_DEPENDENCY:
				printf("A circular service dependency was specified.\n");
				break;
			case ERROR_DUPLICATE_SERVICE_NAME:
				printf("The display name already exists in the service control "
				"manager database either as a service name or as another display "
				"name.\n");
				break;
			case ERROR_INVALID_NAME:
				printf("The specified service name is invalid.\n");
				break;
			case ERROR_INVALID_PARAMETER:
				printf("A parameter that was specified is invalid.\n");
				break;
			case ERROR_INVALID_SERVICE_ACCOUNT:
				printf("The user account name specified in the lpServiceStartName "
					"parameter does not exist.\n");
				break;
			case ERROR_SERVICE_EXISTS:
				printf("The specified service already exists in this database.\n");
				break;
			default:
				printf("Failed to install the service (error %u)\n", GetLastError());
				break;
 			}
		}

		CloseServiceHandle(scm);
	}
}

static void
win32_service_uninstall(void)
{
	SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

	if (scm) {
		SC_HANDLE service = OpenService(scm, serviceName, 
                    SERVICE_QUERY_STATUS | DELETE);
		if (service) {
			SERVICE_STATUS sst;
			if (QueryServiceStatus(service, &sst )) {
				if (sst.dwCurrentState == SERVICE_STOPPED)
					DeleteService(service);
			}

			CloseServiceHandle(service);
		}

		CloseServiceHandle(scm);
	}
}

static SERVICE_STATUS sst;
static SERVICE_STATUS_HANDLE ssth = 0;
static HANDLE stop_me = 0;

void WINAPI
ServiceControlHandler(DWORD controlCode)
{
	switch (controlCode) {
		case SERVICE_CONTROL_INTERROGATE:
			break;

		case SERVICE_CONTROL_SHUTDOWN:
		case SERVICE_CONTROL_STOP:
			sst.dwCurrentState = SERVICE_STOP_PENDING;
			SetServiceStatus(ssth, &sst);

			SetEvent(stop_me);
			return;

		case SERVICE_CONTROL_PAUSE:
			break;

		case SERVICE_CONTROL_CONTINUE:
			break;

		default:
			if ((controlCode>=128) && (controlCode<=255))
				// user defined control code
				break;
			else
				// unrecognized control code
				break;
	}

	SetServiceStatus(ssth, &sst);
}

static void
win32_service_stop(void)
{
    SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

	if (scm) {
		SC_HANDLE service = OpenService(scm, serviceName, 
                    SERVICE_QUERY_STATUS | DELETE | SERVICE_STOP);
		if (service) {
			SERVICE_STATUS sst;
			if (QueryServiceStatus(service, &sst )) {
				if (sst.dwCurrentState == SERVICE_STOPPED) { 
					printf("service is already stopped\n");
				}
				else {
					if (!ControlService(service, SERVICE_CONTROL_STOP, &sst)) {
					    printf("ControlService failed (%d)\n", GetLastError());
    				}
				}
			}

			CloseServiceHandle(service);
		}

		CloseServiceHandle(scm);
	}
}

static void
win32_service_start(void)
{
	SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

	if (scm) {
		SC_HANDLE service = OpenService(scm, serviceName, 
                    SERVICE_QUERY_STATUS | SERVICE_START | DELETE);
		if (service) {
			SERVICE_STATUS sst;
			if (QueryServiceStatus(service, &sst )) {
				if (sst.dwCurrentState != SERVICE_STOPPED 
						&& sst.dwCurrentState != SERVICE_STOP_PENDING) {
					printf("service is already running\n");
				}
				else {
					if (!StartService(service, 0, NULL)) {
					    printf("StartService failed (%d)\n", GetLastError());
    				}
				}
			}

			CloseServiceHandle(service);
		}

		CloseServiceHandle(scm);
	}
}

void WINAPI 
ServiceMain(DWORD argc, TCHAR *argv[])
{
	ssth = RegisterServiceCtrlHandler(serviceName, ServiceControlHandler);
	if (ssth) {
		// do initialisation here
		stop_me = CreateEvent(0, FALSE, FALSE, 0);

		// running
		sst.dwControlsAccepted |= (SERVICE_ACCEPT_STOP 
                                    | SERVICE_ACCEPT_SHUTDOWN);
		sst.dwCurrentState = SERVICE_RUNNING;
		SetServiceStatus(ssth, &sst);

		do {
			/* this is the main loop */;
		} while (WaitForSingleObject(stop_me, 5000)==WAIT_TIMEOUT);

		// service was stopped
		sst.dwCurrentState = SERVICE_STOP_PENDING;
		SetServiceStatus(ssth, &sst);

		// do cleanup here
		CloseHandle(stop_me);
		stop_me = 0;

		// service is now stopped
		sst.dwControlsAccepted &= ~(SERVICE_ACCEPT_STOP 
                                | SERVICE_ACCEPT_SHUTDOWN);
		sst.dwCurrentState = SERVICE_STOPPED;
		SetServiceStatus(ssth, &sst);
	}
}

static void
win32_service_run(void)
{
	DWORD ret;
	SERVICE_TABLE_ENTRY serviceTable[] =
	{
		{ serviceName, ServiceMain },
		{ 0, 0 }
	};

	// initialize service status
	sst.dwServiceType = SERVICE_WIN32;
	sst.dwCurrentState = SERVICE_STOPPED;
	sst.dwControlsAccepted = 0;
	sst.dwWin32ExitCode = NO_ERROR;
	sst.dwServiceSpecificExitCode = NO_ERROR;
	sst.dwCheckPoint = 0;
	sst.dwWaitHint = 0;

	// service is starting
	sst.dwCurrentState = SERVICE_START_PENDING;
	SetServiceStatus(ssth, &sst);

	ret=StartServiceCtrlDispatcher(serviceTable);
	if (!ret) {
		printf("StartServiceCtrlDispatcher failed with error %u\n", GetLastError());
	}
}
#endif

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param=0, *configfile=0, *pidfile=0;
    unsigned e, foreground=0;
    ham_srv_t *srv=0;
    ham_srv_config_t cfg;
    config_table_t *params=0;
#ifdef WIN32
	char configbuffer[_MAX_PATH*2];
#endif
	int win32_action=ARG_RUN;

    ham_u32_t maj, min, rev;
    const char *licensee, *product;
    ham_get_license(&licensee, &product);
    ham_get_version(&maj, &min, &rev);

    memset(&cfg, 0, sizeof(cfg));
    getopts_init(argc, argv, EXENAME);

	f=fopen("g:\\log.txt", "wt");
	if (!f)
		exit(-1);
	fprintf(f, "%s:%d - initializing (argc: %u)\n", __FILE__, __LINE__, argc);

	while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case ARG_FOREGROUND:
                foreground=1;
                break;
            case ARG_CONFIG:
                configfile=param;
                break;
            case ARG_PIDFILE:
                pidfile=param;
                break;
            case ARG_HELP:
                printf("hamsterdb server %d.%d.%d - Copyright (C) 2005-2010 "
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

                printf("usage: %s [-f] -c configfile\n", EXENAME);
                printf("usage: %s -h\n", EXENAME);
                printf("       -h:         this help screen (alias: --help)\n");
                printf("       -f:         run in foreground\n");
                printf("       configfile: path of configuration file\n");
                return (0);
#ifdef WIN32
            case ARG_INSTALL:
                win32_action=ARG_INSTALL;
                break;
            case ARG_UNINSTALL:
                win32_action=ARG_UNINSTALL;
                break;
            case ARG_STOP:
                win32_action=ARG_STOP;
                break;
            case ARG_START:
                win32_action=ARG_START;
                break;
#endif
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `hamsterd --help' for usage.", param);
                return (-1);
        }
    }

	fprintf(f, "%s:%d - action is %d\n", __FILE__, __LINE__, win32_action);

	/* on Windows, it's very tricky to specify a configuration file for
	 * a service. Instead, we just look for a configuration file with the
	 * same name (but a different extension ".config") in the same directory
	 * as hamsvc.exe */
#ifdef WIN32
	if (!configfile) {
		char *p;
		strcpy(configbuffer, argv[0]);
		p=configbuffer+strlen(configbuffer)-1;
		while (*p!='.')
			p--;
		*p='\0';
		strcat(configbuffer, ".config");
		configfile=&configbuffer[0];
	}
#else
	/* On Unix, specifying a configuration file is mandatory. */
	if (!configfile) {
 	    printf("configuration file missing - please specify path with -c\n");
        printf("run `%s --help' for more information.\n", EXENAME);
		exit(-1);
	}
#endif

	/* now read and parse the configuration file */
	if (win32_action==ARG_RUN && configfile) {
		fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
        read_config(configfile, &params);
	}
	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);

    /* register signals; these are the signals that will terminate the daemon */
#ifndef WIN32
    signal(SIGHUP, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGQUIT, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGKILL, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

#ifdef WIN32
    switch (win32_action) {
        case ARG_INSTALL:
			printf("hamsrv is installing...\n");
            win32_service_install();
            goto cleanup;
        case ARG_UNINSTALL:
			printf("hamsrv is uninstalling...\n");
            win32_service_uninstall();
            goto cleanup;
        case ARG_STOP:
			printf("hamsrv is stopping...\n");
            win32_service_stop();
            goto cleanup;
        case ARG_START:
			printf("hamsrv is starting...\n");
            win32_service_start();
            goto cleanup;
    }
#else
	printf("hamsrv is starting...\n");
#endif
	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);

	if (params) {
		cfg.port=params->globals.port;
		if (params->globals.enable_access_log)
			cfg.access_log_path=params->globals.access_log;
		if (params->globals.enable_error_log)
			cfg.error_log_path=params->globals.error_log;
	}

	if ((0!=ham_srv_init(&cfg, &srv)))
		exit(-1);

	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
	if (params)
		initialize_server(srv, params);
	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);

    /* on Unix we first daemonize, then write the pidfile (otherwise we do
     * not know the pid of the daemon process). On Win32, we first write
     * the pidfile and then call the service startup routine. */
#ifndef WIN32
    if (!foreground) {
        daemonize();
    }
#endif
fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
    if (pidfile)
        write_pidfile(pidfile);
fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
    /* This is the unix "main loop" which waits till the server is terminated.
     * Any registered signal will terminate the server by setting the 
     * 'running' flag to 0. (The Win32 main loop is hidden in
     * win32_service_start()). */
#ifndef WIN32
    while (running)
        sleep(1);
#else
fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
if (win32_action==ARG_RUN) {
	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
        win32_service_run();
}
	fprintf(f, "%s:%d - \n", __FILE__, __LINE__);
#endif

    printf("hamsrv is stopping...\n");

cleanup:
	fprintf(f, "%s:%d - cleaning up\n", __FILE__, __LINE__);

    /* clean up */
	if (srv)
		ham_srv_close(srv);
	if (params) {
		for (e=0; e<params->env_count; e++) {
			(void)ham_env_close(params->envs[e].env, HAM_AUTO_CLEANUP);
		}
		config_clear_table(params);
	}

	fclose(f);

    return (0);
}
