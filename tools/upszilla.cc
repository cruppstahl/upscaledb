/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * This is the upscaledb Database Server.
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
#include <stdarg.h>
#ifndef WIN32
#  include <unistd.h>
#  include <signal.h>
#  include <sys/signal.h>
#  include <sys/ioctl.h>
#  include <sys/stat.h>
#  include <sys/types.h>
#  include <fcntl.h>
#  include <syslog.h>
#  define STRTOK_SAFE strtok_r
#  define EXENAME "upszilla"
#  define MAX_PATH_LENGTH   FILENAME_MAX
#else
#  include <process.h> /* _getpid() */
#  include <tchar.h>
#  include <winsock2.h>
#  include <windows.h>
#  include <winioctl.h>
#  define STRTOK_SAFE strtok_s
#  define EXENAME "upszilla.exe"
#  define MAX_PATH_LENGTH   _MAX_PATH
static TCHAR *serviceName = TEXT("upscaledb Database Server");
static TCHAR *serviceDescription = TEXT("Provides network access to upscaledb Databases.");
#endif
#include <signal.h>
#include <errno.h>

#include <ups/upscaledb.h>
#include <ups/upscaledb_srv.h>

#include "getopts.h"
#include "common.h"
#include "config.h"

#define ARG_HELP                1
#define ARG_FOREGROUND          2
#define ARG_CONFIG              3
#define ARG_PIDFILE             4
#define ARG_INSTALL             5
#define ARG_UNINSTALL           6
#define ARG_STOP                7
#define ARG_START               8
#define ARG_RUN                 9
#define ARG_LOG_LEVEL           10

/*
 * command line parameters
 */
static option_t opts[] = {
  {
    ARG_HELP,         // symbolic name of this option
    "h",          // short option
    "help",         // long option
    "this help screen",   // help string
    0 },          // no flags
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
  {
    ARG_LOG_LEVEL,
    "l",
    "log_level",
    "sets the logging level (0: Debug; 1: Info; 2: Warnings; 3: Fatal)",
    GETOPTS_NEED_ARGUMENT },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

#define LOG_DBG     0
#define LOG_NORMAL  1
#define LOG_WARN    2
#define LOG_FATAL   3

static sig_atomic_t running  = 1;
static int foreground = 0;
static int log_level  = LOG_NORMAL;

static void
init_syslog() {
#ifndef WIN32
  openlog(EXENAME, LOG_PID, LOG_DAEMON);
#endif
}

static void
close_syslog() {
#ifndef WIN32
  closelog();
#endif
}

extern "C" {
void
hlog(int level, const char *format, ...) {
  va_list ap;
  char buffer[1024];

  if (level < log_level)
    return;

  va_start(ap, format);
  vsprintf(buffer, format, ap);
  va_end(ap);

  if (foreground) {
    fprintf(stderr, "%s", &buffer[0]);
  }
  else {
#ifdef WIN32
    TCHAR msg[1024];

    mbstowcs(msg, buffer, 1024);

    switch(level) {
      case LOG_DBG:
        OutputDebugString(TEXT("DBG "));
        break;
      case LOG_NORMAL:
        OutputDebugString(TEXT("INFO "));
        break;
      case LOG_WARN:
        OutputDebugString(TEXT("WARN "));
        break;
      default:
        OutputDebugString(TEXT("ERROR "));
        break;
    }

    OutputDebugString(msg);
#else
    unsigned code;

    switch (level) {
      case LOG_DBG:
        code = LOG_DEBUG;
        break;
      case LOG_NORMAL:
        code = LOG_INFO;
        break;
      case LOG_WARN:
        code = LOG_WARNING;
        break;
      default: /* LOG_FATAL */
        code = LOG_EMERG;
        break;
    }
    syslog(code, "%s", buffer);
#endif
  }
}
}

static void
signal_handler(int sig) {
  (void)sig;
  running = 0;
}

#ifndef WIN32

static void
daemonize() {
  int fd;

  switch(fork()) {
  case 0:  /* i'm the child */
    break;
  case -1:
    hlog(LOG_FATAL, "fork failed: %s\n", strerror(errno));
    break;
  default: /* i'm the parent */
    exit(0);
  }

  /* go to root directory */
  /* chdir("/"); */

  /* reset umask */
  /* umask(0); */

  /* disassociate from process group */
  setpgrp();

  /* disassociate from control terminal */
  if ((fd = open("dev/tty", O_RDWR)) >= 0) {
    ioctl(fd, TIOCNOTTY, NULL);
    close(fd);
  }
}
#endif

void
read_config(const char *configfile, config_table_t **params) {
  ups_status_t st;
  char *buf;
  FILE *fp;
  long len;
  size_t r;

  hlog(LOG_DBG, "Parsing configuration file %s\n", configfile);

  /* read the whole file into 'buf' */
  fp = fopen(configfile, "rt");
  if (!fp) {
    hlog(LOG_FATAL, "Failed to open config file %s: %s\n",
        configfile, strerror(errno));
    exit(-1);
  }
  fseek(fp, 0, SEEK_END);
  len = ftell(fp);
  fseek(fp, 0, SEEK_SET);
  buf = (char *)malloc(len + 1); /* for zero-terminating byte */
  r = fread(buf, 1, len, fp);
  fclose(fp);

  if (r != (size_t)len) {
    hlog(LOG_FATAL, "failed to read configuration file: %s\n",
                    strerror(errno));
    exit(-1);
  }

  buf[len] = '\0';

  /* parse the file */
  st = config_parse_string(buf, params);
  if (st) {
    hlog(LOG_FATAL, "failed to read configuration file: %s\n",
        ups_strerror(st));
    exit(-1);
  }

  /* clean up */
  free(buf);
}

void
write_pidfile(const char *pidfile) {
  FILE *fp = fopen(pidfile, "wt");
  if (!fp) {
    hlog(LOG_FATAL, "failed to write pidfile: %s\n", strerror(errno));
    exit(-1);
  }
#ifdef WIN32
  fprintf(fp, "%u", (unsigned)_getpid());
#else
  fprintf(fp, "%u", (unsigned)getpid());
#endif
  fclose(fp);
}

#define COMPARE_FLAG(n)       if (!strcmp(#n, p)) f|=n

uint32_t
format_flags(char *flagstr) {
  uint32_t f = 0;
  char *saveptr = 0;
  char *p;

  if (!flagstr || flagstr[0] == '\0')
    return 0;
  p = STRTOK_SAFE(flagstr, "|", &saveptr);
  while (p) {
    COMPARE_FLAG(UPS_ENABLE_FSYNC);
    else COMPARE_FLAG(UPS_DISABLE_MMAP);
    else COMPARE_FLAG(UPS_CACHE_UNLIMITED);
    else COMPARE_FLAG(UPS_ENABLE_TRANSACTIONS);
    else COMPARE_FLAG(UPS_READ_ONLY);
    else COMPARE_FLAG(UPS_ENABLE_DUPLICATE_KEYS);
    else COMPARE_FLAG(UPS_RECORD_NUMBER);
    else {
      hlog(LOG_WARN, "Ignoring unknown flag %s\n", p);
    }
    p = STRTOK_SAFE(0, "|", &saveptr);
  }

  return f;
}

void
initialize_server(ups_srv_t *srv, config_table_t *params) {
  unsigned e, d;
  ups_env_t *env;
  ups_status_t st;

  for (e = 0; e < params->env_count; e++) {
    uint32_t flags = format_flags(params->envs[e].flags);
    ups_bool_t created_env = UPS_FALSE;

    /* First try to open the Environment */
    hlog(LOG_DBG, "Opening Environment %s (flags 0x%x)\n",
        params->envs[e].path, flags);
    st = ups_env_open(&env, params->envs[e].path, flags, 0);
    if (st) {
      /* Not found? if open_exclusive is false then we create the
       * Environment */
      if (st == UPS_FILE_NOT_FOUND && !params->envs[e].open_exclusive) {
        hlog(LOG_DBG, "Env was not found; trying to create it\n");
        st = ups_env_create(&env, params->envs[e].path, flags, 0644, 0);
        if (st) {
          hlog(LOG_FATAL, "Failed to create Env %s: %s\n",
              params->envs[e].path, ups_strerror(st));
          exit(-1);
        }
        hlog(LOG_DBG, "Env %s created successfully\n",
              params->envs[e].path);
        created_env = UPS_TRUE;
      }
      else {
        hlog(LOG_FATAL, "Failed to open Environment %s: %s\n",
              params->envs[e].path, ups_strerror(st));
        exit(-1);
      }
    }

    /* Now create each of the Databases if the Environment was
     * created */
    if (created_env) {
      ups_db_t *db;

      for (d = 0; d < params->envs[e].db_count; d++) {
        uint32_t flags = format_flags(params->envs[e].dbs[d].flags);

        hlog(LOG_DBG, "Creating Database %u\n",
            params->envs[e].dbs[d].name);
        st = ups_env_create_db(env, &db, params->envs[e].dbs[d].name,
                  flags, 0);
        if (st) {
          hlog(LOG_FATAL, "Failed to create Database %u: %s\n",
            params->envs[e].dbs[d].name, ups_strerror(st));
          exit(-1);
        }

        hlog(LOG_DBG, "Created Database %u successfully\n",
            params->envs[e].dbs[d].name);

        ups_db_close(db, 0);
      }
    }

    hlog(LOG_DBG, "Attaching Env to Server (url %s)\n",
        params->envs[e].url);

    /* Add the Environment to the server */
    st = ups_srv_add_env(srv, env, params->envs[e].url);
    if (st) {
      hlog(LOG_FATAL, "Failed to attach Env to Server: %s\n",
          ups_strerror(st));
      exit(-1);
    }

    /* Store env in configuration object */
    params->envs[e].env = env;
  }
}

#ifdef WIN32
static void
win32_service_install() {
  SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CREATE_SERVICE);

  if (scm) {
    TCHAR path[MAX_PATH_LENGTH+1];
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
        hlog(LOG_DBG, "Service was installed successfully.\n");
      }
      else
      switch(GetLastError()) {
      case ERROR_ACCESS_DENIED:
        hlog(LOG_FATAL, "The handle to the SCM database does not "
            "have the SC_MANAGER_CREATE_SERVICE access right.\n");
        break;
      case ERROR_CIRCULAR_DEPENDENCY:
        hlog(LOG_FATAL, "A circular service dependency was "
            "specified.\n");
        break;
      case ERROR_DUPLICATE_SERVICE_NAME:
        hlog(LOG_FATAL, "The display name already exists in the "
            "service control manager database either as a "
            "service name or as another display name.\n");
        break;
      case ERROR_INVALID_NAME:
        hlog(LOG_FATAL, "The specified service name is invalid.\n");
        break;
      case ERROR_INVALID_PARAMETER:
        hlog(LOG_FATAL, "A parameter that was specified is invalid.\n");
        break;
      case ERROR_INVALID_SERVICE_ACCOUNT:
        hlog(LOG_FATAL, "The user account name specified in the "
            "lpServiceStartName parameter does not exist.\n");
        break;
      case ERROR_SERVICE_EXISTS:
        hlog(LOG_FATAL, "The specified service already exists in "
            "this database.\n");
        break;
      default:
        hlog(LOG_FATAL, "Failed to install the service (error %u)\n",
            GetLastError());
        break;
      }
    }
    else
      hlog(LOG_FATAL, "GetModuleFileName failed\n");

    CloseServiceHandle(scm);
  }
  else
    hlog(LOG_FATAL, "OpenSCManager failed\n");
}

static void
win32_service_uninstall() {
  SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

  if (scm) {
    SC_HANDLE service = OpenService(scm, serviceName,
          SERVICE_QUERY_STATUS | DELETE);
    if (service) {
      SERVICE_STATUS sst;
      if (QueryServiceStatus(service, &sst )) {
        if (sst.dwCurrentState == SERVICE_STOPPED) {
          DeleteService(service);
          hlog(LOG_DBG, "Service was uninstalled.\n");
        }
        else
          hlog(LOG_FATAL, "Failed to uninstall - service was "
              "not stopped\n");
      }
      else
        hlog(LOG_FATAL, "QueryServiceStatus failed\n");

      CloseServiceHandle(service);
    }
    else
      hlog(LOG_FATAL, "OpenService failed\n");

    CloseServiceHandle(scm);
  }
  else
    hlog(LOG_FATAL, "OpenSCManager failed\n");
}

static SERVICE_STATUS sst;
static SERVICE_STATUS_HANDLE ssth = 0;
static HANDLE stop_me = 0;

void WINAPI
ServiceControlHandler(DWORD controlCode) {
  switch (controlCode) {
    case SERVICE_CONTROL_INTERROGATE:
      break;

    case SERVICE_CONTROL_SHUTDOWN:
    case SERVICE_CONTROL_STOP:
      hlog(LOG_DBG, "Service received STOP request\n");
      sst.dwCurrentState = SERVICE_STOP_PENDING;
      SetServiceStatus(ssth, &sst);

      SetEvent(stop_me);
      return;

    case SERVICE_CONTROL_PAUSE:
      break;

    case SERVICE_CONTROL_CONTINUE:
      break;

    default:
      if ((controlCode >= 128) && (controlCode <= 255))
        // user defined control code
        break;
      else
        // unrecognized control code
        break;
  }

  SetServiceStatus(ssth, &sst);
}

static void
win32_service_stop() {
  SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

  if (scm) {
    SC_HANDLE service = OpenService(scm, serviceName,
          SERVICE_QUERY_STATUS | DELETE | SERVICE_STOP);
    if (service) {
      SERVICE_STATUS sst;
      if (QueryServiceStatus(service, &sst )) {
        if (sst.dwCurrentState == SERVICE_STOPPED) {
          hlog(LOG_NORMAL, "Service is already stopped\n");
        }
        else {
          if (!ControlService(service, SERVICE_CONTROL_STOP, &sst)) {
            hlog(LOG_FATAL, "ControlService failed (%d)\n",
                GetLastError());
          }
        }
      }
      else
        hlog(LOG_FATAL, "QueryServiceStatus failed\n");

      CloseServiceHandle(service);
    }
    else
      hlog(LOG_FATAL, "OpenService failed\n");

    CloseServiceHandle(scm);
  }
  else
    hlog(LOG_FATAL, "OpenSCManager failed\n");
}

static void
win32_service_start() {
  SC_HANDLE scm = OpenSCManager(0, 0, SC_MANAGER_CONNECT);

  if (scm) {
    SC_HANDLE service = OpenService(scm, serviceName,
          SERVICE_QUERY_STATUS | SERVICE_START | DELETE);
    if (service) {
      SERVICE_STATUS sst;
      if (QueryServiceStatus(service, &sst )) {
        if (sst.dwCurrentState != SERVICE_STOPPED
            && sst.dwCurrentState != SERVICE_STOP_PENDING) {
          hlog(LOG_NORMAL, "Service is already running\n");
        }
        else {
          if (!StartService(service, 0, NULL)) {
            hlog(LOG_FATAL, "StartService failed (%d)\n",
                GetLastError());
          }
        }
      }
      else
        hlog(LOG_FATAL, "QueryServiceStatus failed\n");

      CloseServiceHandle(service);
    }
    else
      hlog(LOG_FATAL, "OpenService failed\n");

    CloseServiceHandle(scm);
  }
  else
    hlog(LOG_FATAL, "OpenSCManager failed\n");
}

void WINAPI
ServiceMain(DWORD argc, TCHAR *argv[]) {
  ssth = RegisterServiceCtrlHandler(serviceName, ServiceControlHandler);
  if (ssth) {
    // do initialisation here
    stop_me = CreateEvent(0, FALSE, FALSE, 0);

    // running
    sst.dwControlsAccepted |= (SERVICE_ACCEPT_STOP
                  | SERVICE_ACCEPT_SHUTDOWN);
    sst.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(ssth, &sst);

    hlog(LOG_DBG, "Service is entering main loop\n");

    do {
      /* this is the main loop */;
    } while (WaitForSingleObject(stop_me, 5000)==WAIT_TIMEOUT);

    // service was stopped
    hlog(LOG_DBG, "Service is leaving main loop\n");
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
  else
    hlog(LOG_FATAL, "RegisterServiceCtrlHandler failed\n");
}

static void
win32_service_run_fg() {
  hlog(LOG_DBG, "Service is entering main loop\n");
  while (running)
    Sleep(1000);
  hlog(LOG_DBG, "Service is leaving main loop\n");
}

static void
win32_service_run() {
  DWORD ret;
  SERVICE_TABLE_ENTRY serviceTable[] = {
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
    /* This fails if upszilla is started from the console. */
    printf("Please run `upszilla.exe -s` to start the service.\n");
  }
}
#endif

int
main(int argc, char **argv) {
  unsigned opt;
  const char *param = 0, *configfile = 0, *pidfile = 0;
  unsigned e;
  ups_srv_t *srv = 0;
  ups_srv_config_t cfg;
  config_table_t *params = 0;
  char configbuffer[MAX_PATH_LENGTH * 2];
#ifdef WIN32
  int win32_action = ARG_RUN;
#endif

  memset(&cfg, 0, sizeof(cfg));
  getopts_init(argc, argv, EXENAME);
  strcpy(configbuffer, argv[0]);

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_FOREGROUND:
        hlog(LOG_DBG, "Paramter: Running in foreground\n");
        foreground = 1;
        break;
      case ARG_CONFIG:
        configfile = param;
        hlog(LOG_DBG, "Paramter: configuration file is %s\n",
            configfile);
        break;
      case ARG_PIDFILE:
        pidfile = param;
        hlog(LOG_DBG, "Paramter: pid file is %s\n", pidfile);
        break;
      case ARG_HELP:
        print_banner(EXENAME);

        printf("usage: %s [-f] --config=<configfile>\n", EXENAME);
        printf("usage: %s -h\n", EXENAME);
        printf("     -h:     this help screen (alias: --help)\n");
        printf("     -f:     run in foreground\n");
        printf("     configfile: path of configuration file\n");
        return 0;
#ifdef WIN32
      case ARG_INSTALL:
        hlog(LOG_DBG, "Paramter: Installing service\n");
        win32_action=ARG_INSTALL;
        break;
      case ARG_UNINSTALL:
        hlog(LOG_DBG, "Paramter: Uninstalling service\n");
        win32_action = ARG_UNINSTALL;
        break;
      case ARG_STOP:
        hlog(LOG_DBG, "Paramter: Stopping service\n");
        win32_action = ARG_STOP;
        break;
      case ARG_START:
        hlog(LOG_DBG, "Paramter: Starting service\n");
        win32_action = ARG_START;
        break;
#endif
      case ARG_LOG_LEVEL:
        log_level = strtoul(param, 0, 0);
        if (log_level > LOG_FATAL)
          log_level = LOG_FATAL;
        hlog(LOG_DBG, "Paramter: Log level is %u\n", log_level);
        break;
      default:
        printf("Invalid or unknown parameter `%s'. "
             "Enter `./upszilla --help' for usage.\n", param);
        return -1;
    }
  }

  /* daemon/win32 service: initialize syslog/Eventlog */
  if (!foreground)
    init_syslog();

  /* if there's no configuration file then load a default one:
   * Just look for a configuration file with the same name (but a
   * different extension ".config") in the same directory
   * as upssvc[.exe] */
  if (!configfile) {
#ifdef WIN32
    char *p = configbuffer + strlen(configbuffer) - 1;
    while (*p != '.')
      p--;
    *p = '\0';
#endif
    strcat(configbuffer, ".config");
    configfile = &configbuffer[0];
    hlog(LOG_DBG, "Parameter: No config file specified - using %s\n",
        configfile);
  }

  /* now read and parse the configuration file */
  if (configfile)
    read_config(configfile, &params);

  /* register signals; these are the signals that will terminate the daemon */
  hlog(LOG_DBG, "Registering signal handlers\n");
#ifndef WIN32
  signal(SIGHUP, signal_handler);
  signal(SIGQUIT, signal_handler);
  signal(SIGKILL, signal_handler);
#endif
  signal(SIGABRT, signal_handler);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

#ifdef WIN32
  switch (win32_action) {
    case ARG_INSTALL:
      hlog(LOG_NORMAL, "upszilla is installing...\n");
      win32_service_install();
      goto cleanup;
    case ARG_UNINSTALL:
      hlog(LOG_NORMAL, "upszilla is uninstalling...\n");
      win32_service_uninstall();
      goto cleanup;
    case ARG_STOP:
      hlog(LOG_NORMAL, "upszilla is stopping...\n");
      win32_service_stop();
      goto cleanup;
    case ARG_START:
      hlog(LOG_NORMAL, "upszilla is starting...\n");
      win32_service_start();
      goto cleanup;
  }
#else
  hlog(LOG_NORMAL, "upszilla is starting...\n");
#endif

  if (params) {
    cfg.port = params->globals.port;
    hlog(LOG_DBG, "Config: port is %u\n", cfg.port);
    if (params->globals.enable_access_log) {
      cfg.access_log_path = params->globals.access_log;
      hlog(LOG_DBG, "Config: http access hlog is %s\n",
          cfg.access_log_path);
    }
    if (params->globals.enable_error_log) {
      cfg.error_log_path = params->globals.error_log;
      hlog(LOG_DBG, "Config: http error hlog is %s\n",
          cfg.error_log_path);
    }
  }

  /* on Unix we first daemonize, then write the pidfile (otherwise we do
   * not know the pid of the daemon process). On Win32, we first write
   * the pidfile and then call the service startup routine later. */
#ifndef WIN32
  if (!foreground) {
    hlog(LOG_DBG, "Running in background...\n");
    daemonize();
  }
#endif
  if (pidfile) {
    hlog(LOG_DBG, "Writing pid file\n");
    write_pidfile(pidfile);
  }

  /* Initialize the server */
  if ((0 != ups_srv_init(&cfg, &srv))) {
    hlog(LOG_FATAL, "Failed to initialize the server; terminating\n");
    exit(-1);
  }
  if (params)
    initialize_server(srv, params);

  /* This is the unix "main loop" which waits till the server is terminated.
   * Any registered signal will terminate the server by setting the
   * 'running' flag to 0. (The Win32 main loop is hidden in
   * win32_service_run()). */
#ifndef WIN32
  hlog(LOG_DBG, "Daemon is entering main loop\n");
  while (running)
    sleep(1);
  hlog(LOG_DBG, "Daemon is leaving main loop\n");
#else
  if (win32_action == ARG_RUN) {
    if (foreground) {
      hlog(LOG_DBG, "Running in foreground\n");
      win32_service_run_fg();
    }
    else {
      hlog(LOG_DBG, "Running in background (Win32 service)\n");
      win32_service_run();
    }
  }
#endif

  hlog(LOG_NORMAL, "upszilla is stopping...\n");

  /* avoid warning on linux that the cleanup label is never used */
  goto cleanup;

cleanup:
  /* clean up */
  hlog(LOG_DBG, "Cleaning up\n");
  if (srv)
    ups_srv_close(srv);
  if (params) {
    for (e = 0; e < params->env_count; e++) {
      if (params->envs[e].env)
        (void)ups_env_close(params->envs[e].env, UPS_AUTO_CLEANUP);
    }
    config_clear_table(params);
  }

  hlog(LOG_DBG, "Terminating process\n");

  /* daemon/win32 service: close syslog/Eventlog */
  if (!foreground)
    close_syslog();

  return 0;
}

