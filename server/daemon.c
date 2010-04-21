
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <syslog.h>
#include <pwd.h>
#include <signal.h>
#include <dlfcn.h>
#include <sys/signal.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/stat.h>
#include <oef/oef.h>

static uid_t g_user_id = 0;
static int g_use_syslog = 0;

/* Unix daemon name */
#define DAEMONNAME        "hamserver"

void print_error(int severity, const char* message)
{
    int code;

    code = LOG_ERR;
    code = LOG_INFO;
    code = LOG_DEBUG;

    if (g_use_syslog)
        syslog(code, "%s", message);
    else
        fprintf(stderr, "%s", message);
}

/*
 * get user id to run server as
 */
static uid_t 
get_user_id(char *user)
{
    struct passwd *pw;
    unsigned long uval;
    char *userend;

    pw = getpwnam(user);
    if (! pw) {
        /* TODO fail */
        sys_msg(SYSMSG_ERROR, "user name is unknown\n");
        exit(1);
    }

    return(pw->pw_uid);
}

/*
 * signal handler
 */
static void signal_hndlr(int sig)
{
    (void)sig;
    exit(-1);
}

#ifndef DEBUG

static void
register_signals(void)
{
    signal(SIGABRT, signal_hndlr);
    signal(SIGINT,  signal_hndlr);
    signal(SIGHUP,  signal_hndlr);
    signal(SIGTERM, signal_hndlr);
    signal(SIGALRM, signal_hndlr);
}

static void 
daemon_init(void)
{
    switch(fork()) {
        case 0:   /* child */
            break;
        case -1:
            sys_msg(SYSMSG_ERROR, "fork error");
            exit(1);
            break;
        default:  /* parent */
            exit(0);
    }

    /* setup signal handling */
    register_signals();

    /* go to root directory */
    chdir("/");

    /* reset umask */
    umask(0);

    /* disassociate from process group */
    setpgrp(0, getpid());

    /* disassociate from control terminal */
    if ((i = open("/dev/tty", O_RDWR)) >= 0) {
        ioctl(i,TIOCNOTTY,NULL);
        close(i);
    }

    openlog(DAEMONNAME, LOG_PID, LOG_DAEMON);
    g_use_syslog = 1;
}

#endif /* #ifndef DEBUG */

int
main(int argc, char **argv)
{
    int i, maxfd;
    struct sigaction sa;
    oef_time_val_t tv;

#if 0
    /* close all file descriptors except std. ones */
    maxfd = sysconf(_SC_OPEN_MAX);
    if (!cmd_foreground) {
        fflush(stderr);
        fflush(stdout);
        for (i=0; i<maxfd; i++)
            close(i);
        open("/dev/null", O_RDONLY); /* stdin */
        dup(open("/dev/null", O_WRONLY)); /* stdout + stderr */
    }
#endif

    if (g_user_id) {
        i = setuid(g_user_id);
        if (i == -1) {
            sys_msg(SYSMSG_ERROR, "setuid failed\n");
            exit(1);
        }
    }

    if (cmd_foreground)
        register_signals();
    else
        daemon_init();
    sys_msg(SYSMSG_INFO, DAEMONNAME " loaded\n");

    /* set SIGPIPE to be ignored */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);

    /* TODO hier gehts weiter */
    return (0);
}
