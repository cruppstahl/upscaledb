/* --------------------------------------------------------------------------
                           Copyright (c) 2005-2008 Aladdin Knowledge Systems
                                                  WonderByte Production Team

 $Id: service.c,v 1.17 2010-03-15 09:24:34 andrea.mazzoleni Exp $

 Monster LLM/CLM implementation windows service related stuff
 most things were ripped from hls/nt/service.c
-------------------------------------------------------------------------- */

#define WIN32_LEAN_AND_MEAN
#ifdef __GNUC__
#  define WINVER 0x0500
#endif
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32_WCE
#include <wince/process.h>
#include <wince/errno.h>
#else
#include <process.h>
#include <errno.h>
#endif
#include <tchar.h>
#include <winioctl.h>
#include <winuser.h>
#include <dbt.h>
#ifndef PBT_APMRESUMEAUTOMATIC
#define PBT_APMRESUMEAUTOMATIC 0x12  /* mingw headers don't define it yet */
#endif

#include <oef/oef.h>
#include <root/akstypes.h>
#include <guidlib/guidlib.h>
#include "servicemsg.h"
#include "lm.h"
#include "service.h"
#include "queue.h"
#include "debuglm.h"

HANDLE devaction_sem;

#ifndef _WIN32_WCE
#include <powrprof.h>
#include <pbt.h> /* PBT_APMxxx constants */
// internal variables
static SERVICE_STATUS          service_status;          // current status
static SERVICE_STATUS_HANDLE   service_status_handle;
#endif /* #ifndef _WIN32_WCE */

#include <fridge/fridge.h>

/* Win32 service names */
#define SZSERVICENAME        "hasplms"                       /* internal name  */
#define SZSERVICEDISPLAYNAME "Sentinel HASP License Manager" /* displayed name */
#define SZSERVICEACCOUNT     "NT Authority\\LocalService"    /* credentials    */

static DWORD                   glob_err = 0;
static BOOL                    service_debug = FALSE;
static TCHAR                   error_text[256];

// this event is signalled when the service should end
static HANDLE llm_stop_event = NULL;
static HANDLE threads_gone_event = NULL;
static HANDLE helper_event = NULL; /* controlled termination of service_control() */

static HANDLE mainwthr;  // main worker thread

// list of service dependencies - "dep1\0dep2\0\0"
#define SZDEPENDENCIES       ""             // set by hls_main.WriteRegistry
#define EVENT_REGISTRY_PATH "SYSTEM\\CurrentControlSet\\Services\\EventLog\\System\\" SZSERVICENAME

// internal function prototypes
static DWORD WINAPI service_ctrl(DWORD dwCtrlCode, DWORD dwEventType, 
                LPVOID lpEventData, LPVOID lpContext);
static VOID WINAPI service_main(DWORD dwArgc, LPTSTR *lpszArgv);
static BOOL CmdInstallService(void);
static BOOL CmdRemoveService(void);
static BOOL CmdStartService(void);
static BOOL CmdStopService(void);
static LPTSTR GetLastErrorText( LPTSTR lpszBuf, DWORD dwSize );
static void AddToMessageLog(DWORD code, LPCTSTR lpszMsg);
static void WriteEventRegistry (void);
static BOOL ReportStatusToSCMgr(DWORD dwCurrentState,
                                DWORD dwWin32ExitCode,
                                DWORD dwWaitHint);

#if !defined(WIN9X) && !defined(_WIN32_WCE)
static SERVICE_TABLE_ENTRY dispatch_table[] = {
    { TEXT(SZSERVICENAME), service_main },
    { NULL, NULL }
};
#endif

////////////////////////////////////////////////////////////////////////////////

void sys_msg(int severity, const char* message)
{
	if (sys_msg_get_level() >= severity) {
		DWORD code;
	
		switch (severity) {
		case SYSMSG_ERROR : 
			code = EV_ERROR; 
			break;
		default:
		case SYSMSG_INFO : 
		case SYSMSG_DEBUG : 
			code = EV_INFO; 
			break;
		}
		
		AddToMessageLog(code, message);
	}
}

////////////////////////////////////////////////////////////////////////////////

/* TODO make thread safe! */
const char *svc_strerror(int errnum)
{
    static LPVOID msg_buf=0;
    (void)errnum;  /* unused parameter */
    if (msg_buf) LocalFree(msg_buf);  /* free old string */
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER
                  | FORMAT_MESSAGE_FROM_SYSTEM
                  | FORMAT_MESSAGE_IGNORE_INSERTS,
                  NULL,
                  GetLastError(),
                  MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                  (LPTSTR) &msg_buf,
                  0,
                  NULL);
    return msg_buf;
}

//*****************************************************************************

static void CheckSema(void)
{
    HANDLE hsema;

    hsema = CreateSemaphore(NULL, 0, 1, "AKSMONSTERSEM");
    if (GetLastError() == ERROR_ALREADY_EXISTS) {
        sys_msg(SYSMSG_ERROR, "Service is already running on this machine\n");
        sys_abnormal_program_termination();
    }
}

//*****************************************************************************
//
//  AddToMessageLog(LPCTSTR lpszMsg)
//
//  Allows any thread to log an error message
//
//  PARAMETERS:
//    lpszMsg - text for message

static void AddToMessageLog(DWORD code, LPCTSTR lpszMsg)
{
    TCHAR   szMsg[256];
    HANDLE  hEventSource;
    LPCTSTR lpszStrings[2];
    WORD    evtype;

    if (!service_debug) {
#ifndef _WIN32_WCE
        hEventSource = RegisterEventSource(NULL, TEXT(SZSERVICENAME));
#endif
        trio_snprintf(ptr_and_sizeof(szMsg), SZSERVICENAME);
        lpszStrings[0] = szMsg;
        lpszStrings[1] = lpszMsg;

        switch (code) {

            case 0:
                code = EV_INFO;
            case EV_INFO:
                evtype = EVENTLOG_INFORMATION_TYPE;
                break;

            case 1:
                code = EV_WARNING;
            case EV_WARNING:
                evtype = EVENTLOG_WARNING_TYPE;
                break;

            default:
                code = EV_ERROR;
                evtype = EVENTLOG_ERROR_TYPE;
        }

#ifndef _WIN32_WCE
        if (hEventSource != NULL) {
            ReportEvent(hEventSource,         // handle of event source
                        evtype,               // event type
                        0,                    // event category
                        code,                 // event ID
                        NULL,                 // current user's SID
                        2,                    // strings in lpszStrings
                        0,                    // no bytes of raw data
                        lpszStrings,          // array of error strings
                        NULL);                // no raw data

            (VOID) DeregisterEventSource(hEventSource);
        }
#endif
    }
}

void svc_shutdown(void)
{
    /* debug version doesn't use this */
    if (threads_gone_event)
        SetEvent(threads_gone_event);
}

//*****************************************************************************
//
// simple command line parser for service_main
//

static void srvc_commandline(int argc, char **argv)
{
    int i = 0;
    int log_mode = FALSE;
    char *tmpbuf, *err_arg = NULL;
    unsigned long helplong;

    argc--;
    while(i++, argc--) {
        unsigned tmpbuf_size;
        if (log_mode) {
            helplong = strtoul(*(argv + i), &err_arg, 0);
            if (helplong > SYSMSG_DEBUG || *err_arg) { /* overflow */
                sys_msg(SYSMSG_ERROR, "Invalid command line argument to '-l'");
                continue;
            }
            log_mode = FALSE;
            sys_msg_set_level(helplong);
            continue;
        }
        if (! strcmp(*(argv + i), "-l")) {
            log_mode = TRUE;
            err_arg = *(argv + i);
            continue;
        }

        tmpbuf_size = oef_str_length(*(argv+i) + 128);
        tmpbuf = oef_mem_alloc(tmpbuf_size);
        if (tmpbuf) {
            trio_snprintf(tmpbuf, tmpbuf_size, "Invalid command line option '%s'", *(argv+i));
            sys_msg(SYSMSG_ERROR, tmpbuf);
            oef_mem_free(tmpbuf);
        } else {
            sys_msg(SYSMSG_ERROR, "Low memory");
        }

        break;
    }

    if (log_mode) {
        sys_msg(SYSMSG_ERROR, "'-l' needs a parameter");
    }
}

//*****************************************************************************
//
// WriteEventRegistry
//

static void WriteEventRegistry (void)
{
    int     rc;
    HKEY    hkey;
    DWORD   valbuf;
    char    szbuf[512];

    rc = RegCreateKeyEx(HKEY_LOCAL_MACHINE,
                        EVENT_REGISTRY_PATH,
                        (DWORD) 0,
                        NULL,   // class
                        REG_OPTION_NON_VOLATILE,
                        KEY_ALL_ACCESS,
                        NULL,
                        &hkey,
                        &valbuf);

    if (rc != ERROR_SUCCESS) return;

    if (GetModuleFileName(NULL, szbuf, 511))
        RegSetValueEx(hkey, "EventMessageFile", 0, REG_SZ, szbuf, 
                        strlen(szbuf)+1);

    valbuf = 7;
    RegSetValueEx(hkey, "TypesSupported", 0, REG_DWORD, (BYTE*) &valbuf, 4);

    RegCloseKey(hkey);
} // WriteEventRegistry


//*****************************************************************************
//
// DeleteEventRegistry
//

static void DeleteEventRegistry (void)
{
    RegDeleteKey(HKEY_LOCAL_MACHINE, EVENT_REGISTRY_PATH);
}

//*****************************************************************************
//
//  GetLastErrorText -- copies error message text to string
//
//  PARAMETERS:
//    lpszBuf - destination buffer
//    dwSize - size of buffer
//
//  RETURN VALUE:
//    destination buffer
//

static LPTSTR GetLastErrorText( LPTSTR lpszBuf, DWORD dwSize )
{
    DWORD   dwRet;
    LPTSTR  lpszTemp = NULL;

    dwRet = FormatMessage( FORMAT_MESSAGE_ALLOCATE_BUFFER |
                           FORMAT_MESSAGE_FROM_SYSTEM |
                           FORMAT_MESSAGE_ARGUMENT_ARRAY,
                           NULL,
                           GetLastError(),
                           LANG_NEUTRAL,
                           (LPTSTR)&lpszTemp,
                           0,
                           NULL );

    // supplied buffer is not long enough
    if ( !dwRet || ( (long)dwSize < (long)dwRet+14 ) )
        lpszBuf[0] = TEXT('\0');
    else {
        //remove cr and newline character
        lpszTemp[lstrlen(lpszTemp)-2] = TEXT('\0');  
        trio_snprintf( lpszBuf, dwSize, TEXT("%s (0x%lx)"), lpszTemp, 
                        (unsigned long)GetLastError() );
    }
    if ( lpszTemp ) LocalFree((HLOCAL) lpszTemp );
    return lpszBuf;
}

static void exit_bad_params(void)
{
    sys_msg(SYSMSG_ERROR, "Bad command line parameter\n");
    sys_abnormal_program_termination();
}

oef_u32_t svc_get_pid(void)
{
    return GetCurrentProcessId();
}

static oef_s64_t start_time;
oef_s64_t svc_get_start_time(void)
{
    return start_time;
}

//*****************************************************************************
//
// this function either performs the command line task, or call
// StartServiceCtrlDispatcher to register the main service
// thread.  When the this call returns, the service has
// stopped, so exit.
//
void svc_control(void)
{
    oef_time_val_t tv;

    mem_init();

    if (oef_time_get_gmtime(&tv)) {
        sys_msg(SYSMSG_ERROR, "Call to gmtime failed\n");
        return;
    }
    else
        start_time=tv.tv_sec;

#if defined(DEBUG) && !defined(UNIT_TEST)
    if (cmd_start) {
        svc_main();
    }
#else
    if (cmd_install) {
        if (cmd_remove || cmd_start || cmd_stop) exit_bad_params();
        CmdInstallService();
        dbg_trace(1397, "MAIN", ("cmd_install finished"));
        exit(0);
    }
    else if (cmd_remove) {
        if (cmd_install || cmd_start || cmd_stop) exit_bad_params();
        CmdRemoveService();
        dbg_trace(1398, "MAIN", ("cmd_remove finished"));
        exit(0);
    }
    else if (cmd_start) {
        if (cmd_install || cmd_remove || cmd_stop) exit_bad_params();
        CheckSema();
        CmdStartService();
        dbg_trace(1399, "MAIN", ("cmd_start finished"));
        exit(0);
    }
    else if (cmd_stop) {
        if (cmd_install || cmd_remove || cmd_start) exit_bad_params();
        CmdStopService();
        dbg_trace(1400, "MAIN", ("cmd_stop finished"));
        exit(0);
    }

    helper_event = CreateEvent(NULL,    // no security attributes
                               TRUE,    // manual reset event
                               FALSE,   // not-signalled
                               NULL);   // no name
    if (helper_event == NULL) {
        dbg_trace(1401, "MAIN", ("helper event bäh!"));
    }

    // service was called from SCM and he wait for the following call of us
    if (! StartServiceCtrlDispatcher(dispatch_table)) {
        sys_msg(SYSMSG_ERROR, "Server start failed\n");
    }

    if (helper_event) {
        WaitForSingleObject(helper_event, INFINITE /*500*/);
        /*if (rc == WAIT_OBJECT_0) {*/
    }

    dbg_trace(1402, "MAIN", ("leaving service control"));

    Sleep(1000);
#endif
}

#if defined(WIN9X) || defined (_WIN32_WCE)

#define MAXNUMOFPARAM 64

/*
 ***************************************
 *
 *  convert_cmdln()
 *
 *  Description:  Converts command line from WinMain of 32 bit windows
 *                application to standard command line - argc \ argv - of
 *                console application.
 *
 *  Input:        CmdLine real program command line - null terminated string
 *                argc - real program's command line counter
 *                argv - real program's command line list
 *
 *  Output:       ParamCounter <
 *
 ***************************************
 */
void convert_cmdln(char* cmdline, int *argc, char **argv)
{
    char *pstr = cmdline;
    int i;

    for (i=0;i<MAXNUMOFPARAM;i++) {
        argv[i] = pstr;
        pstr = strchr(pstr,' ');
        if (pstr == NULL)
            break;
        else
            for(; *pstr == ' '; *pstr++ = '\0')
                ;
    }
    *argc = i;
    if (i < MAXNUMOFPARAM)
        *argc += 1;
}


// funky WinMain for Win9x

extern int main(int, char **);

#ifdef	_WIN32_WCE
	static wchar_t appname[] = TEXT("HASPLMS9X");
	static wchar_t wintitle[] = TEXT("HASPLM32");
#else
	static char appname[] = "HASPLMS9X";
	static char wintitle[] = "HASPLM32";
#endif

/* hack alert: global variables to pass parameters from WinMain to start_win98() */
static HINSTANCE hack_instance, hack_prev_instance;

/* hack alert: global variables to pass parameters from EnumWindows callback
 * function to start_win98()/stop_win98() */
static HWND glob_query_win_hand = NULL;
static DWORD glob_query_pid;


int PASCAL WinMain (HINSTANCE instance, HINSTANCE prev_instance, LPSTR cmdline, int cmdshow)
{
    int argc_origin = 0;
    char *argv_origin[MAXNUMOFPARAM] = {{ 0 }};

    hack_instance = instance;
    hack_prev_instance = prev_instance;

    convert_cmdln(cmdline, &argc_origin, argv_origin + 1);
    argc_origin++;
    argv_origin[0] = "HASPLM.EXE";

    return main(argc_origin, argv_origin);
}

/****************************************************************************/
LRESULT CALLBACK WndProc(HWND hwnd, UINT message, WPARAM wParam, LPARAM lParam)
/****************************************************************************/
{
#if 0
    HDC          hdc;
    PAINTSTRUCT  ps;
#endif

    switch (message) {

#if 0
        case WM_PAINT:
            InvalidateRect(hwnd,NULL,TRUE);
            hdc=(HDC)BeginPaint(hwnd,&ps);
            HardlockFunction(hdc);
            EndPaint(hwnd,&ps);
            return 0;
#endif

        case WM_DESTROY:
            sys_terminate();
            PostQuitMessage (0);
            return 0;
    }

    return DefWindowProc (hwnd, message, wParam, lParam);
}


static BOOL CALLBACK query_cb_func(HWND cbwinhand, LPARAM param)
{
    char procname[256];

    /* get procId of the window */
    GetWindowThreadProcessId(cbwinhand, &glob_query_pid);

    /*
     * if it has no parents then it is the main window of an application
     */
    if(GetParent(cbwinhand) == 0) {
        GetWindowText(cbwinhand, procname, 256);
        if (! strncmp(procname, wintitle, sizeof(wintitle) - 1)) {
            /* get procId of the window */
            GetWindowThreadProcessId(cbwinhand, &glob_query_pid);
            glob_query_win_hand = cbwinhand;
            return(FALSE);
        }
    }
    return(TRUE);
}

#endif // #if defined(WIN9X) || defined (_WIN32_WCE)

#if !defined(WIN9X) && !defined(_WIN32_WCE)
//*****************************************************************************
//  FUNCTION: start_service
//
//  Actual code of the service that does the work
//
//  PARAMETERS:
//    dwArgc   - number of command line arguments
//    lpszArgv - array of command line arguments

static VOID start_service(DWORD dwArgc, LPTSTR *lpszArgv)
{
    DWORD  rc;
    HDEVNOTIFY dev_notify_handle = NULL;
    DEV_BROADCAST_DEVICEINTERFACE notification_filter;
    BYTE cd[MAX_PATH + 1];

    (void)dwArgc; (void)lpszArgv;  // unused arguments

    GetCurrentDirectory(MAX_PATH, cd);
    dbg_trace(1404, "MAIN", ("enter start_service (dir: '%s')", cd));

    // Service initialization
    // report the status to the service control manager.
    if (!ReportStatusToSCMgr(SERVICE_START_PENDING, // service state
                             NO_ERROR,              // exit code
                             5000)) {               // wait hint
        dbg_trace(1405, "MAIN", ("report failed"));
        goto cleanup;
    }

    // create the event object. The control handler function signals
    // this event when it receives the "stop" control code.
    llm_stop_event = CreateEvent(NULL,    // no security attributes
                                 TRUE,    // manual reset event
                                 FALSE,   // not-signalled
                                 NULL);   // no name

    if (llm_stop_event == NULL /*INVALID_HANDLE_VALUE*/) goto cleanup;

    // create another event object. The main thread signals
    // this event after cleanup
    threads_gone_event = CreateEvent(NULL,    // no security attributes
                                     TRUE,    // manual reset event
                                     FALSE,   // not-signalled
                                     NULL);   // no name

    if (threads_gone_event == NULL /*INVALID_HANDLE_VALUE*/) goto cleanup;

    // report the status to the service control manager.
    if (!ReportStatusToSCMgr(SERVICE_START_PENDING, // service state
                             NO_ERROR,              // exit code
                             5000)) {               // wait hint
        sys_msg(SYSMSG_ERROR, "Report failed\n");
        goto cleanup;
    }

    //hls_init();

    devaction_sem = CreateSemaphore(NULL, 0, 32, NULL);
    if (devaction_sem == NULL) {
        sys_msg(SYSMSG_ERROR, "Failed to create devaction semaphore\n");
        goto cleanup;
    }

    // start the main worker thread. it will eventually fire up additional 
    // threads
    mainwthr = (HANDLE)_beginthread((void(__cdecl *)(void *))svc_main, 
                    0, NULL);
    if (mainwthr == NULL) {
        sys_msg(SYSMSG_ERROR, "Failed to create main thread\n");
        goto cleanup;
    }

    // simulate fake device action to let the feature handler device 
    // monitor initialize its key table
    ReleaseSemaphore(devaction_sem, 1, NULL);

    // register for device events
    oef_mem_set(&notification_filter, 0, sizeof(notification_filter));
    notification_filter.dbcc_size = sizeof(DEV_BROADCAST_DEVICEINTERFACE);
    notification_filter.dbcc_devicetype = DBT_DEVTYP_DEVICEINTERFACE;
    notification_filter.dbcc_classguid = 
            *Aks_GetGUID(GUID_DEVINTERFACE_AKSHASPHLFULL_ID);
    dev_notify_handle = RegisterDeviceNotification(service_status_handle,
                                           &notification_filter,
                                           DEVICE_NOTIFY_SERVICE_HANDLE);
    if (! dev_notify_handle) {
        sys_msg(SYSMSG_ERROR, "Cannot register device notification\n");
        /*goto cleanup;*/
    }
    else
        dbg_trace(1410, "MAIN", ("successfully registered device notification"));

    // report the status to the service control manager.
    if (!ReportStatusToSCMgr(SERVICE_RUNNING,       // service state
                             NO_ERROR,              // exit code
                             5000)) {                  // wait hint
        sys_msg(SYSMSG_ERROR, "Report failed\n");
        goto cleanup;
    }

    sys_msg(SYSMSG_INFO, "Sentinel HASP License Manager starting\n");

    // End of initialization

    // Service is now running, park this thread until the service is stopped
    while (1) {
        rc = WaitForSingleObject(llm_stop_event, INFINITE /*500*/);
        if (rc == WAIT_OBJECT_0)
            break;
    }

    sys_msg(SYSMSG_INFO, "Sentinel HASP License Manager terminated\n");
    Sleep(1000);

cleanup:
    //Cleanup(); // essential for W2K
    if (dev_notify_handle)
        UnregisterDeviceNotification(dev_notify_handle);
    if (llm_stop_event != NULL /*INVALID_HANDLE_VALUE*/) 
        CloseHandle(llm_stop_event);
    dbg_trace(1412, "MAIN", ("leave start_service"));
}

//*****************************************************************************
//  FUNCTION: stop_service
//
//    If a stop_service procedure is going to take longer than 3 seconds to
//    execute, it should spawn a thread to execute the stop code, and return.
//    Otherwise, the ServiceControlManager will believe that the service has
//    stopped responding.

static VOID stop_service(void)
{
    dbg_trace(1413, "MAIN", ("enter stop_service"));

    sys_terminate();

    // report the status to the service control manager.
    if (!ReportStatusToSCMgr(SERVICE_STOP_PENDING, // service state
                             NO_ERROR,             // exit code
                             5000)) {              // wait hint
        dbg_trace(1414, "MAIN", ("report failed"));
    }

    // wait for threads to finish
    WaitForSingleObject(threads_gone_event, 30000);

    if (llm_stop_event != NULL /*INVALID_HANDLE_VALUE*/)
        SetEvent(llm_stop_event);
    dbg_trace(1415, "MAIN", ("leave stop_service"));
}

//*****************************************************************************
//
//  service_ctrl
//
//  called by the SCM whenever ControlService() is called on this service.
//
//  PARAMETERS:
//    dwCode - type of control requested

static DWORD WINAPI service_ctrl(DWORD dwCode, DWORD dwEventType, 
                LPVOID lpEventData, LPVOID lpContext)
{
    DWORD status = NO_ERROR;

    (void)dwEventType;
    (void)lpEventData;
    (void)lpContext;

    dbg_trace(1416, "MAIN", ("enter service_ctrl"));

    switch(dwCode) {
        case SERVICE_CONTROL_STOP:        // Stop the service.
            dbg_trace(1417, "MAIN", ("service_ctrl -> stop!"));
            //logmsg_level(SYSMSG_DEBUG, "service_ctrl -> Stop!");
            if (! ReportStatusToSCMgr(SERVICE_STOP_PENDING, NO_ERROR, 5000)) {
                dbg_trace(1418, "MAIN", ("cannot report status to STOP_PENDING"));
                return status;
            }
            stop_service();
            break;
        case SERVICE_CONTROL_INTERROGATE: // Update the service status.
            dbg_trace(1419, "MAIN", ("service_ctrl -> Interrogate"));
            break;
        case SERVICE_CONTROL_DEVICEEVENT:
            dbg_trace(1420, "MAIN", ("service_ctrl -> Device Event!"));
            // notify feature handler device monitor thread
            if (!ReleaseSemaphore(devaction_sem, 1, NULL)) { 
                sys_msg(SYSMSG_ERROR, "Failed to release semaphore\n");
                sys_abnormal_program_termination();
            }
            break;
        case SERVICE_CONTROL_POWEREVENT:
            dbg_trace(1422, "MAIN", ("service_ctrl -> Power Event!"));
            if (dwEventType==PBT_APMRESUMESUSPEND ||
                dwEventType==PBT_APMRESUMEAUTOMATIC ||
                dwEventType==PBT_APMRESUMECRITICAL) {
                oef_status_t st;
                ULONG size, buffer[2];

                time_reset();

                dbg_trace(1423, "MAIN", ("service_ctrl -> resume after suspend"));

                st=fridge_driver_ioctl(FRIDGE_CMD_GET_LAST_POWER_STATE,
                                NULL,
                                0,
                                buffer,
                                sizeof(buffer),
                                &size);
                if (st) {
                    dbg_trace(1424, "MAIN", ("service_ctrl -> last power state "
                                           "failed, status %u", st));
                    break;
                }
                dbg_trace(1425, "MAIN", ("last power state: size %u, result %u, "
                         "status %u", size, buffer[0], st));
                if (buffer[0]==PowerSystemHibernate)
                    workqueue_add(XLMREQ_START_HIBERNATE, 0, 0, 1);
            }
            break;
        default:                          // invalid control code
            dbg_trace(1426, "MAIN", ("service_ctrl -> UNKNOWN (0x%x)", dwCode));
            status = ERROR_CALL_NOT_IMPLEMENTED;
            break;
    } // switch dwCode

    if (! ReportStatusToSCMgr(service_status.dwCurrentState, NO_ERROR, 5000)) {
        dbg_trace(1427, "MAIN", ("service_ctrl: cannot report status (0x%08x)", 
                                GetLastError()));
        return status;
    }
    dbg_trace(1428, "MAIN", ("leave service_ctrl"));
    return status;
}

//*****************************************************************************
//
//  ReportStatusToSCMgr()
//
//  Sets the current status of the service and
//  reports it to the Service Control Manager
//
//  PARAMETERS:
//    dwCurrentState - the state of the service
//    dwWin32ExitCode - error code to report
//    dwWaitHint - worst case estimate to next checkpoint
//
//  RETURN VALUE:
//    TRUE  - success
//    FALSE - failure

static BOOL ReportStatusToSCMgr(DWORD dwCurrentState,
                                DWORD dwWin32ExitCode,
                                DWORD dwWaitHint)
{
    static DWORD dwCheckPoint = 1;
    BOOL r = TRUE;
    DWORD last_error;

    if ( !service_debug ) { // when debugging we don't report to the SCM
        if (dwCurrentState == SERVICE_START_PENDING)
            service_status.dwControlsAccepted = 0;
        else
            service_status.dwControlsAccepted = SERVICE_ACCEPT_STOP;

        /* need power-events to detect hibernate */
        service_status.dwControlsAccepted|=SERVICE_ACCEPT_POWEREVENT;

        service_status.dwCurrentState = dwCurrentState;
        service_status.dwWin32ExitCode = dwWin32ExitCode;
        service_status.dwWaitHint = dwWaitHint;

        if ( ( dwCurrentState == SERVICE_RUNNING ) ||
             ( dwCurrentState == SERVICE_STOPPED ) )

            service_status.dwCheckPoint = 0;
        else
            service_status.dwCheckPoint = dwCheckPoint++;


        // Report the status of the service to the service control manager.
        //
        if (!(r = SetServiceStatus(service_status_handle, &service_status))) {
            last_error = GetLastError();
            sys_msg(SYSMSG_ERROR, "Failed to set service status\n");
            SetLastError(last_error);
        }
    }
    return r;
}

//*****************************************************************************
//
//  The following code handles service installation and removal
//
//  CmdInstallService() -- Installs the service

static BOOL CmdInstallService(void)
{
    SC_HANDLE   schService;
    SC_HANDLE   schSCManager;
    TCHAR       szPath[MAX_PATH + 16];
    TCHAR       szRoot[10];
    UINT        DriveType;
    BOOL        bRet = FALSE;

    if ( GetModuleFileName( NULL, szPath, MAX_PATH ) == 0 ) {
        fprintf(stderr, "  Unable to install %s - %s\n",
                SZSERVICEDISPLAYNAME, GetLastErrorText(error_text, 
                        sizeof(error_text)));
        return bRet;
    }

    if (strlen(szPath) > 2) {
        if (szPath[1] == ':') {
            if (szPath[2] == '\\') {
                szRoot[0] = szPath[0];
                szRoot[1] = szPath[1];
                szRoot[2] = szPath[2];
                szRoot[3] = 0;
                DriveType = GetDriveType(szRoot);

                switch (DriveType) {

                    case DRIVE_REMOVABLE:
                        printf("\n  WARNING: You are installing the service "
                                        "from a removable drive!\n");
                        break;

                    case DRIVE_REMOTE:
                        printf("\n  %s must be located on a local drive.\n", 
                                        svc_get_progname());
                        printf("  %s NOT installed.\n", SZSERVICEDISPLAYNAME);
                        return bRet;

                    case DRIVE_CDROM:
                        printf("\n  WARNING: You are installing the service "
                                        "from a CDROM drive!\n");
                        break;

                    case DRIVE_RAMDISK:
                        printf("\n  WARNING: You are installing the service "
                                        "from a RAM disk!\n");
                        break;
                } /* switch DriveType */
            }
        }
    }

    WriteEventRegistry();

    schSCManager = OpenSCManager(NULL,           // machine (NULL == local)
                         NULL,                   // database (NULL == default)
                         SC_MANAGER_ALL_ACCESS); // access required

    if (schSCManager) {
        util_strcat(ptr_and_sizeof(szPath), " -run"); // add cmdline parameter
        schService = CreateService(schSCManager,     // SCManager database
                         TEXT(SZSERVICENAME),        // name of service
                         TEXT(SZSERVICEDISPLAYNAME), // name to display
                         SERVICE_ALL_ACCESS,         // desired access
                         SERVICE_WIN32_OWN_PROCESS,  // service type
                         SERVICE_AUTO_START,         // start type
                         SERVICE_ERROR_NORMAL,       // error control type
                         szPath,                     // service's binary
                         NULL,                       // no load ordering group
                         NULL,                       // no tag identifier
                         TEXT(SZDEPENDENCIES),       // dependencies

// use NULL=LOCAL_SYSTEM
// LOCAL_SERVICE does not allow writing to registry
                         NULL, //TEXT(SZSERVICEACCOUNT),     // credentials (NULL=LOCAL_SYSTEM)
                         NULL);                      // no password

        if ( schService ) {
            /*
             * success - now change the service description
             */
            SERVICE_DESCRIPTION sd;
            sd.lpDescription="Manages licenses secured by Sentinel HASP.";
            ChangeServiceConfig2(schService, SERVICE_CONFIG_DESCRIPTION, &sd);
            CloseServiceHandle(schService);
            printf("  %s installed.\n", SZSERVICEDISPLAYNAME);
            bRet = TRUE;
        } else {
            fprintf(stderr, "  %s\n", 
                            GetLastErrorText(ptr_and_sizeof(error_text)));
        }
        CloseServiceHandle(schSCManager);
    }
    else
        fprintf(stderr, "  %s\n", 
                        GetLastErrorText(ptr_and_sizeof(error_text)));
    return bRet;
}

//*****************************************************************************
//
//  CmdRemoveService() -- Stops and removes the service
//

static BOOL CmdRemoveService(void)
{
    SC_HANDLE   schService;
    SC_HANDLE   schSCManager;
    BOOL        bRet = FALSE;

    DeleteEventRegistry();

    schSCManager = OpenSCManager(NULL,           // machine (NULL == local)
                         NULL,                   // database (NULL == default)
                         SC_MANAGER_ALL_ACCESS); // access required

    if (schSCManager) {
        dbg_trace(1430, "MAIN", ("  openservice(%s)", TEXT(SZSERVICENAME)));
        schService = OpenService(schSCManager, TEXT(SZSERVICENAME), 
                        SERVICE_ALL_ACCESS);
        if (schService) {
            // try to stop the service
            dbg_trace(1431, "MAIN", ("  ControlService(STOP)"));
            if (ControlService(schService, SERVICE_CONTROL_STOP, 
                                    &service_status ) ) {
                dbg_trace(1432, "MAIN", ("Stopping %s.", SZSERVICEDISPLAYNAME));
                Sleep( 1000 );

                while( QueryServiceStatus( schService, &service_status ) ) {
                    dbg_trace(1433, "MAIN", ("QueryServiceStatus=%x", 
                                            service_status.dwCurrentState));
                    if (service_status.dwCurrentState == SERVICE_STOP_PENDING) {
                        printf(".");
                        Sleep(1000);
                    } else
                        break;
                }

                if (service_status.dwCurrentState == SERVICE_STOPPED)
                    printf("\n  %s stopped.\n", SZSERVICEDISPLAYNAME);
                else
                    fprintf(stderr, "\n  %s failed to stop.\n", 
                                    TEXT(SZSERVICEDISPLAYNAME) );
            }

            // now remove the service
            if (DeleteService(schService)) {
                printf("  %s removed.\n", TEXT(SZSERVICEDISPLAYNAME) );
                bRet = TRUE;
            }
            else
                fprintf(stderr, "  %s\n", GetLastErrorText(error_text, 
                                        sizeof(error_text)));

            CloseServiceHandle(schService);
        }
        else
            fprintf(stderr, "  %s\n", GetLastErrorText(error_text, 
                                    sizeof(error_text)));

        CloseServiceHandle(schSCManager);
    }
    else
        fprintf(stderr, "  %s\n", GetLastErrorText(error_text, 
                                sizeof(error_text)));
    return bRet;
}

//*****************************************************************************
//
//  CmdStartService() -- Starts the service
//

static BOOL CmdStartService(void)
{
    SC_HANDLE   schService;
    SC_HANDLE   schSCManager;

    char argv_0[] = "-l";
    char argv_1[16];

    BOOL status;
    BOOL bRet = FALSE;

    LPCTSTR argv[] = { argv_0, argv_1 };

    TCHAR szPath[MAX_PATH + 16];

    dbg_trace(1434, "MAIN", ("StartService ..."));

    if ( GetModuleFileName( NULL, szPath, MAX_PATH ) == 0 ) {
        _tprintf(TEXT("Unable to install %s - %s\n"), 
                        TEXT(SZSERVICEDISPLAYNAME), 
                        GetLastErrorText(ptr_and_sizeof(error_text)));
        return bRet;
    }

    schSCManager = OpenSCManager(NULL,            // machine (NULL == local)
                         NULL,                    // database (NULL == default)
                         SC_MANAGER_ALL_ACCESS);  // access required

    if (schSCManager) {
        schService = OpenService(schSCManager, TEXT(SZSERVICENAME), 
                        SERVICE_ALL_ACCESS);
        if (schService) {
            dbg_trace(1435, "MAIN", ("  StartService"));
            // try to start the service

            if (sys_msg_get_level() != SYSMSG_ERROR) {
                trio_snprintf(ptr_and_sizeof(argv_1), "%d", sys_msg_get_level());
                status = StartService(schService, 2, argv);
            }
            else
                status = StartService(schService, 0, NULL);

            if (status /*StartService(schService, 0, NULL)*/) {
                printf("  %s started.\n", SZSERVICEDISPLAYNAME);
                bRet = TRUE;
            } else {
                fprintf(stderr, "  %s cannot be started.\n", 
                                SZSERVICEDISPLAYNAME);
                fprintf(stderr, "  %s\n", GetLastErrorText(error_text, 
                                        sizeof(error_text)));
                fprintf(stderr, "\n  - Assure that you are on a local drive, since services can be started\n"
                        "    from local drives only. It is recommended to place %s in\n"
                        "    the SYSTEM32 directory.\n"
                        "  - Assure that the service isn't already running.\n"
                        "    %s can be started only once.\n", 
                        svc_get_progname(), svc_get_progname());
            }
            CloseServiceHandle(schService);
        }
        else {
            _tprintf(TEXT("  %s\n"), GetLastErrorText(error_text, 
                                    sizeof(error_text)));
            fprintf(stderr, "  A service must be installed before it "
                            "can be started.\n");
        }

        CloseServiceHandle(schSCManager);
    }
    else {
        fprintf(stderr, "  %s\n", GetLastErrorText(error_text, 
                                sizeof(error_text)));
        fprintf(stderr, "  A service must be installed before it "
                        "can be started.\n");
    }
    return bRet;
}

//*****************************************************************************
//
//  CmdStopService() -- Stops the service
//

static BOOL CmdStopService(void)
{
    SC_HANDLE   schService;
    SC_HANDLE   schSCManager;
    BOOL bRet = FALSE;

    printf(TEXT("  Stopping %s ..."), TEXT(SZSERVICEDISPLAYNAME));
    dbg_trace(1436, "MAIN", ("CmdStopService called"));

    schSCManager = OpenSCManager(NULL,            // machine (NULL == local)
                         NULL,                    // database (NULL == default)
                         SC_MANAGER_ALL_ACCESS);  // access required
    if (schSCManager) {
        schService = OpenService(schSCManager, TEXT(SZSERVICENAME), 
                        SERVICE_ALL_ACCESS);
        if (schService) {
            // try to stop the service
            if (ControlService( schService, SERVICE_CONTROL_STOP, 
                                    &service_status ) ) {
                Sleep( 1000 );

                while (QueryServiceStatus( schService, &service_status)) {
                    if (service_status.dwCurrentState == SERVICE_STOP_PENDING) {
                        _tprintf(TEXT("."));
                        Sleep( 1000 );
                    }
                    else
                        break;
                }

                if (service_status.dwCurrentState == SERVICE_STOPPED)
                {
                    _tprintf(TEXT("\n  %s stopped.\n"), 
                                    TEXT(SZSERVICEDISPLAYNAME) );
                    bRet = TRUE;
                }
                else
                    _tprintf(TEXT("\n  %s failed to stop.\n"), 
                                    TEXT(SZSERVICEDISPLAYNAME) );
            }
            else
                fprintf(stderr, "\n  %s is not running.\n", 
                                SZSERVICEDISPLAYNAME);

            CloseServiceHandle(schService);
        }
        else {
            fprintf(stderr, "\n  %s\n", 
                            GetLastErrorText(error_text,sizeof(error_text)));
        }

        CloseServiceHandle(schSCManager);
    }
    else {
        fprintf(stderr, "\n  OpenSCManager failed - %s\n", 
                        GetLastErrorText(error_text,sizeof(error_text)));
    }
    dbg_trace(1437, "MAIN", ("CmdStopService leave"));
    return bRet;
}

//*****************************************************************************
//
//  service_main
//
//  performs actual initialization of the service
//
//  PARAMETERS:
//    dwArgc   - number of command line arguments
//    lpszArgv - array of command line arguments
//
//  COMMENTS:
//    This routine performs the service initialization and then calls
//    the user defined start_service() routine to perform majority
//    of the work.

static void WINAPI service_main(DWORD dwArgc, LPTSTR *lpszArgv)
{
    dbg_trace(1438, "MAIN", ("enter service_main"));

    if (dwArgc) srvc_commandline(dwArgc, lpszArgv);

    // register our service control handler:
    service_status_handle = RegisterServiceCtrlHandlerEx( TEXT(SZSERVICENAME), 
                    service_ctrl, NULL);

    if (! service_status_handle) return;

    // SERVICE_STATUS members that don't change in example
    //
    service_status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    service_status.dwServiceSpecificExitCode = 0;

    // report the status to the service control manager.
    //
    if (!ReportStatusToSCMgr(SERVICE_START_PENDING, // service state
                             NO_ERROR,              // exit code
                             5000)) {               // wait hint

        dbg_trace(1439, "MAIN", ("service_main: cannot report status"));
        goto cleanup;
    }

    WriteEventRegistry();

    start_service(dwArgc, lpszArgv);  // won't return until service is stopped

  cleanup: //-----------------------------------------------------------------

    // try to report the stopped status to the service control manager.
    if (service_status_handle)
        (VOID)ReportStatusToSCMgr(SERVICE_STOPPED, glob_err, 5000);

    dbg_trace(1440, "MAIN", ("leave service_main"));
    if (helper_event != NULL)
            SetEvent(helper_event);
}

#endif // #if !defined(WIN9X) && !defined(_WIN32_WCE)
