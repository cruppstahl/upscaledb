#!/usr/bin/env python

# Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
# see file LICENSE for license and copyright information
#
# test skript; run ./dbtest.py --help if you don't know how to use it
#

import os
import stat
import copy
import sys
import string
import re
import popen2

# be verbose?
g_verbose=0
# global logfile counter
g_logfile_count=0
# global error counter
g_error_count=0
# global log file
g_log=0

def help():
    print "dbtest.py - hamsterdb test suite"
    print "(C) Christoph Rupp, 2005, 2006 chris@crupp.de"
    print "See file LICENSE for the license details"
    print ""
    print "     --verbose               enable verbose output"
    print "     --enable-valgrind       use valgrind's memcheck"
    print "     --filelist=<file>       test skripts (see below)"
    print "     --file=<file>           test file (see below)"
    print ""
    print "test skripts "
    print "    test skripts are files where each line in the file has a"
    print "    complete set of options for ./dbtest.py, i.e."
    print "    --enable-valgrind --file=../test1.tst -- --profile"
    print ""
    print "test files "
    print "    test files are 'executed' by the ./test executable. Their"
    print "    syntax is described in the documentation."

def lprint(s):
    global g_log

    if not g_log:
        g_log=open("./dbtest.log", "wt")
    g_log.write(s+"\n")
    g_log.flush()
    print s

def vprint(s):
    global g_verbose

    if g_verbose:
        lprint(s)

def parse_args(hargs, argv):
    # reset arguments
    test_opts=0

    if hargs.has_key('filelist'):
        hargs['filelist']=[]
    if hargs.has_key('files'):
        hargs['files']=[]

    if not argv:
        print "Missing parameters!"
        print ""
        help()
        sys.exit(-1)

    for arg in argv:
        if test_opts:
            hargs['test_opts'].append(arg)
            vprint("Appending option "+arg+" for ./test executable")
            continue

        a=arg.split('=', 1)
        if a[0] in ['--verbose']:
            global g_verbose
            g_verbose=1;
            vprint("Enabling verbose output")
        elif a[0] in ['-h', '--help', '/?']:
            help()
            sys.exit(-1)
        elif a[0] in ['--enable-valgrind']:
            hargs['valgrind']=1;
            vprint("Enabling valgrind")
        elif a[0] in ['--filelist']:
            if not hargs.has_key('filelist'):
                hargs['filelist']=[]
            hargs['filelist'].append(a[1]);
            vprint("Appending filelist "+a[1])
        elif a[0] in ['--file']:
            if not hargs.has_key('file'):
                hargs['file']=[]
            hargs['file'].append(a[1])
            vprint("Appending file "+a[1])
        elif a[0] in ['--']:
            test_opts=1
            if not hargs.has_key('test_opts'):
                hargs['test_opts']=[]
        else:
            unknown=0
            try:
                if a[0]==['-'] and a[1]=='-':
                    unknown=1
            except:
                unknown=1
            if unknown:
                print "unknown argument", a[0]
                print ""
                help()
                sys.exit(-1)
            else: # treat unknown parameters as filelists
                if not hargs.has_key('filelist'):
                    hargs['filelist']=[]
                hargs['filelist'].append(a[0]);
                vprint("Appending filelist "+a[0])
    return hargs

def save_file(path):
    new_path="dbtest_log_"+g_logfile_count;
    g_logfile_count=g_logfile_count+1
    os.shutil.copyfile(path, new_path)
    return new_path

def exec_file(hargs, file):
    global g_error_count

    cmd=''
    if hargs.has_key('valgrind'):
        cmd="valgrind --tool=memcheck "
    cmd+="./test --db "+file+" "
    if hargs.has_key('test_opts'):
        cmd+=string.join(hargs['test_opts'])
    cmd+=" > /tmp/dbtest.out"

    lprint("Running test "+cmd)
    # run the test
    status=os.system(cmd)

    # check for errors
    f=open("/tmp/dbtest.out", "r")
    out=f.readlines()
    for o in out:
        if "progress: " in o:
            print o,
            continue
        fnd1=re.findall("asserts\s+\d+\s+\d+\s+\d+\s+(\d+)", o)
        fnd2=re.findall("ASSERT", o)
        fnd3=re.findall("integrity check failed", o)
        fnd5=re.findall("profile", o)
        fnd6=re.findall("in use at exit: (\d+),", o)
        if (fnd6 and int(fnd6[0])):
            lprint("==> memory leaks found!")
            f.close()
            log=save_file("/tmp/dbtest.out")
            lprint("Logfile stored in", log)
            g_error_count=g_error_count+1
            return
        if (fnd1 and int(fnd1[0])!=0) or \
            fnd2 or fnd3 or o[0:11]=='------ page':
            lprint("==> errors found!")
            f.close()
            log=save_file("/tmp/dbtest.out")
            lprint("Logfile stored in", log)
            g_error_count=g_error_count+1
            return
        if fnd5: # profile
            lprint("   ", o,)

    #if status==2: # exit on strg-c
        #sys.exit(-1)
    if status:
        g_error_count=g_error_count+1
    lprint("    status is "+str(status)+" ("+str(g_error_count)+" errors)")

def exec_filelist(hargs, file):
    f=open(file, "r")
    out=f.readlines()
    # try to set the title (only works on xterms)
    sys.stderr.write("\x1b]0; dbtest.py "+str(hargs)+"\x07")
    sys.stderr.flush()
    # execute each line
    lprint("Filelist "+file+"("+str(hargs)+")")
    for o in out:
        if len(o)<=2:
            continue
        if o[0]=='#':
            continue
        args=string.split(o)
        if hargs.has_key('file'):
            hargs['file']=[]
        if hargs.has_key('filelist'):
            hargs['filelist']=[]
        myargs=copy.deepcopy(hargs)
        myargs=parse_args(myargs, args)
        exec_args(myargs)

def exec_args(hargs):
    # first, we run all tests which were specified with --file=XXX
    if hargs.has_key('file'):
        files=hargs['file']
        for file in files:
            vprint("Executing file test "+file)
            exec_file(hargs, file)

    # then read, parse and execute the filelists
    if hargs.has_key('filelist'):
        filelists=hargs['filelist']
        for file in filelists:
            vprint("Executing file list "+file)
            exec_filelist(copy.deepcopy(hargs), file)


# parse command line arguments
args=parse_args({}, sys.argv[1:])

# and execute the argument(s)
exec_args(args)

