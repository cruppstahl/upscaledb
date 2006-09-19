#!/usr/bin/env python

# a simple skript: iterate over all files in ./testfiles/db and call
# the test program, till we get an error

import os
import stat
import sys
import re
import popen2

TESTFILES = "../../../hamsterdb-tests/trunk"

def dump(out):
    for o in out:
        print o,

def run_db(path, args):
    # enable valgrind?
    if (os.environ.has_key("USE_VALGRIND")):
        cmd="valgrind --tool=memcheck ./test --db "+path+" "+args+" --profile --check --check --quiet &> /tmp/dbtest.out"
    else:
        cmd="./test --db "+path+" "+args+" --profile --check --check --quiet > /tmp/dbtest.out"

    # run the test
    print cmd
    status=os.system(cmd)
    print "    status is", status
    if status: 
        return 0

    f=open("/tmp/dbtest.out", "r")
    out=f.readlines()
    for o in out:
        fnd1=re.findall("asserts\s+\d+\s+\d+\s+\d+\s+(\d+)", o)
        fnd2=re.findall("ASSERT", o)
        fnd3=re.findall("integrity check failed", o)
        fnd5=re.findall("profile", o)
        fnd6=re.findall("in use at exit: (\d+),", o)
        if (fnd6 and int(fnd6[0])):
            dump(out)
            print "==> memory leaks found!"
            return 0
        if (fnd1 and int(fnd1[0])!=0) or \
            fnd2 or fnd3 or o[0:11]=='------ page':
            dump(out)
            print "==> errors found!"
            return 0
        if fnd5:
            print "   ", o,

    return 1

if len(sys.argv)>1:
    param=" ".join(sys.argv[1:-1]) + " " + sys.argv[-1]
else:
    param=''

list=os.listdir(TESTFILES+"/testfiles/db")
list.sort()
for f in list:
    if stat.S_ISDIR(os.stat(TESTFILES+"/testfiles/db/"+f)[stat.ST_MODE]):
        continue
    # run the test
    if not run_db(TESTFILES+"/testfiles/db/"+f, param):
        break

