#!/usr/bin/env python

# a simple skript: iterate over all files in ./testfiles/db and call
# the test program, till we get an error

import os
import stat
import sys
import re
import popen2

def dump(out):
    for o in out:
        print o,

def run_db(path):
    # run ./test db <file>
    print "running test ", path
    status=os.system("./test db --profile --quiet "+path+" > /tmp/dbtest.out")
    print "    status is", status
    if status: 
        return 0

    f=open("/tmp/dbtest.out", "r")
    out=f.readlines()
    for o in out:
        fnd1=re.findall("asserts\s+\d+\s+\d+\s+\d+\s+(\d+)", o)
        fnd2=re.findall("ASSERT", o)
        fnd3=re.findall("integrity check failed", o)
        fnd4=re.findall("error", o)
        fnd5=re.findall("profile", o)
        if (fnd1 and int(fnd1[0])!=0) or \
            fnd2 or fnd3 or fnd4 or o[0:11]=='------ page':
            dump(out)
            return 0
        if fnd5:
            print "   ", o,
    return 1

try:
    param=sys.argv[1]
except:
    param=''

if param:
    run_db(param);
else:
    list=os.listdir("./testfiles/db")
    list.sort()
    for f in list:
        if stat.S_ISDIR(os.stat("./testfiles/db/"+f)[stat.ST_MODE]):
            continue
        # run the test
        if not run_db("./testfiles/db/"+f):
            break

