#!/usr/bin/env python

# a simple skript: iterate over all files in ./testfiles/row and call
# the test program, till we get an error

import os
import stat
import sys
import popen2
import random
import re
import time
from sets import Set

def dump(out):
    for o in out:
        print o,

def run_random(binary, len):
    r=''
    basket=[]
    for i in range(0, len):
        p=random.randint(0, len*10)+1
        basket.append(p)
    for b in Set(basket):
        r=r+" "+str(b)+"\n"
    for b in Set(basket):
        r=r+" -"+str(b)+"\n"
    # run "echo $r | ./test btree_row - "
    try:
        pipe=popen2.Popen4("./test "+binary+" - --quiet")
        pipe.tochild.write(r)
        pipe.tochild.close()
        while pipe.poll()==-1:
            i=0
        t1=time.time()
        out=pipe.fromchild.readlines()
    except:
        print r
        print "failed with status", pipe.poll()
        return 0
    t2=time.time()
    for o in out:
        fnd1=re.findall("asserts\s+\d+\s+\d+\s+\d+\s+(\d+)", o)
        fnd2=re.findall("ASSERT", o)
        fnd3=re.findall("integrity check failed", o)
        if pipe.poll() or (fnd1 and int(fnd1[0])!=0) or fnd2 or fnd3:
            print "status", pipe.poll()
            dump(out)
            print "failed with random data ", r
            f=open('/tmp/ham_dump.txt', 'w');
            for n in out:
                f.write(n)
            f.write("\nfailed with random data "+r)
            print "dumped to /tmp/ham_dump.txt"
            return 0
    print "  -->", (t2-t1), " seconds",
    return 1

def run_row(binary, path):
    # run ./test btree_row <file>
    print "running test ", path
    try:
        pipe=popen2.Popen4("./test "+binary+" --quiet "+path)
        while pipe.poll()==-1:
            i=0
        t1=time.time()
        out=pipe.fromchild.readlines()
    except:
        print "failed with status", pipe.poll()
        return 0
    if pipe.poll():
        print "failed with status", pipe.poll()
        return 0
    for o in out:
        fnd1=re.findall("asserts\s+\d+\s+\d+\s+\d+\s+(\d+)", o)
        fnd2=re.findall("ASSERT", o)
        fnd3=re.findall("integrity check failed", o)
        if (fnd1 and int(fnd1[0])!=0) or fnd2 or fnd3 or o[0:11]=='------ page':
            dump(out)
            return 0
    return 1

random.seed()

try:
    param=sys.argv[1]
except:
    param=''

if param=="--random" or param=="-r":
    try:
        len=int(sys.argv[2])
    except:
        len=5
    while run_random('btree_row', len):
        print ""
    sys.exit(0)

if param=="--fullrandom":
    while 1:
        len=random.randint(0, 1024*30)+1
        print "test of", len, "items... "
        if not run_random('btree_row', len):
            sys.exit(-1)

for f in os.listdir("./testfiles/row"):
    if stat.S_ISDIR(os.stat("./testfiles/row/"+f)[stat.ST_MODE]):
        continue
    # run the test
    run_row('btree_row', "./testfiles/row/"+f);

