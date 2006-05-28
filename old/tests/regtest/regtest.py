#!/usr/bin/python

# regression test 
#
# @@@ COPYRIGHT @@@
#
# we assume that regtest.py is run from the parent directory (mydb/tests). 

import re
import os
import sys
import stat
import time
import math
import shelve

db=None
testonly=False

class TestResult:

    BUILDNO = 0

    def __init__(self):
        self.m_buildno=TestResult.BUILDNO
        self.m_date=time.time()
        self.m_time=() # tuple: (real, user, sys)
        self.m_errors=0
        self.m_dump=''

    def __str__(self):
        s = "errors: %d; " % self.m_errors
        s+= "times: real %f, user %f, sys %f\n" % self.m_time
        s+= self.m_dump
        return s

def main():
    # first we get the buildnumber
    TestResult.BUILDNO=int(os.popen("./btree -b").read())
    print "buildno is", TestResult.BUILDNO

    # open the regtest-shelve
    global db
    db=shelve.open("regtest/regtest.db")

    # get a list of all the test-files
    files=getTestFiles()

    # for each test-file
    for file in files:
        if not runTest(file):
            break;
    
def getTestFiles():
    list=[]
    files=os.listdir("./regtest/tests")
    for f in files:
        if stat.S_ISDIR(os.stat("./regtest/tests/"+f)[stat.ST_MODE]):
            continue
        list.append(f)
    return list

def parseTime(t):
    # 0m0.002s
    min=int(t[0:1])
    sec=float(t[2:-1])
    return min*60+sec

def bail(testfile, message):
    print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    print "test '%s' failed with message:\n%s" % (testfile, message)
    sys.exit(-1)

def parseOutput(output):
    res=TestResult()
    # dump()-ausgabe 
    i=0
    while i in range(0, len(output)):
        if "@@@ dump start @@@" in output[i]:
            i+=1
            while not "@@@ dump end @@@" in output[i]:
                res.m_dump+=output[i]
                i+=1
            i+=1

        elif "@@@ results @@@" in output[i]:
            i+=1
            res.m_errors=int(output[i][8:])
            i+=2
            real=parseTime(output[i][5:-1])
            i+=1
            user=parseTime(output[i][5:-1])
            i+=1
            sys=parseTime(output[i][4:-1])
            i+=1
            res.m_time=(real, user, sys)

        else:
            i+=1

    return res

def runTest(testfile):
    # run ./btree -q -i <file>
    output=os.popen4("time ./btree -q -i regtest/tests/"+testfile)[1].readlines()
    # parse the output
    testresult=parseOutput(output)

    # get the previous search for this file
    if not db.has_key(testfile):
        # no previous results? then ask if the output of this file is korrekt;
        # if no, bail. if yes, add it to the database
        print "no previous results found for %s - please check results" \
                % testfile
        print testresult
        print "---------------------------------------------------------"
        print "and enter 'yes' (or 'no') if the output is (not) correct."
        if 'yes' in sys.__stdin__.readline():
            db[testfile]=[testresult]
            print "Result stored"
            return True

    # otherwise compare the results to the last run - bail, if 
    # they are different
    else:
        resultlist=db[testfile] 
        lastresult=resultlist[-1:][0]
        if lastresult.m_dump!=testresult.m_dump:
            print testresult
            bail(testfile, 'dump-mismatch')
        if lastresult.m_errors!=testresult.m_errors:
            print testresult
            bail(testfile, 'error-mismatch')

        # get the fastest result and compare it with this run - bail, 
        # if the time(1)-output is significantly different
        minreal=99*9999
        minuser=99*9999
        minsys =99*9999
        # nur writeback, wenn diese buildno noch nicht gespeichert wurde
        writeback=True 
        for res in resultlist:
            if res.m_buildno==TestResult.BUILDNO:
                writeback=False
            if res.m_time[0]<minreal:
                minreal=res.m_time[0]
            if res.m_time[1]<minuser:
                minuser=res.m_time[1]
            if res.m_time[2]<minsys:
                minsys=res.m_time[2]

        alloweddiff=0.1 #minreal/10.0
        abs=math.fabs(testresult.m_time[0]-minreal)
        if abs>alloweddiff:
            bail(testfile, 'real-time abnormality (diff: %f > %f)' % (abs,
                    alloweddiff))
        alloweddiff=0.1 #minuser/10.0
        abs=math.fabs(testresult.m_time[1]-minuser)
        if abs>alloweddiff:
            bail(testfile, 'user-time abnormality (diff: %f > %f)' % (abs,
                    alloweddiff))
        alloweddiff=0.1 #minsys/10.0
        abs=math.fabs(testresult.m_time[2]-minsys)
        if abs>alloweddiff:
            bail(testfile, 'sys-time abnormality (diff: %f > %f)' % (abs,
                    alloweddiff))

        # still no problem - test passed successfully
        # and store it
        if not testonly:
            if writeback:
                resultlist.append(testresult)
                db[testfile]=resultlist
        print "test '%s' passed successfully" % testfile
        return True

def usage():
    print "regtest/regtest.py [--testonly]"

if __name__=='__main__':
    for arg in sys.argv[1:]:
        if arg=='--testonly':
            testonly=True
        elif arg=='--help':
            usage()
            sys.exit(0)
        else:
            usage()
            sys.exit(-1)

    main()

