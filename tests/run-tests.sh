#!/bin/sh
#
# hamsterdb test script
#
# Copyright (C) 2005-2007, Christoph Rupp (chris@crupp.de). 
# All rights reserved. See file LICENSE for license and copyright 
# information.
#

total=0
errors=0
testdir=../../../hamsterdb-tests/trunk/testfiles

function call {
    ./test --file $1 $2 $3 $4 $5 $6 $7 $8 $9 #&> .tmp

    if [[ $? = 0 ]]
        then echo "[OK]   $1"
        else echo "[FAIL] $1"; cat .tmp; (( errors+=1 ))
    fi
    (( total+=1 ))
}

echo "----------------------------------------------------------------"
echo -n "new test starts on "
date
echo "parameters: ($1) $2 $3 $4 $5 $6 $7 "

maxdir=$1; shift

\rm -f *.db

for dir in `echo "1 2 3 4"`
do
    for file in `ls $testdir/$dir/*.tst`
    do
        call $file  $1 $2 $3 $4 $5 $6 $7 
    done

    if [[ $dir = $maxdir ]]; then
        echo "$total tests; $errors error(s)"
        exit $errors
    fi
done

echo "$total tests; $errors error(s)"
exit $errors
