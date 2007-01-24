#!/bin/sh
#
# hamsterdb test script
#
# Copyright (C) 2005-2007, Christoph Rupp (chris@crupp.de). 
# All rights reserved. See file LICENSE for license and copyright 
# information.
#

dir=../../../hamsterdb-tests/trunk/testfiles

files="1/45.tst 1/220.tst 1/ext_020.tst 1/ext_060.tst 1/blb-001.tst"

function call {
    for file in $files
    do
        echo "running $dir/$file $1 $2 $3 $4 $5"
        valgrind --tool=memcheck ./test --file $dir/$file $1 $2 $3 $4 $5 
    done
}

call "--reopen=1" 
call "--overwrite=1 --reopen=1" 
call "--inmemorydb=1" 
call "--overwrite=1 --inmemorydb=1" 
call "--mmap=0 --overwrite=1 --reopen=1" 
call "--keysize=8 --overwrite=1 --reopen=1" 
call "--keysize=12 --overwrite=1 --reopen=1" 
call "--keysize=33 --overwrite=1 --reopen=1" 
call "--keysize=680 --overwrite=1 --reopen=1" 
call "--pagesize=1024 --overwrite=1 --reopen=1" 
call "--pagesize=3072 --overwrite=1 --reopen=1" 
call "--pagesize=8192 --overwrite=1 --reopen=1" 
call "--cachesize=0 --overwrite=1 --reopen=1" 
call "--cachepolicy=strict --overwrite=1 --reopen=1" 
