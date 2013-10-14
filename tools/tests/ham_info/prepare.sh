#!/bin/sh

TESTDIR=ham_info

../ham_info db1.db > $TESTDIR/db1.txt
../ham_info db1.db -f > $TESTDIR/db1-f.txt
../ham_info db1.db -db 0xf000 > $TESTDIR/db1-0xf000.txt
../ham_info db1.db -db 1 -f > $TESTDIR/db1-1-f.txt
../ham_info env3.db > $TESTDIR/env3.txt
../ham_info env3.db -db 1 > $TESTDIR/env3-1.txt
../ham_info env3.db -db 2 > $TESTDIR/env3-2.txt
../ham_info env3.db -db 3 > $TESTDIR/env3-3.txt
../ham_info env3.db -f > $TESTDIR/env3-f.txt
../ham_info env3.db -db 1 -f > $TESTDIR/env3-1-f.txt
../ham_info env3.db -db 2 -f > $TESTDIR/env3-2-f.txt
../ham_info env3.db -db 3 -f > $TESTDIR/env3-3-f.txt
../ham_info extkeys.db > $TESTDIR/extkeys.txt
../ham_info extkeys.db -f > $TESTDIR/extkeys-f.txt
../ham_info extkeys.db -db 0xf000 > $TESTDIR/extkeys-0xf000.txt
../ham_info extkeys.db -db 2 -f > $TESTDIR/extkeys-2-f.txt
