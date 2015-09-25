#!/bin/sh

TESTDIR=ham_dump

../ham_dump db1.db > $TESTDIR/db1.txt

../ham_dump env3.db > $TESTDIR/env3.txt

../ham_dump env3.db -db 1 > $TESTDIR/env3-1.txt

../ham_dump env3.db -db 2 > $TESTDIR/env3-2.txt

../ham_dump env3.db -db 3 > $TESTDIR/env3-3.txt

../ham_dump env3.db -db 1 -key numeric > $TESTDIR/env3-1-keynumeric.txt

../ham_dump env3.db -db 2 -key numeric -rec string > $TESTDIR/env3-2-keynumeric-recstring.txt

../ham_dump env3.db -db 2 -key numeric -maxrec 4 > $TESTDIR/env3-2-keynumeric-maxrec4.txt

../ham_dump extkeys.db -key string -maxkey 3 > $TESTDIR/extkeys-keystring-maxkey3.txt

../ham_dump extkeys.db -key string -maxkey 3000 > $TESTDIR/extkeys-keystring-maxkey3000.txt

../ham_dump extkeys.db -key string -maxkey 3 -maxrec 2000 > $TESTDIR/extkeys-keystring-maxkey3-maxrec2000.txt

