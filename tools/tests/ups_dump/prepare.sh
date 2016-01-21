#!/bin/sh

TESTDIR=ups_dump

../ups_dump db1.db > $TESTDIR/db1.txt

../ups_dump env3.db > $TESTDIR/env3.txt

../ups_dump env3.db -db 1 > $TESTDIR/env3-1.txt

../ups_dump env3.db -db 2 > $TESTDIR/env3-2.txt

../ups_dump env3.db -db 3 > $TESTDIR/env3-3.txt

../ups_dump env3.db -db 1 > $TESTDIR/env3-1-keynumeric.txt

../ups_dump env3.db -db 2 -rec string > $TESTDIR/env3-2-keynumeric-recstring.txt

../ups_dump env3.db -db 2 -maxrec 4 > $TESTDIR/env3-2-keynumeric-maxrec4.txt

../ups_dump extkeys.db -maxkey 3 > $TESTDIR/extkeys-keystring-maxkey3.txt

../ups_dump extkeys.db -maxkey 3000 > $TESTDIR/extkeys-keystring-maxkey3000.txt

../ups_dump extkeys.db -maxkey 3 -maxrec 2000 > $TESTDIR/extkeys-keystring-maxkey3-maxrec2000.txt

