#!/bin/sh

TESTDIR=ups_dump

../ups_dump db1.db > x
diff --brief x $TESTDIR/db1.txt
if [ $? = 1 ]; then
    echo "db1.txt differs"
    exit 1
fi

../ups_dump env3.db > x
diff --brief x $TESTDIR/env3.txt
if [ $? = 1 ]; then
    echo "env3.txt differs"
    exit 1
fi

../ups_dump env3.db -db 1 > x
diff --brief x $TESTDIR/env3-1.txt
if [ $? = 1 ]; then
    echo "env3-1.txt differs"
    exit 1
fi

../ups_dump env3.db -db 2 > x
diff --brief x $TESTDIR/env3-2.txt
if [ $? = 1 ]; then
    echo "env3-2.txt differs"
    exit 1
fi

../ups_dump env3.db -db 3 > x
diff --brief x $TESTDIR/env3-3.txt
if [ $? = 1 ]; then
    echo "env3-3.txt differs"
    exit 1
fi

../ups_dump env3.db -db 1 > x
diff --brief x $TESTDIR/env3-1-keynumeric.txt
if [ $? = 1 ]; then
    echo "env3-1-keynumeric.txt differs"
    exit 1
fi

../ups_dump env3.db -db 2 -rec string > x
diff --brief x $TESTDIR/env3-2-keynumeric-recstring.txt
if [ $? = 1 ]; then
    echo "env3-2-keynumeric-recstring.txt differs"
    exit 1
fi

../ups_dump env3.db -db 2 -maxrec 4 > x
diff --brief x $TESTDIR/env3-2-keynumeric-maxrec4.txt
if [ $? = 1 ]; then
    echo "env3-2-keynumeric-maxrec4.txt differs"
    exit 1
fi

../ups_dump extkeys.db -maxkey 3 > x
diff --brief x $TESTDIR/extkeys-keystring-maxkey3.txt
if [ $? = 1 ]; then
    echo "extkeys-keystring-maxkey3.txt differs"
    exit 1
fi

../ups_dump extkeys.db -maxkey 3000 > x
diff --brief x $TESTDIR/extkeys-keystring-maxkey3000.txt
if [ $? = 1 ]; then
    echo "extkeys-keystring-maxkey3000.txt differs"
    exit 1
fi

../ups_dump extkeys.db -maxkey 3 -maxrec 2000 > x
diff --brief x $TESTDIR/extkeys-keystring-maxkey3-maxrec2000.txt
if [ $? = 1 ]; then
    echo "extkeys-keystring-maxkey3-maxrec2000.txt differs"
    exit 1
fi

rm x
echo ups_dump: ok
