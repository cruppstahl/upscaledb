#!/bin/sh

TESTDIR=ups_info

../ups_info db1.db > x
diff --brief x $TESTDIR/db1.txt
if [ $? = 1 ]; then
    echo "db1.txt differs"
    exit 1
fi

../ups_info db1.db -f > x
diff --brief x $TESTDIR/db1-f.txt
if [ $? = 1 ]; then
    echo "db1-f.txt differs"
    exit 1
fi

../ups_info db1.db -db 1 -f > x
diff --brief x $TESTDIR/db1-1-f.txt
if [ $? = 1 ]; then
    echo "db1-1-f.txt differs"
    exit 1
fi

../ups_info env3.db > x
diff --brief x $TESTDIR/env3.txt
if [ $? = 1 ]; then
    echo "env3.txt differs"
    exit 1
fi

../ups_info env3.db -db 1 > x
diff --brief x $TESTDIR/env3-1.txt
if [ $? = 1 ]; then
    echo "env3-1.txt differs"
    exit 1
fi

../ups_info env3.db -db 2 > x
diff --brief x $TESTDIR/env3-2.txt
if [ $? = 1 ]; then
    echo "env3-2.txt differs"
    exit 1
fi

../ups_info env3.db -db 3 > x
diff --brief x $TESTDIR/env3-3.txt
if [ $? = 1 ]; then
    echo "env3-3.txt differs"
    exit 1
fi

../ups_info env3.db -f > x
diff --brief x $TESTDIR/env3-f.txt
if [ $? = 1 ]; then
    echo "env3-f.txt differs"
    exit 1
fi

../ups_info env3.db -db 1 -f > x
diff --brief x $TESTDIR/env3-1-f.txt
if [ $? = 1 ]; then
    echo "env3-1-f.txt differs"
    exit 1
fi

../ups_info env3.db -db 2 -f > x
diff --brief x $TESTDIR/env3-2-f.txt
if [ $? = 1 ]; then
    echo "env3-2-f.txt differs"
    exit 1
fi

../ups_info env3.db -db 3 -f > x
diff --brief x $TESTDIR/env3-3-f.txt
if [ $? = 1 ]; then
    echo "env3-3-f.txt differs"
    exit 1
fi

../ups_info extkeys.db > x
diff --brief x $TESTDIR/extkeys.txt
if [ $? = 1 ]; then
    echo "extkeys.txt differs"
    exit 1
fi

../ups_info extkeys.db -f > x
diff --brief x $TESTDIR/extkeys-f.txt
if [ $? = 1 ]; then
    echo "extkeys-f.txt differs"
    exit 1
fi

../ups_info extkeys.db -db 2 -f > x
diff --brief x $TESTDIR/extkeys-2-f.txt
if [ $? = 1 ]; then
    echo "extkeys-2-f.txt differs"
    exit 1
fi

rm x
echo ups_info: ok
