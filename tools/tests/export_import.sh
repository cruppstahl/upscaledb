#!/bin/sh

echo "========== Importing env1.db ===================================="
rm -f env1-import.db
../ham_export --output=env1.bin env1.db
../ham_import env1.bin env1-import.db
../ham_dump env1.db > dump1
../ham_dump env1-import.db > dump2
diff --brief dump1 dump2
if [ $? = 1 ]; then
    echo "Export of env1.db differs"
    exit 1
fi

echo "========== Importing env3.db ===================================="
rm -f env3-import.db
../ham_export --output=env3.bin env3.db
../ham_import env3.bin env3-import.db
../ham_dump -maxkey 1000 -maxrec 1000 env3.db > dump1
../ham_dump -maxkey 1000 -maxrec 1000 env3-import.db > dump2
diff --brief dump1 dump2
if [ $? = 1 ]; then
    echo "Export of env3.db differs"
    exit 1
fi

echo "========== Importing extkeys.db ================================="
rm -f extkeys-import.db
../ham_export --output=extkeys.bin extkeys.db
../ham_import extkeys.bin extkeys-import.db
../ham_dump extkeys.db > dump1
../ham_dump extkeys-import.db > dump2
diff --brief dump1 dump2
if [ $? = 1 ]; then
    echo "Export of extkeys.db differs"
    exit 1
fi

\rm dump*
\rm -f final.db
\rm *.bin

echo "ham_export/ham_import: ok"
