#!/usr/bin/env bash

OLD_VERSION=$1;
NEW_VERSION=$2;

if [ -z $OLD_VERSION ]
then
    echo "usage: update_version.sh OLD_VERSION NEW_VERSION"
    exit
fi
if [ -z $NEW_VERSION ]
then
    echo "usage: update_version.sh OLD_VERSION NEW_VERSION"
    exit
fi

FILES="README
configure.ac
documentation/Doxyfile
tools/tests/ups_info/env3-3-f.txt
tools/tests/ups_info/env3.txt
tools/tests/ups_info/env3-1-f.txt
tools/tests/ups_info/env3-1.txt
tools/tests/ups_info/env3-f.txt
tools/tests/ups_info/db1-f.txt
tools/tests/ups_info/env3-2.txt
tools/tests/ups_info/db1.txt
tools/tests/ups_info/env3-3.txt
tools/tests/ups_info/extkeys.txt
tools/tests/ups_info/extkeys-f.txt
tools/tests/ups_info/env3-2-f.txt
win32/msvc2013/dll.vcxproj
win32/msvc2013/lib.vcxproj
win32/msvc2013/server_dll.vcxproj
win32/msvc2013/server_lib.vcxproj
upscaledb.spec
include/ups/upscaledb.h
include/ups/upscaledb.hpp
include/ups/upscaledb_uqi.h
dotnet/upscaledb-dotnet/NativeMethods.cs
dotnet/unittests/*.cs
java/java/Makefile.am
java/java/win32.bat
java/java/de/crupp/upscaledb/*.java
java/README
java/unittests/Makefile.am
java/unittests/win32.bat"

for f in $FILES
do
    sed -i "s/$OLD_VERSION/$NEW_VERSION/g" $f
done

echo
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
echo Manually update include/ups/upscaledb.h and ChangeLog!
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

