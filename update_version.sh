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
configure.in
documentation/Doxyfile
win32/msvc2008/dll.vcproj
win32/msvc2008/lib.vcproj
win32/msvc2008/server_dll.vcproj
win32/msvc2008/server_lib.vcproj
win32/msvc2010/dll.vcxproj
win32/msvc2010/lib.vcxproj
win32/msvc2010/server_dll.vcxproj
win32/msvc2010/server_lib.vcxproj
hamsterdb.spec
include/ham/hamsterdb.h
include/ham/hamsterdb.hpp
src/version.h
dotnet/hamsterdb-dotnet/NativeMethods.cs
dotnet/unittests/*.cs
java/java/Makefile.am
java/java/win32.bat
java/java/de/crupp/hamsterdb/*.java
java/README
java/unittests/Makefile.am
java/unittests/win32.bat"

for f in $FILES
do
    sed -i "s/$OLD_VERSION/$NEW_VERSION/g" $f
done

echo
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
echo Manually update src/version.h and ChangeLog!
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

