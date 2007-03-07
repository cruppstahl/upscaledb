#!/bin/sh

libtoolize --force
aclocal
automake --foreign --add-missing
autoconf
autoheader

