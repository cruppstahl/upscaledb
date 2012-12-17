#!/bin/sh

libtoolize --force \
&& aclocal --warnings=all \
&& automake --add-missing --foreign --warnings=all \
&& autoconf --warnings=all \
&& autoheader --warnings=all

