#!/bin/sh

libtoolize --force \
&& aclocal \
&& automake --add-missing --foreign \
&& autoconf \
&& autoheader

