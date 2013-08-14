
if [ "`uname`" == "Darwin" ]; then
  LIBTOOLIZE=glibtoolize
fi

set -ex
${LIBTOOLIZE:-libtoolize}
${ACLOCAL:-aclocal -I m4}
${AUTOCONF:-autoconf}
${AUTOMAKE:-automake} --add-missing

