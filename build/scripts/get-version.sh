#!/bin/sh
#
# usage: get-version.sh path/to/version.h
#
# this script extracts and prints the hamsterdb version
#

if test $# = 0; then
  echo "error: argument is missing"
  echo ""
  echo "usage: $0 path/to/version.h"
  exit 1
fi
if test $# != 1; then
  echo "error: too many arguments."
  echo ""
  echo "usage: $0 path/to/version.h"
  exit 1
fi

f="$1"
if test ! -r "$f"; then
  echo "error: cannot open '$f' for reading"
  exit 1
fi

HAM_VERSION_MAJ="`sed -n -e '/HAM_VERSION_MAJ/s/[^0-9]*//gp' $f`"
HAM_VERSION_MIN="`sed -n -e '/HAM_VERSION_MIN/s/[^0-9]*//gp' $f`"
HAM_VERSION_REV="`sed -n -e '/HAM_VERSION_REV/s/[^0-9]*//gp' $f`"

# the following lines are copied from the expat configure infrastructure
#
# Determine how to tell echo not to print the trailing \n. This is
# similar to Autoconf's @ECHO_C@ and @ECHO_N@; however, we don't
#  generate this file via autoconf (in fact, get-version.sh is used
# to *create* ./configure), so we just do something similar inline.
case `echo "testing\c"; echo 1,2,3`,`echo -n testing; echo 1,2,3` in
  *c*,-n*) ECHO_N= ECHO_C='
' ;;
  *c*,*  ) ECHO_N=-n ECHO_C= ;;
  *)      ECHO_N= ECHO_C='\c' ;;
esac

echo $ECHO_N "$HAM_VERSION_MAJ.$HAM_VERSION_MIN.$HAM_VERSION_REV$ECHO_C"
