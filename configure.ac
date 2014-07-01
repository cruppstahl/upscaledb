dnl
dnl configuration script for hamsterdb
dnl
dnl Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
dnl All rights reserved. See file LICENSE for licence and copyright
dnl information
dnl

dnl Initialize autoconf/automake
AC_INIT([hamsterdb], [2.1.7], [http://hamsterdb.com])
AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])
AC_CONFIG_MACRO_DIR(m4)
AC_COPYRIGHT([Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de)])
AC_CANONICAL_HOST
AC_ENABLE_SHARED
AC_ENABLE_STATIC
AC_PROG_CC
AC_PROG_CC_STDC
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_SED
AC_HEADER_STDC
AC_C_CONST
AC_TYPE_SIZE_T
AC_SYS_LARGEFILE
# AM_PROG_AR is not available in automake v0.11 but it's required in v0.12
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
LT_INIT
AC_CONFIG_SRCDIR(src/hamsterdb.cc)
AC_CONFIG_HEADERS(config.h)
AM_PROG_CC_C_O

AC_TYPE_OFF_T
AC_FUNC_MMAP
AC_CHECK_FUNCS([mmap munmap getpagesize fdatasync fsync writev pread pwrite])
AC_CHECK_HEADERS([fcntl.h unistd.h malloc.h uv.h])

m4_include([m4/ax_cxx_gcc_abi_demangle.m4])
AX_CXX_GCC_ABI_DEMANGLE

m4_include([m4/boost.m4])
BOOST_REQUIRE([1.36])
BOOST_SYSTEM()
BOOST_FILESYSTEM()
BOOST_THREADS()

AM_CONDITIONAL([DARWIN],  [AS_CASE([$host_os], [darwin*],  [true], [false])])
AM_CONDITIONAL([FREEBSD], [AS_CASE([$host_os], [freebsd*], [true], [false])])
AM_CONDITIONAL([LINUX],   [AS_CASE([$host_os], [linux*],   [true], [false])])
AM_CONDITIONAL([SUNOS],   [AS_CASE([$host_os], [solaris*], [true], [false])])

# issue 28: do not test for boost_chrono on MacOS
case "$host_os" in
  *darwin*)
    ;;
  *)
    BOOST_CHRONO()
    ;;
esac

# A string describing all enabled/disabled settings
settings=""

# -------------------------------------------------------------------------
# Enable debug mode? This switch is not recommended according to autoconf
# philosophies, but people are used to have it.
# -------------------------------------------------------------------------
AC_ARG_ENABLE(debug,
  AS_HELP_STRING(--enable-debug, build for debugging (slow!)),
    [if test x$enableval = xyes; then
      CFLAGS="-g -O0 -DHAM_DEBUG -Wall"
      settings="$settings (debug)"
     fi])

# -------------------------------------------------------------------------
# Enable gcc safe mode?
# -------------------------------------------------------------------------
AC_ARG_ENABLE(gcc-hardening,
  AS_HELP_STRING(--enable-gcc-hardening, enable compiler security checks),
    [if test x$enableval = xyes; then
      CFLAGS="$CFLAGS -D_FORTIFY_SOURCE=2 -fstack-protector-all"
      CFLAGS="$CFLAGS -fwrapv -fPIE -Wstack-protector --param ssp-buffer-size=1"
     fi])

# -------------------------------------------------------------------------
# Only use tcmalloc if it's installed AND if target is not MacOS!
# tcmalloc has some issues on MacOS,
# i.e. http://code.google.com/p/gperftools/issues/detail?id=413
# -------------------------------------------------------------------------
AC_ARG_WITH(tcmalloc,
  AS_HELP_STRING(--without-tcmalloc, disable tcmalloc for memory allocations))
if test x$with_tcmalloc != xno; then
  case "$host_os" in
    *darwin*)
      this_is_darwin=yes
      ;;
    *)
      AC_CHECK_HEADERS(google/tcmalloc.h)
      AC_CHECK_LIB(tcmalloc_minimal, tc_malloc)
      ;;
  esac
fi

# -------------------------------------------------------------------------
# Disable java wrapper?
# -------------------------------------------------------------------------
AC_ARG_ENABLE(java,
  AS_HELP_STRING([--disable-java], [Do not build the Java wrapper]))
if test x$enable_java = xno; then
  settings="$settings (no java)"
else
  if test "$JDK" = ""; then
    settings="$settings (no java)"
    enable_java="no"
  else
    if test -d $JDK/include; then
      AC_SUBST(JDK_INCLUDE, "$JDK/include")
    fi
    if test -d $JDK/Headers; then
      AC_SUBST(JDK_INCLUDE, "$JDK/Headers")
    fi
    settings="$settings (java)"
  fi
fi
AM_CONDITIONAL(ENABLE_JAVA, test "x$enable_java" != "xno")

# -------------------------------------------------------------------------
# Check if protocol buffers are installed
# -------------------------------------------------------------------------
AC_LANG(C++)
AC_CACHE_CHECK(if google protocol buffers are installed,ac_cv_protobuf,
  [AC_RUN_IFELSE([AC_LANG_SOURCE([[
#include <google/protobuf/message.h>
int main(void) { return 0; }]])],
    ac_cv_protobuf=yes, ac_cv_protobuf=no, ac_cv_protobuf=no)])
AC_LANG(C)

# -------------------------------------------------------------------------
# Disable remote client/server
# -------------------------------------------------------------------------
AC_ARG_ENABLE(remote,
  AS_HELP_STRING([--disable-remote], [Disable access to remote databases]))
if test x$enable_remote = xno; then
  settings="$settings (no remote access)"
else
  if test x$ac_cv_protobuf = xno; then
    settings="$settings (protobuf missing - remote disabled)"
    enable_remote="no"
  fi
  if test x$ac_cv_header_uv_h = xno; then
    settings="$settings (libuv missing - remote disabled)"
    enable_remote="no"
  fi
fi
if test x$enable_remote != xno; then
  settings="$settings (remote)"
fi
AM_CONDITIONAL(ENABLE_REMOTE, test x$enable_remote != xno)

# -------------------------------------------------------------------------
# Build ham_bench with Berkeleydb?
# -------------------------------------------------------------------------
AC_ARG_WITH(berkeleydb,
  AS_HELP_STRING(--without-berkeleydb, enables ham_bench tests against berkeleydb))
if test x$with_berkeleydb = xyes; then
  AC_CHECK_LIB(db, db_create)
  AC_CHECK_HEADERS(db.h)
  AM_CONDITIONAL(WITH_BERKELEYDB, true)
  settings="$settings (berkeleydb)"
else
  AM_CONDITIONAL(WITH_BERKELEYDB, false)
fi


CXXFLAGS=${CFLAGS}

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
AC_CONFIG_FILES(Makefile src/Makefile src/protobuf/Makefile src/protoserde/Makefile include/Makefile include/ham/Makefile samples/Makefile unittests/Makefile 3rdparty/Makefile 3rdparty/json/Makefile tools/Makefile tools/ham_bench/Makefile src/server/Makefile java/Makefile java/java/Makefile java/src/Makefile java/unittests/Makefile)
AC_OUTPUT

# Messages
printf '#================================================================\n'
printf "# Configuring hamsterdb version $PACKAGE_VERSION\n"
printf "# Settings:$settings\n"
if test x$enable_remote = xno; then
printf "#\n"
printf "# The remote functionality is disabled because you specified\n"
printf "# --disable-remote or because of missing dependencies!\n"
fi
if test x$enable_java = xno; then
printf "#\n"
printf "# The Java API is not built because you specified\n"
printf "# --disable-java or because \$JDK is not set\n"
fi

# Recommend to use tcmalloc - but not on MacOS because it has some issues,
if test x$ac_cv_lib_tcmalloc_minimal_tc_malloc != xyes; then
  if test x$this_is_darwin != xyes; then
    printf "# Recommendation: install google's performance tools to increase performance\n"
  fi
fi

printf '#================================================================\n'

# build protobuf files
if test x$enable_remote != xno; then
make --directory=src/protobuf proto
make --directory=tools proto
fi

