
INCLUDES        = -I$(top_builddir)/include

CFLAGS         += -Wno-int-to-pointer-cast -Wno-pointer-to-int-cast

lib_LTLIBRARIES = libhamsterdb-java.la

libhamsterdb_java_la_SOURCES = hamsterdb.c

libhamsterdb_java_la_LDFLAGS = -version-info 1:0:0                           \
                               $(top_builddir)/src/libhamsterdb.la

